import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch
from source_config import configured_source_account_id

from ingestion_store import IngestionStore
from phase3 import Phase3
from workflow_store import SCHEMA_VERSION, WorkflowStore, namespaced_source_key, parse_feishu_url, parse_schedule_calendar

SOURCE_ACCOUNT_ID = configured_source_account_id()


class WorkflowStoreTests(unittest.TestCase):
    def setUp(self):
        scratch=Path(__file__).resolve().parent/"data"/"test-tmp"
        scratch.mkdir(parents=True,exist_ok=True)
        self.tmp = tempfile.TemporaryDirectory(dir=scratch)
        self.root = Path(self.tmp.name)
        self.store = IngestionStore(self.root / "messages.sqlite3")
        self.workflow = WorkflowStore(self.store)

    def tearDown(self):
        self.tmp.cleanup()

    def test_review_filters_accept_search_on_empty_database(self):
        for status in ('machine_evidence','semantic_review'):
            self.assertEqual(self.workflow.processing_page(status,search='客户')['total'],0)

    def add_event(self, source_id="m1"):
        raw={"msgid":source_id,"content":"今天哪里拍？","chatroom":{"id":"g1"},"fromUser":{"id":"u1"}}
        normalized={"source_id":source_id,"group":"群一","sender":"客户一","content":"今天哪里拍？","timestamp":"2026-10-26T10:00:00+08:00"}
        seq,_=self.store.add_event(raw,{},normalized,"test",display=True,parse_state="parsed",direction="inbound",timestamp_valid=True)
        return seq

    def test_migration_idempotence_and_existing_event_preserved(self):
        seq=self.add_event()
        second=WorkflowStore(self.store)
        self.assertEqual(second.health()["schema_version"],SCHEMA_VERSION)
        self.assertEqual(second.health()["schema_version"],self.workflow.health()["schema_version"])
        self.assertEqual(self.store.analysis_event(seq)["sequence"],seq)
        with self.store.connect() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM events").fetchone()[0],1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM workflow_migrations").fetchone()[0],1)

    def test_schema_v2_migrates_legacy_group_key_without_losing_mapping(self):
        legacy_root=self.root/"legacy"
        legacy_root.mkdir()
        legacy_store=IngestionStore(legacy_root/"legacy.sqlite3")
        with legacy_store.connect() as db:
            db.execute("""CREATE TABLE business_groups(group_id TEXT PRIMARY KEY,name TEXT NOT NULL,
                service_profile_id TEXT,material_scope_json TEXT NOT NULL DEFAULT '[]',active INTEGER NOT NULL DEFAULT 1,updated_at TEXT NOT NULL)""")
            db.execute("INSERT INTO business_groups(group_id,name,updated_at) VALUES ('legacy-group','旧群','2026-01-01T00:00:00Z')")
        migrated=WorkflowStore(legacy_store)
        groups=migrated.list_groups()
        self.assertEqual(groups[0]["group_id"],"legacy-group")
        self.assertEqual(groups[0]["source_account_id"],SOURCE_ACCOUNT_ID)
        self.assertEqual(migrated.health()["schema_version"],SCHEMA_VERSION)

    def test_namespaced_message_ids_do_not_collide(self):
        self.assertEqual(namespaced_source_key("account-a","same-id"),"account-a:same-id")
        self.assertNotEqual(namespaced_source_key("account-a","same-id"),namespaced_source_key("account-b","same-id"))
        with self.assertRaises(ValueError): namespaced_source_key("","same-id")

    def test_same_source_message_id_is_isolated_by_account(self):
        events=[]
        for account in ("account-a","account-b"):
            raw={"msgid":"same-source-id","content":"问题","chatroom":{"id":"same-group"},"fromUser":{"id":"same-user"}}
            normalized={"content":"问题","timestamp":"2026-10-26T10:00:00+08:00","group":"群","sender":"用户"}
            seq,added=self.store.add_event(raw,{"_source_account_id":account},normalized,"test",display=True,
                parse_state="parsed",direction="inbound",timestamp_valid=True)
            self.assertTrue(added); events.append(self.store.analysis_event(seq))
        self.assertNotEqual(events[0]["sequence"],events[1]["sequence"])
        self.assertEqual(events[0]["source_account_id"],"account-a")
        self.assertEqual(events[1]["source_account_id"],"account-b")
        self.assertEqual(self.store.analysis_context(events[0]),[])

    def test_feishu_url_parser_rejects_non_feishu_host(self):
        self.assertEqual(parse_feishu_url("https://x.feishu.cn/wiki/abcdefghi?sheet=tab1")["sheet_id"],"tab1")
        with self.assertRaises(ValueError): parse_feishu_url("https://evil.example/wiki/abcdefghi")

    def test_periodic_feishu_sync_is_independent_and_off_by_default(self):
        self.workflow.seed_sources("https://x.feishu.cn/wiki/abcdefghi","https://x.feishu.cn/wiki/abcdefghi?sheet=tab123")
        source=next(item for item in self.workflow.list_sources() if item["id"]=="feishu-copy-test")
        self.assertEqual(source["sync_enabled"],0)
        self.assertEqual(source["sync_interval_minutes"],360)
        self.workflow.upsert_source_account({"id":"account-b","display_name":"账号二","rule_id":"r2","enabled":False})
        saved=self.workflow.set_source("feishu-copy-test",sync_enabled=True,sync_interval_minutes=720,source_account_id="account-b")
        self.assertEqual(saved["sync_enabled"],1)
        self.assertEqual(saved["sync_interval_minutes"],720)
        self.assertEqual(saved["source_account_id"],"account-b")
        with self.assertRaisesRegex(ValueError,"60 分钟"):
            self.workflow.set_source("feishu-copy-test",sync_interval_minutes=5)
        disabled=self.workflow.set_source("feishu-copy-test",sync_enabled=False)
        self.assertEqual(disabled["sync_enabled"],0)

    def test_schedule_calendar_is_candidate_only_and_preserves_coordinates(self):
        grid=[["备选品","周一","周二"],["","1","2"],["备用商品","商品甲","商品乙"],["","商品甲二",""]]
        parsed=parse_schedule_calendar(grid,month=10)
        self.assertFalse(parsed["year_confirmed"])
        self.assertFalse(parsed["publishable"])
        self.assertTrue(all(item["status"]=="needs_confirmation" and not item["is_sales_fact"] for item in parsed["candidates"]))
        self.assertEqual(parsed["candidates"][0]["cell"],"R3C2")
        self.assertEqual(parsed["alternates"][0]["item"],"备用商品")

    def test_feishu_sheet_reads_in_bounded_ranges_and_preserves_row_coordinates(self):
        def response(body):
            value=Mock(); value.json.return_value=body; value.raise_for_status.return_value=None; return value
        meta={"code":0,"data":{"sheets":[{"sheet_id":"tab123","title":"10月发品计划","grid_properties":{"row_count":1001,"column_count":2}}]}}
        first={"code":0,"data":{"valueRange":{"values":[[str(i)] for i in range(500)]}}}
        second={"code":0,"data":{"valueRange":{"values":[["第501行"]]}}}
        third={"code":0,"data":{"valueRange":{"values":[]}}}
        with patch("workflow_store.requests.get",side_effect=[response(meta),response(first),response(second),response(third)]) as get:
            result=self.workflow._read_sheet("spreadsheet123","tab123",{"Authorization":"Bearer dummy"},False)
        self.assertEqual(get.call_count,4)
        self.assertEqual(result["rows_read"],1001)
        self.assertEqual(result["values"][500],["第501行"])
        self.assertEqual(result["values"][1000],[])
        self.assertFalse(result["truncated"])
        self.assertEqual(result["ranges"],['tab123!A1:B500','tab123!A501:B1000','tab123!A1001:B1001'])

    def test_schedule_selects_current_month_over_old_sheet_id(self):
        def response(body):
            value=Mock();value.json.return_value=body;value.raise_for_status.return_value=None;return value
        meta={'code':0,'data':{'sheets':[{'sheet_id':'oct','title':'10月发品计划','row_count':1,'column_count':1},
                                        {'sheet_id':'nov','title':'11月发品计划','row_count':1,'column_count':1}]}}
        data={'code':0,'data':{'valueRange':{'values':[['11月内容']]}}}
        with patch('workflow_store.requests.get',side_effect=[response(meta),response(data)]):
            result=self.workflow._read_sheet('book','oct',{},False,target_month=11)
        self.assertEqual(result['sheet_id'],'nov')
        self.assertEqual(result['values'],[['11月内容']])

    def test_schedule_ambiguous_month_fails_closed(self):
        response=Mock();response.raise_for_status.return_value=None
        response.json.return_value={'code':0,'data':{'sheets':[{'sheet_id':'a','title':'11月发品计划'},
                                                              {'sheet_id':'b','title':'11月排期'}]}}
        with patch('workflow_store.requests.get',return_value=response):
            with self.assertRaisesRegex(ValueError,'唯一'):
                self.workflow._read_sheet('book','oct',{},False,target_month=11)

    def test_feishu_sheet_index_query_follows_page_token(self):
        def response(body):
            value=Mock(); value.json.return_value=body; value.raise_for_status.return_value=None; return value
        pages=[response({"code":0,"data":{"sheets":[],"has_more":True,"page_token":"next-page"}}),
               response({"code":0,"data":{"sheets":[{"sheet_id":"tab123","title":"目标","row_count":1,"column_count":1}],"has_more":False}}),
               response({"code":0,"data":{"valueRange":{"values":[["正文"]]}}})]
        with patch("workflow_store.requests.get",side_effect=pages) as get:
            result=self.workflow._read_sheet("spreadsheet123","tab123",{},False)
        self.assertEqual(result["values"],[["正文"]])
        self.assertEqual(get.call_count,3)
        self.assertEqual(get.call_args_list[1].kwargs["params"],{"page_size":100,"page_token":"next-page"})

    def test_feishu_sheet_byte_limit_is_reported_as_truncated(self):
        def response(body):
            value=Mock(); value.json.return_value=body; value.raise_for_status.return_value=None; return value
        meta={"code":0,"data":{"sheets":[{"sheet_id":"tab123","title":"数据","row_count":600,"column_count":1}]}}
        large={"code":0,"data":{"valueRange":{"values":[["x"*8000] for _ in range(500)]}}}
        with patch("workflow_store.requests.get",side_effect=[response(meta),response(large)]):
            result=self.workflow._read_sheet("spreadsheet123","tab123",{},False)
        self.assertTrue(result["truncated"])
        self.assertLess(result["rows_read"],600)
        self.assertEqual(len(result["ranges"]),1)

    def test_feishu_sheet_without_declared_bounds_fails_instead_of_empty_success(self):
        response=Mock(); response.json.return_value={"code":0,"data":{"sheets":[{"sheet_id":"tab123","title":"数据"}]}}; response.raise_for_status.return_value=None
        with patch("workflow_store.requests.get",return_value=response):
            with self.assertRaisesRegex(ValueError,"不能把空快照"):
                self.workflow._read_sheet("spreadsheet123","tab123",{},False)

    def test_groups_use_distinct_service_profiles_and_missing_scope_is_not_guessed(self):
        a=self.workflow.upsert_profile({"name":"客服A","url":"https://a.example/service"})
        b=self.workflow.upsert_profile({"name":"客服B","url":"https://b.example/service"})
        self.workflow.upsert_group({"group_id":"g1","name":"群一","service_profile_id":a["id"]})
        self.workflow.upsert_group({"group_id":"g2","name":"群二","service_profile_id":b["id"]})
        groups={g["group_id"]:g["service_url"] for g in self.workflow.list_groups()}
        self.assertEqual(groups,{"g1":"https://a.example/service","g2":"https://b.example/service"})
        self.workflow.upsert_group({"group_id":"g3","name":"群三"})
        self.assertIsNone(next(g for g in self.workflow.list_groups() if g["group_id"]=="g3")["service_url"])

    def test_suggestion_review_correct_or_correction_reason(self):
        seq=self.add_event()
        case=self.workflow.create_case(account_id="a",group_id="g1",sender_id="u1",conversation_id="c1",
            topic_key="qa",intent="qa",assignee="group_human",event_sequences=[seq])
        suggestion=self.workflow.save_suggestion(case["id"],{"intent":"qa","response_needed":True,"reply_draft":"人工核实"})
        with self.assertRaisesRegex(ValueError,"原因"):
            self.workflow.review_suggestion(suggestion["id"],decision="corrected",corrections={"intent":"service"})
        reviewed=self.workflow.review_suggestion(suggestion["id"],decision="corrected",corrections={"intent":"service"},reason="需转一对一客服")
        self.assertEqual(reviewed["decision"],"corrected")
        self.assertEqual(self.workflow.list_suggestions("todo"),[])
        self.assertEqual(len(self.workflow.list_suggestions("reviewed")),1)
        history=self.workflow.suggestion_page("reviewed",10)["items"][0]["review"]
        self.assertEqual(history["reason"],"需转一对一客服")
        self.assertEqual(history["corrections"]["intent"],"service")

    def test_suggestion_keyset_pagination_is_stable_when_created_at_matches(self):
        for number in range(3):
            seq=self.add_event("page-"+str(number))
            case=self.workflow.create_case(account_id="a",group_id="g1",sender_id="u1",conversation_id="c"+str(number),
                topic_key="qa",intent="qa",assignee="group_human",event_sequences=[seq])
            self.workflow.save_suggestion(case["id"],{"intent":"qa","draft":"版本 "+str(number)})
        with self.store.connect() as db:
            db.execute("UPDATE process_suggestions SET created_at='2026-10-26T00:00:00Z'")
        first=self.workflow.suggestion_page("todo",1)
        second=self.workflow.suggestion_page("todo",1,before_created=first["next_before"]["created_at"],before_id=first["next_before"]["id"])
        third=self.workflow.suggestion_page("todo",1,before_created=second["next_before"]["created_at"],before_id=second["next_before"]["id"])
        ids=[first["items"][0]["id"],second["items"][0]["id"],third["items"][0]["id"]]
        self.assertEqual(len(set(ids)),3)
        self.assertFalse(third["has_more"])

    def test_manual_case_split_and_merge_preserve_customer_scope(self):
        first=self.add_event("first")
        second=self.add_event("second")
        case=self.workflow.create_case(account_id="a",group_id="g1",sender_id="u1",conversation_id="c1",
            topic_key="qa",intent="qa",assignee="group_human",event_sequences=[first,second])
        split=self.workflow.split_case_message(case["id"],second)
        new_id=split["new_case"]["id"]
        self.assertNotEqual(case["id"],new_id)
        self.assertEqual(len(split["old_case"]["messages"]),1)
        merged=self.workflow.merge_cases(new_id,case["id"])
        self.assertEqual(len(merged["target_case"]["messages"]),2)
        self.assertEqual(merged["source_case"]["state"],"merged")

    def test_permission_error_is_persisted_without_erasing_snapshot(self):
        env=self.root/".env"
        env.write_text("飞书app_id=cli_dummy12345\n飞书app secret=dummy-secret\n",encoding="utf-8")
        workflow=WorkflowStore(self.store,env)
        workflow.seed_sources("https://example.feishu.cn/wiki/test-copy-token",
                              "https://example.feishu.cn/wiki/test-schedule-token?sheet=test-sheet")
        ok=Mock(); ok.json.return_value={"code":0,"tenant_access_token":"dummy-token"}; ok.raise_for_status.return_value=None
        denied=Mock(); denied.json.return_value={"code":131006,"msg":"permission denied: node permission denied"}; denied.raise_for_status.return_value=None
        with patch("workflow_store.requests.post",return_value=ok), patch("workflow_store.requests.get",return_value=denied):
            result=workflow.read_source("feishu-copy-test")
        self.assertEqual(result["state"],"permission_denied")
        source=next(x for x in workflow.list_sources() if x["id"]=="feishu-copy-test")
        self.assertEqual(source["error_code"],"131006")
        self.assertEqual(source["sync_enabled"],0)
        self.assertEqual(source["resource_token"],"[stored]")

    def test_confirmed_product_link_requires_group_scope_and_https(self):
        self.workflow.upsert_group({"group_id":"g1","name":"群一"})
        with self.assertRaises(ValueError):
            self.workflow.upsert_product_link({"product_name":"商品","url":"http://example.com","business_date":"2026-10-26","group_scope":["g1"],"state":"confirmed"})
        with self.assertRaises(ValueError):
            self.workflow.upsert_product_link({"product_name":"商品","url":"https://example.com","business_date":"2026-10-26","group_scope":[],"state":"confirmed"})
        saved=self.workflow.upsert_product_link({"product_name":"商品","url":"https://example.com","business_date":"2026-10-26","group_scope":["g1"],"state":"confirmed"})
        self.assertEqual(saved["state"],"confirmed")

    def test_same_group_id_across_accounts_keeps_product_and_service_data_isolated(self):
        account_b="account-b"
        self.workflow.upsert_source_account({"id":account_b,"display_name":"账号二","rule_id":"r2","enabled":False})
        profile_a=self.workflow.upsert_profile({"id":"service-a","name":"群A客服","url":"https://a.example/service"})
        profile_b=self.workflow.upsert_profile({"id":"service-b","name":"群B客服","url":"https://b.example/service"})
        self.workflow.upsert_group({"source_account_id":SOURCE_ACCOUNT_ID,"group_id":"shared","name":"账号一同名群","service_profile_id":profile_a["id"]})
        self.workflow.upsert_group({"source_account_id":account_b,"group_id":"shared","name":"账号二同名群","service_profile_id":profile_b["id"]})
        for account,slug in ((SOURCE_ACCOUNT_ID,"a"),(account_b,"b")):
            self.workflow.upsert_product_link({"source_account_id":account,"product_name":"当日商品",
                "business_date":"2026-10-26","url":"https://"+slug+".example/item",
                "copy":"账号"+slug+"商品文案","group_scope":["shared"],"state":"confirmed"})
        service_ids={g["source_account_id"]:g["service_url"] for g in self.workflow.list_groups() if g["group_id"]=="shared"}
        self.assertEqual(service_ids,{SOURCE_ACCOUNT_ID:"https://a.example/service",account_b:"https://b.example/service"})

        phase=Phase3(self.store, self.root/"missing.env")
        recommendations={}
        for account in (SOURCE_ACCOUNT_ID,account_b):
            raw={"msgid":"same-message-id","content":"今天哪里拍？","_source_account_id":account,
                 "chatroom":{"id":"shared"},"fromUser":{"id":"same-user"}}
            normalized={"source_id":"same-message-id","source_account_id":account,"group_id":"shared","sender_id":"same-user",
                "group":"同名群","sender":"同名用户","content":"今天哪里拍？","timestamp":"2026-10-26T10:00:00+08:00"}
            seq,_=self.store.add_event(raw,{},normalized,"test",display=True,parse_state="parsed",direction="inbound",timestamp_valid=True)
            analysis={"primary":"product_link","labels":["product_link"],"response_needed":True,"response_state":"open",
                "features":{},"evidence":["询问当天商品链接"],"version":17}
            item=phase.ensure(seq,analysis)
            recommendations[account]=item["recommendation"]["draft"]
        self.assertEqual(recommendations,{SOURCE_ACCOUNT_ID:"账号a商品文案",account_b:"账号b商品文案"})
        with self.store.connect() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM product_links WHERE product_name='当日商品'").fetchone()[0],2)

    def test_schedule_candidates_and_confirmation_are_scoped_to_source_account(self):
        account_b="account-b"
        self.workflow.upsert_source_account({"id":account_b,"display_name":"账号二","rule_id":"r2","enabled":False})
        for account in (SOURCE_ACCOUNT_ID,account_b):
            self.workflow.upsert_group({"source_account_id":account,"group_id":"shared","name":"同名群"})
            self.workflow._save_schedule_candidates("feishu-product-schedule",1,"tab-oct",{
                "candidates":[{"item":"商品甲","cell":"R3C2","date_cell":"R2C2","date_label":"1","kind":"schedule_candidate"}],
                "alternates":[],"year_confirmed":False},account_id=account)
        with self.store.write_lock,self.store.connect() as db:
            db.execute("INSERT INTO knowledge_sources(id,title,source_type,url,updated_at) VALUES ('feishu-product-schedule','10月发品计划','sheet','https://x.feishu.cn/wiki/abcdefghi','now')")
            db.execute("INSERT INTO knowledge_snapshots(id,source_id,content_hash,state,captured_at,payload_json) VALUES (1,'feishu-product-schedule','hash','preview','now',?)",
                (json.dumps({"title":"10月发品计划","schedule_preview":{"month":10}}),))
        for account in (SOURCE_ACCOUNT_ID,account_b):
            result=self.workflow.confirm_schedule(2026,["shared"],account_id=account)
            self.assertEqual(result["confirmed_schedule_items"],1)
        with self.store.connect() as db:
            rows=db.execute("SELECT source_account_id,business_date,group_scope_json,is_sales_fact FROM schedule_candidates WHERE state='confirmed_schedule' ORDER BY source_account_id").fetchall()
        self.assertEqual([row["source_account_id"] for row in rows],["account-b",SOURCE_ACCOUNT_ID])
        self.assertTrue(all(row["business_date"]=="2026-10-01" and json.loads(row["group_scope_json"])==["shared"] and row["is_sales_fact"]==0 for row in rows))
        now=datetime(2026,10,26)
        self.assertEqual(self.workflow._confirmed_current_schedule_year('feishu-product-schedule',SOURCE_ACCOUNT_ID,10,now),2026)
        self.assertIsNone(self.workflow._confirmed_current_schedule_year('feishu-product-schedule','other-account',10,now))
        self.assertIsNone(self.workflow._confirmed_current_schedule_year('feishu-product-schedule',SOURCE_ACCOUNT_ID,9,now))
        self.assertIsNone(self.workflow._confirmed_current_schedule_year('feishu-product-schedule',SOURCE_ACCOUNT_ID,10,datetime(2027,10,26)))
        self.assertIsNone(self.workflow._confirmed_current_schedule_year('other-source',SOURCE_ACCOUNT_ID,10,now))

    def test_manual_material_imports_are_namespaced_by_source_account(self):
        self.workflow.upsert_source_account({"id":"account-b","display_name":"账号二","rule_id":"r2","enabled":False})
        self.workflow.seed_sources("https://x.feishu.cn/wiki/abcdefghi","https://x.feishu.cn/wiki/abcdefghi?sheet=tab123")
        rows=[{"title":"每日文案","body":"同一段测试文案"}]
        self.workflow.import_materials("feishu-copy-test",rows,account_id=SOURCE_ACCOUNT_ID)
        self.workflow.import_materials("feishu-copy-test",rows,account_id="account-b")
        materials=self.workflow.list_materials()
        self.assertEqual(len(materials),2)
        self.assertEqual({item["source_account_id"] for item in materials},{SOURCE_ACCOUNT_ID,"account-b"})
        self.assertEqual(len({item["id"] for item in materials}),2)

    def test_product_alias_selects_matching_today_link_and_generic_question_blocks_ambiguity(self):
        self.workflow.upsert_group({"group_id":"g1","name":"群一"})
        first=self.workflow.upsert_product({"id":"p1","name":"正式款一","aliases":["白色款"],"state":"active"})
        second=self.workflow.upsert_product({"id":"p2","name":"正式款二","aliases":["小白款"],"state":"active"})
        for product,slug in ((first,"a"),(second,"b")):
            self.workflow.upsert_product_link({"product_id":product["id"],"product_name":product["name"],
                "business_date":"2026-10-26","url":"https://"+slug+".example/item","copy":"当天链接"+slug,
                "group_scope":["g1"],"state":"confirmed"})
        phase=Phase3(self.store,self.root/"missing.env")
        def draft_for(text,source_id):
            raw={"msgid":source_id,"content":text,"chatroom":{"id":"g1"},"fromUser":{"id":"u1"}}
            normalized={"content":text,"group":"群一","sender":"客户一","timestamp":"2026-10-26T10:00:00+08:00"}
            seq,_=self.store.add_event(raw,{},normalized,"test",display=True,parse_state="parsed",direction="inbound",timestamp_valid=True)
            analysis={"primary":"product_link","labels":["product_link"],"response_needed":True,"response_state":"open",
                "features":{},"evidence":["询问当天商品链接"],"version":17}
            return phase.ensure(seq,analysis)["recommendation"]
        matched=draft_for("小白款今天哪里拍？","specific")
        generic=draft_for("今天哪里拍？","generic")
        self.assertEqual(matched["draft"],"当天链接b")
        self.assertFalse(generic["draft"])
        self.assertTrue(any("多个商品链接" in blocker for blocker in generic["blockers"]))

    def test_product_requirement_and_after_sales_keep_one_customer_card_with_distinct_tasks(self):
        sequences=[]
        for index,primary in enumerate(("service","requirement")):
            raw={"msgid":"intent-"+primary,"content":"消息内容","chatroom":{"id":"g1"},"fromUser":{"id":"u1"}}
            normalized={"content":"消息内容","group":"群一","sender":"客户一","timestamp":"2026-10-26T10:0"+str(index)+":00+08:00"}
            seq,_=self.store.add_event(raw,{},normalized,"test",display=True,parse_state="parsed",direction="inbound",timestamp_valid=True)
            self.store.save_analysis(seq,17,{"primary":primary,"labels":[primary],"assignee":"aftercare",
                "response_needed":True,"response_state":"open","conversation_id":"same-customer-session",
                "features":{"is_product_request":primary=="requirement"},"evidence":[primary],"version":17})
            sequences.append(seq)
        report=self.workflow.rebuild_cases(Phase3(self.store,self.root/"missing.env"),17)
        with self.store.connect() as db:
            rows=db.execute("SELECT intent,topic_key FROM support_cases ORDER BY intent").fetchall()
        self.assertEqual(len(rows),1)
        self.assertEqual(report["cases_seen"],1)
        item=self.workflow.list_suggestions("all")[0]
        self.assertEqual(len(item["payload"]["messages"]),2)
        self.assertEqual(set(item["payload"]["processing_plan"]["labels"]),{"service","requirement"})
        self.assertIn("requirement",{x["route"] for x in item["payload"]["processing_plan"]["tasks"]})

    def test_manual_ai_preview_reuses_same_input_without_second_api_call(self):
        seq=self.add_event("ai-preview")
        case=self.workflow.create_case(account_id=SOURCE_ACCOUNT_ID,group_id="g1",sender_id="u1",
            conversation_id="ai-preview-session",topic_key="qa",intent="qa",assignee="group_human",event_sequences=[seq])
        self.workflow.save_suggestion(case["id"],{"intent":"qa","knowledge_refs":[],"response_needed":True,"reply_draft":"人工核对"})
        env=self.root/".env"; env.write_text("deepseekapi=test-key\n",encoding="utf-8")
        workflow=WorkflowStore(self.store,env)
        class Response:
            def raise_for_status(self): pass
            def json(self): return {"choices":[{"message":{"content":json.dumps({
                "intent":"qa","assignee":"group_human","response_needed":True,"draft":"请补充具体问题。",
                "missing_information":[],"knowledge_ids":[],"evidence_sequences":[seq]})}}]}
        with patch("workflow_store.requests.post",return_value=Response()) as post:
            first=workflow.ai_preview(case["id"])
            repeated=workflow.ai_preview(case["id"])
        self.assertEqual(post.call_count,1)
        self.assertEqual(first["api_calls"],1)
        self.assertEqual(repeated["api_calls"],0)
        self.assertTrue(repeated["reused"])
        self.assertEqual(repeated["suggestion"]["id"],first["suggestion"]["id"])


if __name__ == "__main__":
    unittest.main()
