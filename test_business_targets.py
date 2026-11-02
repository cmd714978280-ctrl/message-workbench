"""Business configuration and schema preflight; all requests are simulated."""
import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from feishu_business_targets import TargetClient, ORDER_FIELDS, order_schema_plan, demand_field_mapping, parse_target
from ingestion_store import IngestionStore
from workflow_store import WorkflowStore
from phase3 import Phase3, faq_service_contact, recommendation
from processing_core import eligible_faqs


def contact_faq(identifier="general", question="老群推售后", url="http://example.com/help", **values):
    return {"id":identifier,"question":question,"keywords":question,"answer":"姐妹，产品售后、问题解答都可以联系小助理："+url,
            "enabled":True,"approved":True,"version":1,**values}


class ContactFAQTests(unittest.TestCase):
    def test_general_and_repurchase_are_separate_from_purchase_assistance(self):
        general=contact_faq()
        history=contact_faq("history","老群回购","http://example.com/rebuy")
        purchase=contact_faq("buy","死活不会拍的","http://example.com/buy")
        self.assertEqual(faq_service_contact([general,history,purchase])["url"],"http://example.com/help")
        self.assertEqual(faq_service_contact([general,history,purchase],historical=True)["url"],"http://example.com/rebuy")
        self.assertEqual(faq_service_contact([purchase])["state"],"missing")

    def test_disabled_wrong_scope_and_expired_contact_cannot_be_used(self):
        faqs=[contact_faq(enabled=False),contact_faq("group",group_scope=["other"]),
              contact_faq("old",valid_until="2026-10-20"),contact_faq("account",source_account_id="other")]
        eligible=eligible_faqs(faqs,business_date="2026-10-21",group_id="g1",account_id="a1")
        self.assertEqual(faq_service_contact(eligible)["state"],"missing")

    def test_distinct_urls_for_the_same_purpose_are_not_arbitrarily_selected(self):
        result=faq_service_contact([contact_faq(),contact_faq("other",url="https://example.com/other")])
        self.assertEqual(result["state"],"ambiguous")
        self.assertNotIn("url",result)

    def test_faq_source_builds_a_real_draft_and_keeps_evidence(self):
        faq=contact_faq()
        result=recommendation("service",{"content":"产品坏了","timestamp":"2026-10-21T10:00:00+08:00","group_id":"g1"},
            {"features":{},"labels":["service"]},{"useFaqServiceContacts":True,"aftercareScopeResolved":"missing_group_mapping"},[faq],[],[])
        self.assertEqual(result["draft"],faq["answer"])
        self.assertFalse(result["blockers"])
        self.assertEqual(result["faq"][0]["id"],faq["id"])
        self.assertEqual(result["service_contact_source"]["state"],"resolved")

    def test_product_request_with_customer_service_draft_is_not_blocked_by_missing_qa_faq(self):
        result=recommendation("qa",{"content":"糯玉米啥时候再来一波"},
            {"features":{"is_product_request":True},"labels":["qa","requirement"]},
            {"useFaqServiceContacts":True,"demandTableUrl":"https://example.feishu.cn/demand"},[contact_faq()],[],[])
        self.assertEqual(result["assignee"],"aftercare")
        self.assertIn("http://example.com/help",result["draft"])
        self.assertEqual(result["blockers"],[])

    def test_explicit_group_contact_takes_precedence(self):
        result=recommendation("service",{"content":"产品坏了"},{"features":{}},
            {"useFaqServiceContacts":True,"aftercareScopeResolved":"group","aftercareUrl":"https://example.com/group","aftercareTemplate":"本群客服：{{售后链接}}"},
            [contact_faq()],[],[])
        self.assertEqual(result["draft"],"本群客服：https://example.com/group")
        self.assertNotIn("service_contact_source",result)

    def test_uninitialized_order_schema_keeps_image_task_blocked(self):
        url="https://example.feishu.cn/base/APP123?table=tbl123"
        result=recommendation("order_image",{"content":"订单图"},{"features":{}},
            {"orderTableUrl":url,"businessTargetSchemas":{"orderTableUrl":{"url":url,"schema_ready":False,"error_summary":"订单图表字段待补建"}}},
            [],[{"id":1,"state":"stored"}],[])
        self.assertIn("订单图表字段待补建",result["blockers"])


class SchemaPreflightTests(unittest.TestCase):
    url="https://example.feishu.cn/base/APP123?table=tbl123"

    def test_target_rejects_non_feishu_or_ambiguous_resource(self):
        for url in ["https://feishu.cn.evil.test/base/APP?table=tbl123","http://example.feishu.cn/base/APP?table=tbl123",
                    "https://user:pass@example.feishu.cn/base/APP?table=tbl123","https://example.feishu.cn/base/APP"]:
            with self.assertRaises(ValueError): parse_target(url)
        self.assertEqual(parse_target(self.url),("APP123","tbl123"))

    def test_wrong_existing_field_type_blocks_all_mutations(self):
        fields=[{"field_name":"文本","field_id":"primary","is_primary":True,"type":1},
                {"field_name":"订单图","field_id":"wrong","type":1}]
        with self.assertRaises(ValueError): order_schema_plan(fields)

    def test_nonempty_table_is_preserved_without_any_write(self):
        client=TargetClient("test",self.url)
        fields=[{"field_name":"文本","field_id":"primary","is_primary":True,"type":1}]
        with patch.object(client,"list_items",side_effect=[fields,[{"fields":{"文本":"真实业务"}}]]),patch.object(client,"request") as request:
            with self.assertRaises(ValueError): client.initialize_empty_order_table()
            request.assert_not_called()

    def test_blank_rows_are_preserved_and_second_run_does_not_write(self):
        client=TargetClient("test",self.url)
        fields=[{"field_name":"文本","field_id":"primary","is_primary":True,"type":1}]
        writes=[]
        def request(method,suffix,**values):
            if method=="GET" and suffix=="/fields": return {"items":copy.deepcopy(fields),"has_more":False}
            if method=="GET" and suffix=="/records": return {"items":[{"record_id":"blank","fields":{}}],"total":1,"has_more":False}
            writes.append((method,suffix))
            body=copy.deepcopy(values["json"])
            if method=="PUT": fields[0].update(body)
            elif method=="POST": fields.append({**body,"field_id":"field-"+str(len(fields)),"is_primary":False})
            else: self.fail("unexpected write")
            return {}
        with patch.object(client,"request",side_effect=request):
            first=client.initialize_empty_order_table()
            self.assertEqual(len(first["fields"]),len(ORDER_FIELDS))
            self.assertEqual(first["records_written"],0)
            count=len(writes)
            second=client.initialize_empty_order_table()
            self.assertEqual(len(writes),count)
            self.assertTrue(second["schema_ready"])
        self.assertFalse(any("/records" in suffix for _,suffix in writes))

    def test_existing_demand_table_maps_by_actual_name_and_type(self):
        fields=[{"field_name":name,"field_id":name,"type":kind} for name,kind in
            [("群名",1),("用户名",1),("需求产品",1),("时间",5),("类目",3),("状态",3),("SourceID",1),("备注",17)]]
        result=demand_field_mapping(fields)
        self.assertFalse(result["issues"])
        self.assertEqual(result["fields"]["customer_name"]["field_name"],"用户名")
        self.assertNotIn("备注",[row["field_name"] for row in result["fields"].values()])


class IntegratedContactTests(unittest.TestCase):
    def setUp(self):
        scratch=Path(__file__).parent/"data/test-tmp";scratch.mkdir(parents=True,exist_ok=True)
        self.temp=tempfile.TemporaryDirectory(dir=scratch)
        self.store=IngestionStore(Path(self.temp.name)/"messages.sqlite3")
        self.workflow=WorkflowStore(self.store)
        self.phase=Phase3(self.store,Path(self.temp.name)/"missing.env")
        normalized={"id":"test","content":"我要退款","sender":"客户","sender_id":"u1","group_id":"g1","group":"测试群","timestamp":"2026-10-21T10:00:00+08:00"}
        self.sequence,_=self.store.add_event({"msgid":"test","chatroom":{"id":"g1"},"fromUser":{"id":"u1"}}, {},normalized,"test",
            display=True,timestamp_valid=True,parse_state="parsed",direction="inbound")

    def tearDown(self): self.temp.cleanup()

    def test_contact_reloads_from_faq_and_does_not_require_duplicate_group_mapping(self):
        faq=contact_faq()
        self.store.save_workspace_config({"useFaqServiceContacts":True},[faq])
        analysis={"primary":"service","labels":["service"],"version":17,"features":{}}
        first=self.phase.ensure(self.sequence,analysis)["recommendation"]
        self.assertEqual(first["draft"],faq["answer"])
        self.assertFalse(first["blockers"])
        self.assertNotIn("group_service",[row["field"] for row in self.workflow.processing_summary()["missing_configuration"]])
        self.store.save_workspace_config({"useFaqServiceContacts":True},[{**faq,"enabled":False}])
        second=self.phase.ensure(self.sequence,analysis)["recommendation"]
        self.assertFalse(second["draft"])
        self.assertTrue(second["blockers"])
        self.assertIn("group_service",[row["field"] for row in self.workflow.processing_summary()["missing_configuration"]])

    def test_disabled_old_copy_source_does_not_block_current_readiness(self):
        self.workflow.seed_sources("https://example.feishu.cn/wiki/COPY1234567890abcdef","https://example.feishu.cn/wiki/PLAN1234567890abcdef")
        self.workflow.set_source("feishu-copy-test",enabled=False,sync_enabled=False)
        summary=self.workflow.processing_summary()
        self.assertNotIn("source:feishu-copy-test",[row["field"] for row in summary["missing_configuration"]])


if __name__=="__main__": unittest.main()
