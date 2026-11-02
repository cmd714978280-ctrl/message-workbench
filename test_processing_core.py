"""Independent acceptance examples for the current, user-confirmed business rules."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from source_config import configured_source_account_id

from analysis_engine import analyze
from ingestion_store import IngestionStore
from phase3 import Phase3, recommendation
from processing_core import answer_conflicts, build_processing_plan, eligible_faqs, select_daily_copy, extend_reply_coverage
from workflow_store import WorkflowStore, _faq_reply_comparison, _faq_reply_verdict, _classification_reply_audit, resolve_schedule_context

SOURCE_ACCOUNT_ID = configured_source_account_id()


class CurrentRuleRegression(unittest.TestCase):
    def classify(self, content, *, minute=0, context=None, image=False, media=None, raw_type=""):
        timestamp=f"2026-11-01T10:{minute:02d}:00+08:00"
        normalized={"content":content,"group_id":"g1","sender_id":"u1","group":"群一",
                    "sender":"客户一","sequence":100,"timestamp":timestamp,
                    "attachments":[{"mime":"image/jpeg"}] if image else []}
        raw={"content":content,"type":raw_type,"msgid":"current","fromUser":{"id":"u1"}}
        return analyze(raw,normalized,context or [],media or [],{"rules":{}})

    @staticmethod
    def context(content, minute, *, sequence, image=False, vision=None):
        return {"group_id":"g1","sender_id":"u1","direction":"inbound","sequence":sequence,
                "source_timestamp":f"2026-11-01T10:{minute:02d}:00+08:00","content":content,
                "has_image":image,"image_understanding":vision or {},"reply_target_ids":[],"source_reference_ids":[]}

    def test_product_reference_link_request_is_product_need_unless_today_is_explicit(self):
        vision={"image_kind":"product_reference","confidence":.9,"evidence":["商品包装实拍"],"needs_human":True}
        context=[self.context("[图片消息]",0,sequence=99,image=True,vision=vision)]
        result=self.classify("这个还有链接吗",minute=1,context=context)
        self.assertEqual(result["primary"],"requirement")
        self.assertFalse(result["features"]["is_today_product_link"])
        self.assertEqual(self.classify("今天这个哪里拍",minute=1,context=context)["primary"],"product_link")

    def test_product_reference_image_inherits_nearby_product_demand(self):
        vision={"image_kind":"product_reference","confidence":.9,"evidence":["抽纸包装实拍"],"needs_human":True}
        context=[self.context("豆妈啥时候有抽纸啊",6,sequence=98),
                 self.context("没有这个吗",11,sequence=101)]
        result=self.classify("[图片消息]",minute=10,context=context,image=True,
                             media=[{"state":"stored","mime":"image/jpeg","image_understanding":vision}],raw_type="image")
        self.assertEqual(result["primary"],"requirement")

    def test_service_question_does_not_get_a_duplicate_daily_qa_label(self):
        result=self.classify("@豆妈 我的货怎么都被商家召回了？")
        self.assertEqual(result["primary"],"service")
        self.assertEqual(result["labels"],["service"])

    def test_promotion_with_question_words_is_not_a_product_requirement(self):
        result=self.classify("各位邻居有没有外边欠钱要不回来的？可以加这个顾问，你们谁有需要可以加他问一下，非常专业")
        self.assertEqual(result["primary"],"warning")
        self.assertNotIn("requirement",result["labels"])

    def test_purchase_confirmations_close_even_with_mentions_emojis_or_praise(self):
        for content in ("@豆妈 已拍早餐，太划算了[愉快]","拍了两件卫衣[呲牙]","谢谢找到了[呲牙]再下面"):
            with self.subTest(content=content):
                self.assertEqual(self.classify(content)["primary"],"closing")

    def test_product_photo_and_experiential_repurchase_are_feedback(self):
        vision={"image_kind":"product_feedback","confidence":.9,"evidence":["真实使用反馈"],"needs_human":False}
        context=[self.context("[图片消息]",0,sequence=98,image=True,vision=vision),
                 self.context("[图片消息]",1,sequence=99,image=True,vision=vision)]
        result=self.classify("足浴包真不错，泡脚舒服，用后再复购",minute=2,context=context)
        self.assertEqual(result["primary"],"feedback")
        self.assertFalse(result["features"]["is_product_request"])

    def test_receipt_caption_after_photo_creates_feedback_candidate(self):
        result=self.classify("[图片消息]",minute=1,image=True,raw_type="image",
                             media=[{"state":"stored","mime":"image/jpeg"}],
                             context=[self.context("到了",2,sequence=101)])
        self.assertEqual(result["primary"],"feedback")

    def test_short_usage_followup_inherits_qa_and_accidental_voice_stays_nonsemantic(self):
        followup=self.classify("这么回事啊",minute=1,context=[self.context("这个感觉有点涂不开",0,sequence=99)])
        self.assertEqual(followup["primary"],"qa")
        audio=self.classify("[语音消息]",minute=0,raw_type="voice",
                            media=[{"mime":"audio/mpeg","state":"not_image"}],
                            context=[self.context("不好意思，是宝宝玩手机发的",2,sequence=101)])
        self.assertEqual(audio["primary"],"ordinary")
        unresolved=self.classify("[语音消息]",minute=0,raw_type="voice",
                                 media=[{"mime":"audio/mpeg","state":"not_image"}])
        self.assertEqual(unresolved["primary"],"unknown")


class ProcessingPolicyAcceptance(unittest.TestCase):
    def test_category_auto_audit_needs_direct_reply_and_clear_category_evidence(self):
        reply={"sequence":9,"content":"请联系售后小助理处理","reply_link_type":"direct_reply"}
        self.assertEqual(_classification_reply_audit("service","收到破损了",[reply],{},1)["status"],"matched")
        reply["reply_link_type"]="recipient_inferred"
        self.assertEqual(_classification_reply_audit("service","收到破损了",[reply],{},1)["status"],"uncertain")
        reply["reply_link_type"]="direct_reply"
        self.assertEqual(_classification_reply_audit("service","这是什么商品",[reply],{},1)["status"],"uncertain")
        link_reply={"sequence":10,"content":"下单入口 https://example.test/item","reply_link_type":"direct_reply"}
        self.assertEqual(_classification_reply_audit("product_link","@链接客服 谢谢",[link_reply],{},1)["status"],"uncertain")

    def test_only_full_reply_match_is_automatically_matched(self):
        faq={"id":"one","question":"两件怎么拍","answer":"任选两件加入购物车一起下单"}
        question=[{"content":"两件怎么拍"}]
        exact=_faq_reply_comparison("qa",[faq],[{"sequence":3,"content":"任选两件加入购物车一起下单"}],question)
        self.assertEqual(exact["status"],"matched")
        similar=_faq_reply_comparison("qa",[faq],[{"sequence":3,"content":"任选两件加入购物车一起下单，再领取优惠"}],question)
        self.assertNotEqual(similar["status"],"matched")
        self.assertNotEqual(_faq_reply_verdict({"question":"宝宝能用吗","answer":"宝宝可以用"},["宝宝能用吗"],[{"content":"可以用"}])["verdict"],"match")

    def test_recalled_or_complaint_evidence_does_not_create_gallery_task(self):
        p={"case_id":"case","messages":[
            {"sequence":1,"content":"[图片消息]","timestamp":"2026-11-01T10:00:00+08:00","primary":"feedback"},
            {"sequence":2,"content":"到货就破了","timestamp":"2026-11-01T10:00:08+08:00","primary":"service"}]}
        turns=[{"sequence":1,"primary":"feedback","labels":["feedback"],"recommendation":{"destination_url":"https://x.feishu.cn/base"},"media":[{"id":1,"event_sequence":1,"state":"stored"}]},
               {"sequence":2,"primary":"service","labels":["service"],"recommendation":{"assignee":"aftercare"},"media":[]}]
        self.assertNotIn("feedback",[x["route"] for x in build_processing_plan(p,turns)["tasks"]])
        p["messages"][0]["revoked"]=True
        p["messages"][1]["content"]="很喜欢"
        self.assertNotIn("feedback",[x["route"] for x in build_processing_plan(p,turns)["tasks"]])

    def test_explicit_reply_covers_question_fragments_but_not_an_unrelated_or_later_question(self):
        messages=[{"sequence":1,"content":"@豆妈","primary":"qa","timestamp":"2026-10-21T10:00:00+08:00"},
            {"sequence":2,"content":"能么拍","primary":"qa","timestamp":"2026-10-21T10:00:05+08:00"},
            {"sequence":3,"content":"什么时候发货","primary":"qa","timestamp":"2026-10-21T10:00:10+08:00"},
            {"sequence":4,"content":"流程","primary":"qa","timestamp":"2026-10-21T10:00:15+08:00"},
            {"sequence":6,"content":"视频打不开","primary":"qa","timestamp":"2026-10-21T10:00:25+08:00"}]
        reply={"sequence":5,"content":"看教程视频去下单","reply_to_case":True,"reply_to_sequences":[4],"reply_link_type":"mention","timestamp":"2026-10-21T10:00:20+08:00"}
        extend_reply_coverage(messages,[reply],{})
        self.assertEqual(reply["reply_to_sequences"],[1,2,4])
        self.assertEqual(reply["direct_reply_to_sequences"],[4])
        quoted={**reply,"reply_to_sequences":[4],"reply_link_type":"quoted"}
        extend_reply_coverage(messages,[quoted],{})
        self.assertEqual(quoted["reply_to_sequences"],[4])

    def test_followup_and_reference_picture_share_the_explicit_answer(self):
        messages=[{"sequence":1,"content":"[图片消息]","primary":"requirement","media":[{"id":8}],"timestamp":"2026-10-21T10:00:00+08:00"},
            {"sequence":2,"content":"这个还有吗","primary":"qa","timestamp":"2026-10-21T10:00:10+08:00"},
            {"sequence":3,"content":"我想买","primary":"qa","timestamp":"2026-10-21T10:00:20+08:00"}]
        reply={"sequence":4,"content":"联系小助理咨询复购链接","reply_to_case":True,"reply_to_sequences":[3],"timestamp":"2026-10-21T10:00:30+08:00"}
        extend_reply_coverage(messages,[reply],{3:{"features":{"followup_question_sequence":2}}})
        self.assertEqual(reply["reply_to_sequences"],[1,2,3])

    def test_schedule_question_does_not_generate_demand_or_customer_service_tasks(self):
        rec=recommendation("qa",{"content":"羊毛衫几点上链接"},
            {"labels":["qa","requirement"],"features":{"is_schedule_question":True,"is_product_request":True}},
            {},[],[],[])
        self.assertEqual(rec["assignee"],"group_human")
        self.assertFalse(any(t["route"] in {"requirement","aftercare_contact"} for t in rec.get("secondary_actions",[])))

    def test_product_reference_image_stays_with_demand_instead_of_gallery(self):
        rec=recommendation("requirement",{"content":"[图片消息]"},
            {"labels":["requirement"],"features":{"is_product_request":True}},
            {"demandTableUrl":"https://example.feishu.cn/demand"},[],
            [{"id":8,"state":"stored","image_understanding":{"image_kind":"product_feedback","confidence":.95}}],[])
        self.assertEqual(rec["evidence_media_ids"],[8])
        self.assertEqual(rec["attachment_tasks"],[])

    def test_inactive_expired_other_product_group_and_account_are_excluded_before_ranking(self):
        base={"id":"valid","question":"两件怎么拍","answer":"一起下单","enabled":True,"approved":True,
              "scope":"product","product_name":"羊毛衫","valid_from":"2026-10-21","valid_until":"2026-10-21",
              "source_account_id":"account-a","group_scope":["*"]}
        variants=[base,{**base,"id":"off","enabled":False},{**base,"id":"old","valid_until":"2026-10-20"},
                  {**base,"id":"other-product","product_name":"鞋子"},{**base,"id":"other-account","source_account_id":"account-b"},
                  {**base,"id":"other-group","group_scope":["g2"]}]
        selected=eligible_faqs(variants,business_date="2026-10-21",products=["羊毛衫"],group_id="g1",account_id="account-a")
        self.assertEqual([x["id"] for x in selected],["valid"])
        self.assertEqual(eligible_faqs([base],business_date="2026-10-22",products=["羊毛衫"],group_id="g1",account_id="account-a"),[])

    def test_operator_copy_is_reference_not_customer_script_fact(self):
        page={"values":[["10:00","豆妈","羊毛衫任选两件加入购物车","文案"],
                        ["10:01","12","我拍了58元质量特别好","文字"],
                        ["10:02","豆妈","羊毛衫【链接】","链接"]]}
        rows=select_daily_copy(page,["羊毛衫"],"两件怎么拍")
        self.assertTrue(rows)
        self.assertTrue(all(x["author"]=="豆妈" and not x["usable_as_fact"] for x in rows))
        self.assertFalse(any("质量特别好" in x["body"] for x in rows))
        self.assertTrue(any(x["has_placeholder_link"] for x in rows))

    def test_daily_copy_does_not_create_a_faq_hit_for_an_unrelated_question(self):
        faq={"id":"purchase","question":"两件怎么拍","keywords":"两件怎么拍","answer":"任选两件加入购物车一起下单", "enabled":True,"approved":True}
        rec=recommendation("qa",{"content":"这件会不会有色差","timestamp":"2026-10-21T10:00:00+08:00"},
            {"features":{},"evidence":[]},{},[faq],[],[],{"business_date":"2026-10-21","matched_products":["羊毛衫"],"daily_copy_excerpt":"两件怎么拍，任选两件加入购物车一起下单"})
        self.assertEqual(rec["faq"],[])
        self.assertFalse(rec["draft"])

    def test_conflicting_price_and_polarity_override_text_similarity(self):
        verdict=_faq_reply_verdict({"question":"一件多少克","answer":"一件300克左右，尺码不同重量有区别"},["一件多少克"],[{"content":"一件200克左右，尺码不同重量有区别"}])
        self.assertEqual(verdict["verdict"],"outdated")
        self.assertTrue(answer_conflicts("可以机洗","不可以机洗"))
        self.assertFalse(answer_conflicts("不可以机洗","不可以机洗"))

    def test_all_real_reply_steps_must_be_covered(self):
        v=_faq_reply_verdict({"question":"两件怎么拍","answer":"任选两件加入购物车一起下单"},["两件怎么拍"],
            [{"content":"任选两件加入购物车一起下单"},{"content":"还需要重新打开链接领取优惠券"}])
        self.assertEqual(v["verdict"],"partial")

    def test_no_reference_is_not_an_automatic_pass(self):
        self.assertEqual(_faq_reply_comparison("qa",[],[],[{"content":"哪里拍"}])["status"],"no_reference")

    def test_closing_does_not_remove_order_task_and_problem_evidence_is_not_an_order(self):
        payload={"case_id":"c1","messages":[{"sequence":1,"content":"订单图","primary":"order_image"},{"sequence":2,"content":"已拍","primary":"closing"}]}
        picture={"id":8,"state":"stored"}
        turns=[{"sequence":1,"primary":"order_image","recommendation":{"action":"订单图","destination_url":"https://x.feishu.cn/table"},"media":[picture]},
               {"sequence":2,"primary":"closing","recommendation":{},"media":[]}]
        plan=build_processing_plan(payload,turns)
        self.assertFalse(plan["response_needed"])
        self.assertEqual([t["key"] for t in plan["tasks"]],["media:8:order_image"])
        turns[0]["media"][0]["usage"]="problem_evidence"
        self.assertEqual(build_processing_plan(payload,turns)["tasks"],[])

    def test_old_classification_conflict_stays_visible_when_picture_is_problem_evidence(self):
        p={"case_id":"c1","messages":[{"sequence":1,"content":"图片","primary":"order_image"}]}
        turns=[{"sequence":1,"primary":"order_image","media":[{"id":8,"state":"stored","usage":"problem_evidence"}],
                "recommendation":{"requires_classification_review":True,"blockers":["旧人工分类尚未针对当前规则复核"]}}]
        result=build_processing_plan(p,turns)
        self.assertEqual(result["status"],"human")
        self.assertEqual([x["route"] for x in result["tasks"]],["classification"])

    def test_service_evidence_uses_configured_table_and_retains_caption(self):
        url='https://example.feishu.cn/base/APP?table=tbl1'
        p={'case_id':'c','messages':[{'sequence':1,'content':'[图片消息]','timestamp':'2026-11-02T10:00:00+08:00'},
             {'sequence':2,'content':'收到后发现破损','timestamp':'2026-11-02T10:00:10+08:00'}]}
        turns=[{'sequence':1,'primary':'service','labels':['service'],'media':[{'id':8,'event_sequence':1,'state':'stored'}],
                'recommendation':{'assignee':'aftercare','secondary_actions':[{'route':'feedback','media_id':8,'destination_url':url}]}}]
        plan=build_processing_plan(p,turns,{'aftercareEvidenceUrl':url})
        task=next(t for t in plan['tasks'] if t['route']=='service_evidence')
        self.assertEqual(task['destination_url'],url)
        self.assertIn('破损',task['note'])
        self.assertEqual(task['blockers'],[])
        self.assertNotIn('feedback',[t['route'] for t in plan['tasks']])

    def test_old_service_conversation_does_not_turn_unrelated_photo_into_evidence(self):
        p={'case_id':'c','messages':[{'sequence':1,'content':'[图片消息]','timestamp':'2026-11-02T10:00:00+08:00'},
             {'sequence':2,'content':'桶破了','timestamp':'2026-11-02T10:07:00+08:00'}]}
        turns=[{'sequence':1,'primary':'service','labels':['service'],'media':[{'id':8,'event_sequence':1,'state':'stored'}],
                'recommendation':{'assignee':'aftercare','secondary_actions':[{'route':'feedback','media_id':8}]}}]
        plan=build_processing_plan(p,turns,{'aftercareEvidenceUrl':'https://example.feishu.cn/base/APP?table=tbl1'})
        self.assertIn('image_review',[t['route'] for t in plan['tasks']])
        self.assertNotIn('service_evidence',[t['route'] for t in plan['tasks']])
        self.assertNotIn('feedback',[t['route'] for t in plan['tasks']])

    def test_nearby_feedback_caption_before_or_after_image_is_one_task(self):
        for caption_first in (True,False):
            with self.subTest(caption_first=caption_first):
                image_seq=2 if caption_first else 1;caption_seq=1 if caption_first else 2
                messages=[{"sequence":image_seq,"content":"[图片消息]","primary":"feedback","timestamp":f"2026-10-21T10:0{image_seq}:00+08:00"},
                    {"sequence":caption_seq,"content":"湿厕纸收到了，很划算","primary":"feedback","timestamp":f"2026-10-21T10:0{caption_seq}:00+08:00"}]
                turns=[{"sequence":image_seq,"primary":"feedback","media":[{"id":8,"event_sequence":image_seq,"state":"stored"}],
                    "recommendation":{"destination_url":"https://example.feishu.cn/gallery"}},
                    {"sequence":caption_seq,"primary":"feedback","media":[],"recommendation":{"blockers":["未找到可核验的图片附件"]}}]
                turns.sort(key=lambda t:t["sequence"])
                result=build_processing_plan({"case_id":"c","messages":messages},turns)
                self.assertEqual(result["status"],"ready")
                self.assertEqual(len(result["tasks"]),1)
                self.assertEqual(result["tasks"][0]["source_sequences"],[1,2])
                self.assertIn("湿厕纸收到了，很划算",result["tasks"][0]["note"])

    def test_actual_missing_image_distant_caption_and_other_route_stay_blocked(self):
        for content,primary,stamp in [("[图片消息]","feedback","10:01"),
                                      ("另一个商品收到了","feedback","10:10"),
                                      ("订单图","order_image","10:01")]:
            with self.subTest(content=content,primary=primary):
                payload={"case_id":"c","messages":[
                    {"sequence":1,"content":"[图片消息]","primary":"feedback","timestamp":"2026-10-21T10:00:00+08:00"},
                    {"sequence":2,"content":content,"primary":primary,"timestamp":"2026-10-21T"+stamp+":00+08:00"}]}
                turns=[{"sequence":1,"primary":"feedback","media":[{"id":8,"event_sequence":1,"state":"stored"}],
                    "recommendation":{"destination_url":"https://example.feishu.cn/gallery"}},
                    {"sequence":2,"primary":primary,"media":[],"recommendation":{}}]
                result=build_processing_plan(payload,turns)
                self.assertEqual(result["status"],"needs_configuration")
                self.assertTrue(any("没有收到" in reason for reason in result["blockers"]))
                self.assertEqual(len(result["tasks"]),2)


class CoreStoreAcceptance(unittest.TestCase):
    def setUp(self):
        scratch=Path(__file__).parent/"data"/"test-tmp";scratch.mkdir(parents=True,exist_ok=True)
        self.temp=tempfile.TemporaryDirectory(dir=scratch);self.root=Path(self.temp.name)
        self.store=IngestionStore(self.root/"messages.sqlite3");self.workflow=WorkflowStore(self.store)
        self.phase=Phase3(self.store,self.root/"missing.env")

    def tearDown(self): self.temp.cleanup()

    def add(self,source,content,minute,*,direction="inbound",target="",session="s1",primary="qa",customer="u1"):
        raw={"msgid":source,"content":content,"chatroom":{"id":"g1"},"fromUser":{"id":customer if direction=="inbound" else "operator"}}
        if target:
            # Yunzhongke's at_wxid is a mentioned-user list. Reply-recipient
            # inference uses to_wxid/toUser.id, so model that field directly.
            raw["to_wxid"]=target
        norm={"content":content,"group":"群一","sender":"客户一","sender_id":customer if direction=="inbound" else "operator","timestamp":f"2026-10-21T10:{minute:02d}:00+08:00"}
        seq,_=self.store.add_event(raw,{},norm,"acceptance",display=True,parse_state="parsed",direction=direction,timestamp_valid=True)
        if direction=="inbound":
            self.store.save_analysis(seq,17,{"primary":primary,"labels":[primary],"response_needed":primary=="qa","assignee":"group_human","response_state":"open","conversation_id":session,"features":{},"evidence":[],"version":17})
        return seq

    def test_direct_real_reply_auto_records_category_but_inferred_reply_does_not(self):
        direct=self.add("direct-q","今天链接在哪",0,primary="product_link",customer="u1")
        answer=self.add("direct-a","入口 https://example.test/item",1,direction="outbound",target="u1")
        inferred=self.add("inferred-q","今天链接在哪",0,primary="product_link",customer="u2")
        self.add("inferred-a","入口 https://example.test/item",1,direction="outbound",target="u2")
        with self.store.connect() as db:
            raw=json.loads(db.execute("SELECT raw_json FROM events WHERE sequence=?",(answer,)).fetchone()[0])
            raw["data"]["replyMessageId"]="direct-q"
            db.execute("UPDATE events SET raw_json=? WHERE sequence=?",(json.dumps(raw),answer))
        self.workflow.rebuild_cases(self.phase,17)
        self.assertEqual(self.store.development_audit(direct,17)["actual_primary"],"product_link")
        self.assertIsNone(self.store.development_audit(inferred,17))
        summary=self.store.development_audit_summary(17)
        self.assertEqual(summary["automatic_reviewed"],1)
        self.assertEqual(summary["comparison_count"],0)
        self.assertIsNone(summary["category_accuracy"])

    def test_checkout_problem_is_evidence_but_explicit_human_order_classification_wins(self):
        self.store.save_workspace_config({"orderTableUrl":"https://example.feishu.cn/order"},[])
        for index,reviewed in enumerate((False,True)):
            customer="u"+str(index)
            image=self.add("image"+customer,"[图片消息]",0,primary="order_image",customer=customer)
            self.store.save_analysis(image,17,{"primary":"order_image","labels":["order_image","qa"],"version":17,
                "conversation_id":"s1","features":{"image_vision_results":[{"image_kind":"order_screenshot","needs_human":True}]}})
            with self.store.connect() as db:
                db.execute("INSERT INTO media(event_sequence,url,url_hash,state) VALUES (?,?,?,'stored')",(image,"https://example.test/image","image"+customer))
            self.add("price"+customer,"为什么我是8.9",1,customer=customer)
            if reviewed:self.store.save_review(image,"approved","order_image",["order_image"],"确认保留订单截图",rule_version=17)
        self.workflow.rebuild_cases(self.phase,17)
        for row in self.workflow.processing_page("all")["items"]:
            payload=self.workflow.processing_detail(row["case_id"])["payload"]
            order_tasks=[t for t in payload["processing_plan"]["tasks"] if t["route"]=="order_image"]
            self.assertEqual(len(order_tasks),1 if payload["sender_id"]=="u1" else 0)

    def test_cms_direct_reply_id_is_not_mistaken_for_a_wechat_quote(self):
        first=self.add("price1","价格怎么是58",0)
        second=self.add("price2","还是58",1)
        reply=self.add("answer","48-58都是正确价格，款式不同价格不同",2,direction="outbound",target="u1")
        with self.store.connect() as db:
            raw=json.loads(db.execute("SELECT raw_json FROM events WHERE sequence=?",(reply,)).fetchone()[0])
            raw["data"].update(replyMessageId="price2",is_yinyong="N")
            db.execute("UPDATE events SET raw_json=? WHERE sequence=?",(json.dumps(raw),reply))
        self.workflow.rebuild_cases(self.phase,17)
        item=self.workflow.processing_page("all")["items"][0]
        payload=self.workflow.processing_detail(item["case_id"])["payload"]
        self.assertEqual(payload["answered_message_sequences"],[first,second])
        with self.store.connect() as db:
            raw["data"]["is_yinyong"]="Y"
            db.execute("UPDATE events SET raw_json=? WHERE sequence=?",(json.dumps(raw),reply))
        self.workflow.rebuild_cases(self.phase,17)
        payload=self.workflow.processing_detail(item["case_id"])["payload"]
        self.assertEqual(payload["answered_message_sequences"],[second])
        self.assertIn(first,payload["processing_plan"]["unanswered_sequences"])

    def test_real_reply_targets_preceding_question_and_later_question_stays_open(self):
        first=self.add("q1","两件怎么拍",0)
        self.add("a1","任选两件加入购物车一起下单",1,direction="outbound",target="u1")
        second=self.add("q2","物流什么时候发货",2)
        self.workflow.rebuild_cases(self.phase,17)
        row=self.workflow.processing_page("all")["items"][0]
        p=self.workflow.processing_detail(row["case_id"])["payload"]
        self.assertEqual(p["answered_message_sequences"],[first])
        self.assertIn(second,p["processing_plan"]["unanswered_sequences"])
        self.assertEqual(len(p["messages"]),2)
        # The first question has a real reply but no FAQ reference; the later
        # unanswered question has no gold reply. The combined case is mixed.
        self.assertEqual(p["faq_comparison"]["status"],"mixed")
        self.assertEqual([turn["status"] for turn in p["faq_comparison"]["turn_comparisons"]],
                         ["no_hit","no_reference"])
        # FAQ matching for the replied turn is automatically recorded in the
        # development comparison queue; the later unanswered turn remains open
        # in the processing plan, but is not a second FAQ-review item.
        self.assertEqual(len(self.workflow.list_suggestions("todo")),0)
        self.assertEqual(self.workflow.processing_page("all")["items"][0]["processing_state"],"human")

    def test_unresolved_reply_reference_uses_unique_to_wxid_but_keeps_multiple_candidates_ambiguous(self):
        one_question=self.add("one-question","我咋一件58？",0,customer="u1")
        one_reply=self.add("one-reply","任选两件加入购物车一起下单",1,direction="outbound",target="u1")
        two_first=self.add("two-first","这款怎么拍",0,customer="u2")
        two_second=self.add("two-second","物流什么时候发",1,customer="u2")
        two_reply=self.add("two-reply","联系一下售后",2,direction="outbound",target="u2")
        for sequence in (one_reply,two_reply):
            with self.store.connect() as db:
                raw=json.loads(db.execute("SELECT raw_json FROM events WHERE sequence=?",(sequence,)).fetchone()[0])
                raw["data"].update(replyMessageId="unmapped-cms-reference-"+str(sequence),is_yinyong="N")
                db.execute("UPDATE events SET raw_json=? WHERE sequence=?",(json.dumps(raw),sequence))
        self.workflow.rebuild_cases(self.phase,17)
        cases={}
        for row in self.workflow.processing_page("all")["items"]:
            payload=self.workflow.processing_detail(row["case_id"])["payload"]
            cases[payload["sender_id"]]=payload
        unique=cases["u1"]
        self.assertEqual(unique["gold_reply_sequences"],[one_reply])
        self.assertEqual(unique["answered_message_sequences"],[one_question])
        inferred=next(m for m in unique["reply_context"]["messages"] if m["sequence"]==one_reply)
        self.assertEqual(inferred["reply_link_type"],"recipient_inferred")

        ambiguous=cases["u2"]
        self.assertEqual(ambiguous["gold_reply_sequences"],[])
        self.assertEqual(ambiguous["answered_message_sequences"],[])
        unresolved=next(m for m in ambiguous["reply_context"]["messages"] if m["sequence"]==two_reply)
        self.assertEqual(unresolved["reply_link_type"],"recipient_ambiguous")
        self.assertEqual(set(ambiguous["message_sequences"]),{two_first,two_second})

    def test_future_operator_reply_cannot_drive_the_original_faq_match(self):
        self.store.save_workspace_config({},[{"id":"purchase","question":"两件怎么拍","answer":"任选两件加入购物车一起下单","enabled":True,"approved":True}])
        first=self.add("q1","这件会不会有色差",0)
        self.add("a1","两件怎么拍，任选两件加入购物车一起下单",1,direction="outbound",target="u1")
        rec=self.phase.ensure(first,self.store.analysis_map([first],17)[0][first])["recommendation"]
        self.assertEqual(rec["faq"],[])

    def test_processing_decisions_are_versioned_separate_from_faq_audits(self):
        self.add("q1","你好，请问有其他颜色吗",0)
        with patch('requests.post',side_effect=AssertionError('unexpected external call')):
            self.workflow.rebuild_cases(self.phase,17)
            first=self.workflow.processing_detail(self.workflow.processing_page("all")["items"][0]["case_id"])
            saved=self.workflow.decide_processing(first["case_id"],first["id"],decision="waiting",plan_fingerprint=first["payload"]["plan_fingerprint"],note="等待商品资料")
            self.assertEqual(saved["processing_state"],"waiting")
            self.workflow.rebuild_cases(self.phase,17)
        self.assertEqual(self.workflow.processing_page("waiting")["total"],1)
        self.assertEqual(self.workflow.processing_page("all")["total"],1)
        with self.assertRaisesRegex(ValueError,"版本|更新"):
            self.workflow.decide_processing(first["case_id"],first["id"],decision="confirmed",plan_fingerprint="wrong")
        with self.store.connect() as db:
            self.assertEqual(db.execute('select count(*) from processing_decisions').fetchone()[0],1)
            self.assertEqual(db.execute('select count(*) from development_audits').fetchone()[0],0)

    def test_core_lists_only_one_current_version_per_case(self):
        seq=self.add("q1","怎么拍",0)
        self.workflow.rebuild_cases(self.phase,17)
        row=self.workflow.processing_page("all")["items"][0]
        payload=self.workflow.processing_detail(row["case_id"])["payload"]
        for index in range(3): self.workflow.save_suggestion(row["case_id"],{**payload,"revision_example":index})
        self.assertEqual(self.workflow.processing_page("all")["total"],1)
        self.assertEqual(len(self.workflow.list_suggestions("all")),1)

    def test_changed_plan_preserves_prior_decision_and_requires_reconfirmation(self):
        self.add("q1","请问怎么操作",0)
        self.workflow.rebuild_cases(self.phase,17)
        row=self.workflow.processing_detail(self.workflow.processing_page("all")["items"][0]["case_id"])
        self.workflow.decide_processing(row["case_id"],row["id"],decision="human",plan_fingerprint=row["payload"]["plan_fingerprint"])
        payload=json.loads(json.dumps(row["payload"]))
        payload["plan_fingerprint"]="changed-inputs"
        self.workflow.save_suggestion(row["case_id"],payload)
        current=self.workflow.processing_detail(row["case_id"])
        self.assertEqual(current["processing_state"],"stale")
        self.assertEqual(current["decision"],"human")
        with self.assertRaisesRegex(ValueError,"更新"):
            self.workflow.decide_processing(row["case_id"],row["id"],decision="handled",plan_fingerprint=row["payload"]["plan_fingerprint"],note="旧版本")

    def test_approval_is_blocked_when_required_business_configuration_is_missing(self):
        self.add("q1","我要退款",0,primary="service")
        self.workflow.rebuild_cases(self.phase,17)
        row=self.workflow.processing_detail(self.workflow.processing_page("all")["items"][0]["case_id"])
        self.assertEqual(row["processing_state"],"needs_configuration")
        with self.assertRaisesRegex(ValueError,"资料|图片"):
            self.workflow.decide_processing(row["case_id"],row["id"],decision="confirmed",plan_fingerprint=row["payload"]["plan_fingerprint"],draft="联系客服")

    def test_confirmed_material_does_not_reset_on_identical_manual_reimport(self):
        self.workflow.seed_sources("https://x.feishu.cn/wiki/abcdefgh","https://x.feishu.cn/wiki/abcdefgh?sheet=tab1")
        rows=[{"title":"当天文案","body":"羊毛衫购买 https://example.com/product","business_date":"2026-10-21","group_scope":["*"],"product_name":"羊毛衫"}]
        self.workflow.import_materials("feishu-copy-test",rows)
        material=self.workflow.list_materials()[0]
        self.workflow.set_material_state(material["id"],state="confirmed",business_date="2026-10-21",group_scope=["*"])
        self.workflow.import_materials("feishu-copy-test",rows)
        self.assertEqual(self.workflow.list_materials()[0]["state"],"confirmed")

    def test_manual_ai_keeps_context_and_reuses_identical_business_inputs(self):
        self.add("q1","请问怎么操作",0)
        self.workflow.rebuild_cases(self.phase,17)
        row=self.workflow.processing_page("all")["items"][0]
        env=self.root/"test.env";env.write_text("deepseekapi=isolated-fake-key",encoding="utf-8")
        workflow=WorkflowStore(self.store,env)
        result={"summary":"需要具体商品","intent":"qa","assignee":"group_human","response_needed":True,
                "draft":"请问您想购买哪款商品？","missing_information":[],"evidence_sequences":[1],"knowledge_ids":[]}
        from unittest.mock import Mock
        response=Mock();response.json.return_value={"choices":[{"message":{"content":json.dumps(result,ensure_ascii=False)}}]}
        with patch('workflow_store.requests.post',return_value=response) as call:
            first=workflow.ai_preview(row["case_id"]);second=workflow.ai_preview(row["case_id"])
        self.assertEqual(call.call_count,1)
        self.assertTrue(second["reused"])
        self.assertTrue(first["suggestion"]["payload"]["messages"])
        self.assertIn("processing_plan",first["suggestion"]["payload"])

    def test_ai_cannot_add_price_or_nonexistent_message_evidence(self):
        self.add("q1","多少钱",0)
        self.workflow.rebuild_cases(self.phase,17)
        row=self.workflow.processing_page("all")["items"][0]
        env=self.root/"test.env";env.write_text("deepseekapi=isolated-fake-key",encoding="utf-8")
        workflow=WorkflowStore(self.store,env)
        result={"summary":"价格","intent":"qa","assignee":"group_human","response_needed":True,
                "draft":"现在29元一件","missing_information":[],"evidence_sequences":[999],"knowledge_ids":[]}
        from unittest.mock import Mock
        response=Mock();response.json.return_value={"choices":[{"message":{"content":json.dumps(result,ensure_ascii=False)}}]}
        with patch('workflow_store.requests.post',return_value=response): saved=workflow.ai_preview(row["case_id"])
        preview=saved["suggestion"]["payload"]["ai_preview"]
        self.assertFalse(preview["draft"])
        self.assertGreaterEqual(len(preview["missing_information"]),2)

    def test_incremental_rebuild_keeps_unaffected_customer_and_every_original_message(self):
        self.add("qa","怎么拍",0);self.add("qb","还有其他颜色吗",1,customer="u2")
        self.workflow.rebuild_cases(self.phase,17)
        before={row["case_id"]:row["id"] for row in self.workflow.processing_page("all")["items"]}
        self.add("qa2","两件一起付款吗",2)
        report=self.workflow.rebuild_cases(self.phase,17,scopes=[(SOURCE_ACCOUNT_ID,"g1","u1")])
        self.assertEqual(report["rebuild_mode"],"incremental")
        self.assertEqual(report["cases_seen"],1);self.assertEqual(report["events_considered"],2)
        self.assertEqual(self.workflow.processing_page("all")["total"],2)
        with self.store.connect() as db:
            unaffected=db.execute("SELECT s.case_id,s.id FROM support_cases c JOIN process_suggestions s ON s.case_id=c.id WHERE c.sender_id='u2' AND s.is_current=1").fetchone()
            self.assertEqual(unaffected["id"],before[unaffected["case_id"]])
            self.assertEqual(db.execute('select count(*) from case_messages').fetchone()[0],3)

    def test_changed_faq_configuration_uses_full_rebuild_but_new_message_uses_scopes(self):
        self.add("qa","怎么拍",0)
        checkpoint=self.workflow.processing_checkpoint(17)
        self.add("qa2","两件一起付款吗",2)
        _,scopes=self.workflow.processing_changes(checkpoint,17)
        self.assertIsNotNone(scopes);self.assertIn((SOURCE_ACCOUNT_ID,"g1","u1"),scopes)
        checkpoint=self.workflow.processing_checkpoint(17)
        self.store.save_workspace_config({},[{"id":"f","question":"怎么拍","answer":"按商品详情操作","enabled":True,"approved":True}])
        self.assertIsNone(self.workflow.processing_changes(checkpoint,17)[1])

    def test_shanghai_message_date_uses_matching_historical_month_not_latest_month(self):
        self.workflow.seed_sources("https://x.feishu.cn/wiki/abcdefgh","https://x.feishu.cn/wiki/abcdefgh?sheet=oct")
        self.workflow.upsert_product({"name":"羊毛衫","state":"active","aliases":["羊毛针织衫"]})
        for month in (10,11):
            payload={"source_account_id":SOURCE_ACCOUNT_ID,"title":str(month)+"月发品计划",
                     "schedule_preview":{"month":month,"year":2026,"year_confirmed":True,"candidates":[{"date_label":"21","business_date":f"2026-{month:02d}-21","item":"羊毛衫-特如","cell":"R8C4"}]},
                     "daily_pages":{f"2026-{month:02d}-21":{"status":"ready","title":f"2026.{month}.21羊毛衫","values":[["10:00","豆妈","羊毛衫当日文案","文案"]]}}}
            with self.store.connect() as db:
                db.execute("INSERT INTO knowledge_snapshots(source_id,content_hash,state,captured_at,payload_json) VALUES ('feishu-product-schedule',?,'candidate','2026-10-27',?)",(str(month),json.dumps(payload,ensure_ascii=False)))
        result=resolve_schedule_context(self.store,SOURCE_ACCOUNT_ID,"2026-10-20T17:00:00Z",["羊毛衫哪里拍"],group_id="g1")
        self.assertEqual(result["business_date"],"2026-10-21")
        self.assertEqual(result["schedule_title"],"10月发品计划")
        self.assertEqual(result["matched_products"],["羊毛衫"])
        self.assertTrue(result["year_verified"])

    def test_same_answer_different_group_scope_is_not_deduplicated_together(self):
        from workflow_store import _faq_candidate_fingerprint
        base={"scope":"product","product_name":"羊毛衫","valid_from":"2026-10-21","valid_until":"2026-10-21","answer":"任选两件加入购物车一起下单"}
        self.assertNotEqual(_faq_candidate_fingerprint({**base,"group_scope":["g1"]}),_faq_candidate_fingerprint({**base,"group_scope":["g2"]}))
        self.assertNotEqual(_faq_candidate_fingerprint({**base,"answer":"联系 https://example.com/a"}),_faq_candidate_fingerprint({**base,"answer":"联系 https://example.com/b"}))

    def test_confirming_unknown_product_merges_an_identical_scoped_candidate(self):
        self.add("q1","两件怎么拍",0)
        self.add("a1","任选两件加入购物车一起下单",1,direction="outbound",target="u1")
        self.workflow.rebuild_cases(self.phase,17)
        candidate=self.workflow.list_faq_candidates()[0]
        data={key:candidate[key] for key in ("scope","product_name","valid_from","valid_until","question","answer","keywords")}
        data.update(product_name="羊毛衫",source_suggestion_id=candidate["evidence"][0]["suggestion_id"],source_reply_sequences=candidate["evidence"][0]["reply_sequences"])
        known=self.workflow.save_faq_candidate(data)
        merged=self.workflow.save_faq_candidate({**data,"id":candidate["id"]})
        self.assertEqual(merged["id"],known["id"])
        self.assertEqual(len(self.workflow.list_faq_candidates()),1)
        self.assertEqual(self.workflow.get_faq_candidate(candidate["id"])["status"],"merged")

    def test_rebuild_preserves_edited_candidate_when_original_answer_reappears(self):
        self.add("q1","两件怎么拍",0)
        self.add("a1","任选两件加入购物车一起下单",1,direction="outbound",target="u1")
        self.workflow.rebuild_cases(self.phase,17)
        candidate=self.workflow.list_faq_candidates()[0]
        with self.store.connect() as db:
            db.execute("UPDATE faq_candidates SET fingerprint=?,answer=? WHERE id=?",
                       ("edited-fingerprint","人工编辑后的答复",candidate["id"]))
        self.workflow.rebuild_cases(self.phase,17)
        candidates=self.workflow.list_faq_candidates()
        self.assertEqual(len(candidates),2)
        self.assertEqual(self.workflow.get_faq_candidate(candidate["id"])["answer"],"人工编辑后的答复")

    def test_conflict_filter_shows_label_only_disagreement_and_clears_after_rebuild(self):
        sequence=self.add("q1","还是58",0)
        self.add("q2","好的",1,primary="closing",customer="u2")
        self.store.save_review(sequence,"approved","qa",["qa","closing"],"历史意见",rule_version=16)
        self.workflow.rebuild_cases(self.phase,17)
        # Keep the primary label unchanged: the conflict concerns extra labels.
        page=self.workflow.processing_page("classification_conflict")
        self.assertEqual(page["total"],1)
        self.assertEqual(page["items"][0]["classification_conflict_count"],1)
        self.assertEqual(self.workflow.processing_summary()["classification_conflicts"],{"cases":1,"messages":1})
        self.assertEqual(self.workflow.processing_page("classification_conflict",search="不存在的客户")["total"],0)
        self.store.save_review(sequence,"approved","qa",["qa"],"本轮已复核",rule_version=17)
        self.workflow.rebuild_cases(self.phase,17)
        self.assertEqual(self.workflow.processing_page("classification_conflict")["total"],0)


if __name__=="__main__": unittest.main()
