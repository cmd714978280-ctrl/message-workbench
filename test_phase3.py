"""Phase-three review behavior against a temporary database; no external API calls."""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ingestion_store import IngestionStore
from phase3 import Phase3, faq_candidates, minimize_for_ai, recommendation, observed_group_service_contact


class Phase3Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path(__file__).parent / "data")
        root = Path(self.temp.name)
        assert root.resolve().is_relative_to(Path(__file__).parent.resolve())
        self.store = IngestionStore(root / "messages.sqlite3")
        self.phase3 = Phase3(self.store, root / ".env")

    def tearDown(self):
        self.temp.cleanup()

    def test_warning_draft_does_not_claim_unverified_removal(self):
        faq={'id':'kick','question':'广告踢','keywords':'广告','answer':'发广告者已踢，请勿点击',
             'enabled':True,'approved':True}
        analysis={'primary':'warning','labels':['warning'],'version':30,'evidence':['广告']}
        result=recommendation('warning',{'content':'群外广告','sender_id':'u','group_id':'g'},analysis,{},[faq],[],[])
        self.assertNotIn('已踢',result['draft'])
        self.assertTrue(result['blockers'])

    def test_unchanged_machine_disagreement_does_not_repeat_human_review(self):
        seq=self.event('我的衣服尺码太小')
        self.store.save_analysis(seq,30,{'primary':'qa','labels':['qa'],'version':30})
        review={'status':'approved','primary_label':'service','labels':['service'],'rule_version':30}
        item=self.phase3.ensure(seq,{'primary':'qa','labels':['qa'],'version':31},review)
        self.assertFalse(item['recommendation']['requires_classification_review'])
        item=self.phase3.ensure(seq,{'primary':'feedback','labels':['feedback'],'version':31},review)
        self.assertTrue(item['recommendation']['requires_classification_review'])

    def test_observed_service_link_requires_prior_explicit_reply_in_same_group_and_day(self):
        target=self.event('订单出了问题')
        account=self.store.source_account_id
        raw={'msgid':'r1','replyMessageId':'订单出了问题','content':'请联系售后小助理 http://example.test/help',
             'chatroom':{'id':'g1'},'fromUser':{'id':'operator'}}
        self.store.add_event(raw,{},None,'test',display=False,parse_state='context_only',direction='outbound',
            timestamp_valid=True,source_timestamp='2026-10-22T10:01:00+08:00')
        result=observed_group_service_contact(self.store,account,'g1','2026-10-22T10:05:00+08:00')
        self.assertEqual(result['state'],'resolved')
        self.assertEqual(result['url'],'http://example.test/help')
        self.assertEqual(observed_group_service_contact(self.store,account,'g1','2026-10-22T09:59:00+08:00')['state'],'missing')
        self.assertEqual(observed_group_service_contact(self.store,account,'g2','2026-10-22T10:05:00+08:00')['state'],'missing')
        self.assertEqual(observed_group_service_contact(self.store,account,'g1','2026-10-23T10:05:00+08:00')['state'],'missing')

    def test_same_group_observed_link_is_suggested_with_traceable_source(self):
        from workflow_store import WorkflowStore
        WorkflowStore(self.store)
        self.store.save_workspace_config({'useFaqServiceContacts':True},[])
        self.event('先前售后问题')
        raw={'msgid':'r1','replyMessageId':'先前售后问题','content':'请联系售后小助理 http://example.test/help',
             'chatroom':{'id':'g1'},'fromUser':{'id':'operator'}}
        self.store.add_event(raw,{},None,'test',display=False,parse_state='context_only',direction='outbound',
            timestamp_valid=True,source_timestamp='2026-10-22T10:01:00+08:00')
        incoming={'msgid':'new','content':'需要售后','chatroom':{'id':'g1'},'fromUser':{'id':'u2'}}
        normalized={'id':'new','content':'需要售后','group_id':'g1','sender_id':'u2','group':'测试群',
                    'sender':'成员二','timestamp':'2026-10-22T10:05:00+08:00'}
        seq,_=self.store.add_event(incoming,{},normalized,'test',display=True,parse_state='parsed',direction='inbound',timestamp_valid=True)
        item=self.phase3.ensure(seq,{'primary':'service','labels':['service'],'version':32})
        self.assertIn('http://example.test/help',item['recommendation']['draft'])
        self.assertEqual(item['recommendation']['service_contact_source']['state'],'observed_group_reply')

    def event(self, content: str):
        raw = {"msgid": content, "content": content, "chatroom": {"id": "g1"}, "fromUser": {"id": "u1"}}
        normalized = {"id": content, "content": content, "group_id": "g1", "sender_id": "u1",
                      "group": "测试群", "sender": "测试成员", "timestamp": "2026-10-22T10:00:00+08:00"}
        sequence, _ = self.store.add_event(raw, {}, normalized, "test", display=True, timestamp_valid=True,
                                           parse_state="parsed", direction="inbound")
        return sequence

    def test_qa_uses_approved_faq_and_is_idempotent(self):
        faq = {"id": "faq-1", "question": "牛奶保质期多久", "keywords": "保质期,牛奶", "answer": "该批次保质期请以商品详情页为准。",
               "enabled": True, "approved": True, "version": 1}
        self.store.save_workspace_config({}, [faq])
        sequence = self.event("牛奶保质期多久")
        analysis = {"primary": "qa", "labels": ["qa"], "version": 6, "evidence": ["提问"]}
        first = self.phase3.ensure(sequence, analysis)
        second = self.phase3.ensure(sequence, analysis)
        self.assertEqual(first["recommendation"]["draft"], faq["answer"])
        self.assertEqual(first["revision"], second["revision"])
        self.assertEqual(self.phase3.stats()["total"], 1)
        saved = self.phase3.save_decision(sequence, "approved", faq["answer"], "已核对")
        self.assertEqual(saved["status"], "approved")
        self.assertEqual(len(self.phase3.history()), 1)

    def test_missing_aftercare_link_blocks_approval_then_marks_stale(self):
        self.store.save_workspace_config({"aftercareTemplate": "联系：{{售后链接}}"}, [])
        sequence = self.event("我要退款")
        analysis = {"primary": "service", "labels": ["service"], "version": 6, "evidence": ["退款"]}
        item = self.phase3.ensure(sequence, analysis)
        self.assertIn("尚未配置有效的售后链接", item["recommendation"]["blockers"])
        with self.assertRaises(ValueError):
            self.phase3.save_decision(sequence, "approved", "联系售后")
        self.phase3.save_decision(sequence, "needs_human", "", "待补链接")
        self.store.save_workspace_config({"aftercareTemplate": "联系：{{售后链接}}", "aftercareUrl": "https://example.com/help"}, [])
        updated = self.phase3.ensure(sequence, analysis)
        self.assertTrue(updated["stale"])
        self.assertEqual(updated["status"], "needs_human")

    def test_unapproved_imported_faq_cannot_authorize_answer(self):
        self.store.save_workspace_config({}, [{"id": "source-1", "question": "怎么下单", "answer": "在链接中下单",
                                               "enabled": True, "approved": False}])
        sequence = self.event("怎么下单")
        item = self.phase3.ensure(sequence, {"primary": "qa", "labels": ["qa"], "version": 6})
        self.assertEqual(item["recommendation"]["faq"], [])
        self.assertEqual(item["recommendation"]["draft"], "")
        self.assertTrue(item["recommendation"]["blockers"])
        with self.assertRaises(ValueError):
            self.phase3.save_decision(sequence, "approved", "在链接中下单")

    def test_qa_uses_approved_match_when_unapproved_candidate_scores_higher(self):
        sequence = self.event("牛奶保质期多久")
        unapproved = {"id": "pending", "question": "牛奶保质期多久", "answer": "未审核答案",
                      "enabled": False, "approved": False}
        approved = {"id": "approved", "question": "牛奶保质期", "answer": "请以商品详情页标注为准。",
                    "enabled": True, "approved": True}
        self.store.save_workspace_config({}, [unapproved, approved])
        analysis = {"primary": "qa", "labels": ["qa"], "version": 7, "evidence": ["包含提问"]}
        item = self.phase3.ensure(sequence, analysis)
        self.assertEqual([x["id"] for x in item["recommendation"]["faq"]], ["approved"])
        self.assertEqual(item["recommendation"]["draft"], approved["answer"])
        self.assertIn("已审核启用 FAQ：牛奶保质期", " ".join(item["recommendation"]["evidence"]))

    def test_faq_match_handles_paraphrased_questions(self):
        faqs = [
            {"id": "video", "question": "看教程视频", "keywords": "教程视频", "answer": "查看教程入口", "enabled": False},
            {"id": "purchase", "question": "怎么拍", "keywords": "不会拍", "answer": "按链接步骤下单", "enabled": False},
        ]
        video = faq_candidates("视频在哪儿", faqs)
        purchase = faq_candidates("两件如何拍", faqs)
        self.assertEqual(video[0]["id"], "video")
        self.assertEqual(purchase[0]["id"], "purchase")
        self.assertFalse(video[0]["enabled"])

    def test_faq_match_ignores_generic_shared_phrases(self):
        faqs = [
            {"question": "今天有没有好物", "answer": "请留意群内商品", "enabled": False},
            {"question": "会不会发货？什么时候发货？", "answer": "订单按下单顺序发出", "enabled": False},
        ]
        self.assertEqual(faq_candidates("有没有带帽子的", faqs), [])
        self.assertEqual(faq_candidates("这件会不会有色差", faqs), [])

    def test_mini_program_message_shows_related_faq_without_using_unapproved_answer(self):
        answer = "速度撤回，群内禁止发此类消息。"
        self.store.save_workspace_config({}, [{"id": "mini-faq", "question": "发小程序", "keywords": "发小程序",
                                               "answer": answer, "enabled": False, "approved": False}])
        sequence = self.event("不支持显示的消息类型【小程序】")
        analysis = {"primary": "mini", "labels": ["mini"], "version": 9,
                    "evidence": ["来源标记为小程序"], "reason": "来源标记为小程序"}
        item = self.phase3.ensure(sequence, analysis)
        self.assertEqual(item["recommendation"]["faq"], [])
        self.assertNotEqual(item["recommendation"]["draft"], answer)

        ad_faq = {"id": "ad-faq", "question": "广告踢", "keywords": "小程序",
                  "answer": "广告专用话术", "enabled": True, "approved": True}
        item = recommendation("mini", {"content": "不支持显示的消息类型【小程序】"}, analysis, {},
                              [self.store.get_workspace_config()["faqs"][0], ad_faq], [], [])
        self.assertEqual(item["draft"], "")
        self.assertTrue(any("小程序专用" in blocker for blocker in item["blockers"]))

    def test_new_routes_make_contextual_manual_recommendations(self):
        self.store.save_workspace_config({
            "aftercareUrl": "https://example.com/service",
            "previousActivityTemplate": "请联系售后询问之前活动链接：{{售后链接}}",
            "todayProductCopy": "今日商品：https://example.com/today",
            "galleryUrl": "https://example.feishu.cn/base/app?table=tblGallery",
        }, [])

        sequence = self.event("这款怎么拍，价格不对")
        daily = self.phase3.ensure(sequence, {"primary": "qa", "labels": ["qa"], "version": 11,
                                                "features": {"is_daily_purchase": True}})
        self.assertEqual(daily["recommendation"]["action"], "日常拍单疑问，人工答复")
        self.assertEqual(daily["recommendation"]["draft"], "")
        self.assertEqual(daily["recommendation"]["blockers"], [])

        sequence = self.event("风衣之前活动的链接")
        historic = self.phase3.ensure(sequence, {"primary": "service", "labels": ["service"], "version": 11,
                                                   "features": {"is_historical_activity": True}})
        self.assertEqual(historic["recommendation"]["draft"], "请联系售后询问之前活动链接：https://example.com/service")

        sequence = self.event("希望上这个款")
        demand = self.phase3.ensure(sequence, {"primary": "requirement", "labels": ["requirement"], "version": 11})
        self.assertIn("尚未配置有效的产品需求表地址", demand["recommendation"]["blockers"])

        sequence = self.event("找不到链接，今天的商品哪里拍")
        product_link = self.phase3.ensure(sequence, {"primary": "product_link", "labels": ["product_link"], "version": 11})
        self.assertEqual(product_link["recommendation"]["draft"], "今日商品：https://example.com/today")

    def test_warning_uses_approved_fixed_faq_and_context_includes_group_reply(self):
        answer = "请撤回小程序消息，群内禁止发送此类内容。"
        faq = {"id": "mini-faq", "question": "发小程序", "keywords": "小程序", "answer": answer,
               "enabled": True, "approved": True}
        normalized = {"content": "不支持显示的消息类型【小程序】", "sender_id": "u1", "group_id": "g1",
                      "timestamp": "2026-10-22T10:00:00+08:00"}
        analysis = {"primary": "mini", "labels": ["mini"], "version": 11,
                    "evidence": ["来源标记为小程序"]}
        item = recommendation("mini", normalized, analysis, {}, [faq], [], [])
        self.assertEqual(item["draft"], answer)
        self.assertFalse(item["sendable"])

        ad_answer = "群外链接请撤回。"
        ad_faq = {"id": "ad-faq", "question": "广告踢", "keywords": "群外链接,广告",
                  "answer": ad_answer, "enabled": True, "approved": True}
        unapproved_ad = {"id": "unapproved-ad", "question": "捣乱、发广告", "keywords": "免费领取",
                         "answer": "未审核话术", "enabled": False, "approved": False}
        normalized["content"] = "https://external.example/product"
        analysis = {"primary": "warning", "labels": ["warning"], "version": 12,
                    "evidence": ["包含群外链接", "图片文字:免费领取"]}
        item = recommendation("warning", normalized, analysis, {}, [unapproved_ad, ad_faq], [], [])
        self.assertEqual(item["draft"], ad_answer)

        normalized["content"] = "谢谢"
        analysis = {"primary": "thanks", "labels": ["thanks"], "version": 11}
        context = [{"sequence": 5, "sender_id": "staff", "direction": "outbound",
                    "source_timestamp": "2026-10-22T09:59:00+08:00", "content": "链接在群公告"},
                   {"sequence": 4, "sender_id": "u1", "direction": "inbound",
                    "source_timestamp": "2026-10-22T09:58:00+08:00", "content": "豆媽@豆媽，怎么拍？"}]
        item = recommendation("thanks", normalized, analysis, {}, [], [], context)
        self.assertIn("群内回复：链接在群公告", item.get("context_text", ""))

    def test_ai_calls_are_hard_disabled(self):
        sequence = self.event("怎么下单")
        item = self.phase3.ensure(sequence, {"primary": "qa", "labels": ["qa"], "version": 9})
        self.assertEqual(item["ai_state"], "disabled")
        with self.assertRaisesRegex(ValueError, "已关闭"):
            self.phase3.retry_ai(sequence)
        from unittest.mock import patch
        with patch("phase3.requests.post") as post:
            self.assertEqual(self.phase3.ai_batch(), 0)
            post.assert_not_called()

    def test_import_preserves_local_edits_and_refreshes_faq_candidates(self):
        sequence = self.event("怎么下单")
        analysis = {"primary": "qa", "labels": ["qa"], "version": 6}
        first = self.phase3.ensure(sequence, analysis)
        source = [{"group": "答疑", "n": "#1", "title": "怎么下单", "answer": "从页面购买"},
                  {"group": "答疑", "n": "#2", "title": "图片", "answer": "", "media": "image"}]
        result = self.phase3.import_quick_replies(source)
        self.assertEqual(result["faq_candidates_added"], 1)
        self.assertEqual(self.phase3.quick_reply_status()["source_entries"], 2)
        faq = self.store.get_workspace_config()["faqs"][0]
        self.assertFalse(faq["enabled"])
        self.assertFalse(faq["approved"])
        faq["answer"] = "本地审核后的内容"
        faq["enabled"] = True
        faq["approved"] = True
        self.store.save_workspace_config({}, [faq])
        changed_source = [{**source[0], "answer": "云中客新内容"}, source[1]]
        second = self.phase3.import_quick_replies(changed_source)
        self.assertEqual(second["source_changes"], 1)
        self.assertEqual(second["faq_candidates_added"], 0)
        self.assertEqual(self.store.get_workspace_config()["faqs"][0]["answer"], "本地审核后的内容")
        updated = self.phase3.ensure(sequence, analysis)
        self.assertGreater(updated["revision"], first["revision"])
        self.assertEqual(updated["recommendation"]["faq"][0]["answer"], "本地审核后的内容")
        self.assertEqual(updated["ai_state"], "disabled")

    def test_ai_disagreement_requires_human_classification_review(self):
        sequence = self.event("已拍")
        self.phase3.ensure(sequence, {"primary": "ordinary", "labels": ["ordinary"], "version": 6})
        with self.store.write_lock, self.store.connect() as db:
            db.execute("UPDATE work_items SET ai_state='done',ai_json=? WHERE event_sequence=?",
                       (self.store.dumps({"route": "service", "labels": ["service"], "draft": "请直接下单"}), sequence))
        with self.assertRaisesRegex(ValueError, "先人工确认分类"):
            self.phase3.save_decision(sequence, "approved", "")
        self.assertEqual(self.phase3.get(sequence)["ai"]["draft"], "")
        self.store.save_review(sequence, "approved", "ordinary", ["ordinary"], "确认仅为状态说明")
        approved = self.phase3.save_decision(sequence, "approved", "")
        self.assertEqual(approved["status"], "approved")

    def test_ai_input_masks_contact_details(self):
        minimized = minimize_for_ai("电话13812345678，订单123456789012345，链接https://example.com/private?id=123")
        self.assertIn("[手机号]", minimized)
        self.assertIn("[编号]", minimized)
        self.assertIn("[链接:example.com]", minimized)
        self.assertNotIn("13812345678", minimized)
        self.assertNotIn("/private", minimized)


if __name__ == "__main__":
    unittest.main()
