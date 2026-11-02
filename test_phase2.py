import tempfile
import unittest
from datetime import timedelta, timezone
from pathlib import Path

from analysis_engine import RULE_VERSION, analyze
from ingestion_store import IngestionStore


TZ = timezone(timedelta(hours=8))


def message(text, *, group="g1", sender="u1", stamp="2026-10-21T10:00:00+08:00", image=False):
    raw = {"sourceType": "group", "content": text, "type": "image" if image else "text"}
    normalized = {"content": text, "group_id": group, "sender_id": sender, "group": group,
                  "sender": sender, "timestamp": stamp, "image_urls": ["https://oss.iyunzk.com/test.jpg"] if image else [],
                  "attachments": 1 if image else 0}
    return raw, normalized


class Phase2Tests(unittest.TestCase):
    def test_real_link_requests_and_product_package_scan_are_distinguished(self):
        for text in ('@豆妈\u2005鞋的链接呢', '我怎么没找到链接那'):
            raw, normalized = message(text)
            self.assertEqual(analyze(raw, normalized, [], [])['primary'], 'product_link', text)
        raw, normalized = message('嗯嗯好的蹲守')
        self.assertEqual(analyze(raw, normalized, [], [])['primary'], 'closing')
        raw, normalized = message('[图片消息]', image=True)
        media = [{'state':'stored', 'ocr_text':'扫码', 'image_understanding':{
            'confidence':.86, 'image_kind':'product_feedback', 'evidence':['商品实物包装'], 'needs_human':False}}]
        result=analyze(raw, normalized, [], media)
        self.assertEqual(result['primary'], 'feedback')
        self.assertTrue(result['uncertainty'])
        media[0]['ocr_text']='扫码加微信'
        self.assertEqual(analyze(raw, normalized, [], media)['primary'], 'warning')
        raw, normalized = message('请扫码看二维码')
        self.assertEqual(analyze(raw, normalized, [], [])['primary'], 'warning')

    def test_ingredient_question_is_not_a_product_request(self):
        for text in ('有额外加糖什么的吗', '有没有加糖？', '这款有添加防腐剂吗'):
            raw, normalized = message(text)
            self.assertEqual(analyze(raw, normalized, [], [])['primary'], 'qa', text)
        raw, normalized = message('@豆妈 有没有白糖')
        self.assertEqual(analyze(raw, normalized, [], [])['primary'], 'requirement')

    def test_size_complaint_is_service_but_purchase_hope_is_closing(self):
        raw, normalized = message("@豆妈 保暖套尺码不合适，偏小")
        self.assertEqual(analyze(raw, normalized, [], [])["primary"], "service")
        raw, normalized = message("巳拍，希望喜欢")
        self.assertEqual(analyze(raw, normalized, [], [])["primary"], "closing")

    def test_real_relist_purchase_and_named_thanks(self):
        for text,expected in [('豆妈豆妈，再整点湿纸巾活动呗，上次忙没囤上','requirement'),
                              ('拍啦','closing'),('已0元拍','closing'),('感谢豆妈，超给力','thanks')]:
            raw,normalized=message(text)
            self.assertEqual(analyze(raw,normalized,[],[])['primary'],expected,text)

    def test_human_reply_examples_distinguish_post_sale_and_buying_location(self):
        for text,expected in [('@豆妈 我买的毛衣怎么起毛球呢','service'),
                              ('上次买了2瓶水2瓶乳，商家帮我发错了','service'),
                              ('这个在哪儿买','product_link'),('这件毛衣会不会起球','qa')]:
            raw,normalized=message(text)
            self.assertEqual(analyze(raw,normalized,[],[])['primary'],expected,text)

    def test_service_issue_image_is_not_routed_to_feedback_gallery(self):
        raw, normalized = message("用了三天效果不错，但坏了想退款", image=True)
        result = analyze(raw, normalized, [], [{"state": "stored"}])
        self.assertEqual(result["primary"], "service")
        self.assertIn("service", result["labels"])
        self.assertNotIn("feedback", result["labels"])

    def test_unexplained_image_and_short_acknowledgement_need_review(self):
        raw, normalized = message("[图片消息]", image=True)
        result = analyze(raw, normalized, [], [{"state": "failed"}])
        self.assertEqual(result["primary"], "unknown")
        self.assertEqual(result["features"]["image_state"], "failed")
        raw, normalized = message("好")
        self.assertEqual(analyze(raw, normalized, [], [])["primary"], "closing")
        context = [{"sequence": 1, "group_id": "g1", "sender_id": "u2", "direction": "inbound",
                    "source_timestamp": "2026-10-21T09:59:50+08:00", "content": "请查收"}]
        self.assertEqual(analyze(raw, normalized, context, [])["primary"], "closing")

    def test_burst_uses_same_group_and_sender(self):
        raw, normalized = message("继续")
        context = [{"sequence": n, "group_id": "g1", "sender_id": "u1", "direction": "inbound",
                    "source_timestamp": f"2026-10-21T09:59:{50+n:02d}+08:00", "content": "继续"}
                   for n in range(7)]
        context.append({"sequence": 9, "group_id": "other", "sender_id": "u1", "direction": "inbound",
                        "source_timestamp": "2026-10-21T09:59:59+08:00", "content": "继续"})
        result = analyze(raw, normalized, context, [], {"rules": {"burstCount": 8, "burstSeconds": 30}})
        self.assertEqual(result["primary"], "burst")
        self.assertEqual(result["features"]["same_sender_window_count"], 8)

    def test_real_message_patterns_promotions_orders_and_followup_photo(self):
        raw, normalized = message("借宝地发一下，征婚咨询微信 13800138000")
        self.assertEqual(analyze(raw, normalized, [], [])["primary"], "warning")
        raw, normalized = message("下单就不是这个价，优惠券没了")
        self.assertEqual(analyze(raw, normalized, [], [])["primary"], "qa")
        for text in ("什么材质的", "穿起来扎不扎", "没券了", "下单就不是无个价"):
            raw, normalized = message(text)
            self.assertEqual(analyze(raw, normalized, [], [])["primary"], "qa", text)
        raw, normalized = message("[卡片链接消息]<a href='https://example.com/index?sid=1'>商品</a>")
        raw["type"] = "card_link"
        self.assertEqual(analyze(raw, normalized, [], [])['primary'], "warning")
        raw, normalized = message("[图片消息]", image=True)
        same_sender_followup = [{"sequence": 2, "group_id": "g1", "sender_id": "u1", "direction": "inbound",
                                 "source_timestamp": "2026-10-21T10:01:00+08:00", "content": "用了三天，效果不错"}]
        with_followup = analyze(raw, normalized, same_sender_followup, [{"state": "stored"}])
        self.assertEqual(with_followup["primary"], "feedback")
        self.assertIn(2, with_followup["context_sequences"])
        same_sender_followup[0]["sender_id"] = "someone_else"
        self.assertEqual(analyze(raw, normalized, same_sender_followup, [{"state": "stored"}])["primary"], "unknown")

        raw, normalized = message("用了三天效果不错")
        image_context = [{"sequence": 20, "group_id": "g1", "sender_id": "u1", "direction": "inbound",
                          "source_timestamp": "2026-10-21T10:01:00+08:00", "content": "[图片消息]", "has_image": True}]
        self.assertEqual(analyze(raw, normalized, image_context, [])["primary"], "feedback")

        raw, normalized = message("订单截图发一下")
        self.assertEqual(analyze(raw, normalized, image_context, [])["primary"], "qa")
        self.assertNotIn("order_image", analyze(raw, normalized, image_context, [])["labels"])

    def test_share_types_are_flagged_and_allowlisted_hosts_are_not(self):
        cases = [
            ("不支持显示的消息类型【小程序】", {"type": "mini_program"}, "mini"),
            ("https://mp.weixin.qq.com/s/abc", {"type": "text"}, "warning"),
            ("视频号分享", {"type": "text"}, "warning"),
            ("请扫码看二维码", {"type": "text"}, "warning"),
        ]
        for content, fields, expected in cases:
            raw, normalized = message(content)
            raw.update(fields)
            result = analyze(raw, normalized, [], [])
            self.assertEqual(result["primary"], expected, content)
        raw, normalized = message("https://shop.example.com/item/1")
        result = analyze(raw, normalized, [], [], {"allowedShareDomains": "example.com"})
        self.assertNotIn("warning", result["labels"])

    def test_product_service_and_daily_purchase_routes(self):
        raw, normalized = message("枕头在那想买枕头")
        result = analyze(raw, normalized, [], [])
        self.assertEqual(result["primary"], "requirement")
        self.assertTrue(result["features"]["is_product_request"])
        self.assertFalse(result["features"]["is_today_product_link"])
        self.assertEqual(result["assignee"], "aftercare")

        raw, normalized = message("找不到链接，今天的商品哪里拍")
        result = analyze(raw, normalized, [], [])
        self.assertEqual(result["primary"], "product_link")
        self.assertTrue(result["features"]["is_today_product_link"])

        raw, normalized = message("希望以后上这个款")
        self.assertEqual(analyze(raw, normalized, [], [])["primary"], "requirement")

        raw, normalized = message("@豆妈 羊毛衫几点上链接？")
        result = analyze(raw, normalized, [], [])
        self.assertEqual(result["primary"], "qa")
        self.assertEqual(result["assignee"], "aftercare")
        self.assertIn("requirement", result["labels"])

        raw, normalized = message("这款怎么拍，价格不对")
        result = analyze(raw, normalized, [], [])
        self.assertEqual(result["primary"], "qa")
        self.assertTrue(result["features"]["is_daily_purchase"])

        raw, normalized = message("这款风衣什么时候有链接")
        result = analyze(raw, normalized, [], [], {"soldLastWeekProducts": "风衣", "soldNextWeekProducts": ""})
        self.assertIn("requirement", result["labels"])
        self.assertEqual(result["assignee"], "aftercare")
        self.assertTrue(result["features"]["is_historical_activity"])

        raw, normalized = message("这款风衣链接在哪")
        result = analyze(raw, normalized, [], [], {"soldLastWeekProducts": "风衣", "soldNextWeekProducts": "风衣"})
        self.assertFalse(result["features"]["is_historical_activity"])

    def test_order_photo_and_same_customer_context_are_kept_together(self):
        raw, normalized = message("订单截图发一下", image=True)
        result = analyze(raw, normalized, [], [{"state": "stored"}])
        self.assertEqual(result["primary"], "order_image")
        self.assertTrue(result["features"]["is_order_image"])

    def test_explicit_order_ocr_overrides_nearby_price_issue_context(self):
        raw, normalized = message("[图片消息]", image=True, stamp="2026-10-21T13:48:16+08:00")
        context = [{"sequence": 332, "group_id": "g1", "sender_id": "u1", "direction": "inbound",
                    "source_timestamp": "2026-10-21T13:36:16+08:00", "content": "我咋一件58?"},
                   {"sequence": 334, "group_id": "g1", "sender_id": "u1", "direction": "inbound",
                    "source_timestamp": "2026-10-21T13:37:36+08:00", "content": "重新拍一下"}]
        result = analyze(raw, normalized, context, [{"state": "stored",
            "ocr_text": "淘宝 全部订单 待发货 订单信息 实付款￥58.8"}])
        self.assertEqual(result["primary"], "order_image")
        self.assertTrue(result["features"]["is_order_image"])
        self.assertEqual(result["features"]["image_usage"], "message_attachment")
        self.assertNotIn("qa", result["labels"])

        result = analyze(raw, normalized, context, [{"state": "stored", "ocr_text": "优惠券无法使用"}])
        self.assertEqual(result["primary"], "qa")
        self.assertEqual(result["features"]["image_usage"], "problem_evidence")

        raw, normalized = message("豆妈@豆妈 谢谢", stamp="2026-10-21T10:02:00+08:00")
        context = [
            {"sequence": 10, "group_id": "g1", "sender_id": "u1", "direction": "inbound",
             "source_timestamp": "2026-10-21T10:00:00+08:00", "content": "豆妈@豆妈 这个怎么拍？"},
            {"sequence": 11, "group_id": "g1", "sender_id": "staff", "direction": "outbound",
             "source_timestamp": "2026-10-21T10:01:00+08:00", "content": "商品链接在群公告"},
        ]
        normalized["sequence"] = 12
        result = analyze(raw, normalized, context, [])
        self.assertEqual(result["primary"], "thanks")
        self.assertIn(10, result["conversation_sequences"])
        self.assertTrue(result["features"]["strong_mention"])

        raw, normalized = message("豆妈@豆妈 这个怎么拍？")
        result = analyze(raw, normalized, [], [])
        self.assertEqual(result["primary"], "qa")
        self.assertTrue(result["features"]["human_answer_required"])
        for text in ("好的", "已拍", "已经下单", "好，已拍"):
            raw, normalized = message(text)
            self.assertEqual(analyze(raw, normalized, [], [])["primary"], "closing", text)

    def test_acknowledgement_closes_prior_question_without_inheriting_its_route(self):
        context = [{"sequence": 41, "group_id": "g1", "sender_id": "u1", "direction": "inbound",
                    "source_timestamp": "2026-10-21T10:00:00+08:00", "content": "这件怎么拍？"},
                   {"sequence": 42, "group_id": "g1", "sender_id": "staff", "direction": "outbound",
                    "source_timestamp": "2026-10-21T10:01:00+08:00", "content": "点群里的商品链接下单"}]
        raw, normalized = message("好的", stamp="2026-10-21T10:02:00+08:00")
        normalized["sequence"] = 43
        result = analyze(raw, normalized, context, [])
        self.assertEqual(result["primary"], "closing")
        self.assertEqual(result["labels"], ["closing"])
        self.assertIn(41, result["conversation_sequences"])

        raw, normalized = message("好的，这个链接在哪？", stamp="2026-10-21T10:02:00+08:00")
        result = analyze(raw, normalized, context, [])
        self.assertEqual(result["primary"], "product_link")

        mentioned_context = [{"sequence": 51, "group_id": "g1", "sender_id": "u1", "direction": "inbound",
                              "source_timestamp": "2026-10-21T10:00:00+08:00", "content": "@豆妈，这个怎么拍？"},
                             {"sequence": 52, "group_id": "g1", "sender_id": "staff", "direction": "outbound",
                              "source_timestamp": "2026-10-21T10:01:00+08:00", "content": "看群里的商品链接"}]
        raw, normalized = message("好的", stamp="2026-10-21T10:02:00+08:00")
        self.assertEqual(analyze(raw, normalized, mentioned_context, [])['primary'], "closing")
        raw, normalized = message("谢谢", stamp="2026-10-21T10:02:00+08:00")
        self.assertEqual(analyze(raw, normalized, mentioned_context, [])['primary'], "closing")

    def test_stable_conversation_anchor_keeps_rolling_messages_in_one_session(self):
        anchors = []
        for seq, minute in ((50, 0), (51, 20), (52, 40)):
            raw, normalized = message("继续", stamp=f"2026-10-21T10:{minute:02d}:00+08:00")
            normalized["sequence"] = seq
            normalized["conversation_anchor_sequence"] = 50
            anchors.append(analyze(raw, normalized, [], [])["conversation_id"])
        self.assertEqual(len(set(anchors)), 1)

    def test_context_window_can_be_tuned_without_borrowing_old_questions(self):
        raw, normalized = message("好", stamp="2026-10-21T10:00:00+08:00")
        context = [{"sequence": 30, "group_id": "g1", "sender_id": "u1", "direction": "inbound",
                    "source_timestamp": "2026-10-21T09:50:00+08:00", "content": "怎么拍？"}]
        result = analyze(raw, normalized, context, [], {"rules": {"conversationSeconds": 300}})
        self.assertEqual(result["primary"], "closing")
        self.assertEqual(result["conversation_sequences"], [])

    def test_local_image_ocr_signals_are_classified_without_model_calls(self):
        raw, normalized = message("[图片消息]", image=True)
        media = [{"state": "stored", "ocr_text": "扫码添加微信，关注公众号"}]
        result = analyze(raw, normalized, [], media)
        self.assertEqual(result["primary"], "warning")
        self.assertTrue(result["features"]["is_image_promotion"])
        self.assertTrue(result["features"]["has_image_ocr"])

        media = [{"state": "stored", "ocr_text": "我的订单 订单编号 123456"}]
        result = analyze(raw, normalized, [], media)
        self.assertEqual(result["primary"], "order_image")

        media = [{"state": "stored", "ocr_text": "买家秀 晒单"}]
        result = analyze(raw, normalized, [], media)
        self.assertEqual(result["primary"], "feedback")

    def test_review_survives_restart_and_reanalysis(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            path = Path(folder) / "phase2.sqlite3"
            store = IngestionStore(path)
            raw, normalized = message("请问如何使用")
            raw.update({"msgid": "source-1", "sendTime": normalized["timestamp"],
                        "chatroom": {"id": "g1"}, "fromUser": {"id": "u1"}})
            sequence, _ = store.add_event(raw, {}, normalized, "history", display=True, parse_state="parsed",
                                          direction="inbound", timestamp_valid=True)
            store.save_analysis(sequence, RULE_VERSION, analyze(raw, normalized, [], []))
            store.save_review(sequence, "approved", "service", ["service", "qa"], "人工确认是售后")
            restarted = IngestionStore(path)
            restarted.save_analysis(sequence, RULE_VERSION, analyze(raw, normalized, [], []))
            analysis, reviews = restarted.analysis_map([sequence], RULE_VERSION)
            self.assertEqual(analysis[sequence]["primary"], "qa")
            self.assertEqual(reviews[sequence]["primary_label"], "service")
            self.assertEqual(reviews[sequence]["labels"], ["service", "qa"])
            self.assertEqual(len(restarted.review_history()), 1)
            filtered = restarted.review_queue_page(RULE_VERSION, kind="service", status="approved", limit=10)
            self.assertEqual(filtered["total"], 1)
            self.assertEqual(filtered["messages"][0]["sequence"], sequence)
            self.assertEqual(restarted.review_queue_page(RULE_VERSION, kind="qa")["total"], 0)
            self.assertEqual(restarted.analysis_stats(RULE_VERSION)["by_category"]["service"], 1)
            report = restarted.evaluation_report(RULE_VERSION, ["qa", "service"])
            self.assertEqual(report["reviewed_sample"], 1)
            self.assertEqual(report["labels"]["qa"]["tp"], 1)
            self.assertEqual(report["labels"]["service"]["fn"], 1)

    def test_rule_version_switch_preserves_review_and_config(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            store = IngestionStore(Path(folder) / "versions.sqlite3")
            store.save_workspace_config({"rules": {"burstCount": 8}, "aftercareUrl": "example"}, [])
            store.ensure_rule_version(RULE_VERSION, {"rules": {"burstCount": 8}})
            newer = store.create_rule_version({"rules": {"burstCount": 10}}, RULE_VERSION)
            self.assertEqual(newer, RULE_VERSION + 1)
            self.assertEqual(store.active_rule_version(RULE_VERSION), newer)
            self.assertEqual(store.activate_rule_version(RULE_VERSION, RULE_VERSION), RULE_VERSION)
            config = store.get_workspace_config()
            self.assertEqual(config["settings"]["rules"]["burstCount"], 8)
            self.assertEqual(config["settings"]["aftercareUrl"], "example")


if __name__ == "__main__":
    unittest.main()
