import base64
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from analysis_engine import analyze
from image_understanding import ImageInterpreter, validate_result
from ingestion_store import IngestionStore
from phase3 import Phase3, recommendation
from test_phase2 import message


class InformationProcessingTests(unittest.TestCase):
    def test_short_followups_preserve_only_the_question_topic(self):
        context = [{"sequence": 3, "group_id": "g1", "sender_id": "u1", "direction": "inbound",
                    "source_timestamp": "2026-10-21T09:45:00+08:00", "content": "为什么下单是58？"}]
        for text in ("还是58", "我看了 没有"):
            raw, normalized = message(text)
            result = analyze(raw, normalized, context, [])
            self.assertEqual(result["primary"], "qa", text)
            self.assertEqual(result["features"]["followup_question_sequence"], 3)
        context.append({"sequence": 4, "group_id": "g1", "sender_id": "u1", "direction": "inbound",
                        "source_timestamp": "2026-10-21T09:50:00+08:00", "content": "好的[微笑]"})
        raw, normalized = message("还是58")
        self.assertIsNone(analyze(raw, normalized, context, [])["features"]["followup_question_sequence"])

    def test_local_ocr_cache_uses_content_hash_without_loading_a_model(self):
        import server
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent / "data") as folder:
            store = IngestionStore(Path(folder)/"test.sqlite3")
            with store.connect() as db:
                db.execute("INSERT INTO image_text_cache VALUES('hash-a','rapidocr-v1','订单详情',?)", (store.now(),))
            with patch.object(server, "STORE", store), patch.object(server, "_ocr_cache", {}), patch.object(server, "_ocr_checked", True), patch.object(server, "_ocr_engine", None):
                self.assertEqual(server.local_ocr_text({"state": "stored", "file_name": "a.jpg", "sha256": "hash-a"}), "订单详情")
                self.assertEqual(server.local_ocr_text({"state": "stored", "file_name": "a.jpg", "sha256": "hash-b"}), "")

    def test_reply_reference_resolves_within_the_same_group_only(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent / "data") as folder:
            store = IngestionStore(Path(folder)/"test.sqlite3")
            raw = {"id": "internal-a", "msgid": "original-a", "chatroom": {"id": "g1"}}
            seq, _ = store.add_event(raw, {}, {"content": "还有吗", "group_id": "g1"}, "test", display=True,
                                     parse_state="parsed", direction="inbound", timestamp_valid=False)
            outgoing, _ = store.add_event({"msgid": "reply-a", "replyMessageId": "internal-a", "chatroom": {"id": "g1"}}, {}, None,
                "test", display=False, parse_state="context_only", direction="outbound", timestamp_valid=False)
            self.assertEqual(store.explicit_reply_target(outgoing)["sequence"], seq)
            other_group, _ = store.add_event({"msgid": "reply-b", "replyMessageId": "internal-a", "chatroom": {"id": "g2"}}, {}, None,
                "test", display=False, parse_state="context_only", direction="outbound", timestamp_valid=False)
            self.assertIsNone(store.explicit_reply_target(other_group))

    def test_purchase_assistance_does_not_inherit_receipt_or_after_sale(self):
        context = [{"sequence": 1, "group_id": "g1", "sender_id": "u1", "direction": "inbound",
                    "source_timestamp": "2026-10-21T09:59:40+08:00", "content": "订单截图", "has_image": True,
                    "ocr_text": "订单详情 已付款"}]
        for text in ("现在可以退掉从新选吗", "我把那单退了直接再拍就行吧", "拍一款发同样的吗"):
            raw, normalized = message(text)
            result = analyze(raw, normalized, context, [])
            self.assertEqual(result["primary"], "qa", text)
            self.assertNotIn("service", result["labels"], text)
            self.assertNotIn("order_image", result["labels"], text)
        raw, normalized = message("想要下单，还能买吗")
        self.assertEqual(analyze(raw, normalized, [], [])["primary"], "qa")
        raw, normalized = message("@豆妈 退款没到账怎么办")
        self.assertEqual(analyze(raw, normalized, [], [])["primary"], "service")
        raw, normalized = message("@豆妈 还没发货，我想换一件可以吗")
        self.assertEqual(analyze(raw, normalized, [], [])["primary"], "qa")

    def test_no_reply_turn_keeps_the_related_receipt_task(self):
        raw, normalized = message("买了两件")
        context = [{"sequence": 5, "group_id": "g1", "sender_id": "u1", "direction": "inbound",
                    "source_timestamp": "2026-10-21T09:59:40+08:00", "content": "[图片消息]", "has_image": True,
                    "ocr_text": "订单详情 已付款"}]
        analysis = analyze(raw, normalized, context, [])
        self.assertEqual(analysis["primary"], "closing")
        self.assertFalse(analysis["response_needed"])
        rec = recommendation("closing", normalized, analysis, {}, [], [{"id": 7, "event_sequence": 5, "state": "stored"}], context)
        self.assertFalse(rec["response_needed"])
        self.assertEqual(rec["attachment_tasks"][0]["action_key"], "media:7:order_image")

    def test_requirement_and_service_owner_are_independent(self):
        for text in ("@豆妈 想要洗衣液", "@豆妈 旅行箱收纳袋找一找", "@豆妈 之前你们卖的深睡被没有了吗"):
            raw, normalized = message(text)
            result = analyze(raw, normalized, [], [])
            self.assertEqual(result["primary"], "requirement", text)
            self.assertEqual(result["assignee"], "aftercare")
            self.assertNotIn("service", result["labels"])
            rec = recommendation(result["primary"], normalized, result,
                {"aftercareUrl": "http://example.com/customer", "demandTableUrl": "https://example.com/demand",
                 "customerServiceTemplate": "一对一客服：{{售后链接}}"}, [], [], [])
            self.assertEqual(rec["assignee"], "aftercare")
            self.assertIn("http://example.com/customer", rec["draft"])
            self.assertEqual({action["route"] for action in rec["actions"]}, {"requirement", "aftercare_contact"})

    def test_weak_promotional_word_is_not_an_ad_but_contact_ad_remains_visible(self):
        raw, normalized = message("[图片消息]", image=True)
        result = analyze(raw, normalized, [], [{"state": "stored", "ocr_text": "淘宝订单详情 实付款 免费领取"}])
        self.assertEqual(result["primary"], "order_image")
        self.assertNotIn("warning", result["labels"])
        result = analyze(raw, normalized, [], [{"state": "stored", "ocr_text": "免费领取"}])
        self.assertEqual(result["primary"], "unknown")
        result = analyze(raw, normalized, [], [{"state": "stored", "ocr_text": "扫码添加微信 招商加盟"}])
        self.assertEqual(result["primary"], "warning")

    def test_feedback_caption_keeps_image_while_plain_acknowledgement_closes(self):
        context = [{"sequence": 5, "group_id": "g1", "sender_id": "u1", "direction": "inbound",
                    "source_timestamp": "2026-10-21T09:59:40+08:00", "content": "[图片消息]", "has_image": True}]
        raw, normalized = message("收到了益生菌")
        self.assertEqual(analyze(raw, normalized, context, [])["primary"], "feedback")
        for text in ("买啦", "买了两件", "已排", "好的，谢谢"):
            raw, normalized = message(text)
            result = analyze(raw, normalized, [], [])
            self.assertEqual(result["primary"], "closing", text)

    def test_visual_sticker_cannot_close_a_product_question(self):
        visual = {"image_kind": "emoji", "confidence": .95, "evidence": ["感谢表情包"], "needs_human": False}
        raw, normalized = message("[图片消息]", image=True)
        self.assertEqual(analyze(raw, normalized, [], [{"state": "stored", "image_understanding": visual}])["primary"], "closing")
        visual["needs_human"] = True
        self.assertNotEqual(analyze(raw, normalized, [], [{"state": "stored", "image_understanding": visual}])["primary"], "closing")
        visual["needs_human"] = False
        raw, normalized = message("这件还有吗", image=True)
        self.assertNotEqual(analyze(raw, normalized, [], [{"state": "stored", "image_understanding": visual}])["primary"], "closing")
        visual["confidence"] = .4
        raw, normalized = message("[图片消息]", image=True)
        self.assertEqual(analyze(raw, normalized, [], [{"state": "stored", "image_understanding": visual}])["primary"], "unknown")

    def test_only_explicit_reply_reference_marks_the_response_state(self):
        raw, normalized = message("上链接")
        raw["id"] = "internal-user-message"
        reply = {"sequence": 8, "group_id": "g1", "sender_id": "staff", "direction": "outbound",
                 "source_timestamp": "2026-10-21T10:01:00+08:00", "content": "今天的商品：https://shop.example.com"}
        self.assertEqual(analyze(raw, normalized, [reply], [])["response_state"], "open")
        reply["reply_to_source_id"] = "internal-user-message"
        result = analyze(raw, normalized, [reply], [])
        self.assertEqual(result["primary"], "product_link")
        self.assertEqual(result["response_state"], "replied")
        rec = recommendation("product_link", normalized, result, {}, [], [], [reply])
        self.assertEqual(rec["draft"], "")
        self.assertEqual(rec["answered_by_sequences"], [8])
        raw, normalized = message("这件还有吗")
        photo = {"sequence": 6, "group_id": "g1", "sender_id": "u1", "direction": "inbound",
                 "source_timestamp": "2026-10-21T09:59:50+08:00", "content": "[图片消息]", "has_image": True,
                 "source_reference_ids": ["photo-ref"]}
        reply["reply_to_source_id"] = "photo-ref"
        result = analyze(raw, normalized, [photo, reply], [])
        self.assertEqual(result["response_state"], "replied")
        self.assertEqual(result["features"]["confirmed_reply_sequences"], [8])

    def test_new_rules_compare_historical_labels_without_claiming_new_human_review(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent / "data") as folder:
            store = IngestionStore(Path(folder)/"test.sqlite3")
            seq, _ = store.add_event({"msgid": "one"}, {}, {"content": "想要洗衣液"}, "test", display=True,
                                     parse_state="parsed", direction="inbound", timestamp_valid=False)
            store.save_development_audit(seq, 15, predicted_primary="qa", predicted_labels=["qa"],
                predicted_faq_id="", predicted_faqs=[], actual_primary="requirement", actual_labels=["requirement"],
                faq_verdict="not_reviewed", expected_faq_id="", note="产品需求")
            store.save_analysis(seq, 16, {"primary": "requirement", "labels": ["requirement"]})
            summary = store.development_audit_summary(16)
            self.assertEqual(summary["category_accuracy"], 1)
            self.assertEqual(summary["current_version_reviewed"], 0)
            self.assertEqual(summary["carried_reviewed"], 1)
            self.assertEqual(store.development_audit(seq, 16)["predicted_primary"], "qa")
            review = store.save_review(seq, "approved", "qa", ["qa"], "保留已有人工结论")
            item = Phase3(store, Path(folder)/".env").ensure(seq, {"primary": "requirement", "labels": ["requirement"], "version": 16}, review)
            self.assertEqual(item["route"], "qa")
            self.assertTrue(item["recommendation"]["human_rule_disagreement"])
            self.assertEqual(item["recommendation"]["machine_intent"], "requirement")
            self.assertEqual(store.development_audit_page(16, only_differences=True)["total"], 0)
            store.save_analysis(seq, 16, {"primary": "qa", "labels": ["qa"]})
            self.assertEqual(store.development_audit_page(16, status="audited", only_differences=True)["total"], 1)

    def test_image_api_receives_real_bytes_and_reuses_cached_result(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent / "data") as folder:
            root = Path(folder)
            store = IngestionStore(root/"test.sqlite3")
            data = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jVhsAAAAASUVORK5CYII=")
            (root/"image.png").write_bytes(data)
            result = {"image_kind": "order_screenshot", "confidence": .95, "evidence": ["订单页面"], "needs_human": False}
            response = Mock()
            response.json.return_value = {"choices": [{"message": {"content": json.dumps(result)}}]}
            interpreter = ImageInterpreter(store, root/".env", root)
            media = {"sha256": hashlib.sha256(data).hexdigest(), "file_name": "image.png", "state": "stored"}
            with patch("image_understanding.local_secrets", return_value={"deepseek_api_key": "test-only"}), patch("image_understanding.requests.post", return_value=response) as post:
                self.assertTrue(interpreter.interpret(media, ["订单图"]))
                self.assertFalse(interpreter.interpret(media, ["订单图"]))
                self.assertEqual(post.call_count, 1)
                parts = post.call_args.kwargs["json"]["messages"][1]["content"]
                self.assertEqual(parts[1]["type"], "image_url")
                self.assertTrue(parts[1]["image_url"]["url"].endswith(base64.b64encode(data).decode()))
            with self.assertRaises(ValueError):
                validate_result({**result, "confidence": float("nan")})


if __name__ == "__main__":
    unittest.main()
