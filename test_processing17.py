"""Business rules clarified by the second classification review."""
import unittest
import tempfile
from pathlib import Path

from analysis_engine import RULE_VERSION, analyze
from image_understanding import image_is_ambiguous
from ingestion_store import IngestionStore
from phase3 import Phase3, recommendation
from test_phase2 import message


def context(sequence, text, at, **fields):
    return {"sequence": sequence, "group_id": "g1", "sender_id": "u1", "direction": "inbound",
            "source_timestamp": "2026-10-21T" + at + "+08:00", "content": text, **fields}


class SecondReviewRulesTests(unittest.TestCase):
    def test_legacy_manual_classification_is_held_until_reconfirmed_for_current_rules(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent / "data") as folder:
            store = IngestionStore(Path(folder)/"test.sqlite3")
            raw, normalized = message("这个怎么拍")
            raw["msgid"] = "manual-recheck"
            seq, _ = store.add_event(raw, {}, normalized, "test", display=True,
                parse_state="parsed", direction="inbound", timestamp_valid=True)
            analysis = {"version": RULE_VERSION, "primary": "qa", "labels": ["qa"], "features": {}}
            store.save_analysis(seq, RULE_VERSION, analysis)
            review = store.save_review(seq, "approved", "closing", ["closing"], "旧人工结论")
            phase = Phase3(store, Path(folder)/".env")
            item = phase.ensure(seq, analysis, review)
            self.assertEqual(item["route"], "closing")
            self.assertTrue(item["recommendation"]["requires_classification_review"])
            with self.assertRaisesRegex(ValueError, "重新复核分类"):
                phase.save_decision(seq, "approved", "")
            _, preserved = store.analysis_map([seq], RULE_VERSION)
            self.assertEqual(preserved[seq]["primary_label"], "closing")
            review = store.save_review(seq, "approved", "closing", ["closing"], "本轮确认无需回复", rule_version=RULE_VERSION)
            current = phase.ensure(seq, analysis, review)
            self.assertFalse(current["recommendation"]["requires_classification_review"])
            self.assertEqual(phase.save_decision(seq, "approved", "")["status"], "approved")

    def test_order_image_with_closing_tag_is_semantically_equivalent(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent / "data") as folder:
            store = IngestionStore(Path(folder)/"test.sqlite3")
            raw, normalized = message("[图片消息]", image=True)
            raw["msgid"] = "order-image-closing-equivalence"
            seq, _ = store.add_event(raw, {}, normalized, "test", display=True,
                parse_state="parsed", direction="inbound", timestamp_valid=True)
            analysis = {"version": RULE_VERSION, "primary": "order_image", "labels": ["order_image"], "features": {}}
            store.save_analysis(seq, RULE_VERSION, analysis)
            review = store.save_review(seq, "approved", "order_image", ["order_image", "closing"],
                "订单截图无需回复", rule_version=17)
            phase = Phase3(store, Path(folder)/".env")
            item = phase.ensure(seq, analysis, review)
            self.assertEqual(item["route"], "order_image")
            self.assertFalse(item["recommendation"]["human_rule_disagreement"])
            self.assertFalse(item["recommendation"]["requires_classification_review"])
            self.assertFalse(item["recommendation"]["response_needed"])

    def test_text_selecting_a_product_cannot_become_an_order_image(self):
        raw, normalized = message("我想选一个开衫，一个半高领")
        photo = context(3, "[图片消息]", "10:00:20", has_image=True, ocr_text="订单详情 已付款")
        result = analyze(raw, normalized, [photo], [])
        self.assertEqual(result["primary"], "qa")
        self.assertNotIn("order_image", result["labels"])
        self.assertTrue(result["features"]["has_related_image"])

    def test_order_screenshot_after_price_question_keeps_order_image_route(self):
        raw, normalized = message("[图片消息]", image=True)
        history = [context(1, "我咋一件58？", "09:47:00"), context(2, "重新拍一下", "09:48:00")]
        picture = {"id": 7, "state": "stored", "ocr_text": "订单详情 实付款58元"}
        result = analyze(raw, normalized, history, [picture])
        self.assertEqual(result["primary"], "order_image")
        self.assertIn("order_image", result["labels"])
        self.assertEqual(result["features"]["image_issue_sequence"], 1)
        self.assertFalse(image_is_ambiguous(result))
        rec = recommendation("order_image", normalized, result, {}, [], [picture], history)
        self.assertEqual([task["route"] for task in rec["attachment_tasks"]], ["order_image"])
        self.assertEqual(rec["evidence_media_ids"], [])
        self.assertEqual(rec["action"], "人工核对订单截图后登记到订单图表")

    def test_price_question_after_a_receipt_does_not_rewrite_the_original_upload(self):
        raw, normalized = message("[图片消息]", image=True)
        following = [context(4, "为什么我是8.9？", "10:00:05")]
        result = analyze(raw, normalized, following, [{"state": "stored", "ocr_text": "订单详情 已付款"}])
        self.assertEqual(result["primary"], "order_image")
        self.assertIn("qa", result["labels"])

    def test_closed_or_other_customers_price_issue_does_not_take_over_a_new_receipt(self):
        raw, normalized = message("[图片消息]", image=True)
        photo = [{"state": "stored", "ocr_text": "订单详情 已付款"}]
        history = [context(1, "怎么还是58？", "09:45:00"), context(2, "好了", "09:50:00")]
        self.assertEqual(analyze(raw, normalized, history, photo)["primary"], "order_image")
        other = [context(1, "怎么还是58？", "09:59:00", sender_id="another")]
        self.assertEqual(analyze(raw, normalized, other, photo)["primary"], "order_image")

    def test_source_sticker_signal_can_resolve_uncertain_vision_but_cannot_hide_ads(self):
        raw, normalized = message("[图片消息]", image=True)
        raw["sub_type"] = "sendBiaoQing"
        photo = [{"state": "stored", "image_understanding": {"image_kind": "uncertain", "confidence": .35,
                  "needs_human": True, "evidence": ["像素风角色，无法确认"]}}]
        result = analyze(raw, normalized, [], photo)
        self.assertEqual(result["primary"], "closing")
        self.assertFalse(image_is_ambiguous(result))
        self.assertEqual(analyze(raw, normalized, [], [{"state": "stored", "ocr_text": "扫码添加微信 招商加盟"}])["primary"], "warning")
        self.assertNotEqual(analyze(raw, normalized, [], [{"state": "failed"}])["primary"], "closing")
        del raw["sub_type"]
        self.assertEqual(analyze(raw, normalized, [], photo)["primary"], "unknown")

    def test_reordering_acknowledgement_closes_only_a_turn_without_a_new_question(self):
        history = [context(1, "怎么下单是58？", "09:50:00")]
        for text in ("重新拍一下", "那我重新下单", "我再拍一单"):
            raw, normalized = message(text)
            self.assertEqual(analyze(raw, normalized, history, [])["primary"], "closing", text)
        raw, normalized = message("我可以重新拍吗")
        self.assertEqual(analyze(raw, normalized, history, [])["primary"], "qa")


if __name__ == "__main__":
    unittest.main()
