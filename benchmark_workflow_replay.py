"""Reproducible, synthetic-only 2,000 message shadow-workflow benchmark.

Writes a temporary SQLite database below data/test-tmp and an aggregate report
below data/reports. No production DB, network API, or model is used.
"""
from __future__ import annotations

import argparse
import json
import random
import statistics
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from analysis_engine import RULE_VERSION, analyze
from ingestion_store import IngestionStore
from phase3 import Phase3
from workflow_store import WorkflowStore
from source_config import configured_source_account_id


ROOT = Path(__file__).resolve().parent
SOURCE_ACCOUNT_ID = configured_source_account_id()
BASE = datetime(2026, 10, 26, 8, 0, tzinfo=timezone(timedelta(hours=8)))


def percentiles(call, repeats: int = 40) -> dict:
    values = []
    for _ in range(repeats):
        started = time.perf_counter()
        call()
        values.append((time.perf_counter() - started) * 1000)
    values.sort()
    return {"p50_ms": round(statistics.median(values), 3),
            "p95_ms": round(values[min(len(values) - 1, int(len(values) * .95))], 3),
            "samples": len(values)}


def run(output: Path) -> dict:
    scratch = ROOT / "data" / "test-tmp"
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=scratch, prefix="replay-") as temporary:
        db_path = Path(temporary) / "synthetic.sqlite3"
        store = IngestionStore(db_path)
        workflow = WorkflowStore(store)
        phase3 = Phase3(store, ROOT / "does-not-exist.env")
        workflow.upsert_source_account({"id": "synthetic:second", "display_name": "模拟账号二",
                                        "rule_id": "synthetic-2", "robot_id": "robot-2", "enabled": False})
        accounts = (SOURCE_ACCOUNT_ID, "synthetic:second")
        category_texts = (
            "今天这个商品哪里拍？", "使用体验不错，给大家看看", "这个商品物流怎么查",
            "我想买之前那个商品", "价格为什么不一样？", "谢谢豆妈，辛苦了",
            "公众号文章分享给大家 https://outside.example/article", "订单截图已发",
        )
        inserted = duplicates = image_metadata = 0
        insert_started = time.perf_counter()
        sequence_to_analyze = []
        for customer_index in range(950):
            account = accounts[customer_index % 2]
            account_index = customer_index // 2
            group_id = "same-group-" + str(account_index % 5)
            sender_id = "same-customer-" + str(account_index)
            timestamp = BASE + timedelta(seconds=account_index * 95)
            if customer_index >= 850:
                texts = ("连续消息测试", "连续消息测试", "连续消息测试")
            else:
                texts = (category_texts[customer_index % len(category_texts)], "好的")
            for message_index, content in enumerate(texts):
                # Both accounts intentionally reuse the same source IDs while
                # still representing distinct source messages.
                source_id = "same-source-id-" + str(account_index) + "-" + str(message_index)
                image = (customer_index % 23 == 0 and message_index == 0) or (content == "订单截图已发")
                image_urls = ["https://synthetic.invalid/media/" + str(customer_index) + ".png"] if image else []
                raw = {"msgid": source_id, "content": content, "content_type": "image" if image else "text",
                       "_source_account_id": account,
                       "chatroom": {"id": group_id}, "fromUser": {"id": sender_id}}
                normalized = {"source_id": source_id, "source_account_id": account,
                              "group_id": group_id, "sender_id": sender_id,
                              "group": group_id, "sender": sender_id, "content": content,
                              "timestamp": (timestamp + timedelta(seconds=message_index * 15)).isoformat(),
                              "image_urls": image_urls}
                sequence, added = store.add_event(raw, {}, normalized, "synthetic-replay", display=True,
                    parse_state="parsed", direction="inbound", timestamp_valid=True)
                inserted += int(added)
                image_metadata += len(image_urls)
                sequence_to_analyze.append(sequence)
                if (customer_index * len(texts) + message_index) % 10 == 0:
                    _, replay_added = store.add_event(raw, {}, normalized, "synthetic-replay-redelivery", display=True,
                        parse_state="parsed", direction="inbound", timestamp_valid=True)
                    duplicates += int(not replay_added)
        ingestion_ms = (time.perf_counter() - insert_started) * 1000
        print(f"synthetic ingestion complete: {inserted} unique + {duplicates} duplicate deliveries", flush=True)

        analysis_started = time.perf_counter()
        for sequence in sequence_to_analyze:
            event = store.analysis_event(sequence)
            normalized = json.loads(event["normalized_json"] or "{}")
            normalized["sequence"] = sequence
            normalized["source_account_id"] = event["source_account_id"]
            normalized["conversation_anchor_sequence"] = store.conversation_anchor_sequence(event)
            raw = json.loads(event["raw_json"] or "{}").get("data") or {}
            context = store.analysis_context(event)
            media = store.media_for_event(sequence)
            result = analyze(raw, normalized, context, media, {"rules": {"conversationSeconds": 1800}})
            result["version"] = RULE_VERSION
            store.save_analysis(sequence, RULE_VERSION, result)
        analysis_ms = (time.perf_counter() - analysis_started) * 1000
        print(f"rule analysis complete: {len(sequence_to_analyze)} events", flush=True)

        replay_started = time.perf_counter()
        replay = workflow.rebuild_cases(phase3, RULE_VERSION,
            progress=lambda done,total: print(f"case replay: {done}/{total}",flush=True))
        replay_ms = (time.perf_counter() - replay_started) * 1000
        print("case replay complete", flush=True)
        with store.connect() as db:
            event_count = db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            analysis_count = db.execute("SELECT COUNT(*) FROM analysis_results WHERE rule_version=?", (RULE_VERSION,)).fetchone()[0]
            case_count = db.execute("SELECT COUNT(*) FROM support_cases WHERE state<>'merged'").fetchone()[0]
            suggestion_count = db.execute("SELECT COUNT(*) FROM process_suggestions").fetchone()[0]
            media_count = db.execute("SELECT COUNT(*) FROM media").fetchone()[0]
            media_actions = db.execute("SELECT COUNT(*) FROM process_suggestions WHERE payload_json LIKE '%media:%'").fetchone()[0]
            foreign_key_errors = len(db.execute("PRAGMA foreign_key_check").fetchall())
            linked_count = db.execute("SELECT COUNT(*) FROM case_messages").fetchone()[0]
            current_count = db.execute("SELECT COUNT(*) FROM process_suggestions WHERE is_current=1").fetchone()[0]
            same_id_cross_account = db.execute("""SELECT COUNT(DISTINCT source_account_id) FROM events
                WHERE source_id='same-source-id-0-0'""").fetchone()[0]

        suggestions = workflow.list_suggestions("todo", limit=1)
        first_case_id = suggestions[0]["case_id"] if suggestions else ""
        api_metrics = {
            "message_page": percentiles(lambda: store.page(after=0, limit=100)),
            "review_queue_page": percentiles(lambda: store.review_queue_page(RULE_VERSION, limit=50)),
            "suggestion_page": percentiles(lambda: workflow.list_suggestions("todo", limit=50)),
            "processing_queue": percentiles(lambda: workflow.processing_page("todo",limit=50)),
            "processing_summary": percentiles(lambda: workflow.processing_summary()),
        }
        if event_count!=2000 or linked_count!=event_count or current_count!=case_count or foreign_key_errors or same_id_cross_account!=2:
            raise AssertionError("Synthetic processing coverage, current-version uniqueness or account isolation failed")
        if first_case_id:
            api_metrics["case_detail"] = percentiles(lambda: workflow.case_detail(first_case_id))
        result = {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "environment": "isolated temporary SQLite; warm repeated local reads; no network/model calls",
            "synthetic_only": True,
            "message_counts": {"attempted": 2000, "stored_unique": event_count,
                               "newly_inserted": inserted, "duplicate_redeliveries": duplicates,
                               "analysis_results": analysis_count, "media_metadata": media_count,
                               "media_action_suggestions": media_actions},
            "workflow": {**replay, "active_cases": case_count, "stored_suggestions": suggestion_count,
                         "cross_account_same_message_id_separated": same_id_cross_account == 2,
                         "foreign_key_errors": foreign_key_errors},
            "timings": {"ingestion_ms": round(ingestion_ms, 3), "analysis_ms": round(analysis_ms, 3),
                        "case_and_suggestion_replay_ms": round(replay_ms, 3),
                        "api": api_metrics},
            "external_calls": {"deepseek": 0, "feishu": 0, "yunzhongke": 0, "outbound_messages": 0},
            "notes": ["Synthetic media records carry metadata only; no real image bytes were fetched.",
                      "This is a local warm-read benchmark, not source-to-browser real-time latency.",
                      "No business accuracy claim is made from synthetic labels."],
        }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "data" / "reports" / "workflow-replay-2026-10-26.json")
    args = parser.parse_args()
    report = run(args.output)
    print(json.dumps(report, ensure_ascii=False, indent=2))
