import tempfile
import unittest
from io import BytesIO
import json
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import server
from ingestion_store import IngestionStore


def raw_event(message_id, stamp, text="收到"):
    return {"msgid": message_id, "sourceType": "group", "sendTime": stamp,
            "content": text, "chatroom": {"id": "group-1", "displayName": "测试群"},
            "fromUser": {"id": "member-1", "nickname": "成员"}}


class DurableReceiverTests(unittest.TestCase):
    def test_history_coverage_uses_latest_robot_inventory_without_deleting_old_records(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            store=IngestionStore(Path(folder)/'inventory.sqlite3')
            store.set_bots([{'wxid':'old-robot'}])
            with store.connect() as db:
                db.execute("UPDATE source_bots SET checked_at='2020-01-01T00:00:00+00:00'")
            store.set_bots([{'wxid':'active-robot'}])
            start='2026-11-02T10:00:00+08:00'; end='2026-11-02T11:00:00+08:00'
            run=store.begin_history(start,end)
            store.end_history(run,'partial',1,1,0,0,['中断'])
            store.save_history_cursor('active-robot','2026-11-02',start,end,None,'complete',1,0)
            self.assertIsNone(store.oldest_unfinished_history())
            self.assertEqual(store.mark_recovered_history_runs(),1)
            with store.connect() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM source_bots').fetchone()[0],2)
                self.assertEqual(db.execute('SELECT state FROM history_runs WHERE id=?',(run,)).fetchone()[0],'recovered')
            self.assertEqual(store.summary()['bot_count'],1)

    def test_send_copy_direction_and_ignored_shape_are_recorded_without_content(self):
        with patch.object(server, 'status', {}), patch.object(server, 'add_message', return_value=True) as add:
            ignored={'type':'wxmsg_send_copy','data':{'content':'private message','to_wxid':'private-id'}}
            self.assertFalse(server.receive_source_event(ignored))
            self.assertEqual(server.status['ignored_send_copy_structure'], {'total':1,'with_msgid':0,'with_group':0,'with_reply_id':0,'with_recipient':1})
            self.assertNotIn('private', str(server.status))
            accepted={'type':'wxmsg_send_copy','data':{'msgid':'m1','sourceType':'group','content':'reply'}}
            self.assertTrue(server.receive_source_event(accepted))
            self.assertEqual(add.call_args.args[0]['terminalType'], 'wxmsg_send_copy')

    def test_history_replay_of_suppressed_event_does_not_interrupt_recovery(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            store=IngestionStore(Path(folder)/'suppressed.sqlite3')
            raw=raw_event('broadcast','2026-11-02 10:00:00','群发内容')
            seq,_=store.add_event(raw,{},server.normalize(raw),'websocket',display=True,
                parse_state='parsed',direction='inbound',timestamp_valid=True)
            with store.connect() as db:
                db.execute("UPDATE events SET display=0,parse_state='suppressed_duplicate_broadcast' WHERE sequence=?",(seq,))
            with patch.object(server,'STORE',store),patch.object(server,'sync_enabled',True):
                self.assertFalse(server.add_message(raw,{},'history'))
            with store.connect() as db:
                row=db.execute('SELECT display,parse_state FROM events WHERE sequence=?',(seq,)).fetchone()
                self.assertEqual((row['display'],row['parse_state']),(0,'suppressed_duplicate_broadcast'))

    def test_recalled_evidence_invalidates_only_automatic_review(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            store=IngestionStore(Path(folder)/"recall-review.sqlite3")
            question=raw_event("question","2026-11-01 10:00:00","怎么买")
            human_question=raw_event("human-question","2026-11-01 10:00:01","请问")
            reply=raw_event("reply","2026-11-01 10:01:00","链接在这里")
            def add(value, direction="inbound"):
                return store.add_event(value,{},server.normalize(value),"history",display=True,
                    parse_state="parsed",direction=direction,timestamp_valid=True)[0]
            q=add(question); h=add(human_question); r=add(reply,"outbound")
            def review(sequence,note):
                store.save_development_audit(sequence,1,predicted_primary="qa",predicted_labels=["qa"],
                    predicted_faq_id="",predicted_faqs=[],actual_primary="qa",actual_labels=["qa"],
                    faq_verdict="not_reviewed",expected_faq_id="",note=note)
            review(q,"系统按明确引用的真人回复确认分类；回复消息："+str(r))
            review(h,"人工确认")
            add({**reply,"is_chehui":1},"outbound")
            self.assertIsNone(store.development_audit(q,1))
            self.assertEqual(store.development_audit(h,1)["note"],"人工确认")
            with store.connect() as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM development_audit_invalidations").fetchone()[0],1)

    def test_recall_flag_survives_later_unmarked_history(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            store=IngestionStore(Path(folder)/"recall.sqlite3")
            raw=raw_event("recall-1","2026-11-01 10:00:00","你好")
            def add(value):
                return store.add_event(value,{},server.normalize(value),"history",display=True,
                    parse_state="parsed",direction="inbound",timestamp_valid=True)
            seq,_=add(raw)
            add({**raw,"is_chehui":1})
            add({**raw,"is_chehui":0})
            with store.connect() as db:
                row=db.execute("SELECT raw_json,normalized_json FROM events WHERE sequence=?",(seq,)).fetchone()
            self.assertEqual(json.loads(row["raw_json"])["data"]["is_chehui"],1)
            self.assertTrue(json.loads(row["normalized_json"])["revoked"])
            self.assertIsNotNone(store.get_meta("source_revoke_revision"))
            self.assertTrue(store.page()["messages"][0]["revoked"])
            self.assertTrue(store.page_before(seq+1)["messages"][0]["revoked"])
            self.assertTrue(store.review_queue_page(1)["messages"][0]["revoked"])

    def test_development_audit_endpoint_persists_and_explains_invalid_faq_verdicts(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            store = IngestionStore(Path(folder) / "audit-endpoint.sqlite3")
            raw = raw_event("dev-api-1", "2026-10-23 10:00:00", "牛奶保质期多久")
            sequence, _ = store.add_event(raw, {}, server.normalize(raw), "history", display=True,
                                          parse_state="parsed", direction="inbound", timestamp_valid=True)
            store.save_analysis(sequence, 15, {"primary": "qa", "labels": ["qa"], "reason": "提问"})
            store.save_workspace_config({}, [{"id": "faq-1", "question": "牛奶保质期多久",
                "keywords": "牛奶,保质期", "answer": "以商品详情为准。", "approved": True, "enabled": True}])

            def submit(**updates):
                body = {"sequence": sequence, "version": 15, "actual_primary": "qa", "actual_labels": ["qa"],
                        "faq_verdict": "correct", "expected_faq_id": "", "note": "接口保存检查"}
                body.update(updates)
                encoded = json.dumps(body).encode("utf-8")
                handler = object.__new__(server.Handler)
                handler.path = "/api/dev-audit"
                handler.client_address = ("127.0.0.1", 50000)
                handler.headers = {"Host": "127.0.0.1:8799", "Origin": "http://127.0.0.1:8799",
                                   "Content-Length": str(len(encoded))}
                handler.rfile = BytesIO(encoded)
                result = {}
                handler.send_json = lambda payload, code=200: result.update(payload=payload, code=code)
                handler.do_POST()
                return result

            with patch.object(server, "STORE", store), patch.object(server, "active_analysis_version", return_value=15):
                saved = submit()
                self.assertEqual(saved["code"], 200)
                self.assertTrue(saved["payload"]["ok"])
                self.assertEqual(store.development_audit_summary(15)["reviewed"], 1)
                self.assertEqual(store.development_audit_page(15, status="todo")["total"], 0)
                self.assertEqual(store.development_audit_page(15, status="audited")["total"], 1)
                rejected = submit(faq_verdict="not_needed")
                self.assertEqual(rejected["code"], 400)
                self.assertIn("机器已经引用 FAQ", rejected["payload"]["error"])
                stale = submit(version=14)
                self.assertEqual(stale["code"], 400)
                self.assertIn("规则版本已更新", stale["payload"]["error"])
                self.assertEqual(store.development_audit_page(15, status="audited")["messages"][0]["audit"]["revision"], 1)

    def test_development_audit_is_versioned_and_separate_from_operational_reviews(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            store = IngestionStore(Path(folder) / "development-audit.sqlite3")
            raw = raw_event("dev-audit-1", "2026-10-21 10:00:00", "这个要怎么拍")
            sequence, _ = store.add_event(raw, {}, server.normalize(raw), "history", display=True,
                                          parse_state="parsed", direction="inbound", timestamp_valid=True)
            store.save_analysis(sequence, 15, {"primary": "qa", "labels": ["qa"], "reason": "问题"})
            page = store.development_audit_page(15, status="todo")
            self.assertEqual(page["total"], 1)
            self.assertEqual(page["messages"][0]["analysis"]["primary"], "qa")
            audit = store.save_development_audit(sequence, 15, predicted_primary="qa", predicted_labels=["qa"],
                predicted_faq_id="faq-1", predicted_faqs=[{"id": "faq-1", "question": "怎么买"}],
                actual_primary="product_link", actual_labels=["product_link", "service"],
                faq_verdict="missed", expected_faq_id="faq-2", note="结合上下文")
            self.assertEqual(audit["actual_primary"], "product_link")
            self.assertEqual(set(audit["actual_labels"]), {"product_link", "service"})
            self.assertEqual(audit["predicted_faqs"][0]["id"], "faq-1")
            self.assertEqual(store.development_audit_page(15, status="todo")["total"], 0)
            audited = store.development_audit_page(15, status="audited")
            self.assertEqual(audited["messages"][0]["audit"]["faq_verdict"], "missed")
            summary = store.development_audit_summary(15)
            self.assertEqual(summary["reviewed"], 1)
            self.assertEqual(summary["category_correct"], 0)
            self.assertEqual(summary["faq_verdicts"], {"missed": 1})
            self.assertEqual(store.development_audit_summary(16)["reviewed"], 1)
            self.assertEqual(store.development_audit_summary(16)["current_version_reviewed"], 0)
            self.assertEqual(store.development_audit_summary(16)["carried_reviewed"], 1)
            self.assertEqual(store.development_audit_page(16, status="audited")["messages"][0]["audit"]["rule_version"], 15)
            with store.connect() as db:
                self.assertIsNone(db.execute("SELECT 1 FROM review_decisions WHERE event_sequence=?", (sequence,)).fetchone())
                self.assertIsNone(db.execute("SELECT 1 FROM review_log WHERE event_sequence=?", (sequence,)).fetchone())

    def test_pending_counts_include_messages_without_lazily_created_work_items(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            store = IngestionStore(Path(folder) / "pending.sqlite3")
            sequences = []
            for number in range(3):
                event = raw_event(f"pending-{number}", f"2026-10-21 10:00:0{number}", "请问怎么拍")
                sequence, _ = store.add_event(event, {}, server.normalize(event), "history", display=True,
                                              parse_state="parsed", direction="inbound", timestamp_valid=True)
                sequences.append(sequence)

            self.assertEqual(store.summary()["pending_review"], 3)
            with store.connect() as db:
                db.execute("""CREATE TABLE work_items (
                    event_sequence INTEGER PRIMARY KEY, status TEXT NOT NULL,
                    ai_state TEXT, ai_json TEXT)""")
                db.execute("INSERT INTO work_items VALUES (?,?,?,?)",
                           (sequences[0], "handled", "not_needed", None))

            self.assertEqual(store.summary()["pending_review"], 2)
            page = store.review_queue_page(1, work_status="pending", limit=10)
            self.assertEqual(page["total"], 2)
            self.assertEqual({item["sequence"] for item in page["messages"]}, set(sequences[1:]))

    def test_conversation_anchor_groups_rolling_customer_messages_and_resets_after_idle_gap(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            store = IngestionStore(Path(folder) / "conversation.sqlite3")
            sequences = []
            for number, stamp in enumerate(("10:00:00", "10:20:00", "10:40:00", "11:15:00")):
                event = raw_event(f"conversation-{number}", "2026-10-21 " + stamp, "继续")
                normalized = server.normalize(event)
                sequence, _ = store.add_event(event, {}, normalized, "history", display=True,
                                              parse_state="parsed", direction="inbound", timestamp_valid=True)
                sequences.append(sequence)
            anchors = [store.conversation_anchor_sequence(store.analysis_event(sequence), 1800)
                       for sequence in sequences]
            self.assertEqual(anchors, [sequences[0], sequences[0], sequences[0], sequences[3]])

    def test_sent_history_is_readable_without_entering_review_queue(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            store = IngestionStore(Path(folder) / "sent.sqlite3")
            store.add_event(raw_event("incoming", "2026-10-21 10:00:00", "请问怎么用"), {},
                            server.normalize(raw_event("incoming", "2026-10-21 10:00:00", "请问怎么用")),
                            "history", display=True, parse_state="parsed", direction="inbound", timestamp_valid=True)
            for number in range(3):
                sent = raw_event(f"sent-{number}", f"2026-10-21 10:00:0{number + 1}", f"已回复 {number}")
                sent.update({"terminalType": "wxmsg_send", "type": "text", "status": "succeed", "is_ai": False})
                store.add_event(sent, {}, None, "history", display=False,
                                parse_state="context_only", direction="outbound", timestamp_valid=True,
                                source_timestamp=server.message_timestamp(sent).isoformat(timespec="seconds"))
            with patch.object(server, "STORE", store):
                first = server.public_outbound_page(limit=2)
                second = server.public_outbound_page(before=first["next_before"], limit=2)
            self.assertEqual(first["total"], 3)
            self.assertEqual([item["source_id"] for item in first["messages"]], ["sent-2", "sent-1"])
            self.assertTrue(first["has_more"])
            self.assertEqual([item["source_id"] for item in second["messages"]], ["sent-0"])
            self.assertEqual(first["messages"][0]["send_status"], "succeed")
            self.assertEqual(store.summary()["message_count"], 1)
            self.assertEqual(len(store.page_before(999, 10)["messages"]), 1)

    def test_image_content_type_comes_from_bytes(self):
        self.assertEqual(server.detect_image_mime(b"GIF89a\x00\x00\x00\x00\x00\x00"), "image/gif")
        self.assertEqual(server.detect_image_mime(b"\xff\xd8\xff\xe0\x00\x10"), "image/jpeg")

    def test_restart_dedupe_and_growing_history(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            path = Path(folder) / "messages.sqlite3"
            first = IngestionStore(path)
            for number in range(1601):
                event = raw_event(f"id-{number}", f"2026-10-21 10:{number // 60:02d}:{number % 60:02d}")
                normalized = server.normalize(event)
                _, added = first.add_event(event, {}, normalized, "history", display=True,
                                           parse_state="parsed", direction="inbound", timestamp_valid=True)
                self.assertTrue(added)
            restarted = IngestionStore(path)
            self.assertEqual(restarted.summary()["message_count"], 1601)
            duplicate = raw_event("id-100", "2026-10-21 10:01:40")
            _, added = restarted.add_event(duplicate, {}, server.normalize(duplicate), "websocket",
                                           display=True, parse_state="parsed", direction="inbound", timestamp_valid=True)
            self.assertFalse(added)
            self.assertEqual(restarted.summary()["message_count"], 1601)
            page = restarted.page_before(999999, 100)
            self.assertEqual(len(page["messages"]), 100)
            self.assertTrue(page["has_more"])
            self.assertEqual(page["messages"][-1]["source_id"], "id-1600")
            same_text = raw_event("id-extra", "2026-10-21 11:00:00")
            _, added = restarted.add_event(same_text, {}, server.normalize(same_text), "websocket",
                                           display=True, parse_state="parsed", direction="inbound", timestamp_valid=True)
            self.assertTrue(added)

    def test_numeric_string_source_time_is_restored(self):
        self.assertTrue(server.valid_source_timestamp({"sendTime": "1761000000000"}))
        expected = server.message_timestamp({"sendTime": "1761000000000"}).isoformat(timespec="seconds")
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            path = Path(folder) / "time.sqlite3"
            store = IngestionStore(path)
            raw = raw_event("numeric-time", "1761000000000")
            wrong = server.normalize(raw)
            wrong["timestamp"] = "2026-10-21T16:00:00+08:00"
            store.add_event(raw, {}, wrong, "history", display=True, parse_state="parsed",
                            direction="inbound", timestamp_valid=False)
            reopened = IngestionStore(path)
            row = reopened.page_before(999, 1)["messages"][0]
            self.assertEqual(row["timestamp"], expected)

    def test_history_uses_create_time_cursor_and_records_partial_scan(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            fake_store = IngestionStore(Path(folder) / "history.sqlite3")
            calls = []
            first = [{"_id": "a", "create_time": "cursor-a", "data": raw_event("a", "2026-10-21 10:00:00")},
                     {"_id": "b", "create_time": "cursor-b", "data": raw_event("b", "2026-10-21 09:59:00")}]
            second = [{"_id": "c", "create_time": "cursor-c", "data": raw_event("c", "2026-10-21 09:58:00")}]

            def source(_session, path, data):
                if path.endswith("getZdRobot"):
                    return {"data": [{"wxid": "robot-1"}]}
                calls.append(data["page"])
                return {"data": {0: first, "cursor-b": second, "cursor-c": []}[data["page"]]}

            with patch.object(server, "STORE", fake_store), patch.object(server, "api_json", side_effect=source), \
                 patch.object(server.time, "sleep", return_value=None):
                start = datetime(2026, 10, 21, 0, 0, tzinfo=server.TZ)
                end = datetime(2026, 10, 21, 23, 59, tzinfo=server.TZ)
                result = server.bootstrap_history(object(), start, end, allow_when_paused=True)
            self.assertEqual(calls, [0, "cursor-b", "cursor-c"])
            self.assertEqual(result["added"], 3)
            self.assertEqual(result["state"], "complete")
            self.assertEqual(fake_store.summary()["message_count"], 3)

    def test_history_resumes_saved_cursor(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            path = Path(folder) / "resume.sqlite3"
            first_store = IngestionStore(path)
            first_store.save_history_cursor("robot-1", "2026-10-21", "2026-10-21T00:00:00+08:00",
                                            "2026-10-21T23:59:59+08:00", "cursor-b", "partial", 1, 20)
            first_store.save_history_cursor("robot-complete", "2026-10-21", "2026-10-21T00:00:00+08:00",
                                            "2026-10-21T23:59:59+08:00", None, "complete", 5, 70)
            restarted = IngestionStore(path)
            calls = []

            def source(_session, path, data):
                if path.endswith("getZdRobot"):
                    return {"data": [{"wxid": "robot-complete"}, {"wxid": "robot-1"}]}
                calls.append(data["page"])
                if data["page"] == "cursor-b":
                    return {"data": [{"create_time": "cursor-c", "data": raw_event("c", "2026-10-21 09:58:00")}]}
                return {"data": []}

            with patch.object(server, "STORE", restarted), patch.object(server, "api_json", side_effect=source), \
                 patch.object(server.time, "sleep", return_value=None):
                start = datetime(2026, 10, 21, 0, 0, tzinfo=server.TZ)
                end = datetime(2026, 10, 21, 23, 59, tzinfo=server.TZ)
                result = server.bootstrap_history(object(), start, end, allow_when_paused=True)
            self.assertEqual(calls, ["cursor-b", "cursor-c"])
            self.assertEqual(result["state"], "complete")
            self.assertEqual(restarted.summary()["unfinished_history_scans"], 0)


if __name__ == "__main__":
    unittest.main()
