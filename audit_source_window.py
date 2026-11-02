"""Reconcile one Yunzhongke rule/time window, then locally ingest missing rows.

This is a read-only call to the source API plus an idempotent local database
backfill. It never sends group messages or writes to Feishu.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import datetime
from pathlib import Path

import server
from ingestion_store import source_is_revoked


def event_identity(raw: dict, outer: dict) -> str:
    # Match the ingestion store's primary dedupe identity. Local records may
    # accumulate additional source_reference_ids after replies are resolved;
    # sorting the whole alias set would make the identity drift over time.
    primary = raw.get("msgid") or raw.get("id") or outer.get("_id") or outer.get("id")
    if primary is not None and str(primary):
        return "source:" + str(primary)
    material = {
        "robot": raw.get("roboot_wxid") or raw.get("robot_wxid") or outer.get("robot_wxid"),
        "group": raw.get("chatroom") or raw.get("chatroom_wxid"),
        "sender": raw.get("fromUser") or raw.get("from_wxid"),
        "time": raw.get("sendTime") or raw.get("create_time") or outer.get("create_time"),
        "type": raw.get("type") or raw.get("content_type"),
        "content": raw.get("content"),
    }
    digest = hashlib.sha256(json.dumps(material, ensure_ascii=False, sort_keys=True, default=str).encode()).hexdigest()
    return "payload:" + digest


def local_identities(start: str, end: str, source_account_id: str) -> set[str]:
    found: set[str] = set()
    with server.STORE.connect() as db:
        rows = db.execute("""SELECT source_id,raw_json FROM events
            WHERE source_account_id=? AND source_timestamp>=? AND source_timestamp<=?""",
            (source_account_id, start, end)).fetchall()
    for row in rows:
        payload = json.loads(row["raw_json"] or "{}")
        raw = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        outer = payload.get("outer") if isinstance(payload.get("outer"), dict) else {}
        # Use the same canonical identity as the source comparison. Adding
        # source_id as a second alias inflated the local total for every row.
        found.add(event_identity(raw, outer))
    return found


def local_revoked_identities(start: str, end: str, source_account_id: str) -> set[str]:
    found=set()
    with server.STORE.connect() as db:
        rows=db.execute("""SELECT raw_json FROM events WHERE source_account_id=?
            AND source_timestamp>=? AND source_timestamp<=?""",(source_account_id,start,end)).fetchall()
    for row in rows:
        payload=json.loads(row["raw_json"] or "{}")
        raw=payload.get("data") if isinstance(payload.get("data"),dict) else {}
        outer=payload.get("outer") if isinstance(payload.get("outer"),dict) else {}
        if source_is_revoked(raw,outer): found.add(event_identity(raw,outer))
    return found


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="起始时间，例如 2026-10-30T09:51:21+08:00")
    parser.add_argument("--end", default="", help="结束时间；省略时使用当前时间")
    parser.add_argument("--output", default="", help="输出目录；省略时创建时间戳目录")
    args = parser.parse_args()
    start = datetime.fromisoformat(args.start).astimezone(server.TZ)
    end = datetime.fromisoformat(args.end).astimezone(server.TZ) if args.end else datetime.now(server.TZ)
    if start >= end:
        parser.error("--start 必须早于 --end")
    start_text, end_text = start.isoformat(timespec="seconds"), end.isoformat(timespec="seconds")
    stamp = datetime.now(server.TZ).strftime("%Y%m%d-%H%M%S")
    output = Path(args.output) if args.output else Path("data/reports") / ("source-integrity-" + stamp)
    output.mkdir(parents=True, exist_ok=True)
    report = {"rule_id": server.RULE_ID, "source_account_id": server.SOURCE_ACCOUNT_ID,
              "start": start_text, "end": end_text, "state": "running", "robots": [],
              "source_rows": 0, "cloud_unique_messages": 0, "local_unique_messages_before": 0,
              "missing_before": [], "local_only_before": [], "backfilled": 0,
              "missing_after": [], "local_only_after": [], "errors": [],
              "inbound_backfilled": 0, "outbound_backfilled": 0,
              "revocation_observations": 0, "revocations_updated": 0}
    cloud: dict[str, tuple[dict, dict]] = {}
    session = None
    try:
        session = server.make_session()
        payload = server.api_json(session, "/wx/GroupXiaoXiZhongKong/getZdRobot",
            {"rule_id": server.RULE_ID, "page": 1, "size": 999, "nickname": "",
             "group_keyword": "", "group_search_type": ""})
        bots = server.data_of(payload)
        if not bots:
            raise RuntimeError("机器人列表为空，不能确认来源覆盖")
        # This may audit an older rule without changing the live rule's robot inventory.
        day = start.date()
        while day <= end.date():
            low = max(start, datetime.combine(day, datetime.min.time(), server.TZ))
            high = min(end, datetime.combine(day, datetime.max.time().replace(microsecond=0), server.TZ))
            for bot in bots:
                robot_id = str(bot.get("wxid") or bot.get("id") or "")
                item = {"robot_id": robot_id, "day": day.isoformat(), "pages": 0, "rows": 0, "state": "running"}
                report["robots"].append(item)
                if not robot_id:
                    item.update(state="partial", error="机器人缺少稳定 ID")
                    report["errors"].append(item["error"])
                    continue
                cursor = 0
                signatures: set[str] = set()
                try:
                    for _ in range(server.MAX_HISTORY_PAGES):
                        response = server.api_json(session, "/wx/GroupXiaoXiZhongKong/getMsg4Review",
                            {"rule_id": server.RULE_ID, "robot_wxid": robot_id,
                             "sub_uid": str(bot.get("sub_uid") or ""),
                             "start_time": low.strftime("%Y-%m-%d %H:%M:%S"),
                             "end_time": high.strftime("%Y-%m-%d %H:%M:%S"), "page": cursor})
                        if not isinstance(response.get("data"), (list, dict)):
                            raise RuntimeError("历史响应结构变化：" + server.response_shape(response))
                        batch = server.data_of(response)
                        item["pages"] += 1
                        if not batch:
                            item["state"] = "complete"
                            break
                        signature = hashlib.sha256(json.dumps(batch, ensure_ascii=False, sort_keys=True, default=str).encode()).hexdigest()
                        if signature in signatures:
                            raise RuntimeError("历史分页重复，覆盖范围不确定")
                        signatures.add(signature)
                        for outer in batch:
                            raw = outer.get("data") if isinstance(outer.get("data"), dict) else outer
                            if not isinstance(raw, dict):
                                continue
                            identity = event_identity(raw, outer)
                            cloud.setdefault(identity, (dict(raw), dict(outer)))
                            item["rows"] += 1
                        next_cursor = batch[-1].get("create_time") if isinstance(batch[-1], dict) else None
                        if next_cursor is None or str(next_cursor) == str(cursor):
                            raise RuntimeError("缺少有效的历史分页游标")
                        cursor = next_cursor
                        time.sleep(2)
                    else:
                        raise RuntimeError("达到历史分页安全上限")
                except Exception as exc:
                    item.update(state="partial", error=server.safe_upstream_text(str(exc), 200))
                    report["errors"].append(item["robot_id"] + "：" + item["error"])
                time.sleep(2)
            day = day.fromordinal(day.toordinal() + 1)

        before = local_identities(start_text, end_text, server.SOURCE_ACCOUNT_ID)
        cloud_ids = set(cloud)
        report["source_rows"] = sum(item["rows"] for item in report["robots"])
        report["cloud_unique_messages"] = len(cloud_ids)
        report["local_unique_messages_before"] = len(before)
        report["missing_before"] = sorted(cloud_ids - before)
        report["local_only_before"] = sorted(before - cloud_ids)
        # Existing IDs may acquire a later recall flag even though they are not
        # missing. Re-ingest only explicitly recalled rows and preserve content.
        for identity in sorted(cloud_ids & before):
            raw, outer = cloud[identity]
            if not source_is_revoked(raw, outer) or not identity.startswith("source:"):
                continue
            report["revocation_observations"] += 1
            with server.STORE.connect() as db:
                previous=db.execute("SELECT raw_json FROM events WHERE source_account_id=? AND source_id=?",
                    (server.SOURCE_ACCOUNT_ID,identity[len("source:"):])).fetchone()
            if not previous: continue
            previous_payload=json.loads(previous["raw_json"] or "{}")
            was_revoked=source_is_revoked(previous_payload.get("data"),previous_payload.get("outer"))
            if str(outer.get("type") or "") == "wxmsg_send": raw["terminalType"]="wxmsg_send"
            server.add_message(raw,outer,"history_audit",allow_when_paused=True)
            if not was_revoked: report["revocations_updated"] += 1
        for identity in report["missing_before"]:
            raw, outer = cloud[identity]
            if str(outer.get("type") or "") == "wxmsg_send":
                raw["terminalType"] = "wxmsg_send"
            if server.add_message(raw, outer, "history_audit", allow_when_paused=True):
                report["backfilled"] += 1
                outbound = "send" in str(raw.get("terminalType") or "").lower() or raw.get("is_self") is True
                report["outbound_backfilled" if outbound else "inbound_backfilled"] += 1
        after = local_identities(start_text, end_text, server.SOURCE_ACCOUNT_ID)
        report["missing_after"] = sorted(cloud_ids - after)
        report["local_only_after"] = sorted(after - cloud_ids)
        revoked_local=local_revoked_identities(start_text,end_text,server.SOURCE_ACCOUNT_ID)
        report["local_only_revoked_count"]=len(set(report["local_only_after"]) & revoked_local)
        report["local_only_unexplained_count"]=len(set(report["local_only_after"]) - revoked_local)
        complete = all(item["state"] == "complete" for item in report["robots"])
        report["state"] = "complete" if complete and not report["missing_after"] and not report["local_only_unexplained_count"] else "partial"
        report["local_unique_messages_after"] = len(after)
        report["local_only_after_count"] = len(report["local_only_after"])
    except Exception as exc:
        report["state"] = "partial"
        report["errors"].append(server.safe_upstream_text(str(exc), 200))
    finally:
        if session is not None:
            session.close()
        (output / "完整性.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({key: value for key, value in report.items() if key not in ("robots", "missing_before", "missing_after", "local_only_before", "local_only_after")}, ensure_ascii=False))
        print("REPORT_DIR", str(output.resolve()))
    return 0 if report["state"] == "complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())
