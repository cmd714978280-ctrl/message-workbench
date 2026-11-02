"""Durable local record of source events, media and history coverage."""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlsplit, parse_qsl, urlencode, urlunsplit
from source_config import configured_source_account_id


def source_is_revoked(raw, outer=None):
    """Only an explicit provider flag is evidence of a recalled message."""
    def marked(value):
        return value is True or str(value).strip().casefold() in {"1", "true", "yes"}
    return any(marked(obj.get("is_chehui")) for obj in (raw, outer or {}) if isinstance(obj, dict))


class IngestionStore:
    def __init__(self, path: Path, source_account_id: str | None = None):
        self.path = path
        self.source_account_id = str(source_account_id or configured_source_account_id()).strip()[:160]
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.write_lock = threading.RLock()
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    dedupe_key TEXT NOT NULL UNIQUE,
                    source_id TEXT NOT NULL DEFAULT '',
                    source_account_id TEXT NOT NULL DEFAULT 'yunzhongke:primary',
                    robot_id TEXT NOT NULL DEFAULT '',
                    group_id TEXT NOT NULL DEFAULT '',
                    sender_id TEXT NOT NULL DEFAULT '',
                    direction TEXT NOT NULL DEFAULT 'inbound',
                    event_type TEXT NOT NULL DEFAULT '',
                    source_timestamp TEXT,
                    timestamp_valid INTEGER NOT NULL DEFAULT 0,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    first_source TEXT NOT NULL,
                    last_source TEXT NOT NULL,
                    seen_count INTEGER NOT NULL DEFAULT 1,
                    display INTEGER NOT NULL DEFAULT 0,
                    parse_state TEXT NOT NULL,
                    raw_json TEXT NOT NULL,
                    normalized_json TEXT
                );
                CREATE INDEX IF NOT EXISTS events_display_sequence ON events(display, sequence);
                CREATE INDEX IF NOT EXISTS events_source_timestamp ON events(source_timestamp);
                CREATE INDEX IF NOT EXISTS events_source_id ON events(source_id);
                CREATE INDEX IF NOT EXISTS events_group_time ON events(group_id,source_timestamp);
                CREATE INDEX IF NOT EXISTS events_group_sender_time ON events(group_id,sender_id,source_timestamp);
                CREATE TABLE IF NOT EXISTS media (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_sequence INTEGER NOT NULL REFERENCES events(sequence),
                    url TEXT NOT NULL,
                    url_hash TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'pending',
                    file_name TEXT,
                    sha256 TEXT,
                    byte_count INTEGER,
                    mime TEXT,
                    error TEXT,
                    attempted_at TEXT,
                    UNIQUE(event_sequence, url_hash)
                );
                CREATE INDEX IF NOT EXISTS media_state ON media(state);
                CREATE TABLE IF NOT EXISTS history_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    start_at TEXT NOT NULL,
                    end_at TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    state TEXT NOT NULL,
                    bot_count INTEGER NOT NULL DEFAULT 0,
                    pages INTEGER NOT NULL DEFAULT 0,
                    rows_seen INTEGER NOT NULL DEFAULT 0,
                    added INTEGER NOT NULL DEFAULT 0,
                    errors_json TEXT NOT NULL DEFAULT '[]'
                );
                CREATE TABLE IF NOT EXISTS source_bots (
                    robot_id TEXT PRIMARY KEY,
                    detail_json TEXT NOT NULL,
                    checked_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS history_progress (
                    robot_id TEXT NOT NULL,
                    day TEXT NOT NULL,
                    start_at TEXT NOT NULL,
                    end_at TEXT NOT NULL,
                    cursor TEXT,
                    state TEXT NOT NULL,
                    pages INTEGER NOT NULL DEFAULT 0,
                    rows_seen INTEGER NOT NULL DEFAULT 0,
                    error TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(robot_id,day)
                );
                CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS rule_versions (
                    version INTEGER PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    settings_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS analysis_results (
                    event_sequence INTEGER NOT NULL REFERENCES events(sequence),
                    rule_version INTEGER NOT NULL,
                    result_json TEXT NOT NULL,
                    analyzed_at TEXT NOT NULL,
                    processing_ms INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(event_sequence,rule_version)
                );
                CREATE INDEX IF NOT EXISTS analysis_version ON analysis_results(rule_version,event_sequence);
                CREATE TABLE IF NOT EXISTS review_decisions (
                    event_sequence INTEGER PRIMARY KEY REFERENCES events(sequence),
                    status TEXT NOT NULL,
                    primary_label TEXT,
                    labels_json TEXT,
                    reason TEXT NOT NULL DEFAULT '',
                    reviewer TEXT NOT NULL DEFAULT '本机审核员',
                    reviewed_at TEXT NOT NULL,
                    revision INTEGER NOT NULL DEFAULT 1
                );
                CREATE TABLE IF NOT EXISTS review_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_sequence INTEGER NOT NULL REFERENCES events(sequence),
                    previous_json TEXT,
                    decision_json TEXT NOT NULL,
                    changed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS workspace_config (
                    id INTEGER PRIMARY KEY CHECK(id=1),
                    settings_json TEXT NOT NULL,
                    faqs_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS client_state_backups (
                    digest TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    imported_at TEXT NOT NULL,
                    audit_count INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS development_audits (
                    event_sequence INTEGER NOT NULL REFERENCES events(sequence),
                    rule_version INTEGER NOT NULL,
                    predicted_primary TEXT NOT NULL,
                    predicted_labels_json TEXT NOT NULL DEFAULT '[]',
                    predicted_faq_id TEXT NOT NULL DEFAULT '',
                    predicted_faqs_json TEXT NOT NULL DEFAULT '[]',
                    actual_primary TEXT NOT NULL,
                    actual_labels_json TEXT NOT NULL DEFAULT '[]',
                    faq_verdict TEXT NOT NULL,
                    expected_faq_id TEXT NOT NULL DEFAULT '',
                    note TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    revision INTEGER NOT NULL DEFAULT 1,
                    PRIMARY KEY(event_sequence,rule_version)
                );
                CREATE INDEX IF NOT EXISTS development_audits_version ON development_audits(rule_version,event_sequence);
                CREATE TABLE IF NOT EXISTS image_understanding (
                    sha256 TEXT NOT NULL, policy_version INTEGER NOT NULL,
                    state TEXT NOT NULL, result_json TEXT, error TEXT,
                    model TEXT NOT NULL DEFAULT 'deepseek-flash', attempts INTEGER NOT NULL DEFAULT 0,
                    processing_ms INTEGER, updated_at TEXT NOT NULL,
                    PRIMARY KEY(sha256,policy_version)
                );
                CREATE TABLE IF NOT EXISTS image_text_cache (
                    sha256 TEXT NOT NULL, engine_version TEXT NOT NULL, text TEXT NOT NULL,
                    updated_at TEXT NOT NULL, PRIMARY KEY(sha256,engine_version)
                );
            """)
            columns = {row["name"] for row in db.execute("PRAGMA table_info(development_audits)")}
            if "actual_labels_json" not in columns:
                db.execute("ALTER TABLE development_audits ADD COLUMN actual_labels_json TEXT NOT NULL DEFAULT '[]'")
            review_columns = {row["name"] for row in db.execute("PRAGMA table_info(review_decisions)")}
            if "rule_version" not in review_columns:
                db.execute("ALTER TABLE review_decisions ADD COLUMN rule_version INTEGER")
            event_columns = {row["name"] for row in db.execute("PRAGMA table_info(events)")}
            if "source_account_id" not in event_columns:
                db.execute("ALTER TABLE events ADD COLUMN source_account_id TEXT NOT NULL DEFAULT 'yunzhongke:primary'")
            db.execute("CREATE INDEX IF NOT EXISTS events_account_group_sender_time ON events(source_account_id,group_id,sender_id,source_timestamp)")
            media_columns = {row["name"] for row in db.execute("PRAGMA table_info(media)")}
            if "attempt_count" not in media_columns:
                db.execute("ALTER TABLE media ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0")
                db.execute("ALTER TABLE media ADD COLUMN retry_after TEXT")
                db.execute("UPDATE media SET retry_after=? WHERE state='failed' AND (error LIKE '%404%' OR error LIKE '%timed out%')", (self.now(),))
            if "resource_key" not in media_columns:
                db.execute("ALTER TABLE media ADD COLUMN resource_key TEXT")
                db.execute("ALTER TABLE media ADD COLUMN canonical_media_id INTEGER")
                owners = {}
                for media in db.execute("SELECT * FROM media ORDER BY id").fetchall():
                    key = self.media_resource_key(media["url"])
                    identity = (media["event_sequence"], key)
                    canonical = owners.get(identity)
                    db.execute("UPDATE media SET resource_key=?,canonical_media_id=? WHERE id=?", (key, canonical, media["id"]))
                    if canonical is None:
                        owners[identity] = media["id"]
                    elif media["state"] == "stored":
                        db.execute("""UPDATE media SET state='stored',file_name=?,sha256=?,byte_count=?,mime=?,error=NULL,retry_after=NULL
                            WHERE id=? AND state!='stored'""", (media["file_name"],media["sha256"],media["byte_count"],media["mime"],canonical))
                db.execute("CREATE UNIQUE INDEX IF NOT EXISTS media_unique_resource ON media(event_sequence,resource_key) WHERE canonical_media_id IS NULL AND resource_key IS NOT NULL")
        self.repair_missing_timestamps()
        self.scrub_media_errors()

    def scrub_media_errors(self):
        with self.write_lock, self.connect() as db:
            rows = db.execute("SELECT id,error FROM media WHERE error IS NOT NULL AND error LIKE '%http%'").fetchall()
            for row in rows:
                clean = re.sub(r"https?://[^\s]+", "[来源图片地址]", row["error"])
                if clean != row["error"]:
                    db.execute("UPDATE media SET error=? WHERE id=?", (clean, row["id"]))

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=20)
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA busy_timeout=20000")
            db.execute("PRAGMA foreign_keys=ON")
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def now():
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    @staticmethod
    def dumps(value):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)

    @staticmethod
    def source_references(raw, outer=None, source_id=""):
        outer = outer or {}
        return {str(value) for value in
                [source_id, raw.get("id"), raw.get("msgid"), outer.get("_id"), outer.get("id"),
                 *(outer.get("source_reference_ids") or [])]
                if value is not None and str(value)}

    @staticmethod
    def media_resource_key(url):
        parsed = urlsplit(url)
        signing_fields = {"key", "time"} if (parsed.hostname or "").lower() == "oss.iyunzk.com" else set()
        query = [(k,v) for k,v in parse_qsl(parsed.query, keep_blank_values=True)
                 if k.casefold() not in signing_fields]
        canonical = urlunsplit((parsed.scheme.lower(),parsed.netloc.lower(),parsed.path,urlencode(sorted(query)),""))
        return hashlib.sha256(canonical.encode()).hexdigest()

    def register_media(self, db, sequence, url):
        key = self.media_resource_key(url)
        existing = db.execute("SELECT id,url,state FROM media WHERE event_sequence=? AND resource_key=? AND canonical_media_id IS NULL", (sequence,key)).fetchone()
        if existing:
            if existing["url"] != url:
                db.execute("""UPDATE media SET url=?,state=CASE WHEN state='failed' THEN 'retry' ELSE state END,
                    retry_after=NULL WHERE id=?""", (url,existing["id"]))
            return
        db.execute("INSERT OR IGNORE INTO media(event_sequence,url,url_hash,resource_key) VALUES (?,?,?,?)",
                   (sequence,url,hashlib.sha256(url.encode()).hexdigest(),key))

    @staticmethod
    def parse_timestamp(raw: dict, outer: dict):
        value = next((item for item in (raw.get("sendTime"), raw.get("create_time"), raw.get("timestamp"),
                                        raw.get("time"), outer.get("create_time")) if item), None)
        if isinstance(value, (int, float)):
            try:
                return datetime.fromtimestamp(value / (1000 if value > 10_000_000_000 else 1),
                                              timezone(timedelta(hours=8))).isoformat(timespec="seconds")
            except (ValueError, OverflowError, OSError):
                return None
        if isinstance(value, str):
            if value.isdigit():
                try:
                    number = int(value)
                    divisor = 1_000_000 if number > 10_000_000_000_000 else 1000 if number > 10_000_000_000 else 1
                    return datetime.fromtimestamp(number / divisor, timezone(timedelta(hours=8))).isoformat(timespec="seconds")
                except (ValueError, OverflowError, OSError):
                    return None
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone(timedelta(hours=8)))
                return parsed.astimezone(timezone(timedelta(hours=8))).isoformat(timespec="seconds")
            except ValueError:
                pass
        return None

    def repair_missing_timestamps(self):
        with self.write_lock, self.connect() as db:
            rows = db.execute("SELECT sequence,raw_json,normalized_json FROM events WHERE timestamp_valid=0 OR source_timestamp IS NULL").fetchall()
            for row in rows:
                try:
                    payload = json.loads(row["raw_json"])
                    stamp = self.parse_timestamp(payload.get("data") or {}, payload.get("outer") or {})
                except (TypeError, ValueError):
                    stamp = None
                if stamp:
                    normalized_json = row["normalized_json"]
                    if normalized_json:
                        normalized = json.loads(normalized_json)
                        parsed = datetime.fromisoformat(stamp)
                        normalized.update({"timestamp": stamp, "time": parsed.strftime("%H:%M"),
                                           "date": parsed.strftime("%m-%d %H:%M")})
                        normalized_json = self.dumps(normalized)
                    db.execute("UPDATE events SET source_timestamp=?,timestamp_valid=1,normalized_json=? WHERE sequence=?",
                               (stamp, normalized_json, row["sequence"]))

    def add_event(self, raw: dict, outer: dict | None, normalized: dict | None, source: str, *, display: bool,
                  parse_state: str, direction: str, timestamp_valid: bool, source_timestamp: str | None = None):
        outer = outer or {}
        source_id = str(raw.get("msgid") or raw.get("id") or outer.get("_id") or "")
        source_account_id = str(raw.get("_source_account_id") or outer.get("_source_account_id") or self.source_account_id).strip()[:160]
        robot_id = str(raw.get("roboot_wxid") or raw.get("robot_wxid") or outer.get("robot_wxid") or "")
        chat = raw.get("chatroom") if isinstance(raw.get("chatroom"), dict) else {}
        sender = raw.get("fromUser") if isinstance(raw.get("fromUser"), dict) else {}
        group_id = str(chat.get("id") or chat.get("wxid") or raw.get("chatroom_wxid") or "")
        sender_id = str(sender.get("id") or sender.get("wxid") or raw.get("from_wxid") or "")
        event_type = str(raw.get("content_type") or raw.get("msg_type") or raw.get("type") or "")
        source_timestamp = normalized.get("timestamp") if normalized else source_timestamp
        if normalized:
            normalized = dict(normalized)
            normalized["revoked"] = source_is_revoked(raw, outer)
        raw_json = self.dumps({"data": raw, "outer": outer})
        if source_id:
            dedupe_key = "source:" + source_id if source_account_id == self.source_account_id else "source:" + source_account_id + ":" + source_id
        else:
            # Exact, ID-less repeats cannot be distinguished by the source. Keep
            # distinct timestamps/payloads; expose the ambiguity in coverage.
            dedupe_key = "payload:" + hashlib.sha256(raw_json.encode("utf-8")).hexdigest()
        now = self.now()
        with self.write_lock, self.connect() as db:
            cursor = db.execute("""
                INSERT OR IGNORE INTO events
                (dedupe_key,source_id,source_account_id,robot_id,group_id,sender_id,direction,event_type,source_timestamp,
                 timestamp_valid,first_seen_at,last_seen_at,first_source,last_source,display,parse_state,raw_json,normalized_json)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (dedupe_key, source_id, source_account_id, robot_id, group_id, sender_id, direction, event_type, source_timestamp,
                  int(timestamp_valid), now, now, source, source, int(display), parse_state, raw_json,
                  self.dumps(normalized) if normalized else None))
            added = cursor.rowcount == 1
            if added:
                sequence = cursor.lastrowid
                for url in (normalized or {}).get("image_urls", []):
                    self.register_media(db, sequence, url)
            else:
                row = db.execute("SELECT sequence,parse_state,raw_json,normalized_json FROM events WHERE dedupe_key=?", (dedupe_key,)).fetchone()
                sequence = row["sequence"]
                if row["parse_state"] == "legacy_no_raw" and source != "legacy_cache":
                    db.execute("""UPDATE events SET last_seen_at=?,last_source=?,seen_count=seen_count+1,
                        source_account_id=?,robot_id=?,group_id=?,sender_id=?,direction=?,event_type=?,source_timestamp=?,
                        timestamp_valid=?,display=?,parse_state=?,raw_json=?,normalized_json=? WHERE sequence=?""",
                               (now, source, source_account_id, robot_id, group_id, sender_id, direction, event_type, source_timestamp,
                                int(timestamp_valid), int(display), parse_state, raw_json,
                                self.dumps(normalized) if normalized else None, sequence))
                else:
                    previous = json.loads(row["raw_json"])
                    old_raw, old_outer = previous.get("data") or {}, previous.get("outer") or {}
                    was_revoked = source_is_revoked(old_raw, old_outer)
                    revoked = was_revoked or source_is_revoked(raw, outer)
                    refs = self.source_references(old_raw, old_outer, source_id) | self.source_references(raw, outer, source_id)
                    merged_raw = dict(old_raw)
                    merged_raw.update({key: value for key, value in raw.items() if value not in (None, "", [], {})})
                    merged_outer = dict(old_outer)
                    merged_outer.update({key: value for key, value in outer.items() if value not in (None, "", [], {})})
                    if revoked:
                        merged_raw["is_chehui"] = 1
                    merged_outer["source_reference_ids"] = sorted(refs)
                    old_normalized = json.loads(row["normalized_json"] or "null")
                    if old_normalized is not None:
                        old_normalized["revoked"] = revoked
                    db.execute("UPDATE events SET last_seen_at=?,last_source=?,seen_count=seen_count+1,raw_json=?,normalized_json=? WHERE sequence=?",
                               (now, source, self.dumps({"data": merged_raw, "outer": merged_outer}), self.dumps(old_normalized) if old_normalized is not None else None, sequence))
                    if revoked and not was_revoked:
                        db.execute("INSERT INTO meta(key,value) VALUES ('source_revoke_revision',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (datetime.now(timezone.utc).isoformat(),))
                        # A recalled question or its quoted reply can no longer
                        # support an automatic review. Preserve its audit trail;
                        # never change an independently submitted human review.
                        prefix = "系统按明确引用的真人回复确认分类；回复消息："
                        automatic = db.execute("SELECT * FROM development_audits WHERE note LIKE ?", (prefix + "%",)).fetchall()
                        invalid = []
                        for audit in automatic:
                            references = {part.strip() for part in str(audit["note"])[len(prefix):].split(",")}
                            if int(audit["event_sequence"]) == sequence or str(sequence) in references:
                                invalid.append(audit)
                        if invalid:
                            db.execute("""CREATE TABLE IF NOT EXISTS development_audit_invalidations(
                                id INTEGER PRIMARY KEY AUTOINCREMENT,event_sequence INTEGER NOT NULL,
                                rule_version INTEGER NOT NULL,reason TEXT NOT NULL,audit_json TEXT NOT NULL,created_at TEXT NOT NULL)""")
                        for audit in invalid:
                            db.execute("INSERT INTO development_audit_invalidations(event_sequence,rule_version,reason,audit_json,created_at) VALUES (?,?,?,?,?)",
                                (audit["event_sequence"],audit["rule_version"],"原消息或作为依据的真人回复已撤回",self.dumps(dict(audit)),now))
                            db.execute("DELETE FROM development_audits WHERE event_sequence=? AND rule_version=?",
                                (audit["event_sequence"],audit["rule_version"]))
                for url in (normalized or {}).get("image_urls", []):
                    self.register_media(db, sequence, url)
        return sequence, added

    def import_legacy(self, items: list[dict]):
        added = 0
        for item in items:
            raw = {"msgid": item.get("source_id") or item.get("id"), "content": item.get("content"),
                   "sendTime": item.get("timestamp"), "content_type": "legacy_normalized",
                   "chatroom": {"displayName": item.get("group")}, "fromUser": {"nickname": item.get("sender")}}
            normalized = dict(item)
            _, new = self.add_event(raw, {}, normalized, "legacy_cache", display=True,
                                    parse_state="legacy_no_raw", direction="inbound", timestamp_valid=True)
            added += int(new)
        return added

    def page(self, after: int = 0, limit: int = 200, *, start: str = "", end: str = ""):
        limit = max(1, min(limit, 500))
        query = "SELECT sequence,normalized_json,raw_json FROM events WHERE display=1 AND sequence>?"
        values: list = [after]
        if start:
            query += " AND source_timestamp>=?"
            values.append(start)
        if end:
            query += " AND source_timestamp<=?"
            values.append(end)
        query += " ORDER BY sequence LIMIT ?"
        values.append(limit + 1)
        with self.connect() as db:
            rows = db.execute(query, values).fetchall()
            latest = db.execute("SELECT COALESCE(MAX(sequence),0) FROM events").fetchone()[0]
            selected = rows[:limit]
            media_by_event = {}
            if selected:
                placeholders = ",".join("?" for _ in selected)
                for media in db.execute(f"SELECT id,event_sequence,url,state,error FROM media WHERE event_sequence IN ({placeholders}) AND canonical_media_id IS NULL ORDER BY id",
                                        [row["sequence"] for row in selected]):
                    media_by_event.setdefault(media["event_sequence"], []).append(dict(media))
        has_more = len(rows) > limit
        items = []
        for row in selected:
            item = json.loads(row["normalized_json"])
            payload=json.loads(row["raw_json"] or "{}")
            item["revoked"]=source_is_revoked(payload.get("data"),payload.get("outer"))
            item["sequence"] = row["sequence"]
            item["media"] = media_by_event.get(row["sequence"], [])
            items.append(item)
        return {"messages": items, "next": selected[-1]["sequence"] if has_more else latest,
                "has_more": has_more, "latest_sequence": latest, "reset_required": after > latest}

    def page_before(self, before: int, limit: int = 100):
        limit = max(1, min(limit, 500))
        with self.connect() as db:
            rows = db.execute("""SELECT sequence,normalized_json,raw_json FROM events
                                 WHERE display=1 AND sequence<? ORDER BY sequence DESC LIMIT ?""",
                              (before, limit + 1)).fetchall()
            selected = rows[:limit]
            media_by_event = {}
            if selected:
                placeholders = ",".join("?" for _ in selected)
                for media in db.execute(f"SELECT id,event_sequence,url,state,error FROM media WHERE event_sequence IN ({placeholders}) AND canonical_media_id IS NULL ORDER BY id",
                                        [row["sequence"] for row in selected]):
                    media_by_event.setdefault(media["event_sequence"], []).append(dict(media))
        items = []
        for row in reversed(selected):
            item = json.loads(row["normalized_json"])
            payload=json.loads(row["raw_json"] or "{}")
            item["revoked"]=source_is_revoked(payload.get("data"),payload.get("outer"))
            item["sequence"] = row["sequence"]
            item["media"] = media_by_event.get(row["sequence"], [])
            items.append(item)
        return {"messages": items, "next_before": selected[-1]["sequence"] if selected else before,
                "has_more": len(rows) > limit}

    def review_queue_page(self, version: int, *, before: int | None = None, limit: int = 100,
                          kind: str = "", urgent: bool = False, conflict: bool = False,
                          status: str = "", work_status: str = "", group: str = "",
                          start: str = "", end: str = "", query: str = ""):
        limit = max(1, min(limit, 500))
        with self.connect() as db:
            has_work_items = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='work_items'").fetchone() is not None
        work_join = " LEFT JOIN work_items w ON w.event_sequence=e.sequence" if has_work_items else ""
        work_status_expr = "COALESCE(w.status,'pending')" if has_work_items else "'pending'"
        work_ai_state_expr = "w.ai_state" if has_work_items else "NULL"
        work_ai_route_expr = "json_extract(w.ai_json,'$.route')" if has_work_items else "NULL"
        base = """ FROM events e LEFT JOIN analysis_results a ON a.event_sequence=e.sequence AND a.rule_version=?
                   LEFT JOIN review_decisions r ON r.event_sequence=e.sequence""" + work_join + " WHERE e.display=1"
        values: list = [version]
        if kind:
            base += " AND COALESCE(r.primary_label,json_extract(a.result_json,'$.primary'))=?"
            values.append(kind)
        if urgent:
            base += " AND COALESCE(r.primary_label,json_extract(a.result_json,'$.primary')) IN ('service','warning','mini','burst')"
        if conflict:
            base += " AND EXISTS (SELECT 1 FROM work_items w WHERE w.event_sequence=e.sequence AND w.ai_state='done' AND json_extract(w.ai_json,'$.route')<>w.route)"
        if status:
            base += " AND COALESCE(r.status,'pending')=?"
            values.append(status)
        if work_status:
            base += " AND " + work_status_expr + "=?"
            values.append(work_status)
        if group:
            base += " AND instr(COALESCE(json_extract(e.normalized_json,'$.group'),''),?)>0"
            values.append(group)
        if start:
            base += " AND e.source_timestamp>=?"
            values.append(start)
        if end:
            base += " AND e.source_timestamp<=?"
            values.append(end)
        if query:
            base += " AND (instr(COALESCE(json_extract(e.normalized_json,'$.content'),''),?)>0 OR instr(COALESCE(json_extract(e.normalized_json,'$.group'),''),?)>0 OR instr(COALESCE(json_extract(e.normalized_json,'$.sender'),''),?)>0)"
            values.extend([query, query, query])
        with self.connect() as db:
            total = db.execute("SELECT COUNT(*)" + base, values).fetchone()[0]
            if before is None:
                before = db.execute("SELECT COALESCE(MAX(sequence),0)+1 FROM events").fetchone()[0]
            rows = db.execute("SELECT e.sequence,e.normalized_json,e.raw_json," + work_status_expr + " AS work_status," +
                              work_ai_state_expr + " AS work_ai_state," + work_ai_route_expr + " AS work_ai_route" +
                              base + " AND e.sequence<? ORDER BY e.sequence DESC LIMIT ?",
                              [*values, before, limit + 1]).fetchall()
            selected = rows[:limit]
            media_by_event = {}
            if selected:
                placeholders = ",".join("?" for _ in selected)
                for media in db.execute(f"SELECT id,event_sequence,url,state,error FROM media WHERE event_sequence IN ({placeholders}) AND canonical_media_id IS NULL ORDER BY id",
                                        [row["sequence"] for row in selected]):
                    media_by_event.setdefault(media["event_sequence"], []).append(dict(media))
        items = []
        for row in selected:
            item = json.loads(row["normalized_json"])
            payload=json.loads(row["raw_json"] or "{}")
            item["revoked"]=source_is_revoked(payload.get("data"),payload.get("outer"))
            item["sequence"] = row["sequence"]
            item["media"] = media_by_event.get(row["sequence"], [])
            item["work_status"] = row["work_status"] or "pending"
            item["work_ai_state"] = row["work_ai_state"]
            item["work_ai_route"] = row["work_ai_route"]
            items.append(item)
        return {"messages": items, "total": total,
                "next_before": selected[-1]["sequence"] if selected else before,
                "has_more": len(rows) > limit}

    def outbound_page_before(self, before: int | None = None, limit: int = 100):
        """Read sent source events separately from the inbound review queue."""
        limit = max(1, min(limit, 500))
        with self.connect() as db:
            if before is None:
                before = db.execute("SELECT COALESCE(MAX(sequence),0)+1 FROM events").fetchone()[0]
            rows = db.execute("""SELECT sequence,source_id,event_type,source_timestamp,first_source,raw_json
                                 FROM events WHERE direction='outbound' AND parse_state<>'suppressed_duplicate_broadcast' AND sequence<?
                                 ORDER BY sequence DESC LIMIT ?""", (before, limit + 1)).fetchall()
        selected = rows[:limit]
        return {"events": [dict(row) for row in selected],
                "next_before": selected[-1]["sequence"] if selected else before,
                "has_more": len(rows) > limit}

    def analysis_candidates(self, version: int, *, force: bool = False, limit: int | None = None):
        query = """SELECT e.sequence,e.raw_json,e.normalized_json,e.group_id,e.sender_id,e.source_timestamp
                   FROM events e LEFT JOIN analysis_results a
                     ON a.event_sequence=e.sequence AND a.rule_version=?
                   WHERE e.display=1"""
        if not force:
            query += " AND a.event_sequence IS NULL"
        query += " ORDER BY e.source_timestamp,e.sequence"
        if limit:
            query += f" LIMIT {max(1, int(limit))}"
        with self.connect() as db:
            return [dict(row) for row in db.execute(query, (version,))]

    def analysis_event(self, sequence: int):
        with self.connect() as db:
            row = db.execute("""SELECT sequence,raw_json,normalized_json,source_account_id,group_id,sender_id,source_timestamp
                                FROM events WHERE sequence=? AND display=1""", (sequence,)).fetchone()
        return dict(row) if row else None

    def analysis_context(self, event: dict, minutes: int = 30, limit: int = 80, same_customer_only: bool = False):
        if not event["group_id"] or not event["source_timestamp"]:
            return []
        try:
            when = datetime.fromisoformat(event["source_timestamp"])
        except ValueError:
            return []
        start = (when - timedelta(minutes=minutes)).isoformat(timespec="seconds")
        end = (when + timedelta(minutes=minutes)).isoformat(timespec="seconds")
        customer_filter = " AND (sender_id=? OR direction='outbound')" if same_customer_only and event.get("sender_id") else ""
        params = [event.get("source_account_id") or self.source_account_id, event["group_id"], start, end, event["sequence"]]
        if customer_filter:
            params.append(event["sender_id"])
        params.append(limit)
        with self.connect() as db:
            rows = db.execute("""SELECT sequence,source_account_id,group_id,sender_id,direction,source_timestamp,normalized_json,raw_json
                                 FROM events WHERE parse_state<>'suppressed_duplicate_broadcast' AND source_account_id=? AND group_id=? AND source_timestamp>=? AND source_timestamp<=?
                                   AND sequence!=""" + "?" + customer_filter + " ORDER BY source_timestamp DESC,sequence DESC LIMIT ?", params).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            normalized = json.loads(item.pop("normalized_json") or "null") or {}
            payload = json.loads(item.pop("raw_json"))
            raw = payload["data"]
            value = normalized.get("content") or (raw.get("content") if isinstance(raw, dict) else "")
            item["content"] = str(value)[:400] if isinstance(value, str) else ""
            item["reply_to_source_id"] = str(raw.get("replyMessageId") or "") if isinstance(raw, dict) else ""
            reply_targets = []
            if isinstance(raw, dict):
                to_user = raw.get("toUser") if isinstance(raw.get("toUser"), dict) else {}
                reply_targets.extend((raw.get("to_wxid"), to_user.get("id")))
            item["reply_target_ids"] = list({str(target) for target in reply_targets if target is not None and str(target)})
            item["source_reference_ids"] = sorted(self.source_references(raw, payload.get("outer"), normalized.get("source_id")))
            raw_type = " ".join(str(raw.get(key) or "") for key in ("type", "msg_type", "wxtype", "content_type")).lower() if isinstance(raw, dict) else ""
            item["has_image"] = bool(normalized.get("image_urls") or normalized.get("attachments") or
                                     "image" in raw_type or "图片" in raw_type or raw_type.strip() == "3")
            result.append(item)
        return result

    def explicit_reply_target(self, sequence: int):
        with self.connect() as db:
            event = db.execute("SELECT source_account_id,group_id,source_timestamp,raw_json FROM events WHERE sequence=?", (sequence,)).fetchone()
            if not event:
                return None
            raw = json.loads(event["raw_json"]).get("data") or {}
            reference = str(raw.get("replyMessageId") or "")
            if not reference:
                return None
            row = db.execute("""SELECT * FROM events WHERE source_account_id=? AND group_id=? AND direction='inbound' AND display=1
                    AND (source_id=? OR CAST(json_extract(raw_json,'$.data.id') AS TEXT)=?
                         OR CAST(json_extract(raw_json,'$.data.msgid') AS TEXT)=?
                         OR json_extract(raw_json,'$.outer._id')=?
                         OR EXISTS(SELECT 1 FROM json_each(events.raw_json,'$.outer.source_reference_ids') WHERE value=?)) LIMIT 1""",
                    (event["source_account_id"], event["group_id"], reference, reference, reference, reference, reference)).fetchone()
            if row:
                return dict(row)
            # A message reference is stronger than a recipient identity. If it
            # cannot be resolved locally, do not guess from to_wxid.
            return None
    def conversation_anchor_sequence(self, event: dict, window_seconds: int = 1800) -> int:
        """Return the first inbound event in the customer's uninterrupted session.

        A session continues while consecutive messages from the same customer in
        the same group are no farther apart than the configured context window.
        Outbound group replies do not split the customer's session.
        """
        sequence = int(event.get("sequence") or 0)
        group_id = str(event.get("group_id") or "")
        sender_id = str(event.get("sender_id") or "")
        timestamp = str(event.get("source_timestamp") or "")
        if not sequence or not group_id or not sender_id or not timestamp:
            return sequence
        try:
            current_at = datetime.fromisoformat(timestamp)
        except ValueError:
            return sequence
        window = max(60, min(1800, int(window_seconds or 1800)))
        with self.connect() as db:
            rows = db.execute("""SELECT sequence,source_timestamp FROM events
                                 WHERE source_account_id=? AND group_id=? AND sender_id=? AND direction='inbound' AND display=1
                                   AND source_timestamp<=? AND sequence<=?
                                 ORDER BY source_timestamp DESC,sequence DESC LIMIT 2000""",
                              (event.get("source_account_id") or self.source_account_id, group_id, sender_id, timestamp, sequence)).fetchall()
        anchor = sequence
        next_at = current_at
        for row in rows:
            if int(row["sequence"]) == sequence:
                continue
            try:
                previous_at = datetime.fromisoformat(str(row["source_timestamp"] or ""))
            except ValueError:
                break
            gap = (next_at - previous_at).total_seconds()
            if gap < 0 or gap > window:
                break
            anchor = int(row["sequence"])
            next_at = previous_at
        return anchor

    def recent_same_sender_events(self, event: dict, minutes: int = 2):
        if not event["group_id"] or not event["sender_id"] or not event["source_timestamp"]:
            return []
        try:
            when = datetime.fromisoformat(event["source_timestamp"])
        except ValueError:
            return []
        start = (when - timedelta(minutes=minutes)).isoformat(timespec="seconds")
        with self.connect() as db:
            rows = db.execute("""SELECT sequence FROM events
                                 WHERE source_account_id=? AND group_id=? AND sender_id=? AND display=1
                                   AND source_timestamp>=? AND source_timestamp<=? AND sequence!=?
                                 ORDER BY source_timestamp DESC LIMIT 20""",
                              (event.get("source_account_id") or self.source_account_id, event["group_id"], event["sender_id"], start,
                               event["source_timestamp"], event["sequence"])).fetchall()
        return [row["sequence"] for row in rows]

    def save_analysis(self, sequence: int, version: int, result: dict, processing_ms: int = 0):
        with self.write_lock, self.connect() as db:
            db.execute("""INSERT INTO analysis_results(event_sequence,rule_version,result_json,analyzed_at,processing_ms)
                          VALUES (?,?,?,?,?) ON CONFLICT(event_sequence,rule_version) DO UPDATE SET
                          result_json=excluded.result_json,analyzed_at=excluded.analyzed_at,
                          processing_ms=excluded.processing_ms""",
                       (sequence, version, self.dumps(result), self.now(), processing_ms))

    def analysis_map(self, sequences: list[int], version: int):
        if not sequences:
            return {}, {}
        placeholders = ",".join("?" for _ in sequences)
        with self.connect() as db:
            results = db.execute(f"SELECT event_sequence,result_json,analyzed_at,processing_ms FROM analysis_results WHERE rule_version=? AND event_sequence IN ({placeholders})",
                                 [version, *sequences]).fetchall()
            reviews = db.execute(f"SELECT * FROM review_decisions WHERE event_sequence IN ({placeholders})", sequences).fetchall()
        return ({row["event_sequence"]: {**json.loads(row["result_json"]), "analyzed_at": row["analyzed_at"],
                                            "processing_ms": row["processing_ms"]} for row in results},
                {row["event_sequence"]: {**dict(row), "labels": json.loads(row["labels_json"] or "null")} for row in reviews})

    def development_audit_page(self, version: int, *, before: int | None = None, limit: int = 50,
                               status: str = "todo", kind: str = "", query: str = "", only_differences: bool = False):
        limit = max(1, min(limit, 200))
        with self.connect() as db:
            base = """ FROM events e
                        LEFT JOIN analysis_results a ON a.event_sequence=e.sequence AND a.rule_version=?
                        LEFT JOIN development_audits d ON d.event_sequence=e.sequence AND d.rule_version=(SELECT MAX(h.rule_version) FROM development_audits h WHERE h.event_sequence=e.sequence AND h.rule_version<=?)
                        WHERE e.display=1"""
            values: list = [version, version]
            if status == "todo":
                base += " AND d.event_sequence IS NULL"
            elif status == "audited":
                base += " AND d.event_sequence IS NOT NULL"
            if only_differences:
                base += """ AND d.event_sequence IS NOT NULL AND a.result_json IS NOT NULL AND (
                    json_extract(a.result_json,'$.primary')<>d.actual_primary OR
                    EXISTS(SELECT value FROM json_each(a.result_json,'$.labels') EXCEPT SELECT value FROM json_each(d.actual_labels_json)) OR
                    EXISTS(SELECT value FROM json_each(d.actual_labels_json) EXCEPT SELECT value FROM json_each(a.result_json,'$.labels')))"""
            if kind:
                base += " AND json_extract(a.result_json,'$.primary')=?"
                values.append(kind)
            if query:
                base += " AND (instr(COALESCE(json_extract(e.normalized_json,'$.content'),''),?)>0 OR instr(COALESCE(json_extract(e.normalized_json,'$.group'),''),?)>0 OR instr(COALESCE(json_extract(e.normalized_json,'$.sender'),''),?)>0)"
                values.extend([query, query, query])
            total = db.execute("SELECT COUNT(*)" + base, values).fetchone()[0]
            if before is None:
                before = db.execute("SELECT COALESCE(MAX(sequence),0)+1 FROM events").fetchone()[0]
            rows = db.execute("""SELECT e.sequence,e.source_id,e.source_timestamp,e.normalized_json,
                                       a.result_json,d.rule_version audit_version,d.predicted_primary audit_predicted_primary,d.actual_primary,d.faq_verdict,d.expected_faq_id,
                                       d.note,d.updated_at,d.revision,d.predicted_faq_id,d.actual_labels_json
                                """ + base + " AND e.sequence<? ORDER BY e.sequence DESC LIMIT ?",
                              [*values, before, limit + 1]).fetchall()
        selected = rows[:limit]
        items = []
        for row in selected:
            message = json.loads(row["normalized_json"] or "null") or {}
            analysis = json.loads(row["result_json"]) if row["result_json"] else None
            audit = None
            if row["actual_primary"] is not None:
                audit = {"rule_version": row["audit_version"], "carried": row["audit_version"] != version, "predicted_primary": row["audit_predicted_primary"], "actual_primary": row["actual_primary"], "faq_verdict": row["faq_verdict"],
                         "expected_faq_id": row["expected_faq_id"], "note": row["note"],
                         "updated_at": row["updated_at"], "revision": row["revision"],
                         "predicted_faq_id": row["predicted_faq_id"],
                         "actual_labels": json.loads(row["actual_labels_json"] or "[]")}
            items.append({"sequence": row["sequence"], "source_id": row["source_id"],
                          "timestamp": row["source_timestamp"], "message": message,
                          "analysis": analysis, "audit": audit})
        return {"messages": items, "total": total,
                "next_before": selected[-1]["sequence"] if selected else before,
                "has_more": len(rows) > limit}

    def development_audit(self, sequence: int, version: int):
        with self.connect() as db:
            row = db.execute("SELECT * FROM development_audits WHERE event_sequence=? AND rule_version<=? ORDER BY rule_version DESC LIMIT 1",
                             (sequence, version)).fetchone()
        if not row:
            return None
        value = dict(row)
        value["carried"] = value["rule_version"] != version
        value["predicted_labels"] = json.loads(value.pop("predicted_labels_json") or "[]")
        value["predicted_faqs"] = json.loads(value.pop("predicted_faqs_json") or "[]")
        value["actual_labels"] = json.loads(value.pop("actual_labels_json") or "[]")
        return value

    def save_development_audit(self, sequence: int, version: int, *, predicted_primary: str,
                               predicted_labels: list[str], predicted_faq_id: str,
                               predicted_faqs: list[dict], actual_primary: str, actual_labels: list[str], faq_verdict: str,
                               expected_faq_id: str, note: str):
        now = self.now()
        with self.write_lock, self.connect() as db:
            if not db.execute("SELECT 1 FROM events WHERE sequence=? AND display=1", (sequence,)).fetchone():
                raise KeyError("message not found")
            existing = db.execute("SELECT revision,created_at FROM development_audits WHERE event_sequence=? AND rule_version=?",
                                  (sequence, version)).fetchone()
            revision = (existing["revision"] if existing else 0) + 1
            created_at = existing["created_at"] if existing else now
            db.execute("""INSERT INTO development_audits(
                            event_sequence,rule_version,predicted_primary,predicted_labels_json,
                            predicted_faq_id,predicted_faqs_json,actual_primary,actual_labels_json,faq_verdict,
                            expected_faq_id,note,created_at,updated_at,revision)
                         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                         ON CONFLICT(event_sequence,rule_version) DO UPDATE SET
                            predicted_primary=excluded.predicted_primary,
                            predicted_labels_json=excluded.predicted_labels_json,
                            predicted_faq_id=excluded.predicted_faq_id,
                            predicted_faqs_json=excluded.predicted_faqs_json,
                            actual_primary=excluded.actual_primary,actual_labels_json=excluded.actual_labels_json,
                            faq_verdict=excluded.faq_verdict,
                            expected_faq_id=excluded.expected_faq_id,note=excluded.note,
                            updated_at=excluded.updated_at,revision=excluded.revision""",
                       (sequence, version, predicted_primary, self.dumps(predicted_labels), predicted_faq_id,
                        self.dumps(predicted_faqs), actual_primary, self.dumps(actual_labels), faq_verdict, expected_faq_id,
                        note[:1000], created_at, now, revision))
        return self.development_audit(sequence, version)

    def development_audit_summary(self, version: int):
        with self.connect() as db:
            total = db.execute("SELECT COUNT(*) FROM events WHERE display=1").fetchone()[0]
            analyzed = db.execute("SELECT COUNT(*) FROM analysis_results a JOIN events e ON e.sequence=a.event_sequence WHERE e.display=1 AND a.rule_version=?", (version,)).fetchone()[0]
            rows = db.execute("""SELECT d.*,a.result_json FROM development_audits d
                JOIN events e ON e.sequence=d.event_sequence AND e.display=1
                LEFT JOIN analysis_results a ON a.event_sequence=d.event_sequence AND a.rule_version=?
                WHERE d.rule_version=(SELECT MAX(h.rule_version) FROM development_audits h
                    WHERE h.event_sequence=d.event_sequence AND h.rule_version<=?)""", (version,version)).fetchall()
        confusion: dict[str, dict[str, int]] = {}
        faq_counts: dict[str, int] = {}
        label_stats: dict[str, dict[str, int]] = {}
        category_correct = snapshot_correct = exact_labels = current_reviewed = compared = automatic_reviewed = 0
        human_categories, machine_categories = {}, {}
        for row in rows:
            current = json.loads(row["result_json"] or "{}")
            predicted, actual = current.get("primary") or row["predicted_primary"], row["actual_primary"]
            current_reviewed += int(row["rule_version"] == version)
            if str(row["note"] or "").startswith("系统按明确引用的真人回复确认分类；"):
                automatic_reviewed += 1
                continue
            snapshot_correct += int(row["predicted_primary"] == actual)
            human_categories[actual] = human_categories.get(actual, 0) + 1
            if row["faq_verdict"] != "not_reviewed":
                faq_counts[row["faq_verdict"]] = faq_counts.get(row["faq_verdict"], 0) + 1
            if not current:
                continue
            compared += 1
            machine_categories[predicted] = machine_categories.get(predicted, 0) + 1
            confusion.setdefault(predicted, {})[actual] = confusion.setdefault(predicted, {}).get(actual, 0) + 1
            category_correct += int(predicted == actual)
            predicted_labels = set(current.get("labels") or json.loads(row["predicted_labels_json"] or "[]"))
            actual_labels = set(json.loads(row["actual_labels_json"] or "[]"))
            predicted_labels.add(predicted)
            actual_labels.add(actual)
            exact_labels += int(predicted == actual and predicted_labels == actual_labels)
            for label in predicted_labels | actual_labels:
                values = label_stats.setdefault(label, {"tp": 0, "fp": 0, "fn": 0, "support": 0})
                if label in actual_labels:
                    values["support"] += 1
                if label in predicted_labels and label in actual_labels:
                    values["tp"] += 1
                elif label in predicted_labels:
                    values["fp"] += 1
                elif label in actual_labels:
                    values["fn"] += 1
        for values in label_stats.values():
            precision_base = values["tp"] + values["fp"]
            recall_base = values["tp"] + values["fn"]
            values["precision"] = round(values["tp"] / precision_base, 4) if precision_base else None
            values["recall"] = round(values["tp"] / recall_base, 4) if recall_base else None
        reviewed = len(rows)
        manual_reviewed = reviewed - automatic_reviewed
        return {"version": version, "total": total, "analyzed": analyzed, "reviewed": reviewed,
                "automatic_reviewed":automatic_reviewed,"manual_reviewed":manual_reviewed,
                "todo": max(0, total - reviewed), "category_correct": category_correct,
                "category_accuracy": round(category_correct / compared, 4) if compared else None,
                "faq_verdicts": faq_counts, "confusion": confusion, "labels": label_stats,
                "current_version_reviewed": current_reviewed, "carried_reviewed": reviewed-current_reviewed,
                "full_label_correct": exact_labels, "comparison_count": compared,
                "human_categories": human_categories, "machine_categories": machine_categories,
                "snapshot_accuracy": round(snapshot_correct / manual_reviewed, 4) if manual_reviewed else None,
                "sampling_note": "准确率仅计算非自动审核的已有标签；历史标签沿用不代表已重新审核本轮规则，非独立测试集"}

    def save_review(self, sequence: int, status: str, primary: str | None, labels: list[str] | None,
                    reason: str, reviewer: str = "本机审核员", rule_version: int | None = None):
        with self.write_lock, self.connect() as db:
            existing = db.execute("SELECT * FROM review_decisions WHERE event_sequence=?", (sequence,)).fetchone()
            if not db.execute("SELECT 1 FROM events WHERE sequence=? AND display=1", (sequence,)).fetchone():
                raise KeyError("message not found")
            revision = (existing["revision"] if existing else 0) + 1
            decision = {"event_sequence": sequence, "status": status, "primary_label": primary,
                        "labels": labels, "reason": reason[:1000], "reviewer": reviewer[:100],
                        "reviewed_at": self.now(), "revision": revision, "rule_version": rule_version}
            db.execute("""INSERT INTO review_decisions(event_sequence,status,primary_label,labels_json,reason,reviewer,reviewed_at,revision,rule_version)
                          VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(event_sequence) DO UPDATE SET
                          status=excluded.status,primary_label=excluded.primary_label,labels_json=excluded.labels_json,
                          reason=excluded.reason,reviewer=excluded.reviewer,reviewed_at=excluded.reviewed_at,
                          revision=excluded.revision,rule_version=excluded.rule_version""",
                       (sequence, status, primary, self.dumps(labels) if labels is not None else None,
                        decision["reason"], decision["reviewer"], decision["reviewed_at"], revision, rule_version))
            db.execute("INSERT INTO review_log(event_sequence,previous_json,decision_json,changed_at) VALUES (?,?,?,?)",
                       (sequence, self.dumps(dict(existing)) if existing else None, self.dumps(decision), self.now()))
        return decision

    def review_history(self, limit: int = 200):
        with self.connect() as db:
            rows = db.execute("""SELECT l.id,l.event_sequence,l.decision_json,l.changed_at,e.normalized_json
                                 FROM review_log l JOIN events e ON e.sequence=l.event_sequence
                                 ORDER BY l.id DESC LIMIT ?""", (max(1, min(limit, 1000)),)).fetchall()
        result = []
        for row in rows:
            message = json.loads(row["normalized_json"] or "null") or {}
            result.append({"id": row["id"], "sequence": row["event_sequence"],
                           "decision": json.loads(row["decision_json"]), "time": row["changed_at"],
                           "group": message.get("group"), "sender": message.get("sender")})
        return result

    def analysis_stats(self, version: int):
        with self.connect() as db:
            rows = db.execute("""SELECT COALESCE(r.primary_label,json_extract(a.result_json,'$.primary')) kind,COUNT(*) total
                                 FROM analysis_results a LEFT JOIN review_decisions r ON r.event_sequence=a.event_sequence
                                 WHERE a.rule_version=? GROUP BY kind""", (version,)).fetchall()
            analyzed = sum(row["total"] for row in rows)
            visible = db.execute("SELECT COUNT(*) FROM events WHERE display=1").fetchone()[0]
            reviewed = db.execute("SELECT COUNT(*) FROM review_decisions WHERE status!='pending'").fetchone()[0]
            attention_pending = db.execute("""SELECT COUNT(*) FROM analysis_results a
                LEFT JOIN review_decisions r ON r.event_sequence=a.event_sequence
                WHERE a.rule_version=? AND COALESCE(r.status,'pending')='pending'
                  AND COALESCE(r.primary_label,json_extract(a.result_json,'$.primary'))
                  IN ('service','warning','mini','burst')""", (version,)).fetchone()[0]
        return {"analyzed": analyzed, "visible": visible, "unprocessed": max(0, visible - analyzed),
                "reviewed": reviewed, "attention_pending": attention_pending,
                "by_category": {row["kind"]: row["total"] for row in rows}}

    def evaluation_report(self, version: int, labels: list[str]):
        with self.connect() as db:
            rows = db.execute("""SELECT a.result_json,r.primary_label,r.labels_json FROM review_decisions r
                                 JOIN analysis_results a ON a.event_sequence=r.event_sequence AND a.rule_version=?
                                 WHERE r.status IN ('approved','rejected') AND r.primary_label IS NOT NULL""", (version,)).fetchall()
        matrix = {label: {"tp": 0, "fp": 0, "fn": 0, "support": 0} for label in labels}
        for row in rows:
            predicted = set(json.loads(row["result_json"]).get("labels") or [])
            actual = set(json.loads(row["labels_json"] or "null") or [row["primary_label"]])
            for label, values in matrix.items():
                if label in actual:
                    values["support"] += 1
                if label in predicted and label in actual:
                    values["tp"] += 1
                elif label in predicted:
                    values["fp"] += 1
                elif label in actual:
                    values["fn"] += 1
        for values in matrix.values():
            precision_base = values["tp"] + values["fp"]
            recall_base = values["tp"] + values["fn"]
            values["precision"] = round(values["tp"] / precision_base, 3) if precision_base else None
            values["recall"] = round(values["tp"] / recall_base, 3) if recall_base else None
            values["sample_sufficient"] = values["support"] >= 30
        return {"version": version, "reviewed_sample": len(rows), "minimum_total": 300,
                "sample_sufficient": len(rows) >= 300,
                "sampling_note": "仅反映已人工复核样本，未进行独立随机抽样；不能据此宣称全量准确率",
                "labels": matrix}

    def get_workspace_config(self):
        with self.connect() as db:
            row = db.execute("SELECT settings_json,faqs_json,updated_at FROM workspace_config WHERE id=1").fetchone()
        return {"initialized": bool(row), "settings": json.loads(row["settings_json"]) if row else {},
                "faqs": json.loads(row["faqs_json"]) if row else [], "updated_at": row["updated_at"] if row else None}

    def save_workspace_config(self, settings: dict, faqs: list, *, only_if_empty: bool = False):
        now = self.now()
        with self.write_lock, self.connect() as db:
            if only_if_empty and db.execute("SELECT 1 FROM workspace_config WHERE id=1").fetchone():
                return self.get_workspace_config()
            db.execute("""INSERT INTO workspace_config(id,settings_json,faqs_json,updated_at) VALUES (1,?,?,?)
                          ON CONFLICT(id) DO UPDATE SET settings_json=excluded.settings_json,
                          faqs_json=excluded.faqs_json,updated_at=excluded.updated_at""",
                       (self.dumps(settings), self.dumps(faqs), now))
        return {"initialized": True, "settings": settings, "faqs": faqs, "updated_at": now}

    def ensure_rule_version(self, version: int, settings: dict):
        with self.write_lock, self.connect() as db:
            db.execute("INSERT OR IGNORE INTO rule_versions(version,created_at,settings_json) VALUES (?,?,?)",
                       (version, self.now(), self.dumps(settings)))

    def active_rule_version(self, default: int = 2):
        value = self.get_meta("active_rule_version")
        try:
            return max(default, int(value)) if value else default
        except ValueError:
            return default

    def create_rule_version(self, settings: dict, minimum: int = 2):
        with self.write_lock, self.connect() as db:
            maximum = db.execute("SELECT COALESCE(MAX(version),?) FROM rule_versions", (minimum - 1,)).fetchone()[0]
            version = max(minimum, maximum + 1)
            db.execute("INSERT INTO rule_versions(version,created_at,settings_json) VALUES (?,?,?)",
                       (version, self.now(), self.dumps(settings)))
            db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES ('active_rule_version',?)", (str(version),))
        return version

    def rule_versions(self):
        with self.connect() as db:
            rows = db.execute("SELECT version,created_at,settings_json FROM rule_versions ORDER BY version DESC").fetchall()
        return [{"version": row["version"], "created_at": row["created_at"],
                 "rules": json.loads(row["settings_json"]).get("rules", {})} for row in rows]

    def activate_rule_version(self, version: int, minimum: int = 2):
        if version < minimum:
            raise ValueError("older algorithm version cannot be reactivated")
        with self.write_lock, self.connect() as db:
            row = db.execute("SELECT settings_json FROM rule_versions WHERE version=?", (version,)).fetchone()
            if not row:
                raise KeyError("rule version not found")
            settings = json.loads(row[0])
            current = db.execute("SELECT settings_json FROM workspace_config WHERE id=1").fetchone()
            if current:
                merged = json.loads(current[0])
                merged["rules"] = settings.get("rules", {})
                db.execute("UPDATE workspace_config SET settings_json=?,updated_at=? WHERE id=1",
                           (self.dumps(merged), self.now()))
            db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES ('active_rule_version',?)", (str(version),))
        return version

    def import_client_state_backup(self, payload: dict):
        serialized = self.dumps(payload)
        digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
        audit = payload.get("audit") if isinstance(payload.get("audit"), list) else []
        with self.write_lock, self.connect() as db:
            db.execute("INSERT OR IGNORE INTO client_state_backups(digest,payload_json,imported_at,audit_count) VALUES (?,?,?,?)",
                       (digest, serialized, self.now(), len(audit)))
        return {"digest": digest, "legacy_audit_count": len(audit)}

    def legacy_audit(self, limit: int = 100):
        with self.connect() as db:
            row = db.execute("SELECT payload_json FROM client_state_backups ORDER BY imported_at DESC LIMIT 1").fetchone()
        if not row:
            return []
        payload = json.loads(row[0])
        audit = payload.get("audit") if isinstance(payload.get("audit"), list) else []
        return audit[:limit]

    def reception_metrics(self, hours=24):
        """Source timestamp to first local persistence; not screen or network latency."""
        since=(datetime.now(timezone.utc)-timedelta(hours=hours)).isoformat(timespec='seconds')
        with self.connect() as db:
            rows=db.execute("""SELECT source_timestamp,first_seen_at,first_source FROM events
                WHERE first_seen_at>=? AND direction='inbound' AND timestamp_valid=1""",(since,)).fetchall()
        groups={};invalid=0
        for row in rows:
            try:
                seconds=(datetime.fromisoformat(row['first_seen_at'])-datetime.fromisoformat(row['source_timestamp'])).total_seconds()
            except (TypeError,ValueError):
                invalid+=1;continue
            if seconds < -5 or seconds > 86400:
                invalid+=1;continue
            groups.setdefault(row['first_source'],[]).append(max(0,seconds))
        output={}
        for source,values in groups.items():
            values.sort()
            output[source]={'count':len(values),'p50_seconds':values[int((len(values)-1)*.5)],
                            'p95_seconds':values[int((len(values)-1)*.95)],'over_60_seconds':sum(v>60 for v in values)}
        return {'hours':hours,'by_first_source':output,'excluded':invalid,
                'note':'来源时间至本地首次保存，受来源时钟精度影响；历史首收不等于已证实漏消息，不含界面显示时间'}

    def summary(self):
        with self.connect() as db:
            has_work_items = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='work_items'").fetchone() is not None
            work_join = " LEFT JOIN work_items w ON w.event_sequence=e.sequence" if has_work_items else ""
            pending_expr = "SUM(CASE WHEN e.display=1 AND COALESCE(w.status,'pending')='pending' THEN 1 ELSE 0 END)" if has_work_items else "SUM(CASE WHEN e.display=1 THEN 1 ELSE 0 END)"
            row = db.execute("""SELECT COUNT(*) total, COALESCE(MAX(e.sequence),0) latest,
                SUM(CASE WHEN e.display=1 THEN 1 ELSE 0 END) displayed,
                {pending_expr} pending_review,
                SUM(CASE WHEN e.parse_state='unparsed' THEN 1 ELSE 0 END) unparsed,
                SUM(CASE WHEN e.parse_state='legacy_no_raw' THEN 1 ELSE 0 END) legacy_no_raw,
                SUM(CASE WHEN e.direction='outbound' AND e.parse_state<>'suppressed_duplicate_broadcast' THEN 1 ELSE 0 END) outbound,
                SUM(CASE WHEN e.source_id='' THEN 1 ELSE 0 END) idless
                FROM events e{work_join}""".format(pending_expr=pending_expr, work_join=work_join)).fetchone()
            media = db.execute("SELECT state,COUNT(*) n FROM media WHERE canonical_media_id IS NULL GROUP BY state").fetchall()
            latest_run = db.execute("SELECT * FROM history_runs ORDER BY id DESC LIMIT 1").fetchone()
            bots = db.execute("SELECT COUNT(*) FROM source_bots WHERE checked_at=(SELECT MAX(checked_at) FROM source_bots)").fetchone()[0]
            pending_progress = db.execute("SELECT COUNT(*) FROM history_progress WHERE state!='complete'").fetchone()[0]
            today = datetime.now(timezone(timedelta(hours=8))).date().isoformat()
            today_progress = db.execute("""SELECT COUNT(*) total,SUM(CASE WHEN state!='complete' THEN 1 ELSE 0 END) pending
                FROM history_progress WHERE day=? AND robot_id IN
                (SELECT robot_id FROM source_bots WHERE checked_at=(SELECT MAX(checked_at) FROM source_bots))""", (today,)).fetchone()
            today_unfinished = max(0, bots - today_progress["total"]) + (today_progress["pending"] or 0)
            today_count = db.execute("SELECT COUNT(*) FROM events WHERE display=1 AND substr(source_timestamp,1,10)=?",
                                     (today,)).fetchone()[0]
        return {"stored_events": row["total"], "message_count": row["displayed"] or 0,
                "pending_review": row["pending_review"] or 0,
                "today_message_count": today_count,
                "latest_sequence": row["latest"], "unparsed_count": row["unparsed"] or 0,
                "legacy_without_raw_count": row["legacy_no_raw"] or 0,
                "outbound_count": row["outbound"] or 0, "idless_count": row["idless"] or 0,
                "media": {item["state"]: item["n"] for item in media}, "bot_count": bots,
                "unfinished_history_scans": pending_progress,
                "today_unfinished_history_scans": today_unfinished,
                "latest_history_run": dict(latest_run) if latest_run else None}

    def set_bots(self, bots: list[dict]):
        now = datetime.now(timezone.utc).isoformat(timespec="microseconds")
        with self.write_lock, self.connect() as db:
            for bot in bots:
                robot_id = str(bot.get("wxid") or bot.get("id") or "")
                if robot_id:
                    db.execute("INSERT OR REPLACE INTO source_bots VALUES (?,?,?)", (robot_id, self.dumps(bot), now))

    def begin_history(self, start: str, end: str):
        with self.write_lock, self.connect() as db:
            return db.execute("INSERT INTO history_runs(start_at,end_at,started_at,state) VALUES (?,?,?,'running')",
                              (start, end, self.now())).lastrowid

    def end_history(self, run_id: int, state: str, bots: int, pages: int, rows: int, added: int, errors: list[str]):
        with self.write_lock, self.connect() as db:
            db.execute("""UPDATE history_runs SET finished_at=?,state=?,bot_count=?,pages=?,rows_seen=?,added=?,errors_json=?
                          WHERE id=?""", (self.now(), state, bots, pages, rows, added, self.dumps(errors), run_id))

    def history_cursor(self, robot_id: str, day: str):
        with self.connect() as db:
            row = db.execute("SELECT * FROM history_progress WHERE robot_id=? AND day=?", (robot_id, day)).fetchone()
        return dict(row) if row else None

    def has_unfinished_history(self, day: str):
        with self.connect() as db:
            row = db.execute("SELECT 1 FROM history_progress WHERE day=? AND state!='complete' LIMIT 1", (day,)).fetchone()
        return row is not None

    def unfinished_history_ids(self, day: str):
        with self.connect() as db:
            return {row[0] for row in db.execute("SELECT robot_id FROM history_progress WHERE day=? AND state!='complete'", (day,))}

    def oldest_unfinished_history(self, minimum_start_at: str | None = None):
        with self.connect() as db:
            windows = {}

            def add_window(day, start_at, end_at, robot_id="", state="partial_run", error="历史扫描尚未被所有机器人覆盖"):
                if not day or not start_at or not end_at or (minimum_start_at and start_at < minimum_start_at):
                    return
                window = windows.setdefault(day, {"robot_id": robot_id, "day": day, "start_at": start_at,
                                                  "end_at": end_at, "state": state, "error": error, "updated_at": ""})
                window["start_at"] = min(window["start_at"], start_at)
                window["end_at"] = max(window["end_at"], end_at)

            cutoff = " AND start_at>=?" if minimum_start_at else ""
            params = (minimum_start_at,) if minimum_start_at else ()
            for row in db.execute("SELECT robot_id,day,start_at,end_at,state,error,updated_at FROM history_progress WHERE state!='complete'" + cutoff,
                                  params).fetchall():
                add_window(row["day"], row["start_at"], row["end_at"], row["robot_id"], row["state"], row["error"])
            bot_ids = [row[0] for row in db.execute("SELECT robot_id FROM source_bots WHERE checked_at=(SELECT MAX(checked_at) FROM source_bots) ORDER BY robot_id").fetchall()]
            partial_runs = db.execute("SELECT start_at,end_at FROM history_runs WHERE state IN ('partial','failed') ORDER BY end_at DESC,start_at DESC").fetchall()
            for run in partial_runs:
                run_start, run_end = str(run["start_at"] or ""), str(run["end_at"] or "")
                if not run_start or not run_end:
                    continue
                if minimum_start_at and run_start < minimum_start_at:
                    continue
                try:
                    start_dt, end_dt = datetime.fromisoformat(run_start), datetime.fromisoformat(run_end)
                except ValueError:
                    continue
                uncovered_start = None
                day = start_dt.date()
                while day <= end_dt.date():
                    chunk_start = max(start_dt, datetime.combine(day, datetime.min.time(), start_dt.tzinfo))
                    chunk_end = min(end_dt, datetime.combine(day, datetime.max.time().replace(microsecond=0), end_dt.tzinfo))
                    day_covered = bool(bot_ids)
                    for robot_id in bot_ids:
                        progress = db.execute("SELECT start_at,end_at,state FROM history_progress WHERE robot_id=? AND day=?",
                                              (robot_id, day.isoformat())).fetchone()
                        if not progress or progress["state"] != "complete":
                            day_covered = False
                            break
                        try:
                            covered_start = datetime.fromisoformat(progress["start_at"])
                            covered_end = datetime.fromisoformat(progress["end_at"])
                        except (TypeError, ValueError):
                            day_covered = False
                            break
                        if covered_start > chunk_start or covered_end < chunk_end:
                            day_covered = False
                            break
                    if not day_covered and uncovered_start is None:
                        uncovered_start = chunk_start
                    day += timedelta(days=1)
                if uncovered_start is not None:
                    add_window(uncovered_start.date().isoformat(), uncovered_start.isoformat(), end_dt.isoformat())
        if not windows:
            return None
        latest_day = max(windows)
        return windows[latest_day]

    def mark_recovered_history_runs(self):
        recovered = 0
        with self.write_lock, self.connect() as db:
            bot_ids = [row[0] for row in db.execute("SELECT robot_id FROM source_bots WHERE checked_at=(SELECT MAX(checked_at) FROM source_bots) ORDER BY robot_id").fetchall()]
            if not bot_ids:
                return 0
            runs = db.execute("SELECT id,start_at,end_at FROM history_runs WHERE state IN ('partial','failed')").fetchall()
            for run in runs:
                start_at, end_at = str(run["start_at"] or ""), str(run["end_at"] or "")
                if not start_at or not end_at:
                    continue
                try:
                    start_dt, end_dt = datetime.fromisoformat(start_at), datetime.fromisoformat(end_at)
                except ValueError:
                    continue
                covered = True
                day = start_dt.date()
                while day <= end_dt.date() and covered:
                    chunk_start = max(start_dt, datetime.combine(day, datetime.min.time(), start_dt.tzinfo))
                    chunk_end = min(end_dt, datetime.combine(day, datetime.max.time().replace(microsecond=0), end_dt.tzinfo))
                    for robot_id in bot_ids:
                        progress = db.execute("SELECT start_at,end_at,state FROM history_progress WHERE robot_id=? AND day=?",
                                              (robot_id, day.isoformat())).fetchone()
                        if not progress or progress["state"] != "complete":
                            covered = False
                            break
                        try:
                            if datetime.fromisoformat(progress["start_at"]) > chunk_start or datetime.fromisoformat(progress["end_at"]) < chunk_end:
                                covered = False
                                break
                        except (TypeError, ValueError):
                            covered = False
                            break
                    day += timedelta(days=1)
                if covered:
                    cursor = db.execute("UPDATE history_runs SET state='recovered' WHERE id=? AND state IN ('partial','failed')", (run["id"],))
                    recovered += cursor.rowcount
        return recovered

    def save_history_cursor(self, robot_id: str, day: str, start_at: str, end_at: str,
                            cursor: str | None, state: str, pages: int, rows: int, error: str = ""):
        with self.write_lock, self.connect() as db:
            db.execute("""INSERT INTO history_progress
                (robot_id,day,start_at,end_at,cursor,state,pages,rows_seen,error,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(robot_id,day) DO UPDATE SET
                start_at=excluded.start_at,end_at=excluded.end_at,cursor=excluded.cursor,
                state=excluded.state,pages=excluded.pages,rows_seen=excluded.rows_seen,
                error=excluded.error,updated_at=excluded.updated_at""",
                (robot_id, day, start_at, end_at, cursor, state, pages, rows, error[:300], self.now()))

    def media_pending(self, limit: int = 20):
        with self.connect() as db:
            return [dict(row) for row in db.execute(
                """SELECT id,event_sequence,url FROM media WHERE canonical_media_id IS NULL AND (state IN ('pending','retry')
                OR (state='failed' AND retry_after IS NOT NULL AND retry_after<=? AND attempt_count<6))
                ORDER BY id LIMIT ?""", (self.now(), limit)).fetchall()]

    def media_stored(self):
        with self.connect() as db:
            return [dict(row) for row in db.execute(
                "SELECT id,event_sequence,file_name,sha256,state FROM media WHERE state='stored' AND canonical_media_id IS NULL AND file_name IS NOT NULL ORDER BY id").fetchall()]

    def media_result(self, media_id: int, state: str, *, file_name: str = "", sha256: str = "",
                     byte_count: int = 0, mime: str = "", error: str = ""):
        error = re.sub(r"https?://[^\s]+", "[来源图片地址]", error)
        with self.write_lock, self.connect() as db:
            row = db.execute("SELECT attempt_count FROM media WHERE id=?", (media_id,)).fetchone()
            attempts = int(row[0] or 0) + 1 if row else 1
            transient = any(word in error.lower() for word in ("404", "408", "429", "500", "502", "503", "504", "timed out", "timeout", "connection"))
            retry = (datetime.now(timezone.utc) + timedelta(seconds=(60,300,1800,7200,21600)[min(attempts-1,4)])).isoformat(timespec="seconds") if state == "failed" and transient and attempts < 6 else None
            db.execute("""UPDATE media SET state=?,file_name=?,sha256=?,byte_count=?,mime=?,error=?,attempted_at=?,attempt_count=?,retry_after=? WHERE id=?""",
                       (state, file_name or None, sha256 or None, byte_count or None, mime or None, error[:300] or None,
                        self.now(), attempts, retry, media_id))

    def media_for_event(self, sequence: int, *, include_file: bool = False):
        file_column = ",file_name" if include_file else ""
        with self.connect() as db:
            items = [dict(row) for row in db.execute("SELECT id,event_sequence,url,state,sha256,byte_count,mime,error" + file_column + " FROM media WHERE event_sequence=? AND canonical_media_id IS NULL ORDER BY id", (sequence,))]
            for item in items:
                result = db.execute("SELECT state,result_json,error FROM image_understanding WHERE sha256=? AND policy_version=1", (item.get("sha256"),)).fetchone()
                if result:
                    item["vision_state"] = result["state"]
                    if result["state"] == "done":
                        item["image_understanding"] = json.loads(result["result_json"] or "{}")
                    elif result["state"] == "error":
                        item["vision_error"] = result["error"]
        return items

    def media_file(self, media_id: int):
        with self.connect() as db:
            row = db.execute("SELECT file_name,mime FROM media WHERE id=? AND state='stored'", (media_id,)).fetchone()
        return dict(row) if row else None

    def latest_source_timestamp(self):
        with self.connect() as db:
            row = db.execute("SELECT MAX(source_timestamp) FROM events WHERE timestamp_valid=1").fetchone()
        return row[0]

    def get_meta(self, key: str):
        with self.connect() as db:
            row = db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def set_meta(self, key: str, value: str | None):
        with self.write_lock, self.connect() as db:
            if value is None:
                db.execute("DELETE FROM meta WHERE key=?", (key,))
            else:
                db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES (?,?)", (key, value))

    def coverage(self, day: str, *, rule_id: str = ""):
        with self.connect() as db:
            events = [dict(row) for row in db.execute("""SELECT source_id,robot_id,group_id,sender_id,direction,
                parse_state,source_timestamp,seen_count FROM events WHERE substr(source_timestamp,1,10)=?
                ORDER BY source_timestamp,sequence""", (day,))]
            bots = [dict(row) for row in db.execute("SELECT robot_id,checked_at FROM source_bots WHERE checked_at=(SELECT MAX(checked_at) FROM source_bots) ORDER BY robot_id")]
            runs = [dict(row) for row in db.execute("""SELECT id,start_at,end_at,state,bot_count,pages,rows_seen,added,errors_json
                FROM history_runs WHERE substr(start_at,1,10)<=? AND substr(end_at,1,10)>=? ORDER BY id DESC LIMIT 20""",
                (day, day))]
            progress = [dict(row) for row in db.execute("SELECT robot_id,state,pages,rows_seen,error,updated_at FROM history_progress WHERE day=? ORDER BY robot_id", (day,))]
            undated = db.execute("SELECT COUNT(*) FROM events WHERE source_timestamp IS NULL").fetchone()[0]
        return {"date": day, "rule_id": str(rule_id or ""), "robots": bots, "source_account_id": self.source_account_id, "source_event_count": len(events),
                "undated_event_count": undated,
                "source_ids": [item["source_id"] for item in events if item["source_id"]],
                "idless_count": sum(not item["source_id"] for item in events),
                "events": events, "history_runs": runs, "progress": progress}


