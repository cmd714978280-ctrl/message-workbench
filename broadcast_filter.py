"""Conservative, reversible filtering of unaddressed cross-group broadcasts."""
import collections
import json
import re
from datetime import datetime, timedelta, timezone


def filter_broadcasts(store, *, now=None, window_seconds=120, group_threshold=10):
    now = now or datetime.now(timezone.utc)
    start = (now - timedelta(days=1)).astimezone(timezone(timedelta(hours=8))).isoformat(timespec="seconds")
    groups = collections.defaultdict(list)
    with store.write_lock, store.connect() as db:
        db.execute("""CREATE TABLE IF NOT EXISTS broadcast_suppressions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,event_sequence INTEGER NOT NULL UNIQUE,
            previous_display INTEGER NOT NULL,previous_parse_state TEXT NOT NULL,
            group_count INTEGER NOT NULL,created_at TEXT NOT NULL)""")
        rows = db.execute("""SELECT e.* FROM events e LEFT JOIN review_decisions r ON r.event_sequence=e.sequence
            WHERE e.source_timestamp>=?
            AND e.direction IN ('inbound','outbound') AND e.group_id<>'' AND r.event_sequence IS NULL""", (start,)).fetchall()
        for row in rows:
            try:
                payload = json.loads(row['raw_json'] or '{}'); raw = payload.get('data') or {}
                stamp = datetime.fromisoformat(row['source_timestamp'])
                if stamp.tzinfo is None: stamp=stamp.replace(tzinfo=timezone(timedelta(hours=8)))
            except (ValueError, TypeError):
                continue
            if not isinstance(raw,dict) or raw.get('replyMessageId') or raw.get('is_yinyong') in (1,'1',True) or raw.get('at_wxid') or raw.get('at_wxids'):
                continue
            if row['direction']=='outbound':
                recipient=str(raw.get('to_wxid') or raw.get('toContactId') or '')
                if not (recipient.startswith('R:') or recipient.endswith('@chatroom')): continue
            else:
                recipient=str(raw.get('to_wxid') or raw.get('toContactId') or '')
                if recipient and not (recipient.startswith('R:') or recipient.endswith('@chatroom')): continue
            content=raw.get('content')
            if not isinstance(content,str) or not content.strip(): continue
            content=content.strip()
            if re.search(r'@|\{\$@\}|不支持显示的消息类型|^\[图片消息\]$',content): continue
            groups[content].append((stamp,row['group_id'],row))
        selected={}
        for items in groups.values():
            items.sort(key=lambda item:item[0]);left=0
            for right in range(len(items)):
                while (items[right][0]-items[left][0]).total_seconds()>window_seconds: left+=1
                cluster=items[left:right+1];count=len({x[1] for x in cluster})
                if count>=group_threshold:
                    for _,_,row in cluster:
                        if row['parse_state']!='suppressed_duplicate_broadcast':
                            selected[row['sequence']]=(row,count)
        stamp=now.isoformat(timespec='seconds')
        for sequence,(row,count) in selected.items():
            db.execute('INSERT OR IGNORE INTO broadcast_suppressions(event_sequence,previous_display,previous_parse_state,group_count,created_at) VALUES (?,?,?,?,?)',
                       (sequence,row['display'],row['parse_state'],count,stamp))
            db.execute("UPDATE events SET display=0,parse_state='suppressed_duplicate_broadcast' WHERE sequence=?",(sequence,))
            # Mark the affected scope dirty using the existing durable change log.
            db.execute('INSERT INTO review_log(event_sequence,previous_json,decision_json,changed_at) VALUES (?,?,?,?)',
                       (sequence,'{}',json.dumps({'action':'auto_cross_group_filter','groups':count}),stamp))
            case_ids=[r[0] for r in db.execute('SELECT case_id FROM case_messages WHERE event_sequence=?',(sequence,))]
            db.execute('DELETE FROM case_messages WHERE event_sequence=?',(sequence,))
            for case_id in case_ids:
                if not db.execute('SELECT 1 FROM case_messages WHERE case_id=?',(case_id,)).fetchone():
                    db.execute("UPDATE support_cases SET state='merged',merged_into=NULL,updated_at=? WHERE id=?",(stamp,case_id))
        return len(selected)
