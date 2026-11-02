"""Read-only source reconciliation; store missing events and all provider identifiers."""
import json
import time
from datetime import datetime, timedelta
from pathlib import Path

import server


def main():
    report_dir = Path(Path('data/reports/latest-semantic-audit.txt').read_text(encoding='utf-8'))
    snapshot = json.loads((report_dir / 'messages.json').read_text(encoding='utf-8'))
    start = datetime.fromisoformat('2026-10-29T16:00:00+08:00')
    end = datetime.fromisoformat(snapshot['started_at'])
    with server.STORE.connect() as db:
        before = {r['source_id'] for r in db.execute('SELECT source_id FROM events')}
    result = {'start': start.isoformat(), 'end': end.isoformat(), 'state': 'running',
              'robots': [], 'new_sequences': [], 'errors': [], 'source_rows': 0}
    source_rows = []
    run = server.STORE.begin_history(start.isoformat(), end.isoformat())
    session = None
    try:
        session = server.make_session()
        bots = server.data_of(server.api_json(session, '/wx/GroupXiaoXiZhongKong/getZdRobot',
            {'rule_id': server.RULE_ID, 'page': 1, 'size': 999, 'nickname': '',
             'group_keyword': '', 'group_search_type': ''}))
        if not bots:
            raise RuntimeError('机器人列表为空，无法核验覆盖')
        server.STORE.set_bots(bots)
        bots.sort(key=lambda b: str(b.get('wxid') or b.get('id') or ''))
        day = start.date()
        while day <= end.date():
            low = max(start, datetime.combine(day, datetime.min.time(), server.TZ))
            high = min(end, datetime.combine(day, datetime.max.time().replace(microsecond=0), server.TZ))
            for bot in bots:
                rid = str(bot.get('wxid') or bot.get('id') or '')
                cursor, seen = 0, set()
                progress = {'robot_id': rid, 'day': str(day), 'start': low.isoformat(),
                            'end': high.isoformat(), 'pages': 0, 'rows': 0, 'state': 'running'}
                result['robots'].append(progress)
                try:
                    for _ in range(server.MAX_HISTORY_PAGES):
                        payload = server.api_json(session, '/wx/GroupXiaoXiZhongKong/getMsg4Review',
                            {'rule_id': server.RULE_ID, 'robot_wxid': rid, 'sub_uid': str(bot.get('sub_uid') or ''),
                             'start_time': low.strftime('%Y-%m-%d %H:%M:%S'),
                             'end_time': high.strftime('%Y-%m-%d %H:%M:%S'), 'page': cursor})
                        if not isinstance(payload.get('data'), (list, dict)):
                            raise RuntimeError('历史响应结构变化：' + server.response_shape(payload))
                        batch = server.data_of(payload)
                        progress['pages'] += 1
                        if not batch:
                            progress['state'] = 'complete'
                            break
                        signature = server.STORE.dumps(batch)
                        if signature in seen:
                            raise RuntimeError('历史分页重复，不能证明覆盖完整')
                        seen.add(signature)
                        for outer in batch:
                            raw = outer.get('data') if isinstance(outer.get('data'), dict) else outer
                            norm = server.normalize(raw, outer)
                            if norm:
                                norm['source'] = 'history'
                            direction = 'outbound' if 'send' in str(raw.get('terminalType') or outer.get('type') or '').lower() else 'inbound'
                            valid = server.valid_source_timestamp(raw) or server.valid_source_timestamp(outer)
                            seq, added = server.STORE.add_event(raw, outer, norm, 'history', display=bool(norm),
                                parse_state='parsed' if norm else 'context_only', direction=direction,
                                timestamp_valid=valid, source_timestamp=server.message_timestamp(raw if server.valid_source_timestamp(raw) else outer).isoformat(timespec='seconds') if valid else None)
                            source_rows.append(outer)
                            if added:
                                result['new_sequences'].append(seq)
                            progress['rows'] += 1
                        next_cursor = batch[-1].get('create_time')
                        if not next_cursor or str(next_cursor) == str(cursor):
                            raise RuntimeError('缺少有效历史下一页游标')
                        cursor = next_cursor
                        time.sleep(2)
                    else:
                        raise RuntimeError('达到历史分页安全上限')
                except Exception as exc:
                    progress.update(state='partial', error=server.safe_upstream_text(str(exc), 200))
                    result['errors'].append(progress['error'])
                    raise
                finally:
                    server.STORE.save_history_cursor(rid, str(day), low.isoformat(), high.isoformat(),
                        None if progress['state'] == 'complete' else str(cursor), progress['state'],
                        progress['pages'], progress['rows'], progress.get('error', ''))
                    (report_dir / 'integrity.json').write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
                print(f"{day} {rid}: {progress['state']}, {progress['rows']} rows", flush=True)
                time.sleep(2)
            day += timedelta(days=1)
        result['state'] = 'complete'
    except Exception as exc:
        result['state'] = 'partial'
        message = server.safe_upstream_text(str(exc), 200)
        if message not in result['errors']:
            result['errors'].append(message)
        print('SOURCE_AUDIT_PARTIAL', message, flush=True)
    finally:
        if session is not None:
            session.close()
        result['source_rows'] = len(source_rows)
        result['unique_source_messages'] = len({str((r.get('data') or r).get('msgid') or (r.get('data') or r).get('id') or r.get('_id')) for r in source_rows})
        result['new_sequences'] = sorted(set(result['new_sequences']))
        server.STORE.end_history(run, result['state'], len({r['robot_id'] for r in result['robots']}),
            sum(r['pages'] for r in result['robots']), len(source_rows), len(result['new_sequences']), result['errors'])
        if result['state'] == 'complete':
            server.STORE.mark_recovered_history_runs()
        (report_dir / 'cloud_rows.json').write_text(json.dumps(source_rows, ensure_ascii=False), encoding='utf-8')
        (report_dir / 'integrity.json').write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
        print('RESULT', json.dumps({k:v for k,v in result.items() if k != 'robots'}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
