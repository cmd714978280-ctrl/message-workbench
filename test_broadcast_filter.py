import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from broadcast_filter import filter_broadcasts
from ingestion_store import IngestionStore
from workflow_store import WorkflowStore


class BroadcastFilterTests(unittest.TestCase):
    def test_ten_groups_cross_accounts_and_late_copy_without_filtering_replies(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            store=IngestionStore(Path(folder)/'events.sqlite3'); WorkflowStore(store)
            def add(n,content,reply=False,recipient=None):
                raw={'msgid':str(n),'sendTime':'2026-11-02 10:00:00','sourceType':'group',
                     'content':content,'chatroom':{'id':'g'+str(n)},'fromUser':{'id':'u'+str(n)}}
                if reply: raw['replyMessageId']='customer-question'
                if recipient: raw['to_wxid']=recipient
                norm={'timestamp':'2026-11-02T10:00:00+08:00','content':content}
                return store.add_event(raw,{},norm,'websocket',display=True,parse_state='parsed',direction='inbound',timestamp_valid=True)[0]
            for n in range(9): add(n,'群发文案')
            now=datetime(2026,11,2,2,1,tzinfo=timezone.utc)
            self.assertEqual(filter_broadcasts(store,now=now),0)
            add(9,'群发文案'); target=add(99,'群发文案',reply=True); targeted=add(98,'群发文案',recipient='customer-id')
            self.assertEqual(filter_broadcasts(store,now=now),10)
            add(10,'群发文案')
            self.assertEqual(filter_broadcasts(store,now=now),1)
            with store.connect() as db:
                self.assertEqual(db.execute('select display from events where sequence=?',(target,)).fetchone()[0],1)
                self.assertEqual(db.execute('select display from events where sequence=?',(targeted,)).fetchone()[0],1)
                self.assertEqual(db.execute('select count(*) from broadcast_suppressions').fetchone()[0],11)


if __name__ == '__main__': unittest.main()
