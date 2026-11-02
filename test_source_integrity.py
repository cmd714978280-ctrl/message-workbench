import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import server
from ingestion_store import IngestionStore
from source_config import configured_rule_id, configured_source_account_id


class SourceIntegrityRegressions(unittest.TestCase):
    def test_local_only_recalled_event_is_identified_without_deleting_it(self):
        from audit_source_window import local_revoked_identities
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            store=IngestionStore(Path(folder)/'messages.sqlite3')
            raw={'msgid':'recalled','is_chehui':1,'chatroom':{'id':'g'},'content':'消息'}
            store.add_event(raw,{}, {'timestamp':'2026-11-01T10:00:00+08:00'},'websocket',
                display=True,parse_state='parsed',direction='inbound',timestamp_valid=True)
            with patch.object(server,'STORE',store):
                found=local_revoked_identities('2026-11-01T09:00:00+08:00','2026-11-01T11:00:00+08:00',store.source_account_id)
            self.assertEqual(found,{'source:recalled'})
            with store.connect() as db:self.assertEqual(db.execute('select count(*) from events').fetchone()[0],1)

    def test_rule_id_is_read_from_config_and_source_identity_is_independent(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            env_file=Path(folder)/'.env'
            env_file.write_text('YUNZHONGKE_RULE_ID=rule-next\nYUNZHONGKE_SOURCE_ACCOUNT_ID=yunzhongke:stable\n',encoding='utf-8')
            self.assertEqual(configured_rule_id(env_file),'rule-next')
            self.assertEqual(configured_source_account_id(env_file),'yunzhongke:stable')

    def test_coverage_uses_the_runtime_rule_id_argument(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            store=IngestionStore(Path(folder)/'messages.sqlite3',source_account_id='account-main')
            coverage=store.coverage('2026-10-30',rule_id='rule-current')
            self.assertEqual(coverage['rule_id'],'rule-current')
            self.assertEqual(coverage['source_account_id'],'account-main')

    def test_refreshed_image_signature_does_not_create_another_attachment(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            store=IngestionStore(Path(folder)/'messages.sqlite3')
            raw={'msgid':'image','chatroom':{'id':'g'}}
            kw=dict(display=True,parse_state='parsed',direction='inbound',timestamp_valid=True)
            seq,_=store.add_event(raw,{}, {'timestamp':'2026-10-30T09:00:00+08:00',
                'image_urls':['https://oss.iyunzk.com/file.jpg?key=old&time=1']},'websocket',**kw)
            store.add_event(raw,{}, {'timestamp':'2026-10-30T09:00:00+08:00',
                'image_urls':['https://oss.iyunzk.com/file.jpg?key=new&time=2']},'history',**kw)
            media=store.media_for_event(seq)
            self.assertEqual(len(media),1)
            self.assertIn('key=new',media[0]['url'])

    def test_voice_url_is_not_registered_as_an_image(self):
        raw={'msgid':'voice','sourceType':'group','type':'audio','sendTime':'2026-10-30 09:00:00',
             'content':'https://oss.iyunzk.com/test.mp3','chatroom':{'id':'g'},'fromUser':{'id':'c'}}
        msg=server.normalize(raw)
        self.assertEqual(msg['image_urls'],[])
        self.assertEqual(msg['attachments'],0)
        self.assertEqual(msg['content'],'[语音消息]')

    def test_temporary_image_404_is_scheduled_for_retry(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            store=IngestionStore(Path(folder)/'messages.sqlite3')
            store.add_event({'msgid':'image','chatroom':{'id':'g'}},{},
                {'timestamp':'2026-10-30T09:00:00+08:00','image_urls':['https://oss.iyunzk.com/image.jpg']},
                'history',display=True,parse_state='parsed',direction='inbound',timestamp_valid=True)
            with store.connect() as db:
                mid=db.execute('SELECT id FROM media').fetchone()[0]
            store.media_result(mid,'failed',error='404 Client Error')
            self.assertEqual(store.media_pending(),[])
            with store.connect() as db:
                row=db.execute('SELECT attempt_count,retry_after FROM media WHERE id=?',(mid,)).fetchone()
                self.assertEqual(row['attempt_count'],1)
                self.assertIsNotNone(row['retry_after'])
                db.execute("UPDATE media SET retry_after='2000-01-01T00:00:00+00:00' WHERE id=?",(mid,))
            self.assertEqual(store.media_pending()[0]['id'],mid)

    def test_websocket_sent_record_is_stored_without_becoming_customer_task(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            store=IngestionStore(Path(folder)/'messages.sqlite3')
            event={'type':'wxmsg_send','_id':'cloud-reply','data':{
                'msgid':'reply-1','sourceType':'group','type':'text','sendTime':'2026-10-30 09:00:01',
                'content':'已回复','replyMessageId':'cloud-question','chatroom':{'id':'g'},
                'fromUser':{'id':'operator'},'to_wxid':'customer'}}
            with patch.object(server,'STORE',store),patch.object(server,'sync_enabled',True):
                self.assertTrue(server.receive_source_event(event))
            with store.connect() as db:
                row=db.execute('select * from events').fetchone()
            self.assertEqual(row['direction'],'outbound')
            self.assertEqual(row['display'],0)
            self.assertEqual(row['parse_state'],'context_only')
            self.assertEqual(json.loads(row['raw_json'])['outer']['_id'],'cloud-reply')

    def test_history_duplicate_enriches_cloud_id_and_links_the_exact_original_message(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            store=IngestionStore(Path(folder)/'messages.sqlite3')
            raw={'msgid':'wx-message','id':1234.5,'content':'有货吗','chatroom':{'id':'g'},'fromUser':{'id':'customer'}}
            kw=dict(display=True,parse_state='parsed',direction='inbound',timestamp_valid=True,source_timestamp='2026-10-30T09:00:00+08:00')
            question,_=store.add_event(raw,{}, {'content':'有货吗','timestamp':kw['source_timestamp']},'websocket',**kw)
            _,added=store.add_event(raw,{'_id':'cloud-question'}, {'content':'有货吗','timestamp':kw['source_timestamp']},'history',**kw)
            self.assertFalse(added)
            # A later duplicate without the envelope must not discard the alias.
            store.add_event(raw,{}, {'content':'有货吗','timestamp':kw['source_timestamp']},'websocket',**kw)
            response,_=store.add_event({'msgid':'response','replyMessageId':'cloud-question','chatroom':{'id':'g'}},
                {},None,'history',display=False,parse_state='context_only',direction='outbound',timestamp_valid=True,
                source_timestamp='2026-10-30T09:01:00+08:00')
            self.assertEqual(store.explicit_reply_target(response)['sequence'],question)
            with store.connect() as db:
                self.assertEqual(db.execute('select count(*) from events').fetchone()[0],2)

    def test_resume_does_not_skip_the_gap_between_scans(self):
        start=server.history_resume_start('2026-10-29T18:07:10+08:00',
            datetime.fromisoformat('2026-10-29T18:08:20+08:00'),{'end_at':'2026-10-29T17:52:29+08:00'})
        self.assertEqual(start.isoformat(),'2026-10-29T17:42:29+08:00')


if __name__=='__main__':unittest.main()
