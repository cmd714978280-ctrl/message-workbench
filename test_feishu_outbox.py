import base64
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import requests

from ingestion_store import IngestionStore
from workflow_store import WorkflowStore
from phase3 import Phase3
from feishu_outbox import FeishuOutbox,REQUIRED,verify_record


class FakeClient:
    def __init__(self):
        self.records={};self.tokens={};self.upload_count=0;self.create_count=0
        self.timeout_after_create=False;self.upload_failure=False;self.read_failure=False
    def fields(self):return [{"field_name":k,"type":v} for k,v in REQUIRED['feedback'].items()]
    def find(self,field,key):return next((r for r in self.records.values() if r['fields'].get(field)==key),None)
    def upload(self,path,app):
        if self.upload_failure:raise requests.Timeout()
        self.upload_count+=1;return 'file-'+str(self.upload_count)
    def create(self,fields,token):
        if token in self.tokens:return self.records[self.tokens[token]]
        self.create_count+=1;rid='rec-'+str(self.create_count)
        record={'record_id':rid,'fields':json.loads(json.dumps(fields))};self.records[rid]=record;self.tokens[token]=rid
        if self.timeout_after_create:self.timeout_after_create=False;raise requests.Timeout()
        return record
    def read(self,rid):
        if self.read_failure:raise requests.Timeout()
        return self.records[rid]


class ExportTests(unittest.TestCase):
    def setUp(self):
        root=Path(__file__).parent/'data/test-tmp';root.mkdir(exist_ok=True,parents=True)
        self.temp=tempfile.TemporaryDirectory(dir=root);self.root=Path(self.temp.name)
        self.store=IngestionStore(self.root/'messages.sqlite3');self.workflow=WorkflowStore(self.store)
        self.media=self.root/'media';self.media.mkdir()
        self.store.save_workspace_config({'galleryUrl':'https://example.feishu.cn/base/APP123?table=tbl123'},[])
        raw={'msgid':'photo','chatroom':{'id':'g1'},'fromUser':{'id':'u1'},'content':'晒图'}
        norm={'content':'晒图，质量很好','group_id':'g1','sender_id':'u1','group':'群一','sender':'客户一','timestamp':'2026-10-21T10:00:00+08:00'}
        seq,_=self.store.add_event(raw,{},norm,'test',display=True,parse_state='parsed',timestamp_valid=True,direction='inbound')
        self.store.save_analysis(seq,17,{'primary':'feedback','labels':['feedback'],'version':17,'conversation_id':'s1','features':{}})
        data=base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jYykAAAAASUVORK5CYII=')
        (self.media/'a.png').write_bytes(data)
        with self.store.connect() as db:
            db.execute("INSERT INTO media(event_sequence,url,url_hash,state,file_name,sha256,byte_count,mime) VALUES (?,?,?,'stored','a.png',?,?,'image/png')",(seq,'https://example.test/a','a',hashlib.sha256(data).hexdigest(),len(data)))
        self.workflow.rebuild_cases(Phase3(self.store,self.root/'missing.env'),17)
        self.item=self.workflow.processing_detail(self.workflow.processing_page('all')['items'][0]['case_id'])
        self.client=FakeClient();self.outbox=FeishuOutbox(self.store,self.workflow,self.media,lambda url:self.client)

    def tearDown(self):self.temp.cleanup()
    def confirm(self):
        self.workflow.decide_processing(self.item['case_id'],self.item['id'],decision='confirmed',plan_fingerprint=self.item['payload']['plan_fingerprint'])
    def queued(self):
        self.confirm();self.outbox.prepare();return self.outbox.page()['items'][0]['id']

    def test_individual_image_can_be_approved_without_confirming_entire_case(self):
        task=next(t for t in self.item['payload']['processing_plan']['tasks'] if t['route']=='feedback')
        self.outbox.approve_image_task(self.item['case_id'],self.item['id'],self.item['payload']['plan_fingerprint'],task['key'])
        self.assertEqual(self.outbox.prepare()['added'],1)
        job=self.outbox.page()['items'][0]['id'];self.outbox.run(job_id=job)
        self.assertEqual(self.outbox._record(job)['state'],'succeeded')
        self.assertEqual(self.client.create_count,1)

    def test_individual_image_approval_rejects_stale_plan(self):
        task=next(t for t in self.item['payload']['processing_plan']['tasks'] if t['route']=='feedback')
        with self.assertRaises(ValueError):
            self.outbox.approve_image_task(self.item['case_id'],self.item['id'],'outdated',task['key'])

    def test_only_confirmed_tasks_are_prepared_and_delivery_is_idempotent(self):
        self.assertEqual(self.outbox.prepare()['added'],0)
        jid=self.queued()
        self.assertEqual(self.outbox.prepare()['existing'],1)
        self.outbox.run()
        self.assertEqual(self.outbox._record(jid)['state'],'succeeded')
        self.assertEqual((self.client.create_count,self.client.upload_count),(1,1))
        self.outbox.prepare();self.outbox.run(retry=True)
        self.assertEqual((self.client.create_count,self.client.upload_count),(1,1))
        self.assertIn('质量很好',self.client.records['rec-1']['fields']['配文'])

    def test_timeout_after_remote_create_is_reconciled_without_uploading_or_creating_again(self):
        jid=self.queued();self.client.timeout_after_create=True;self.outbox.run()
        self.assertEqual(self.outbox._record(jid)['state'],'uncertain')
        self.outbox=FeishuOutbox(self.store,self.workflow,self.media,lambda url:self.client)
        self.outbox.run(job_id=jid,retry=True)
        self.assertEqual(self.outbox._record(jid)['state'],'succeeded')
        self.assertEqual((self.client.create_count,self.client.upload_count),(1,1))

    def test_success_is_only_reported_after_remote_readback(self):
        jid=self.queued();self.client.read_failure=True;self.outbox.run()
        self.assertEqual(self.outbox._record(jid)['state'],'uncertain')
        self.client.read_failure=False;self.outbox.run(job_id=jid,retry=True)
        self.assertEqual(self.outbox._record(jid)['state'],'succeeded')
        self.assertEqual(self.client.create_count,1)

    def test_upload_failure_can_retry_and_is_not_a_successful_record(self):
        jid=self.queued();self.client.upload_failure=True;self.outbox.run()
        self.assertEqual(self.outbox._record(jid)['state'],'failed')
        self.assertEqual(self.client.create_count,0)
        self.client.upload_failure=False;self.outbox.run(job_id=jid,retry=True)
        self.assertEqual(self.outbox._record(jid)['state'],'succeeded')

    def test_changed_approval_target_or_media_stops_before_remote_calls(self):
        jid=self.queued();config=self.store.get_workspace_config()
        config['settings']['galleryUrl']='https://example.feishu.cn/base/APP123?table=tblOther'
        self.store.save_workspace_config(config['settings'],config['faqs']);self.outbox.run()
        self.assertEqual(self.outbox._record(jid)['state'],'stale')
        self.assertEqual(self.client.upload_count,0)

    def test_changed_file_is_rejected_without_upload(self):
        jid=self.queued();(self.media/'a.png').write_bytes(b'changed');self.outbox.run()
        self.assertEqual(self.outbox._record(jid)['state'],'stale')
        self.assertEqual(self.client.create_count,0)

    def test_withdrawn_confirmation_stops_export_before_upload(self):
        jid=self.queued()
        self.workflow.decide_processing(self.item['case_id'],self.item['id'],decision='human',plan_fingerprint=self.item['payload']['plan_fingerprint'])
        self.outbox.run()
        self.assertEqual(self.outbox._record(jid)['state'],'stale')
        self.assertEqual(self.client.upload_count,0)

    def test_existing_inconsistent_remote_key_is_not_overwritten(self):
        jid=self.queued();job=self.outbox._record(jid)
        self.client.records['foreign']={'record_id':'foreign','fields':{'来源键':job['source_key'],'配文':'不同内容'}}
        self.outbox.run()
        self.assertNotEqual(self.outbox._record(jid)['state'],'succeeded')
        self.assertEqual(self.client.create_count,0)

    def test_worker_lease_prevents_two_exporters_running_together(self):
        self.queued()
        import time
        with self.store.connect() as db:db.execute("INSERT INTO feishu_export_lease VALUES (1,'other',?)",(time.time()+300,))
        with self.assertRaisesRegex(ValueError,'正在执行'):self.outbox.run()
        self.assertEqual(self.client.upload_count,0)

    def test_remote_rich_text_and_empty_fields_verify_but_wrong_caption_does_not(self):
        verify_record({'fields':{'配文':[{'text':'原文'}],'图片':[{'file_token':'f','name':'a.png'}]}},
                      {'配文':'原文','图片':[{'file_token':'f'}],'商品':''})
        with self.assertRaisesRegex(ValueError,'配文'):verify_record({'fields':{'配文':'另一句'}},{'配文':'原文'})


if __name__=='__main__':unittest.main()
