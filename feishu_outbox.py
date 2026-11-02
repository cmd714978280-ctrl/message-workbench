"""Explicit, durable Feishu export of confirmed image and demand tasks only."""
from __future__ import annotations

import hashlib
import json
import re
import threading
import time
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

from feishu_business_targets import API, TargetClient, TargetError, parse_target
from ingestion_store import source_is_revoked

LABELS={"pending":"待入库","running":"正在入库","succeeded":"入库成功","failed":"失败待重试",
        "uncertain":"结果待核对","stale":"资料已变更"}
ROUTES={"feedback":("galleryUrl","反馈图库","来源键"),"order_image":("orderTableUrl","订单图表","来源键"),
        "requirement":("demandTableUrl","需求登记表","SourceID"),"service_evidence":("aftercareEvidenceUrl","售后证据表","来源键")}
REQUIRED={
    "feedback":{"图片名称":1,"图片":17,"配文":1,"客户昵称":1,"来源键":1,"来源群号":1,"识别内容":1,"哈希指纹":1,"入库日期":5},
    "order_image":{"记录标识":1,"发生时间":5,"来源账号":1,"群名":1,"群ID":1,"用户名":1,"用户ID":1,
        "商品":1,"订单图":17,"配文":1,"消息ID":1,"客户对话ID":1,"处理状态":3,"登记时间":5,"来源键":1},
    "requirement":{"群名":1,"用户名":1,"需求产品":1,"时间":5,"状态":3,"SourceID":1,"备注":17},
    "service_evidence":{"记录标识":1,"发生时间":5,"群名":1,"群ID":1,"用户名":1,"用户ID":1,"问题描述":1,"售后证据图":17,"来源消息ID":1,"客户对话ID":1,"来源键":1,"登记时间":5},
}
PHASH_VERSION="miaoda-canvas-dct32-v1"
PHASH_RE=re.compile(r"^0x[0-9a-f]{16}$")


def dump(value): return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(",",":"))
def now(): return datetime.now(timezone.utc).isoformat(timespec="seconds")
def digest(value): return hashlib.sha256(dump(value).encode()).hexdigest()
def milliseconds(value):
    at=datetime.fromisoformat(str(value).replace("Z","+00:00"))
    if at.tzinfo is None: at=at.replace(tzinfo=timezone(timedelta(hours=8)))
    return int(at.timestamp()*1000)


class FeishuClient(TargetClient):
    def fields(self): return self.list_items("/fields")

    def find(self,field,key):
        data=self.request("POST","/records/search",params={"page_size":2},json={"filter":{"conjunction":"and",
            "conditions":[{"field_name":field,"operator":"is","value":[key]}]}})
        items=data.get("items") or []
        if data.get("has_more") or len(items)>1: raise ValueError("同一来源键存在多条飞书记录，需要人工核对")
        return items[0] if items else None

    def upload(self,path,app_token):
        with Path(path).open("rb") as stream:
            response=self.session.post(API+"/drive/v1/medias/upload_all",headers=self.headers,
                data={"file_name":Path(path).name,"parent_type":"bitable_image","parent_node":app_token,"size":str(Path(path).stat().st_size)},
                files={"file":(Path(path).name,stream)},timeout=(8,45))
        body=response.json()
        if body.get("code")!=0: raise TargetError(body.get("code"))
        response.raise_for_status()
        token=(body.get("data") or {}).get("file_token")
        if not token: raise ValueError("飞书未返回素材编号")
        return token

    def create(self,fields,client_token):
        record=self.request("POST","/records",params={"client_token":client_token},json={"fields":fields}).get("record") or {}
        if not record.get("record_id"): raise ValueError("飞书未返回记录编号")
        return record

    def read(self,record_id): return self.request("GET","/records/"+record_id).get("record") or {}


def normalized(value):
    if isinstance(value,list):
        if all(isinstance(v,dict) and "text" in v for v in value): return "".join(str(v["text"]) for v in value)
        if all(isinstance(v,dict) and "file_token" in v for v in value): return sorted(v["file_token"] for v in value)
    return value


def verify_record(record,fields):
    actual=record.get("fields") or {}
    for name,value in fields.items():
        if value=="" and actual.get(name) in (None,"",[]): continue
        if normalized(actual.get(name))!=normalized(value):
            raise ValueError("飞书写后核验不一致："+name)


class FeishuOutbox:
    def __init__(self,store,workflow,media_dir,client_factory=None):
        self.store,self.workflow,self.media_dir=store,workflow,Path(media_dir).resolve()
        self.client_factory=client_factory or (lambda url:FeishuClient(self.workflow._token(),url))
        self.owner=uuid.uuid4().hex
        self.dispatch_lock=threading.Lock()
        with self.store.write_lock,self.store.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS feishu_export_jobs(
                    id TEXT PRIMARY KEY,source_key TEXT NOT NULL UNIQUE,case_id TEXT NOT NULL,
                    suggestion_id TEXT NOT NULL,plan_fingerprint TEXT NOT NULL,decision_id INTEGER NOT NULL,
                    route TEXT NOT NULL,target_url TEXT NOT NULL,payload_json TEXT NOT NULL,payload_hash TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'pending',attempts INTEGER NOT NULL DEFAULT 0,
                    client_token TEXT NOT NULL,uploads_json TEXT NOT NULL DEFAULT '{}',request_json TEXT,
                    remote_record_id TEXT NOT NULL DEFAULT '',create_started INTEGER NOT NULL DEFAULT 0,
                    error TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS feishu_export_state ON feishu_export_jobs(state,created_at);
                CREATE TABLE IF NOT EXISTS feishu_export_attempts(id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,state TEXT NOT NULL,message TEXT NOT NULL,created_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS feishu_export_lease(id INTEGER PRIMARY KEY CHECK(id=1),owner TEXT NOT NULL,expires REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS image_task_approvals(case_id TEXT NOT NULL,task_key TEXT NOT NULL,
                    plan_fingerprint TEXT NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(case_id,task_key));
            """)

    def _record(self,job_id):
        with self.store.connect() as db: row=db.execute("SELECT * FROM feishu_export_jobs WHERE id=?",(job_id,)).fetchone()
        if not row: raise ValueError("入库任务不存在")
        return dict(row)

    def _save(self,job_id,**values):
        allowed={"state","uploads_json","request_json","remote_record_id","create_started","error"}
        if not set(values)<=allowed: raise ValueError("入库状态更新字段错误")
        values["updated_at"]=now()
        with self.store.write_lock,self.store.connect() as db:
            db.execute("UPDATE feishu_export_jobs SET "+",".join(k+"=?" for k in values)+" WHERE id=?",[*values.values(),job_id])

    def _log(self,job_id,state,message):
        with self.store.write_lock,self.store.connect() as db:
            db.execute("INSERT INTO feishu_export_attempts(job_id,state,message,created_at) VALUES (?,?,?,?)",(job_id,state,message[:400],now()))

    def _file(self,media_id):
        with self.store.connect() as db: row=db.execute("SELECT * FROM media WHERE id=?",(media_id,)).fetchone()
        if not row or row["state"]!="stored": raise ValueError("图片未完整接收")
        name=str(row["file_name"] or "");path=(self.media_dir/name).resolve()
        if not name or Path(name).name!=name or not path.is_relative_to(self.media_dir) or not path.is_file(): raise ValueError("本地图片不存在")
        if not 0<path.stat().st_size<=15*1024*1024 or hashlib.sha256(path.read_bytes()).hexdigest()!=row["sha256"]: raise ValueError("本地图片内容校验失败")
        return {"id":int(media_id),"sha256":row["sha256"],"file_name":name,"mime":row["mime"],"event_sequence":row["event_sequence"]}

    def _snapshot(self,item,task):
        p=item["payload"];route=task["route"];settings=self.store.get_workspace_config()["settings"]
        target=str(settings.get(ROUTES[route][0]) or "")
        parse_target(target)
        if target!=task.get("destination_url"): raise ValueError("流转目标已变化，请先更新并确认处理方案")
        seqs=sorted(set(task.get("source_sequences") or []));messages={m["sequence"]:m for m in p["messages"]}
        if not seqs or not set(seqs)<=set(messages): raise ValueError("任务缺少对应原消息")
        refs=[]
        with self.store.connect() as db:
            for seq in seqs:
                event=db.execute("SELECT source_id,source_timestamp,source_account_id,group_id,sender_id,direction,raw_json FROM events WHERE sequence=?",(seq,)).fetchone()
                if not event or (event["source_account_id"],event["group_id"],event["sender_id"],event["direction"])!=(p["source_account_id"],p["group_id"],p["sender_id"],"inbound"): raise ValueError("任务来源不属于当前客户")
                raw=json.loads(event["raw_json"] or "{}")
                if source_is_revoked(raw.get("data"),raw.get("outer")): raise ValueError("来源消息已撤回，暂停入库")
                refs.append({"source_id":event["source_id"],"timestamp":event["source_timestamp"],"sequence":seq})
            decision=db.execute("SELECT id FROM processing_decisions WHERE case_id=? ORDER BY id DESC LIMIT 1",(item["case_id"],)).fetchone()
        media=[]
        if route in {"feedback","order_image","service_evidence"}: media=[self._file(int(task.get("media_id") or 0))]
        elif route=="requirement":
            ids={int(m["id"]) for seq in seqs for m in messages[seq].get("media") or []}
            media=[self._file(mid) for mid in sorted(ids)]
        identity=[p["source_account_id"],p["group_id"],route,parse_target(target),refs[0]["source_id"]]
        if media and route!="requirement":identity.append(media[0]["sha256"])
        source_key="mw-"+digest(identity)
        return {"source_key":source_key,"case_id":item["case_id"],"suggestion_id":item["id"],
            "plan_fingerprint":p["plan_fingerprint"],"decision_id":decision["id"] if decision and item["processing_state"]=="confirmed" else 0,"task_key":task["key"],"route":route,"target_url":target,
            "customer":p.get("sender_name") or "客户昵称待补","group_name":p.get("group_name") or p["group_id"],
            "group_id":p["group_id"],"sender_id":p["sender_id"],"source_account_id":p["source_account_id"],
            "product":"、".join((p.get("business_context") or {}).get("matched_products") or []),
            "caption":str(task.get("note") or ""),"source_messages":[messages[seq]["content"] for seq in seqs],
            "source_refs":refs,"media":media,"occurred_at":refs[0]["timestamp"],"registered_at":now()}

    def _confirmed_items(self,case_id=""):
        if case_id:
            item=self.workflow.processing_detail(case_id);items=[item] if item else []
        else:
            items=[];offset=0
            while True:
                page=self.workflow.processing_page("confirmed",limit=200,offset=offset)
                items.extend(self.workflow.processing_detail(r["case_id"]) for r in page["items"])
                if not page["has_more"]:break
                offset+=len(page["items"])
            with self.store.connect() as db:
                approved=[r[0] for r in db.execute('SELECT DISTINCT case_id FROM image_task_approvals')]
            seen={item['case_id'] for item in items if item}
            items.extend(self.workflow.processing_detail(cid) for cid in approved if cid not in seen)
        return items

    def approve_image_task(self,case_id,suggestion_id,plan_fingerprint,task_key):
        item=self.workflow.processing_detail(case_id)
        if not item or item['id']!=suggestion_id or item['payload'].get('plan_fingerprint')!=plan_fingerprint:
            raise ValueError('处理方案已变化，请刷新后重新确认图片')
        task=next((x for x in item['payload']['processing_plan']['tasks'] if x.get('key')==task_key),None)
        if not task or task.get('route') not in {'feedback','order_image','service_evidence'} or task.get('blockers'):
            raise ValueError('图片任务不可确认，请先解决图片或目标表问题')
        if any(t.get('route')=='classification' and set(t.get('source_sequences') or []) & set(task.get('source_sequences') or [])
               for t in item['payload']['processing_plan']['tasks']):
            raise ValueError('该图片存在分类冲突，请先复核分类')
        self._snapshot(item,task)
        with self.store.write_lock,self.store.connect() as db:
            db.execute('INSERT OR REPLACE INTO image_task_approvals(case_id,task_key,plan_fingerprint,created_at) VALUES (?,?,?,?)',
                       (case_id,task_key,plan_fingerprint,now()))
        return {'case_id':case_id,'task_key':task_key,'state':'approved'}

    def _task_authorized(self,item,task):
        if item['processing_state']=='confirmed': return True
        with self.store.connect() as db:
            return bool(db.execute('SELECT 1 FROM image_task_approvals WHERE case_id=? AND task_key=? AND plan_fingerprint=?',
                (item['case_id'],task['key'],item['payload']['plan_fingerprint'])).fetchone())

    @staticmethod
    def _saved_phash(payload):
        media=payload.get("media") or []
        return bool(media and media[0].get("phash_version")==PHASH_VERSION and
                    PHASH_RE.fullmatch(str(media[0].get("phash") or "")))

    def hash_candidates(self,case_id=""):
        """Return only feedback images needing the browser-compatible Miaoda pHash."""
        candidates={};errors=[]
        for item in self._confirmed_items(case_id):
            if not item:continue
            for task in item["payload"]["processing_plan"]["tasks"]:
                if task.get("route")!="feedback" or task.get("blockers") or not self._task_authorized(item,task):continue
                try:
                    payload=self._snapshot(item,task);media=payload["media"][0]
                    if self._saved_phash(payload):continue
                    with self.store.connect() as db:
                        prior=db.execute("SELECT state,request_json,remote_record_id,create_started,payload_json FROM feishu_export_jobs WHERE source_key=?",
                                         (payload["source_key"],)).fetchone()
                    if prior:
                        old=json.loads(prior["payload_json"])
                        if self._saved_phash(old):continue
                        if (prior["state"] in {"running","succeeded","uncertain"} or prior["request_json"] or
                                prior["remote_record_id"] or prior["create_started"]):continue
                        if prior["state"]=="stale":continue
                    candidates[int(media["id"]) ]={"media_id":int(media["id"]),"sha256":media["sha256"],
                        "image_url":"/api/media/"+str(int(media["id"]))}
                except (ValueError,TypeError,KeyError) as exc:
                    errors.append({"case_id":item["case_id"],"error":str(exc)[:200]})
        return {"items":list(candidates.values()),"errors":errors,"algorithm":PHASH_VERSION}

    def prepare(self,case_id="",phashes=None,require_phash=False):
        phash_by_media={}
        if phashes is None:phashes=[]
        if not isinstance(phashes,list) or len(phashes)>5000:raise ValueError("图片指纹清单格式无效")
        for value in phashes:
            if not isinstance(value,dict):raise ValueError("图片指纹清单格式无效")
            try:media_id=int(value.get("media_id"))
            except (TypeError,ValueError):raise ValueError("图片编号无效")
            sha=str(value.get("sha256") or "").lower();phash=str(value.get("phash") or "").lower()
            if media_id<=0 or not re.fullmatch(r"[0-9a-f]{64}",sha) or not PHASH_RE.fullmatch(phash):
                raise ValueError("图片指纹格式无效")
            if media_id in phash_by_media:raise ValueError("图片指纹清单含重复图片")
            phash_by_media[media_id]={"sha256":sha,"phash":phash}
        if require_phash:
            needed=self.hash_candidates(case_id)["items"]
            expected={int(value["media_id"]):value["sha256"] for value in needed}
            if set(phash_by_media)!=set(expected) or any(phash_by_media[mid]["sha256"]!=sha for mid,sha in expected.items()):
                raise ValueError("图片列表已变化或未完成全部妙搭指纹计算，请重新收集")
        items=self._confirmed_items(case_id)
        added=existing=0;errors=[]
        for item in items:
            if not item: continue
            for task in item["payload"]["processing_plan"]["tasks"]:
                if task["route"] not in ROUTES: continue
                if not self._task_authorized(item,task):continue
                try:
                    if task.get("blockers"): raise ValueError("入库任务仍有未解决问题")
                    payload=self._snapshot(item,task);key=payload["source_key"];jid="export-"+key[3:27]
                    with self.store.connect() as db:
                        prior=db.execute("SELECT * FROM feishu_export_jobs WHERE source_key=?",(key,)).fetchone()
                    old_payload=json.loads(prior["payload_json"]) if prior else None
                    if prior and (not require_phash or self._saved_phash(old_payload) or prior["state"] in {"running","succeeded","uncertain"} or
                                  prior["request_json"] or prior["remote_record_id"] or prior["create_started"] or prior["state"]=="stale"):
                        existing+=1;continue
                    if task["route"]=="feedback":
                        media=payload["media"][0];submitted=phash_by_media.get(int(media["id"]))
                        if submitted:
                            if submitted["sha256"]!=media["sha256"]:raise ValueError("计算指纹时图片内容已变化")
                            media["phash"]=submitted["phash"];media["phash_version"]=PHASH_VERSION
                        elif require_phash and not self._saved_phash(payload):
                            raise ValueError("缺少妙搭兼容图片指纹，请重新收集")
                        if require_phash:payload["require_miaoda_phash"]=True
                    with self.store.write_lock,self.store.connect() as db:
                        if prior:
                            if task["route"]=="feedback" and not self._saved_phash(payload):
                                raise ValueError("待入库图库任务缺少妙搭兼容指纹")
                            db.execute("UPDATE feishu_export_jobs SET payload_json=?,payload_hash=?,state='pending',error='',updated_at=? WHERE id=?",
                                (dump(payload),digest(payload),now(),prior["id"]))
                            existing+=1;continue
                        db.execute("""INSERT INTO feishu_export_jobs(id,source_key,case_id,suggestion_id,plan_fingerprint,decision_id,
                            route,target_url,payload_json,payload_hash,client_token,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                            (jid,key,item["case_id"],item["id"],payload["plan_fingerprint"],payload["decision_id"],task["route"],payload["target_url"],dump(payload),digest(payload),str(uuid.uuid4()),now(),now()))
                        added+=1
                except (ValueError,TypeError,KeyError) as exc:errors.append({"case_id":item["case_id"],"error":str(exc)[:200]})
        legacy_hash_count=0
        with self.store.connect() as db:
            rows=db.execute("SELECT route,state,payload_json FROM feishu_export_jobs WHERE route='feedback' AND state='succeeded'").fetchall()
        for row in rows:
            try:
                if not self._saved_phash(json.loads(row["payload_json"])):legacy_hash_count+=1
            except (ValueError,TypeError):legacy_hash_count+=1
        return {"added":added,"existing":existing,"errors":errors,"legacy_hash_repair_required":legacy_hash_count}

    def page(self,limit=100):
        with self.store.connect() as db:
            rows=db.execute("SELECT id,case_id,route,target_url,state,attempts,error,remote_record_id,updated_at,payload_json FROM feishu_export_jobs ORDER BY created_at,id LIMIT ?",(max(1,min(int(limit),500)),)).fetchall()
            counts={r[0]:r[1] for r in db.execute("SELECT state,COUNT(*) FROM feishu_export_jobs GROUP BY state")}
            lease=db.execute("SELECT expires FROM feishu_export_lease WHERE id=1 AND expires>?",(time.time(),)).fetchone()
        items=[]
        for row in rows:
            value=dict(row);p=json.loads(value.pop("payload_json"));value.update(customer=p["customer"],group_name=p["group_name"],
                route_label=ROUTES[value["route"]][1],state_label=LABELS[value["state"]],caption=p["caption"],media_ids=[m["id"] for m in p["media"]],
                media_hashes=[{"id":m["id"],"sha256":m.get("sha256",""),"phash":m.get("phash",""),"phash_version":m.get("phash_version","")} for m in p["media"]])
            items.append(value)
        legacy_hash_repair_required=0
        with self.store.connect() as db:
            legacy=db.execute("SELECT payload_json FROM feishu_export_jobs WHERE route='feedback' AND state='succeeded'").fetchall()
        for row in legacy:
            try:
                if not self._saved_phash(json.loads(row["payload_json"])):legacy_hash_repair_required+=1
            except (ValueError,TypeError):legacy_hash_repair_required+=1
        return {"items":items,"counts":counts,"running":bool(lease),"labels":LABELS,"message_sending_enabled":False,
                "legacy_hash_repair_required":legacy_hash_repair_required}

    def configure(self,create_missing=False):
        settings=self.store.get_workspace_config()["settings"];result=[]
        for route,(setting,label,_) in ROUTES.items():
            if route == "service_evidence" and not settings.get(setting): continue
            client=self.client_factory(settings[setting]);fields=client.fields();by_name={f["field_name"]:f for f in fields}
            for name,kind in REQUIRED[route].items():
                if name not in by_name and create_missing and route=="feedback" and name in {"来源键","客户昵称","配文"}:
                    client.request("POST","/fields",json={"field_name":name,"type":kind})
                    by_name[name]={"type":kind}
                if name not in by_name or by_name[name]["type"]!=kind: raise ValueError(label+"字段不兼容："+name)
            result.append({"route":route,"name":label,"ready":True})
        return result

    def _fields(self,p,uploads):
        key=p["source_key"];route=p["route"];attachments=[{"file_token":uploads[str(m["id"])]} for m in p["media"]]
        occurred=milliseconds(p["occurred_at"]);registered=milliseconds(p["registered_at"])
        if route=="feedback":
            media=p["media"][0];hash_value=media.get("phash") if media.get("phash_version")==PHASH_VERSION else ""
            if not hash_value and not p.get("require_miaoda_phash"):
                hash_value=media["sha256"]  # Legacy direct callers only; the HTTP export flow requires browser Canvas pHash.
            if not PHASH_RE.fullmatch(str(hash_value or "")) and p.get("require_miaoda_phash"):
                raise ValueError("缺少妙搭兼容图片指纹")
            if not hash_value:hash_value=media["sha256"]
            return {"图片名称":p["group_name"]+" · "+p["customer"]+" · "+str(p["source_refs"][0]["sequence"]),
            "图片":attachments,"配文":p["caption"],"客户昵称":p["customer"],"来源键":key,"来源群号":p["group_id"],
            "识别内容":p["caption"]+"\n消息时间："+p["occurred_at"]+"\n来源账号："+p["source_account_id"]+"\n客户对话："+p["case_id"],
            "哈希指纹":hash_value,"入库日期":registered}
        if route=="order_image":return {"记录标识":key,"发生时间":occurred,"来源账号":p["source_account_id"],"群名":p["group_name"],
            "群ID":p["group_id"],"用户名":p["customer"],"用户ID":p["sender_id"],"商品":p["product"],"订单图":attachments,
            "配文":p["caption"],"消息ID":"、".join(str(r["source_id"]) for r in p["source_refs"]),"客户对话ID":p["case_id"],
            "处理状态":"已归档","登记时间":registered,"来源键":key}
        if route=="service_evidence":return {"记录标识":key,"发生时间":occurred,"群名":p["group_name"],"群ID":p["group_id"],
            "用户名":p["customer"],"用户ID":p["sender_id"],"问题描述":p["caption"],"售后证据图":attachments,
            "来源消息ID":"、".join(str(r["source_id"]) for r in p["source_refs"]),"客户对话ID":p["case_id"],"来源键":key,"登记时间":registered}
        text=p["caption"] or "\n".join(p["source_messages"])
        fields={"群名":p["group_name"],"用户名":p["customer"],"需求产品":text,"时间":occurred,"状态":"未提醒","SourceID":key}
        if attachments:fields["备注"]=attachments
        return fields

    def _valid(self,job,p):
        current=self.workflow.processing_detail(job["case_id"])
        if not current or current["payload"]["plan_fingerprint"]!=job["plan_fingerprint"]:raise ValueError("处理方案或审核状态已变化，暂停入库")
        task=next((x for x in current['payload']['processing_plan']['tasks'] if x.get('key')==p.get('task_key') and x.get('route')==job['route']),None)
        # Existing jobs created before per-image approvals have no task_key.
        if current['processing_state']!='confirmed' and (not task or not self._task_authorized(current,task)):
            raise ValueError("图片任务尚未确认或确认已过期，暂停入库")
        target=self.store.get_workspace_config()["settings"].get(ROUTES[job["route"]][0])
        if target!=job["target_url"]:raise ValueError("目标表地址已变化，暂停入库")
        if digest(p)!=job["payload_hash"]:raise ValueError("入库快照内容校验失败")
        with self.store.connect() as db:
            for ref in p.get("source_refs") or []:
                event=db.execute("SELECT raw_json FROM events WHERE sequence=?",(ref.get("sequence"),)).fetchone()
                raw=json.loads(event["raw_json"] or "{}") if event else {}
                if not event or source_is_revoked(raw.get("data"),raw.get("outer")):
                    raise ValueError("来源消息已撤回或不存在，暂停入库")
        if job["route"]=="feedback" and p.get("require_miaoda_phash") and not self._saved_phash(p):
            raise ValueError("缺少妙搭兼容图片指纹，暂停入库")
        for m in p["media"]:
            if self._file(m["id"])["sha256"]!=m["sha256"]:raise ValueError("待上传图片已变化")

    def _execute(self,jid):
        job=self._record(jid);p=json.loads(job["payload_json"])
        try:self._valid(job,p)
        except ValueError as exc:
            self._save(jid,state="stale",error=str(exc));self._log(jid,"stale",str(exc));return
        self._save(jid,state="running",error="")
        with self.store.write_lock,self.store.connect() as db:db.execute("UPDATE feishu_export_jobs SET attempts=attempts+1 WHERE id=?",(jid,))
        self._log(jid,"running","开始核对来源键、上传素材及写入记录")
        create_started=False
        try:
            client=self.client_factory(job["target_url"])
            fields_by_name={f["field_name"]:f for f in client.fields()}
            for name,kind in REQUIRED[job["route"]].items():
                if name not in fields_by_name or fields_by_name[name]["type"]!=kind:raise ValueError("目标字段缺失或类型改变："+name)
            uploads=json.loads(job["uploads_json"]);request_fields=json.loads(job["request_json"]) if job["request_json"] else None
            record=client.read(job["remote_record_id"]) if job["remote_record_id"] else client.find(ROUTES[job["route"]][2],job["source_key"])
            if record:
                if request_fields is None:raise ValueError("目标来源键已存在但缺少本地写入快照，需人工核对")
            else:
                app_token,_=parse_target(job["target_url"])
                for media in p["media"]:
                    if str(media["id"]) not in uploads:
                        uploads[str(media["id"])]=client.upload(self.media_dir/media["file_name"],app_token)
                        self._save(jid,uploads_json=dump(uploads))
                if request_fields is None:
                    request_fields=self._fields(p,uploads);self._save(jid,request_json=dump(request_fields))
                self._save(jid,create_started=1);create_started=True
                record=client.create(request_fields,job["client_token"])
                self._save(jid,remote_record_id=record["record_id"])
                record=client.read(record["record_id"])
            verify_record(record,request_fields)
            self._save(jid,state="succeeded",remote_record_id=record["record_id"],error="")
            self._log(jid,"succeeded","飞书记录、配文和附件编号均已读回核验")
        except (requests.RequestException,TargetError,ValueError,KeyError,TypeError,OSError) as exc:
            message=("飞书接口错误 "+exc.code) if isinstance(exc,TargetError) else "网络超时或连接失败" if isinstance(exc,requests.RequestException) else str(exc)[:240]
            state="uncertain" if create_started or job["create_started"] or job["remote_record_id"] else "failed"
            self._save(jid,state=state,error=message);self._log(jid,state,message)

    def run(self,limit=5,job_id="",retry=False):
        limit=max(1,min(int(limit),100));owner=self.owner+":"+uuid.uuid4().hex
        with self.store.write_lock,self.store.connect() as db:
            db.execute("INSERT INTO feishu_export_lease(id,owner,expires) VALUES (1,?,?) ON CONFLICT(id) DO UPDATE SET owner=excluded.owner,expires=excluded.expires WHERE feishu_export_lease.expires<?",(owner,time.time()+300,time.time()))
            if db.execute("SELECT owner FROM feishu_export_lease WHERE id=1").fetchone()[0]!=owner:raise ValueError("已有入库任务正在执行，请稍后查看")
            # A crashed create is reconciled by the source key and persisted UUID.
            db.execute("UPDATE feishu_export_jobs SET state='uncertain',error='上次执行中断，重试前将核对远端记录' WHERE state='running'")
        try:
            states=("pending","failed","uncertain") if retry else ("pending",)
            with self.store.connect() as db:
                if job_id:
                    rows=db.execute("SELECT id,state FROM feishu_export_jobs WHERE id=?",(job_id,)).fetchall()
                    if not rows:raise ValueError("入库任务不存在")
                    ids=[r["id"] for r in rows if r["state"] in states]
                else:
                    rows=db.execute("SELECT id FROM feishu_export_jobs WHERE state IN ("+",".join("?" for _ in states)+") ORDER BY created_at,id LIMIT ?",[*states,limit]).fetchall();ids=[r["id"] for r in rows]
            for jid in ids:
                with self.store.write_lock,self.store.connect() as db:db.execute("UPDATE feishu_export_lease SET expires=? WHERE id=1 AND owner=?",(time.time()+300,owner))
                self._execute(jid)
            return {"processed":len(ids),"counts":self.page()["counts"]}
        finally:
            with self.store.write_lock,self.store.connect() as db:db.execute("DELETE FROM feishu_export_lease WHERE id=1 AND owner=?",(owner,))

    def dispatch(self,limit=5,job_id="",retry=False):
        if not self.dispatch_lock.acquire(blocking=False):raise ValueError("入库任务正在执行")
        def worker():
            try:self.run(limit,job_id,retry)
            finally:self.dispatch_lock.release()
        threading.Thread(target=worker,name="manual-feishu-export",daemon=True).start()
        return {"accepted":True}
