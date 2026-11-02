"""Local-only, read-only bridge for the configured Yunzhongke rule."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import random
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

import requests
import websockets
from analysis_engine import LABELS, RULE_VERSION, analyze
from ingestion_store import IngestionStore, source_is_revoked
from phase3 import Phase3, ai_worker, local_secrets, update_feishu_credentials, check_feishu_table, recommendation
from image_understanding import ImageInterpreter, image_is_ambiguous
from workflow_store import WorkflowStore
from feishu_outbox import FeishuOutbox
from source_config import configured_rule_id, configured_source_account_id
from broadcast_filter import filter_broadcasts


ROOT = Path(__file__).resolve().parent
ENV_FILE = ROOT.parent / ".env"
SYNC_STATE_FILE = ROOT / "sync-state.json"
CMS = "https://cms.iyunzk.com"
SSO = "https://sso.iyunzk.com"
RULE_ID = configured_rule_id(ENV_FILE)
SOURCE_ACCOUNT_ID = configured_source_account_id(ENV_FILE)
TZ = timezone(timedelta(hours=8), "Asia/Shanghai")
STORE = IngestionStore(ROOT / "data" / "messages.sqlite3", source_account_id=SOURCE_ACCOUNT_ID)
PHASE3 = Phase3(STORE, ENV_FILE)
WORKFLOW = WorkflowStore(STORE, ENV_FILE)
WORKFLOW.seed_sources()
MEDIA_DIR = ROOT / "data" / "media"
MEDIA_DIR.mkdir(parents=True, exist_ok=True)
OUTBOX = FeishuOutbox(STORE, WORKFLOW, MEDIA_DIR)
IMAGE_INTERPRETER = ImageInterpreter(STORE, ENV_FILE, MEDIA_DIR)
MAX_MEDIA_BYTES = 15 * 1024 * 1024
MAX_HISTORY_PAGES = 500
IMAGE_HOSTS = {"oss.iyunzk.com", "wxapp.tc.qq.com"}
NO_REPLY_MESSAGES = {
    "好", "好啊", "好的", "好哒", "好滴", "行", "行吧", "行的", "可以", "可以的",
    "嗯", "嗯嗯", "嗯哼", "恩", "恩恩", "哦", "噢", "收到", "收到啦", "知道了",
    "明白", "了解", "好的谢谢", "谢谢", "谢了", "谢啦", "辛苦了", "ok", "okay", "👌", "👍",
}

lock = threading.RLock()
history_fetch_lock = threading.Lock()
sync_changed = threading.Event()
workflow_job_lock = threading.Lock()
workflow_job_state = {"id": "", "state": "idle", "completed": 0, "total": 0, "report": None, "error": ""}
feishu_sync_stop = threading.Event()


def start_workflow_rebuild(*, incremental=False) -> dict:
    """Run the explicit local case rebuild away from the request/UI thread."""
    global workflow_job_state
    with workflow_job_lock:
        if workflow_job_state.get("state") == "running":
            return dict(workflow_job_state)
        job_id = hashlib.sha256(f"{time.time_ns()}".encode()).hexdigest()[:16]
        workflow_job_state = {"id": job_id, "state": "running", "completed": 0,
                              "total": 0, "report": None, "error": ""}

    def progress(done: int, total: int):
        with workflow_job_lock:
            if workflow_job_state.get("id") == job_id:
                workflow_job_state.update(completed=int(done), total=int(total))

    def run():
        global workflow_job_state
        try:
            version=active_analysis_version()
            previous=STORE.get_meta("processing_core_checkpoint")
            checkpoint,scopes=WORKFLOW.processing_changes(json.loads(previous) if previous else None,version) if incremental else (WORKFLOW.processing_checkpoint(version),None)
            input_signature=WORKFLOW.processing_input_signature(version,checkpoint)
            report = WORKFLOW.rebuild_cases(PHASE3, version, progress=progress,scopes=scopes)
            STORE.set_meta("processing_core_input_signature",input_signature)
            STORE.set_meta("processing_core_checkpoint",STORE.dumps(checkpoint))
            STORE.set_meta("processing_core_report",STORE.dumps(report))
            with workflow_job_lock:
                if workflow_job_state.get("id") == job_id:
                    workflow_job_state.update(state="completed", completed=report.get("suggestions_saved", 0),
                                              total=report.get("suggestions_saved", 0), report=report, error="")
        except Exception as exc:
            with workflow_job_lock:
                if workflow_job_state.get("id") == job_id:
                    workflow_job_state.update(state="failed", error=type(exc).__name__ + ": " + str(exc)[:240])

    threading.Thread(target=run, name="workflow-case-rebuild", daemon=True).start()
    with workflow_job_lock:
        return dict(workflow_job_state)


def processing_core_worker():
    """Debounce changed local inputs; never acquire sources or call a model."""
    previous=""; changed_at=time.monotonic(); failed_at=0.0; failed_job=""; burst_started=0.0
    while not feishu_sync_stop.wait(3):
        try:
            signature=WORKFLOW.processing_input_signature(active_analysis_version())
            if signature==STORE.get_meta("processing_core_input_signature"):
                burst_started=0.0
                continue
            if signature!=previous:
                previous=signature; changed_at=time.monotonic()
                if not burst_started: burst_started=changed_at
            if (time.monotonic()-changed_at<3 and time.monotonic()-burst_started<15) or time.monotonic()-failed_at<30: continue
            with workflow_job_lock:
                running=workflow_job_state.get("state")=="running"
                if workflow_job_state.get("state")=="failed" and workflow_job_state.get("id")!=failed_job:
                    failed_job=workflow_job_state.get("id") or ""; failed_at=time.monotonic()
            if not running and signature!=STORE.get_meta("processing_core_input_signature"):
                if time.monotonic()-failed_at>=30:
                    start_workflow_rebuild(incremental=True);burst_started=0.0
        except Exception as exc:
            failed_at=time.monotonic()
            with workflow_job_lock:
                workflow_job_state.update(state="failed",error="信息处理刷新失败："+type(exc).__name__)


def cross_group_filter_worker():
    while not feishu_sync_stop.wait(30):
        try:
            filter_broadcasts(STORE)
        except Exception as exc:
            with lock:
                status['cross_group_filter_error'] = type(exc).__name__


def feishu_material_sync_worker():
    """Optional read-only source refresh; no request occurs while every source is disabled."""
    while not feishu_sync_stop.wait(30):
        now=datetime.now(timezone.utc)
        for source in WORKFLOW.list_sources():
            if not source.get("enabled") or not source.get("sync_enabled"):
                continue
            last_attempt=source.get("last_attempt_at")
            if last_attempt:
                try:
                    if (now-datetime.fromisoformat(last_attempt)).total_seconds() < int(source.get("sync_interval_minutes") or 360)*60:
                        continue
                except (TypeError,ValueError):
                    pass
            try:
                WORKFLOW.read_source(source["id"],preview=False,account_id=source.get("source_account_id"))
            except Exception as exc:
                # Keep the source usable while recording a bounded local error.
                WORKFLOW._save_source_error(source["id"],"worker_error",type(exc).__name__)


def load_sync_enabled() -> bool:
    if not SYNC_STATE_FILE.exists():
        return True
    try:
        value = json.loads(SYNC_STATE_FILE.read_text(encoding="utf-8-sig"))
        return value.get("enabled") is True if isinstance(value, dict) else False
    except (OSError, ValueError):
        return False


sync_enabled = load_sync_enabled()


def save_sync_enabled(enabled: bool):
    temporary = SYNC_STATE_FILE.with_suffix(".tmp")
    temporary.write_text(json.dumps({"enabled": enabled}, ensure_ascii=False) + "\n", encoding="utf-8")
    temporary.replace(SYNC_STATE_FILE)


def load_history_retry_after():
    value = STORE.get_meta("history_retry_after")
    if value:
        return value
    latest = STORE.summary().get("latest_history_run")
    if latest and ("太过频繁" in latest.get("errors_json", "") or "稍后再试" in latest.get("errors_json", "")):
        try:
            until = datetime.fromisoformat(latest["finished_at"]).astimezone(TZ) + timedelta(minutes=15)
            if until > datetime.now(TZ):
                value = until.isoformat(timespec="seconds")
                STORE.set_meta("history_retry_after", value)
                return value
        except (TypeError, ValueError):
            pass
    return None


status = {
    "connected": False,
    "phase": "正在初始化" if sync_enabled else "消息同步已暂停",
    "error": "",
    "bot_count": None,
    "history_count": 0,
    "history_limited": False,
    "history_note": "",
    "history_retry_after": load_history_retry_after(),
    "coverage_gap": "",
    "received_count": 0,
    "last_message_at": None,
    "ocr_state": "等待本机图片文字识别",
    "started_at": datetime.now(TZ).isoformat(timespec="seconds"),
}

_ocr_lock = threading.Lock()
_ocr_engine = None
_ocr_checked = False
_ocr_error = ""
_ocr_image_errors = 0
_ocr_cache: dict[str, str] = {}

def valid_source_timestamp(raw: dict) -> bool:
    value = raw.get("sendTime") or raw.get("create_time") or raw.get("timestamp") or raw.get("time")
    if isinstance(value, (int, float)):
        try:
            datetime.fromtimestamp(value / (1000 if value > 10_000_000_000 else 1), TZ)
            return True
        except (OverflowError, ValueError, OSError):
            return False
    if isinstance(value, str):
        value = value.strip()
        if value.isdigit():
            try:
                number = int(value)
                datetime.fromtimestamp(number / (1_000_000 if number > 10_000_000_000_000 else 1000 if number > 10_000_000_000 else 1), TZ)
                return True
            except (OverflowError, ValueError, OSError):
                return False
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
            return True
        except ValueError:
            pass
        value = value.replace("T", " ").replace("Z", "")
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M"):
            try:
                datetime.strptime(value[:19], fmt)
                return True
            except ValueError:
                pass
    return False


def load_credentials() -> tuple[str, str]:
    if not ENV_FILE.is_file():
        raise RuntimeError("未找到工作区根目录的 .env 凭证文件")
    values: dict[str, str] = {}
    for line in ENV_FILE.read_text(encoding="utf-8-sig").splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        parts = re.split(r"[：:=]", line, maxsplit=1)
        if len(parts) == 2:
            values[parts[0].strip()] = parts[1].strip().strip('"\'')
    account = next((v for k, v in values.items() if k.lower() in {"账号", "账户", "account", "username", "user"}), "")
    password = next((v for k, v in values.items() if k.lower() in {"密码", "password", "passwd"}), "")
    if not account or not password:
        raise RuntimeError(".env 需包含“账号：...”和“密码：...”两行")
    return account, password


def form_post(session: requests.Session, path: str, data: dict | None = None, host: str = CMS, timeout: int = 20):
    fields = {key: (None, str(value)) for key, value in (data or {}).items()}
    return session.post(host + path, files=fields, timeout=timeout)


def make_session() -> requests.Session:
    account, password = load_credentials()
    session = requests.Session()
    session.headers.update({"X-Requested-With": "XMLHttpRequest", "Origin": CMS, "Referer": CMS + "/im/"})
    redirect_uri = CMS + "/im/#/?rule_id=" + RULE_ID
    response = session.post(
        SSO + "/sso/login",
        data={"account": account, "password": password, "redirect_uri": redirect_uri, "login_type": "session"},
        timeout=25,
    )
    response.raise_for_status()
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError("云中客登录响应格式异常") from exc
    target = (payload.get("data") or {}).get("redirect_uri")
    if not target:
        raise RuntimeError("云中客登录未返回有效跳转地址，请检查账号状态")
    landing = session.get(target, timeout=25)
    landing.raise_for_status()
    return session


def api_json(session: requests.Session, path: str, data: dict | None = None):
    response = form_post(session, path, data)
    response.raise_for_status()
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError("云中客接口返回格式异常") from exc
    if isinstance(payload, dict) and payload.get("code") not in (None, 0, 200, "0", "200"):
        raise RuntimeError(str(payload.get("msg") or payload.get("message") or "云中客接口返回错误")[:160])
    return payload


def response_shape(payload) -> str:
    """Describe an upstream response without exposing its values or credentials."""
    if not isinstance(payload, dict):
        return type(payload).__name__
    data = payload.get("data")
    if isinstance(data, list):
        return f"data=list({len(data)})"
    if isinstance(data, dict):
        keys = ",".join(sorted(str(key)[:32] for key in data.keys())[:8])
        nested = next((data.get(key) for key in ("list", "rows", "records", "data") if isinstance(data.get(key), list)), None)
        return f"data=object(fields={keys or 'none'}{', list=' + str(len(nested)) if nested is not None else ''})"
    return f"data={type(data).__name__}"


def safe_upstream_text(value, limit: int = 100) -> str:
    text = str(value or "").strip()
    text = re.sub(r"https?://\S+", "[链接]", text)
    text = re.sub(r"(?i)\b(token|cookie|authorization|password|passwd|secret|session|access[_-]?token|code)\b\s*[:=：]\s*[^,，;；\s]+", r"\1=[已隐藏]", text)
    text = re.sub(r"(?<!\d)\d{7,}(?!\d)", "[数字已隐藏]", text)
    return text[:limit]


def data_of(payload):
    value = payload.get("data", []) if isinstance(payload, dict) else []
    return value if isinstance(value, list) else (value.get("list", []) if isinstance(value, dict) else [])


def message_timestamp(raw: dict) -> datetime:
    value = raw.get("sendTime") or raw.get("create_time") or raw.get("timestamp") or raw.get("time")
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / (1000 if value > 10_000_000_000 else 1), TZ)
    if isinstance(value, str):
        value = value.strip()
        if value.isdigit():
            try:
                number = int(value)
                return datetime.fromtimestamp(number / (1_000_000 if number > 10_000_000_000_000 else 1000 if number > 10_000_000_000 else 1), TZ)
            except (OverflowError, ValueError, OSError):
                return datetime.now(TZ)
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return (parsed.replace(tzinfo=TZ) if parsed.tzinfo is None else parsed.astimezone(TZ))
        except ValueError:
            pass
        value = value.replace("T", " ").replace("Z", "")
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M"):
            try:
                return datetime.strptime(value[:19], fmt).replace(tzinfo=TZ)
            except ValueError:
                pass
    return datetime.now(TZ)


def content_text(value) -> str:
    if isinstance(value, str):
        text = value.strip()
        if text.startswith(("{", "[")):
            try:
                parsed = json.loads(text)
            except (TypeError, ValueError):
                parsed = None
            if isinstance(parsed, (dict, list)):
                return content_text(parsed)
        return text[:4000]
    if isinstance(value, dict):
        for key in ("text", "msg", "message", "content", "description", "desc", "title", "value"):
            item = value.get(key)
            if item is not None and item is not value:
                result = content_text(item)
                if result and result != "[图片/附件消息]":
                    return result[:4000]
        return "[图片/附件消息]"
    if isinstance(value, list):
        parts = [content_text(item) for item in value]
        text_parts = [item for item in parts if item and item != "[图片/附件消息]"]
        return "\n".join(text_parts)[:4000] if text_parts else "[图片/附件消息]"
    if value is not None:
        try:
            return str(value)[:4000]
        except (TypeError, ValueError):
            return ""
    return ""


def extract_image_urls(value) -> list[str]:
    found: list[str] = []

    def visit(item):
        if isinstance(item, str):
            candidates = re.findall(r"https?://[^\s\"'<>]+", item)
            for candidate in candidates:
                candidate = candidate.rstrip(".,，。;；!！?？)）]}")
                parsed = urlparse(candidate)
                host = (parsed.hostname or "").lower()
                if Path(parsed.path).suffix.lower() in (".mp3", ".amr", ".m4a", ".wav", ".ogg", ".mp4", ".mov", ".pdf"):
                    continue
                if parsed.scheme in ("https", "http") and host in IMAGE_HOSTS:
                    secure_url = parsed._replace(scheme="https").geturl()
                    if secure_url not in found:
                        found.append(secure_url)
        elif isinstance(item, dict):
            for child in item.values():
                visit(child)
        elif isinstance(item, list):
            for child in item:
                visit(child)

    visit(value)
    return found[:12]


def is_no_reply(text: str) -> bool:
    normalized = re.sub(r"[\s，。.!！~～…]+", "", text).lower()
    return normalized in NO_REPLY_MESSAGES


def normalize(raw: dict, outer: dict | None = None) -> dict | None:
    if not isinstance(raw, dict):
        return None
    outer = outer or {}
    source_type = str(raw.get("sourceType") or "").lower()
    if source_type and source_type != "group":
        return None
    terminal = str(raw.get("terminalType") or "").lower()
    if "send" in terminal or raw.get("is_self") is True:
        return None
    chat = raw.get("chatroom") or {}
    sender = raw.get("fromUser") or {}
    group = chat.get("displayName") or chat.get("remark") or chat.get("name") or chat.get("id") or "群聊"
    sender_name = sender.get("nickname") or sender.get("remark") or sender.get("wx_remark") or sender.get("id") or "群成员"
    body = content_text(raw.get("content"))
    image_urls = extract_image_urls(raw.get("content"))
    if not body and not raw.get("content_type"):
        return None
    stamp = message_timestamp(raw if valid_source_timestamp(raw) else outer)
    raw_id = str(raw.get("msgid") or raw.get("id") or outer.get("_id") or "")
    stable_id = raw_id or "payload-" + hashlib.sha256(
        json.dumps(raw, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()[:24]
    raw_type = " ".join(str(raw.get(k) or "") for k in ("type", "msg_type", "wxtype", "content_type")).lower()
    if any(word in raw_type for word in ("audio", "voice", "语音")):
        body, image_urls = "[语音消息]", []
    lower = body.lower()
    mini_value = raw.get("is_mini_parameter")
    mini = mini_value in (True, 1, "1") or any(word in raw_type for word in ("mini", "applet", "小程序"))
    numeric_types = {str(raw.get(key) or "") for key in ("type", "msg_type", "wxtype", "content_type")}
    is_image = bool(image_urls) or any(word in raw_type for word in ("image", "img", "picture", "图片", "photo")) or bool(numeric_types & {"3", "image"}) or any(ext in lower for ext in (".jpg", ".jpeg", ".png", ".webp"))
    if image_urls:
        for image_url in image_urls:
            body = body.replace(image_url, "").replace(image_url.replace("https://", "http://", 1), "").strip()
        body = body or "[图片消息]"
    feedback_words = ("反馈", "用了", "使用", "好用", "效果", "试用", "收到", "推荐", "体验")
    service_words = ("售后", "退款", "退货", "换货", "物流", "快递", "没收到", "未收到", "质量问题", "客服")
    ad_words = ("私聊", "加我", "加群", "引流", "推广", "领福利", "免费领取", "二维码", "加微信", "广告")
    if mini or any(word in lower for word in ad_words):
        kind, label, action = "warning", "广告 / 小程序", "核验群规与白名单"
        reason = "消息包含小程序或疑似引流、推广内容；请结合群规和发送者身份人工核验。"
        suggestion = "发现可能不相关或推广内容。请先核验群规和白名单，再决定是否提醒。"
    elif is_image:
        kind, label, action = "feedback", "反馈图", "审核用户反馈图"
        reason = "消息包含图片" + ("及使用反馈线索" if any(word in body for word in feedback_words) else "") + "；请确认内容、授权情况和个人信息后再处理。"
        suggestion = "建议作为用户反馈图库候选；确认授权且无敏感信息后再决定是否收录。"
    elif any(word in body for word in service_words):
        kind, label, action = "service", "售后", "生成售后答复建议"
        reason = "消息包含售后或物流诉求；建议使用工作台中当前配置的售后话术。"
        suggestion = "这边帮您登记一下，请联系售后客服继续处理： 【售后链接】"
    elif is_no_reply(body):
        kind, label, action = "closing", "无需回复", "对话自然结束"
        reason = "这是一条简短确认或礼貌收尾信息，没有新的问题或待办事项。"
        suggestion = "无需回复，结束对话。"
    else:
        kind, label, action = "qa", "日常答疑", "匹配 FAQ 或转人工"
        reason = "未命中明确的售后或风险关键词；可查看本地 FAQ，低置信度时转人工确认。"
        suggestion = "当前没有匹配到已确认的答复，建议先人工核实。"
    return {
        "id": stable_id,
        "source_id": raw_id,
        "group_id": str(chat.get("id") or chat.get("wxid") or raw.get("chatroom_wxid") or ""),
        "sender_id": str(sender.get("id") or sender.get("wxid") or raw.get("from_wxid") or ""),
        "robot_id": str(raw.get("roboot_wxid") or raw.get("robot_wxid") or ""),
        "source_type": source_type or "unknown",
        "content_type": str(raw.get("content_type") or raw.get("msg_type") or raw.get("type") or ""),
        "kind": kind,
        "label": label,
        "group": str(group),
        "sender": str(sender_name),
        "content": body or "[图片/附件消息]",
        "attachments": max(1, len(image_urls)) if is_image else 0,
        "image_urls": image_urls,
        "timestamp": stamp.isoformat(timespec="seconds"),
        "time": stamp.strftime("%H:%M"),
        "date": stamp.strftime("%m-%d %H:%M"),
        "priority": "优先" if kind in ("warning", "service") else "普通",
        "reason": reason,
        "action": action,
        "suggestion": suggestion,
        "status": "pending",
    }


def add_message(raw: dict, outer: dict | None = None, source: str = "websocket", allow_when_paused: bool = False) -> bool:
    with lock:
        if not sync_enabled and not allow_when_paused:
            return False
    if not isinstance(raw, dict):
        raw = {"unparsed_payload": raw}
    terminal = str(raw.get("terminalType") or "").lower()
    direction = "outbound" if "send" in terminal or raw.get("is_self") is True else "inbound"
    source_type = str(raw.get("sourceType") or "").lower()
    try:
        normalized = normalize(raw, outer)
        if normalized:
            normalized["source_account_id"] = str((outer or {}).get("_source_account_id") or SOURCE_ACCOUNT_ID)[:160]
        parse_state = "parsed" if normalized else ("context_only" if direction == "outbound" or source_type not in ("", "group") else "unparsed")
    except Exception:
        normalized, parse_state = None, "unparsed"
    if normalized:
        normalized["source"] = source
    timestamp_source = raw if valid_source_timestamp(raw) else (outer or {})
    timestamp_valid = valid_source_timestamp(timestamp_source)
    sequence, added = STORE.add_event(raw, outer, normalized, source, display=bool(normalized),
                               parse_state=parse_state, direction=direction,
                               timestamp_valid=timestamp_valid,
                               source_timestamp=message_timestamp(timestamp_source).isoformat(timespec="seconds") if timestamp_valid else None)
    if direction == "outbound":
        target = STORE.explicit_reply_target(sequence)
        if target:
            updated = analyze_saved_event(target["sequence"])
            if updated:
                PHASE3.ensure(target["sequence"], updated)
    if normalized and (added or source == "history"):
        analyze_saved_event(sequence)
        event = STORE.analysis_event(sequence)
        # A duplicate history row may already be deliberately suppressed. It
        # remains stored for deduplication but is not a visible analysis event.
        if event:
            for previous_event in STORE.recent_same_sender_events(event):
                analyze_saved_event(previous_event)
        if added:
            with lock:
                status["received_count"] += 1
                status["last_message_at"] = normalized["timestamp"]
    return bool(added)


def receive_source_event(event):
    """Accept source replies as well as customer messages and preserve the envelope."""
    if not isinstance(event, dict):
        return False
    kind, raw = str(event.get("type") or ""), event.get("data")
    if not isinstance(raw, dict):
        return False
    if kind not in ("wxmsg", "wxmsg_send") and not (
            raw.get("msgid") and (raw.get("sourceType") == "group" or isinstance(raw.get("chatroom"), dict))):
        with lock:
            counts = status.setdefault("ignored_ws_event_types", {})
            counts[kind[:64] or "unknown"] = counts.get(kind[:64] or "unknown", 0) + 1
            # Keep only aggregate structural evidence, never customer content,
            # identifiers or credentials, to diagnose provider event formats.
            if kind == "wxmsg_send_copy":
                stats = status.setdefault("ignored_send_copy_structure", {"total": 0, "with_msgid": 0,
                    "with_group": 0, "with_reply_id": 0, "with_recipient": 0})
                stats["total"] += 1
                stats["with_msgid"] += bool(raw.get("msgid"))
                stats["with_group"] += bool(raw.get("sourceType") == "group" or isinstance(raw.get("chatroom"), dict))
                stats["with_reply_id"] += bool(raw.get("replyMessageId"))
                stats["with_recipient"] += bool(raw.get("to_wxid"))
        return False
    raw = dict(raw)
    if kind in ("wxmsg_send", "wxmsg_send_copy"):
        raw["terminalType"] = kind
    return add_message(raw, event, "websocket")


def analysis_settings() -> dict:
    settings = STORE.get_workspace_config()["settings"]
    return settings if isinstance(settings, dict) else {}


def active_analysis_version() -> int:
    return STORE.active_rule_version(RULE_VERSION)


def development_faq_prediction(rec: dict) -> str:
    """Return the FAQ actually cited in the generated draft, if any."""
    prefix = "待审草稿引用已审核启用 FAQ："
    for evidence in rec.get("evidence") or []:
        if not str(evidence).startswith(prefix):
            continue
        question = str(evidence)[len(prefix):]
        match = next((item for item in rec.get("faq") or [] if item.get("question") == question), None)
        if match:
            return str(match.get("id") or "")
    return ""


def development_audit_material(sequence: int):
    version = active_analysis_version()
    event = STORE.analysis_event(sequence)
    if not event:
        raise KeyError(sequence)
    analyses, _reviews = STORE.analysis_map([sequence], version)
    analysis = analyses.get(sequence)
    if not analysis:
        analysis = analyze_saved_event(sequence, version=version)
    if not analysis:
        raise KeyError(sequence)
    normalized = json.loads(event["normalized_json"] or "null") or {}
    normalized["sequence"] = sequence
    payload = json.loads(event["raw_json"])
    normalized["source_reference_ids"] = sorted(STORE.source_references(payload.get("data") or {}, payload.get("outer"), event.get("source_id")))
    normalized["source_account_id"] = event.get("source_account_id") or SOURCE_ACCOUNT_ID
    context = STORE.analysis_context(event)
    media = STORE.media_for_event(sequence)
    config = STORE.get_workspace_config()
    settings = config.get("settings") or {}
    faqs = config.get("faqs") or []
    rec = recommendation(str(analysis.get("primary") or "unknown"), normalized, analysis,
                         settings, faqs, media, context)
    predicted_faq_id = development_faq_prediction(rec)
    faq_candidates = [{key: item.get(key) for key in ("id", "question", "answer", "group", "score", "enabled", "approved")}
                      for item in rec.get("faq") or []]
    return {"version": version, "event": event, "normalized": normalized,
            "analysis": analysis, "rec": rec, "media": media, "context": context,
            "predicted_faq_id": predicted_faq_id, "faq_candidates": faq_candidates,
            "faqs": faqs}


def local_ocr_text(media: dict) -> str:
    """Read locally; hash-keyed SQLite cache avoids repeated OCR on restart."""
    global _ocr_engine, _ocr_checked, _ocr_error, _ocr_image_errors
    if media.get("state") != "stored":
        return ""
    file_name = str(media.get("file_name") or "")
    if not file_name or Path(file_name).name != file_name:
        return ""
    cache_key = str(media.get("sha256") or file_name)
    with _ocr_lock:
        if cache_key in _ocr_cache:
            return _ocr_cache[cache_key]
        with STORE.connect() as db:
            cached = db.execute("SELECT text FROM image_text_cache WHERE sha256=? AND engine_version='rapidocr-v1'", (cache_key,)).fetchone()
        if cached:
            _ocr_cache[cache_key] = cached["text"]
            return cached["text"]
        if not _ocr_checked:
            _ocr_checked = True
            try:
                from rapidocr_onnxruntime import RapidOCR
                _ocr_engine = RapidOCR()
            except Exception as exc:
                _ocr_error = type(exc).__name__
                _ocr_engine = None
        if _ocr_engine is None:
            _ocr_cache[cache_key] = ""
            return ""
        image_path = MEDIA_DIR / file_name
        if not image_path.is_file():
            _ocr_cache[cache_key] = ""
            return ""
        try:
            rows, _elapsed = _ocr_engine(image_path)
            text = " ".join(str(row[1]).strip() for row in (rows or [])
                            if len(row) > 2 and float(row[2]) >= .45 and str(row[1]).strip())
            _ocr_cache[cache_key] = text[:3000]
            with STORE.write_lock, STORE.connect() as db:
                db.execute("INSERT OR REPLACE INTO image_text_cache(sha256,engine_version,text,updated_at) VALUES(?,'rapidocr-v1',?,?)",
                           (cache_key, text[:3000], STORE.now()))
        except Exception as exc:
            _ocr_image_errors += 1
            _ocr_cache[cache_key] = ""
        return _ocr_cache[cache_key]


def analyze_saved_event(sequence: int, settings: dict | None = None, version: int | None = None,
                        *, include_ocr: bool = False):
    started = time.monotonic()
    version = version or active_analysis_version()
    event = STORE.analysis_event(sequence)
    if not event:
        return None
    raw = json.loads(event["raw_json"]).get("data") or {}
    normalized = json.loads(event["normalized_json"] or "null") or {}
    normalized["sequence"] = sequence
    normalized["source_reference_ids"] = sorted(STORE.source_references(raw, json.loads(event["raw_json"]).get("outer"), event.get("source_id")))
    active_settings = settings if isinstance(settings, dict) else analysis_settings()
    rule_settings = active_settings.get("rules") if isinstance(active_settings.get("rules"), dict) else {}
    normalized["conversation_anchor_sequence"] = STORE.conversation_anchor_sequence(
        event, int(rule_settings.get("conversationSeconds") or 1800))
    context = STORE.analysis_context(event)
    media = STORE.media_for_event(sequence, include_file=include_ocr)
    for item in context:
        if item.get("has_image") and item.get("direction") == "inbound" and item.get("sender_id") == normalized.get("sender_id"):
            visuals = [value.get("image_understanding") for value in STORE.media_for_event(int(item["sequence"])) if value.get("image_understanding")]
            if visuals:
                item["image_understanding"] = visuals[0]
    if include_ocr:
        for item in media:
            item["ocr_text"] = local_ocr_text(item)
        for item in context:
            if item.get("has_image") and item.get("direction") == "inbound" and item.get("sender_id") == normalized.get("sender_id"):
                related = STORE.media_for_event(int(item["sequence"]), include_file=True)
                item["ocr_text"] = " ".join(local_ocr_text(value) for value in related if value.get("state") == "stored")
    try:
        result = analyze(raw, normalized, context, media, active_settings)
        result["version"] = version
    except (TypeError, ValueError, KeyError) as exc:
        result = {"version": version, "primary": "unknown", "labels": ["unknown"],
                  "label_names": [LABELS["unknown"]], "confidence": "low", "needs_review": True,
                  "evidence": [], "uncertainty": ["初步处理失败，需人工核查"],
                  "features": {}, "context_sequences": [], "reason": "初步处理失败，需人工核查",
                  "error_type": type(exc).__name__}
    STORE.save_analysis(sequence, version, result, round((time.monotonic() - started) * 1000))
    return result


def analyze_saved_messages(*, force: bool = False) -> int:
    settings = analysis_settings()
    version = active_analysis_version()
    STORE.ensure_rule_version(version, settings)
    candidates = STORE.analysis_candidates(version, force=force)
    for event in candidates:
        analyze_saved_event(event["sequence"], settings, version)
    return len(candidates)


def analysis_backfill_worker():
    """Fill missing first-pass classifications after the local server is ready."""
    try:
        analyze_saved_messages()
    except Exception as exc:
        with lock:
            status["phase"] = "消息已可查看；分类补齐失败"
            status["error"] = type(exc).__name__


def local_ocr_backfill_worker():
    """OCR archived images locally, then refresh rules and pending suggestions."""
    try:
        images = STORE.media_stored()
        for index, item in enumerate(images, start=1):
            local_ocr_text(item)
            with lock:
                if _ocr_error:
                    status["ocr_state"] = "本机 OCR 不可用，图片转人工判断"
                    return
                status["ocr_state"] = f"本机图片识别中 {index}/{len(images)}"
        if not _ocr_checked:
            # Load the local engine even when there are no stored images.
            try:
                local_ocr_text({"state": "stored", "file_name": ""})
            except Exception:
                pass
        if _ocr_error:
            with lock:
                status["ocr_state"] = "本机 OCR 不可用，图片转人工判断"
            return
        with lock:
            status["ocr_state"] = "正在刷新图片相关分类"
        version = active_analysis_version()
        for candidate in STORE.analysis_candidates(version, force=True):
            sequence = int(candidate["sequence"])
            analyze_saved_event(sequence, version=version, include_ocr=True)
        recognized = sum(bool(value) for value in _ocr_cache.values())
        with lock:
            error_suffix = f"；{_ocr_image_errors} 张识别失败" if _ocr_image_errors else ""
            status["ocr_state"] = f"完成：{recognized}/{len(images)} 张图片识别到文字{error_suffix}，结果仅作本地初筛"
    except Exception as exc:
        with lock:
            status["ocr_state"] = "本机图片识别失败：" + type(exc).__name__


def image_understanding_worker():
    """Interpret ambiguous stored pictures, then refresh their adjacent turns."""
    # Keep reuse of existing visual results, but never send the whole historical
    # archive to DeepSeek just because the local service was restarted.
    with STORE.connect() as db:
        startup_sequence = int(db.execute("SELECT COALESCE(MAX(sequence),0) FROM events").fetchone()[0])
    while True:
        try:
            settings = analysis_settings()
            if settings.get("imageUnderstandingEnabled", True) is False:
                with lock:
                    status["vision_state"] = "图片视觉初筛已暂停"
                time.sleep(15)
                continue
            ocr_state = str(status.get("ocr_state") or "")
            if not ocr_state.startswith(("完成", "本机 OCR 不可用", "本机图片识别失败")):
                time.sleep(2)
                continue
            version = active_analysis_version()
            images = [image for image in STORE.media_stored()
                      if int(image.get("event_sequence") or 0) > startup_sequence]
            for index, image in enumerate(images, 1):
                sequence = int(image["event_sequence"])
                analysis_map, _ = STORE.analysis_map([sequence], version)
                analysis = analysis_map.get(sequence) or {}
                if not image_is_ambiguous(analysis):
                    continue
                cached = IMAGE_INTERPRETER.cached(image.get("sha256"))
                if cached and (cached["state"] == "done" or cached["attempts"] >= 2):
                    continue
                event = STORE.analysis_event(sequence)
                normalized = json.loads(event["normalized_json"] or "{}") or {}
                current_at = datetime.fromisoformat(event["source_timestamp"])
                adjacent = [item for item in STORE.analysis_context(event)
                            if item.get("direction") == "inbound" and item.get("sender_id") == event.get("sender_id")
                            and abs((current_at-datetime.fromisoformat(item["source_timestamp"])).total_seconds()) <= 120]
                with lock:
                    status["vision_state"] = f"图片视觉初筛 {index}/{len(images)}；仅处理规则拿不准的图片"
                captions = [normalized.get("content") or "", *(item.get("content") or "" for item in adjacent)]
                if IMAGE_INTERPRETER.interpret(image, captions):
                    for affected in {sequence, *(int(item["sequence"]) for item in adjacent)}:
                        updated = analyze_saved_event(affected, version=version, include_ocr=True)
                        if updated:
                            PHASE3.ensure(affected, updated)
            with STORE.connect() as db:
                counts = {row["state"]: row["n"] for row in db.execute("SELECT state,COUNT(*) n FROM image_understanding WHERE policy_version=1 GROUP BY state")}
            with lock:
                status["vision_state"] = f"图片视觉初筛完成：{counts.get('done', 0)} 张；失败 {counts.get('error', 0)} 张，失败或低置信度保留人工核验"
        except Exception as exc:
            with lock:
                status["vision_state"] = "图片视觉初筛待检查：" + type(exc).__name__
        time.sleep(15)


def attach_analysis(page: dict) -> dict:
    items = page.get("messages") or []
    results, reviews = STORE.analysis_map([int(item["sequence"]) for item in items], active_analysis_version())
    for item in items:
        sequence = item["sequence"]
        media_items = item.get("media") or []
        if media_items:
            item["image_urls"] = []
        for media in media_items:
            if media.get("error"):
                media["error"] = re.sub(r"https?://[^\s]+", "[来源图片地址]", media["error"])
            if media.get("state") in ("stored", "failed"):
                media.pop("url", None)
        analysis = results.get(sequence)
        review = reviews.get(sequence)
        if analysis:
            primary = review.get("primary_label") if review and review.get("primary_label") else analysis["primary"]
            labels = review.get("labels") if review and review.get("labels") is not None else analysis["labels"]
            item.update({"analysis": analysis, "review": review, "kind": primary,
                         "label": LABELS.get(primary, primary), "labels": labels,
                         "reason": analysis["reason"],
                         "priority": "优先" if primary in ("service", "warning", "mini", "burst") else "普通",
                         "action": "人工核验分类" if analysis["needs_review"] else "确认分类",
                         "status": review["status"] if review else "pending"})
            # Keep queue and live-message pages cheap. Build the full recommendation
            # only when the operator opens a message through /api/work-item.
            item["work_status"] = item.get("work_status") or "pending"
    return page


def bootstrap_history(session: requests.Session, start_at: datetime, end_at: datetime,
                      allow_when_paused: bool = False) -> dict:
    if not sync_enabled and not allow_when_paused:
        return {"state": "cancelled", "added": 0}
    if not history_fetch_lock.acquire(blocking=False):
        raise RuntimeError("历史消息正在读取")
    run_id = None
    pages = rows_seen = history_added = 0
    errors: list[str] = []
    bots: list[dict] = []
    state = "complete"
    rate_limited = False
    try:
        run_id = STORE.begin_history(start_at.isoformat(), end_at.isoformat())
        bots_payload = api_json(session, "/wx/GroupXiaoXiZhongKong/getZdRobot", {
            "rule_id": RULE_ID, "page": 1, "size": 999, "nickname": "", "group_keyword": "", "group_search_type": "",
        })
        bots = data_of(bots_payload)
        if bots:
            STORE.set_bots(bots)
        else:
            # An empty/changed API envelope must not erase the last known bot set.
            errors.append(f"关联机器人列表为空（{response_shape(bots_payload)}）；保留本地旧记录，不作为当前规则机器人")
        if len(bots) >= 999:
            errors.append("关联机器人列表达到请求上限，范围可能不完整")
        current_bot_ids = {str(bot.get("wxid") or bot.get("id") or "") for bot in bots}
        with lock:
            status["bot_count"] = len(bots)
        day = start_at.date()
        while day <= end_at.date():
            day_start = max(start_at, datetime.combine(day, datetime.min.time(), TZ))
            day_end = min(end_at, datetime.combine(day, datetime.max.time(), TZ).replace(microsecond=0))
            day_key = day.isoformat()
            def covers_requested_range(progress):
                if not progress or progress.get("state") != "complete":
                    return False
                try:
                    covered_start = datetime.fromisoformat(progress["start_at"]).astimezone(TZ)
                    covered_end = datetime.fromisoformat(progress["end_at"]).astimezone(TZ)
                    return covered_start <= day_start and covered_end >= day_end
                except (TypeError, ValueError):
                    return False

            # Persist every bot that still needs this interval, including bots
            # not reached before an earlier robot hit the source rate limit.
            progress_by_bot = {}
            for bot in bots:
                robot_id = str(bot.get("wxid") or bot.get("id") or "")
                if not robot_id:
                    continue
                progress = STORE.history_cursor(robot_id, day_key)
                if progress is None or (progress.get("state") == "complete" and not covers_requested_range(progress)):
                    STORE.save_history_cursor(robot_id, day_key, day_start.isoformat(), day_end.isoformat(),
                                              None, "pending", 0, 0)
                    progress = STORE.history_cursor(robot_id, day_key)
                elif progress.get("state") != "complete":
                    try:
                        old_start = datetime.fromisoformat(progress["start_at"]).astimezone(TZ)
                    except (TypeError, ValueError):
                        old_start = day_start
                    new_start = min(day_start, old_start)
                    try:
                        old_end = datetime.fromisoformat(progress["end_at"]).astimezone(TZ)
                    except (TypeError, ValueError):
                        old_end = day_end
                    new_end = max(day_end, old_end)
                    if new_start != old_start or new_end != old_end:
                        STORE.save_history_cursor(robot_id, day_key, new_start.isoformat(), new_end.isoformat(),
                                                  None, "pending", 0, 0)
                    else:
                        STORE.save_history_cursor(robot_id, day_key, new_start.isoformat(), new_end.isoformat(),
                                                  progress.get("cursor"), progress.get("state") or "pending",
                                                  int(progress.get("pages") or 0), int(progress.get("rows_seen") or 0),
                                                  str(progress.get("error") or ""))
                    progress = STORE.history_cursor(robot_id, day_key)
                progress_by_bot[robot_id] = progress
            missing_bots = STORE.unfinished_history_ids(day_key) - current_bot_ids
            if missing_bots:
                errors.append(f"{day_key} 有 {len(missing_bots)} 个未完成机器人已不在当前规则列表，历史范围需人工核对")
            bot_order = sorted(enumerate(bots), key=lambda pair: (
                1 if (progress_by_bot.get(str(pair[1].get("wxid") or pair[1].get("id") or "")) or {}).get("state") in ("running", "partial") else 0,
                pair[0]))
            for _bot_index, bot in bot_order:
                if not sync_enabled and not allow_when_paused:
                    state = "cancelled"
                    break
                robot_id = str(bot.get("wxid") or bot.get("id") or "")
                if not robot_id:
                    errors.append("关联机器人缺少稳定 ID")
                    continue
                progress = STORE.history_cursor(robot_id, day_key)
                if covers_requested_range(progress):
                    continue
                signatures: set[str] = set()
                page_cursor = 0
                query_start, query_end = day_start, day_end
                bot_pages = bot_rows = 0
                bot_request_made = False
                if progress and progress["state"] != "complete":
                    bot_pages = progress["pages"]
                    bot_rows = progress["rows_seen"]
                    try:
                        query_start = min(query_start, datetime.fromisoformat(progress["start_at"]).astimezone(TZ))
                        query_end = max(query_end, datetime.fromisoformat(progress["end_at"]).astimezone(TZ))
                    except ValueError:
                        pass
                    if progress.get("cursor"):
                        page_cursor = progress["cursor"]
                for page_index in range(MAX_HISTORY_PAGES):
                    if not sync_enabled and not allow_when_paused:
                        state = "cancelled"
                        break
                    try:
                        bot_request_made = True
                        payload = api_json(session, "/wx/GroupXiaoXiZhongKong/getMsg4Review", {
                            "rule_id": RULE_ID, "robot_wxid": robot_id, "sub_uid": str(bot.get("sub_uid") or ""),
                            "start_time": query_start.strftime("%Y-%m-%d %H:%M:%S"),
                            "end_time": query_end.strftime("%Y-%m-%d %H:%M:%S"), "page": page_cursor,
                        })
                        batch = data_of(payload)
                        pages += 1
                        if not batch:
                            STORE.save_history_cursor(robot_id, day_key, query_start.isoformat(), query_end.isoformat(),
                                                      None, "complete", bot_pages + 1, bot_rows)
                            break
                        signature = hashlib.sha256(json.dumps(batch, ensure_ascii=False, sort_keys=True, default=str).encode()).hexdigest()
                        if signature in signatures:
                            detail = f"机器人 {robot_id} 在 {day} 第 {page_index + 1} 页重复；游标行为异常"
                            errors.append(detail)
                            STORE.save_history_cursor(robot_id, day_key, query_start.isoformat(), query_end.isoformat(),
                                                      str(page_cursor), "partial", bot_pages, bot_rows, detail)
                            break
                        signatures.add(signature)
                        rows_seen += len(batch)
                        bot_pages += 1
                        bot_rows += len(batch)
                        for row in batch:
                            raw = row.get("data") if isinstance(row, dict) and isinstance(row.get("data"), dict) else row
                            if add_message(raw, row if isinstance(row, dict) else None, "history", allow_when_paused):
                                history_added += 1
                        with lock:
                            status["history_note"] = f"历史读取中：{day}，第 {page_index + 1} 页，已新增 {history_added} 条"
                        next_cursor = batch[-1].get("create_time") if isinstance(batch[-1], dict) else None
                        if not next_cursor or next_cursor == page_cursor:
                            detail = f"机器人 {robot_id} 在 {day} 缺少下一页游标；覆盖范围未证实"
                            errors.append(detail)
                            STORE.save_history_cursor(robot_id, day_key, query_start.isoformat(), query_end.isoformat(),
                                                      str(page_cursor), "partial", bot_pages, bot_rows, detail)
                            break
                        page_cursor = next_cursor
                        STORE.save_history_cursor(robot_id, day_key, query_start.isoformat(), query_end.isoformat(),
                                                  str(page_cursor), "running", bot_pages, bot_rows)
                        time.sleep(2.0)
                    except Exception as exc:
                        detail = str(exc)[:120]
                        errors.append(f"机器人 {robot_id} 在 {day} 第 {page_index + 1} 页失败：{detail}")
                        STORE.save_history_cursor(robot_id, day_key, query_start.isoformat(), query_end.isoformat(),
                                                  str(page_cursor), "partial", bot_pages, bot_rows, detail)
                        if "太过频繁" in detail or "稍后再试" in detail:
                            retry_at = (datetime.now(TZ) + timedelta(minutes=15)).isoformat(timespec="seconds")
                            STORE.set_meta("history_retry_after", retry_at)
                            with lock:
                                status["history_retry_after"] = retry_at
                            rate_limited = True
                            state = "partial"
                        break
                else:
                    detail = f"机器人 {robot_id} 在 {day} 达到 {MAX_HISTORY_PAGES} 页安全上限"
                    errors.append(detail)
                    STORE.save_history_cursor(robot_id, day_key, query_start.isoformat(), query_end.isoformat(),
                                              str(page_cursor), "partial", bot_pages, bot_rows, detail)
                if state == "cancelled":
                    break
                if rate_limited:
                    break
                if bot_request_made:
                    time.sleep(2.0)
            if rate_limited:
                break
            if state == "cancelled":
                break
            day += timedelta(days=1)
        if errors and state != "cancelled":
            state = "partial"
        return {"state": state, "added": history_added, "pages": pages, "rows_seen": rows_seen,
                "bot_count": len(bots), "errors": errors}
    except Exception as exc:
        state = "failed"
        detail = str(exc)[:180]
        errors.append(detail)
        if "太过频繁" in detail or "稍后再试" in detail:
            retry_at = (datetime.now(TZ) + timedelta(minutes=15)).isoformat(timespec="seconds")
            STORE.set_meta("history_retry_after", retry_at)
            with lock:
                status["history_retry_after"] = retry_at
        raise
    finally:
        if run_id is not None:
            STORE.end_history(run_id, state, len(bots), pages, rows_seen, history_added, errors)
        STORE.mark_recovered_history_runs()
        if state != "complete":
            retry_at = status.get("history_retry_after")
            try:
                cooling = bool(retry_at and datetime.now(TZ) < datetime.fromisoformat(retry_at))
            except ValueError:
                cooling = False
            if not cooling:
                retry_at = (datetime.now(TZ) + timedelta(minutes=15)).isoformat(timespec="seconds")
                STORE.set_meta("history_retry_after", retry_at)
        with lock:
            status["history_count"] = history_added
            summary = STORE.summary()
            latest_run = summary.get("latest_history_run") or {}
            recovered = latest_run.get("state") == "recovered"
            status["history_limited"] = latest_run.get("state") not in ("complete", "recovered")
            status["history_note"] = (f"历史缺口已由后续补读覆盖，最近扫描新增 {history_added} 条" if recovered else
                                       "；".join(errors[:2]) if errors else f"历史读取完成：{rows_seen} 条源记录，新增 {history_added} 条")
            if state != "complete":
                status["history_retry_after"] = retry_at
            recent_gap = STORE.oldest_unfinished_history((datetime.now(TZ) - timedelta(days=3)).isoformat(timespec="seconds"))
            if recent_gap:
                status["coverage_gap"] = f"历史补读未完成：{summary['unfinished_history_scans']} 个机器人扫描待续读"
            elif summary["unfinished_history_scans"]:
                status["coverage_gap"] = "仍有超过 3 天的历史缺口未核实；近期缺口继续自动补读"
            elif latest_run.get("state") in ("complete", "recovered"):
                status["coverage_gap"] = ""
        history_fetch_lock.release()


def fetch_history_once(start_at: datetime, end_at: datetime) -> dict:
    session = None
    try:
        retry_after = status.get("history_retry_after")
        if retry_after and datetime.now(TZ) < datetime.fromisoformat(retry_after):
            raise RuntimeError(f"云中客历史接口限流，请在 {retry_after[11:16]} 后重试")
        if retry_after:
            status["history_retry_after"] = None
            STORE.set_meta("history_retry_after", None)
        with lock:
            status["phase"] = "实时消息已连接，正在补读历史" if sync_enabled and status.get("connected") else "正在读取当日历史消息"
            status["history_limited"] = False
            status["history_note"] = ""
        session = make_session()
        return bootstrap_history(session, start_at, end_at, allow_when_paused=True)
    finally:
        if session is not None:
            session.close()
        with lock:
            if sync_enabled and status.get("connected"):
                status["phase"] = "实时消息已连接"
            elif not sync_enabled:
                status["connected"] = False
                status["phase"] = "消息同步已暂停"


def ws_url_and_code(session: requests.Session):
    code_payload = api_json(session, "/wx/GroupXiaoXiZhongKong/getCodeNew", {"rule_id": RULE_ID})
    code = (code_payload.get("data") if isinstance(code_payload, dict) else None)
    if isinstance(code, dict):
        code = code.get("code") or code.get("data")
    if not code:
        raise RuntimeError("云中客未返回实时连接码")
    port_payload = api_json(session, "/wx/GroupXiaoXiZhongKong/getImPort", {"rule_id": RULE_ID})
    port_data = port_payload.get("data") if isinstance(port_payload, dict) else None
    if isinstance(port_data, str):
        direct_url = port_data.strip()
        parsed = urlparse(direct_url)
        if parsed.scheme != "wss" or not parsed.hostname or not (parsed.hostname == "iyunzk.com" or parsed.hostname.endswith(".iyunzk.com")):
            raise RuntimeError("云中客实时连接地址无效")
        return direct_url, str(code)
    if isinstance(port_data, list) and port_data:
        port_data = port_data[0]
    if not isinstance(port_data, dict):
        raise RuntimeError("云中客未返回实时连接地址")
    host = port_data.get("host") or port_data.get("domain")
    path = port_data.get("path") or port_data.get("url")
    if not host or not path:
        raise RuntimeError("云中客实时连接地址不完整")
    url = path if str(path).startswith("ws") else "wss://" + str(host).removeprefix("https://").removeprefix("http://") + "/" + str(path).lstrip("/")
    parsed = urlparse(url)
    if parsed.scheme != "wss" or not parsed.hostname or not (parsed.hostname == "iyunzk.com" or parsed.hostname.endswith(".iyunzk.com")):
        raise RuntimeError("云中客实时连接地址无效")
    return url, str(code)


async def consume_ws(session: requests.Session):
    if not sync_enabled:
        return
    url, code = await asyncio.to_thread(ws_url_and_code, session)
    if not sync_enabled:
        return
    uuid = base64.b64encode((datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") + "-" + "".join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=8))).encode()).decode()
    login = {"type": "login", "data": {
        "ruleId": RULE_ID, "code": code, "uuid": uuid, "web_version": "0.0.1",
        "electron_version": "", "ip": "", "external_ip": "", "tag": "",
        "login_version": str(int(time.time() * 1000)),
    }}
    async with websockets.connect(url, origin=CMS, ping_interval=20, ping_timeout=20,
                                  close_timeout=5, max_size=8 * 1024 * 1024) as ws:
        await ws.send(json.dumps(login, ensure_ascii=False))
        first = await asyncio.wait_for(ws.recv(), timeout=20)
        try:
            response = json.loads(first)
        except (ValueError, TypeError):
            raise RuntimeError("云中客实时连接返回了无法识别的登录结果")
        login_data = response.get("data")
        login_code = login_data.get("code") if isinstance(login_data, dict) else login_data
        login_message = str(response.get("msg") or response.get("message") or "")
        login_succeeded = str(login_code) == "200" or login_message.startswith("登录成功")
        if response.get("type") != "login" or not login_succeeded:
            safe_code = str(login_code) if isinstance(login_code, (int, float)) or str(login_code or "").strip().isdigit() else "非数字或缺失"
            if safe_code.isdigit() and len(safe_code) > 6:
                safe_code = "非标准状态码"
            detail = safe_upstream_text(login_message)
            message = f"云中客实时消息登录失败（type={safe_upstream_text(response.get('type'), 24) or '缺失'}，code={safe_code}"
            if detail:
                message += f"，说明={detail}"
            message += f"，响应结构：{response_shape(response)}）"
            raise RuntimeError(message)
        with lock:
            if not sync_enabled:
                return
            status["connected"] = True
            status["phase"] = "实时消息已连接"
            status["error"] = ""
        heartbeat = asyncio.create_task(ws_heartbeat(ws))
        try:
            while sync_enabled:
                if heartbeat.done():
                    heartbeat.result()
                try:
                    frame = await asyncio.wait_for(ws.recv(), timeout=1)
                except asyncio.TimeoutError:
                    continue
                except websockets.ConnectionClosed:
                    break
                try:
                    event = json.loads(frame)
                except (ValueError, TypeError):
                    continue
                # Persist and classify outside the asyncio loop so a slow SQLite
                # write cannot block WebSocket heartbeats and force reconnects.
                await asyncio.to_thread(receive_source_event,event)
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
            with lock:
                status["connected"] = False
                if not sync_enabled:
                    status["phase"] = "消息同步已暂停"
                    status["error"] = ""


async def ws_heartbeat(ws):
    while True:
        await asyncio.sleep(30)
        await ws.send(json.dumps({"type": "ping", "data": "心跳检测", "cur_time": datetime.now(TZ).strftime("%Y/%m/%d %H:%M:%S")}, ensure_ascii=False))


def history_resume_start(latest, now, last_scan):
    try:
        since = datetime.fromisoformat(latest).astimezone(TZ) - timedelta(minutes=10) if latest else now.replace(hour=0, minute=0, second=0, microsecond=0)
    except (TypeError, ValueError):
        since = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if last_scan.get("end_at"):
        try:
            since = min(since, datetime.fromisoformat(last_scan["end_at"]).astimezone(TZ) - timedelta(minutes=10))
        except (TypeError, ValueError):
            pass
    return since


def connector_worker():
    backoff = 3
    session = None
    permission_blocked = False
    while True:
        if not sync_enabled:
            if session is not None:
                session.close()
                session = None
            with lock:
                status["connected"] = False
                status["phase"] = "消息同步已暂停"
                status["error"] = ""
            sync_changed.wait(0.5)
            sync_changed.clear()
            permission_blocked = False
            continue
        if permission_blocked:
            with lock:
                status["connected"] = False
                status["phase"] = "接收受阻：云中客规则权限不足"
            sync_changed.wait(1.0)
            if not sync_changed.is_set():
                continue
            sync_changed.clear()
            if not sync_enabled:
                continue
            permission_blocked = False
            backoff = 3
        try:
            with lock:
                status["phase"] = "正在登录云中客"
                status["error"] = ""
            if session is None:
                session = make_session()
            if sync_enabled:
                now = datetime.now(TZ)
                retry_after = status.get("history_retry_after")
                if retry_after and now < datetime.fromisoformat(retry_after):
                    with lock:
                        status["coverage_gap"] = "历史接口仍在限流冷却期，断线补读待执行"
                else:
                    retry_after = None
                    if status.get("history_retry_after"):
                        status["history_retry_after"] = None
                        STORE.set_meta("history_retry_after", None)
                latest = STORE.latest_source_timestamp()
                # Customer activity is not a historical coverage watermark.
                # Resume from the previous scan even when newer live messages exist.
                last_scan = STORE.summary().get("latest_history_run") or {}
                since = history_resume_start(latest, now, last_scan)
                pending_history = STORE.oldest_unfinished_history((now - timedelta(days=3)).isoformat(timespec="seconds"))
                history_end = now
                if pending_history:
                    try:
                        since = min(since, datetime.fromisoformat(pending_history["start_at"]).astimezone(TZ))
                        history_end = min(now, datetime.fromisoformat(pending_history["end_at"]).astimezone(TZ))
                    except (TypeError, ValueError):
                        pass
                elif STORE.summary().get("unfinished_history_scans"):
                    with lock:
                        status["coverage_gap"] = "部分历史缺口超过 3 天，暂不自动补读；完整性尚未证实"
                latest_history_run = STORE.summary().get("latest_history_run") or {}
                recent_complete_scan = False
                if not pending_history and latest_history_run.get("state") in ("complete", "recovered"):
                    try:
                        recent_complete_scan = now - datetime.fromisoformat(latest_history_run["finished_at"]).astimezone(TZ) < timedelta(minutes=2)
                    except (KeyError, TypeError, ValueError):
                        pass
                if since < now - timedelta(days=3):
                    with lock:
                        status["coverage_gap"] = "超过 3 天的断线区间需手动补读；完整性尚未证实"
                    since = now - timedelta(days=3)
                if not retry_after and not recent_complete_scan:
                    try:
                        scan = bootstrap_history(session, since, history_end)
                        if (scan.get("state") == "complete" and history_end < now - timedelta(seconds=30) and
                                not STORE.oldest_unfinished_history((now - timedelta(days=3)).isoformat(timespec="seconds"))):
                            # Cover the entire interval after a recovered gap;
                            # clamping to the last ten minutes silently skipped its middle.
                            catchup_start = max(history_end - timedelta(minutes=10), now - timedelta(days=3))
                            bootstrap_history(session, catchup_start, now)
                    except RuntimeError as exc:
                        if "历史消息正在读取" not in str(exc):
                            raise
                        with lock:
                            status["coverage_gap"] = "历史补读正在进行；实时连接继续建立"
            if not sync_enabled:
                continue
            with lock:
                status["phase"] = "正在连接实时消息"
            asyncio.run(consume_ws(session))
            backoff = 3
        except Exception as exc:
            if isinstance(exc, requests.HTTPError):
                session = None
            with lock:
                status["connected"] = False
                error_text = str(exc)[:240]
                permission_blocked = "规则ID不存在或无权限" in error_text
                status["phase"] = ("接收受阻：云中客规则权限不足" if permission_blocked
                                   else "连接中断，准备重连" if sync_enabled else "消息同步已暂停")
                status["error"] = error_text if sync_enabled else ""
                if permission_blocked:
                    status["coverage_gap"] = f"云中客当前账号无法访问规则 {RULE_ID}；修复账号权限后需补读断线历史"
            if permission_blocked:
                if session is not None:
                    session.close()
                    session = None
                backoff = 60
                continue
        sync_changed.wait(backoff)
        sync_changed.clear()
        backoff = min(60, backoff * 2)


def history_recovery_worker():
    """Resume known history gaps after the source cooldown without pausing live reception."""
    while True:
        time.sleep(30)
        if not sync_enabled:
            continue
        with lock:
            if not status.get("connected"):
                continue
            retry_after = status.get("history_retry_after")
        now = datetime.now(TZ)
        if retry_after:
            try:
                if now < datetime.fromisoformat(retry_after):
                    continue
            except ValueError:
                pass
        summary = STORE.summary()
        latest_run = summary.get("latest_history_run") or {}
        pending = STORE.oldest_unfinished_history((now - timedelta(days=3)).isoformat(timespec="seconds"))
        if summary.get("unfinished_history_scans") and not pending:
            with lock:
                status["coverage_gap"] = "仍有超过 3 天的历史缺口未核实；近期缺口继续自动补读"
        if not latest_run:
            continue
        finished = latest_run.get("finished_at")
        if finished:
            try:
                minimum_wait = timedelta(minutes=15) if pending else timedelta(minutes=10)
                if now - datetime.fromisoformat(finished).astimezone(TZ) < minimum_wait:
                    continue
            except ValueError:
                pass
        try:
            if pending:
                start_at = datetime.fromisoformat(pending["start_at"]).astimezone(TZ)
                end_at = min(now, datetime.fromisoformat(pending["end_at"]).astimezone(TZ))
            else:
                end_at = now
                previous_end = datetime.fromisoformat(latest_run["end_at"]).astimezone(TZ)
                start_at = previous_end - timedelta(minutes=10)
        except (KeyError, TypeError, ValueError):
            continue
        if start_at < now - timedelta(days=3):
            start_at = now - timedelta(days=3)
        if end_at <= start_at:
            continue
        try:
            fetch_history_once(start_at, end_at)
        except RuntimeError as exc:
            if "正在读取" not in str(exc) and "限流" not in str(exc):
                retry_at = (datetime.now(TZ) + timedelta(minutes=15)).isoformat(timespec="seconds")
                STORE.set_meta("history_retry_after", retry_at)
                with lock:
                    status["coverage_gap"] = "历史补读待重试：" + str(exc)[:120]
                    status["history_retry_after"] = retry_at
        except Exception as exc:
            retry_at = (datetime.now(TZ) + timedelta(minutes=15)).isoformat(timespec="seconds")
            STORE.set_meta("history_retry_after", retry_at)
            with lock:
                status["coverage_gap"] = "历史补读失败：" + type(exc).__name__
                status["history_retry_after"] = retry_at


def public_status() -> dict:
    stored = STORE.summary()
    reception = STORE.reception_metrics()
    with lock:
        result = dict(status)
        result.update(stored)
        result['reception_metrics'] = reception
        if status["bot_count"] is not None:
            result["bot_count"] = status["bot_count"]
        latest_run = stored.get("latest_history_run")
        if latest_run and not result["history_note"]:
            result["history_limited"] = latest_run["state"] not in ("complete", "recovered")
            label = {"complete": "扫描到空页", "recovered": "后续补读已覆盖", "partial": "部分完成", "failed": "失败", "cancelled": "已取消"}.get(latest_run["state"], latest_run["state"])
            result["history_note"] = f"上次历史扫描：{label}，读取 {latest_run['pages']} 页，新增 {latest_run['added']} 条"
        result["oldest_sequence"] = 1 if stored["latest_sequence"] else 0
        result["rule_id"] = RULE_ID
        result["source_account_id"] = SOURCE_ACCOUNT_ID
        result["mode"] = "read_only_shadow"
        result["source_coverage_verified"] = False
        result["sync_enabled"] = sync_enabled
        result["send_enabled"] = False
        result["upload_enabled"] = True
        result["upload_mode"] = "confirmed_queue"
        result["analysis_version"] = active_analysis_version()
        result["analysis"] = STORE.analysis_stats(result["analysis_version"])
        result["work_items"] = PHASE3.stats()
        return result


def public_outbound_page(before: int | None = None, limit: int = 100) -> dict:
    page = STORE.outbound_page_before(before, limit)
    messages = []
    for event in page.pop("events"):
        payload = json.loads(event["raw_json"])
        raw = payload.get("data") or {}
        if not isinstance(raw, dict):
            raw = {}
        chat = raw.get("chatroom") if isinstance(raw.get("chatroom"), dict) else {}
        sender = raw.get("fromUser") if isinstance(raw.get("fromUser"), dict) else {}
        kind = str(event["event_type"] or raw.get("type") or "").lower()
        content = content_text(raw.get("content"))
        if kind in ("image", "video"):
            content = "[图片消息]" if kind == "image" else "[视频消息]"
        messages.append({
            "sequence": event["sequence"],
            "source_id": event["source_id"],
            "timestamp": event["source_timestamp"],
            "group": str(chat.get("displayName") or chat.get("remark") or chat.get("name") or chat.get("id") or "群聊"),
            "sender": str(sender.get("nickname") or sender.get("remark") or sender.get("id") or "发送账号"),
            "content": content or "[消息内容不可预览]",
            "content_type": kind or "unknown",
            "revoked": source_is_revoked(raw,payload.get("outer")),
            "source": event["first_source"],
            "send_status": str(raw.get("status") or "unknown"),
            "is_ai": raw.get("is_ai") if isinstance(raw.get("is_ai"), bool) else None,
        })
    page["messages"] = messages
    page["total"] = STORE.summary()["outbound_count"]
    return page


def public_context(sequence: int) -> dict:
    event = STORE.analysis_event(sequence)
    if not event:
        raise KeyError("message not found")
    rows = STORE.analysis_context(event, limit=80, same_customer_only=True)
    messages = []
    for row in reversed(rows):
        media = STORE.media_for_event(int(row["sequence"])) if row.get("has_image") else []
        messages.append({"sequence": row["sequence"], "direction": row["direction"],
                         "timestamp": row["source_timestamp"], "content": row["content"],
                         "same_sender": bool(event["sender_id"] and row["sender_id"] == event["sender_id"]),
                         "has_image": bool(row.get("has_image")),
                         "media": [{"id": int(item["id"]), "state": item.get("state") or "pending",
                                    "mime": item.get("mime") or ""} for item in media]})
    return {"sequence": sequence, "messages": messages,
            "note": "展示当前客户前后时间窗内的消息及本群已发消息；已发消息尚未可靠关联到单条收到消息"}


IMAGE_EXTENSIONS = {"image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp", "image/gif": ".gif"}


def detect_image_mime(header: bytes) -> str | None:
    if header.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if header.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if header.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if header.startswith(b"RIFF") and header[8:12] == b"WEBP":
        return "image/webp"
    return None


def repair_media_formats():
    with STORE.connect() as db:
        rows = db.execute("SELECT id,file_name,sha256,mime FROM media WHERE state='stored'").fetchall()
    for row in rows:
        path = MEDIA_DIR / row["file_name"]
        if path.parent != MEDIA_DIR or not path.is_file():
            continue
        with path.open("rb") as file:
            actual_mime = detect_image_mime(file.read(12))
        if actual_mime and actual_mime != row["mime"]:
            corrected_name = row["sha256"] + IMAGE_EXTENSIONS[actual_mime]
            corrected = MEDIA_DIR / corrected_name
            if not corrected.exists():
                path.replace(corrected)
            with STORE.write_lock, STORE.connect() as db:
                db.execute("UPDATE media SET file_name=?,mime=? WHERE id=?", (corrected_name, actual_mime, row["id"]))


def archive_media_worker():
    while True:
        pending = STORE.media_pending(5)
        if not pending:
            time.sleep(3)
            continue
        for item in pending:
            media_id, url = item["id"], item["url"]
            try:
                parsed = urlparse(url)
                if parsed.scheme != "https" or (parsed.hostname or "").lower() not in IMAGE_HOSTS:
                    raise RuntimeError("图片地址不在允许的来源域名内")
                with requests.get(url, stream=True, timeout=(8, 25), allow_redirects=False) as response:
                    response.raise_for_status()
                    digest = hashlib.sha256()
                    total = 0
                    temp = MEDIA_DIR / f".{media_id}.part"
                    try:
                        with temp.open("wb") as file:
                            for chunk in response.iter_content(64 * 1024):
                                if chunk:
                                    total += len(chunk)
                                    if total > MAX_MEDIA_BYTES:
                                        raise RuntimeError("图片超过 15 MB 限制")
                                    digest.update(chunk)
                                    file.write(chunk)
                        if not total:
                            raise RuntimeError("图片响应为空")
                        with temp.open("rb") as check:
                            header = check.read(12)
                        actual_mime = detect_image_mime(header)
                        if not actual_mime:
                            raise RuntimeError("图片内容不是支持的格式")
                        name = digest.hexdigest() + IMAGE_EXTENSIONS[actual_mime]
                        temp.replace(MEDIA_DIR / name)
                        STORE.media_result(media_id, "stored", file_name=name, sha256=digest.hexdigest(),
                                           byte_count=total, mime=actual_mime)
                    finally:
                        temp.unlink(missing_ok=True)
            except Exception as exc:
                STORE.media_result(media_id, "failed", error=str(exc))
            analyze_saved_event(item["event_sequence"], include_ocr=True)
            event = STORE.analysis_event(item["event_sequence"])
            if event:
                for related in STORE.analysis_context(event):
                    if related.get("direction") == "inbound" and related.get("sender_id") == event.get("sender_id"):
                        related_sequence = int(related["sequence"])
                        analyze_saved_event(related_sequence, include_ocr=True)


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def log_message(self, _format, *args):
        return

    def end_headers(self):
        if urlparse(self.path).path in ("/", "/index.html", "/app.js", "/styles.css", "/styles-refresh.css"):
            self.send_header("Cache-Control", "no-cache")
        super().end_headers()

    def send_json(self, payload: dict, code: int = 200):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def local_only(self) -> bool:
        peer = self.client_address[0]
        host = self.headers.get("Host", "").lower()
        return peer in ("127.0.0.1", "::1") and host.split(":")[0].strip("[]") in ("127.0.0.1", "localhost", "::1")

    def forbidden_static_path(self, path: str) -> bool:
        parts = [part for part in unquote(path).replace("\\", "/").split("/") if part]
        return any(part == "data" or part.startswith(".") for part in parts)

    def do_GET(self):
        if not self.local_only():
            self.send_json({"error": "local access only"}, 403)
            return
        parsed = urlparse(self.path)
        if self.forbidden_static_path(parsed.path):
            self.send_json({"error": "not found"}, 404)
            return
        if parsed.path == "/api/status":
            self.send_json(public_status())
            return
        if parsed.path == "/api/config":
            self.send_json(STORE.get_workspace_config())
            return
        if parsed.path == "/api/exports":
            self.send_json(OUTBOX.page())
            return
        if parsed.path == "/api/exports/hash-candidates":
            self.send_json(OUTBOX.hash_candidates())
            return
        if parsed.path == "/api/processing/summary":
            self.send_json({**WORKFLOW.processing_summary(),"job":dict(workflow_job_state),
                "message_sync_enabled":sync_enabled,"generation_signature":STORE.get_meta("processing_core_input_signature") or ""})
            return
        if parsed.path == "/api/processing/cases":
            params=parse_qs(parsed.query)
            try:
                self.send_json(WORKFLOW.processing_page(params.get("status",["todo"])[0],
                    limit=int(params.get("limit",["80"])[0]),offset=int(params.get("offset",["0"])[0]),search=params.get("search",[""])[0]))
            except ValueError as exc: self.send_json({"error":str(exc)},400)
            return
        if parsed.path == "/api/processing/case":
            item=WORKFLOW.processing_detail(parse_qs(parsed.query).get("id",[""])[0])
            self.send_json({"case":item} if item else {"error":"未找到客户对话"},200 if item else 404)
            return
        if parsed.path == "/api/faq-candidates":
            params=parse_qs(parsed.query)
            try:
                self.send_json({"items":WORKFLOW.list_faq_candidates(
                    status=params.get("status",["pending"])[0],scope=params.get("scope",["all"])[0],
                    limit=int(params.get("limit",["300"])[0]))})
            except ValueError as exc:
                self.send_json({"error":str(exc)},400)
            return
        if parsed.path in ("/api/workflow/data", "/api/workflow/status"):
            result = WORKFLOW.health()
            result["sources"] = WORKFLOW.list_sources()
            result["groups"] = WORKFLOW.list_groups()
            result["observed_groups"] = WORKFLOW.observed_groups()
            result["service_profiles"] = WORKFLOW.list_profiles()
            result["products"] = WORKFLOW.list_products()
            result["product_links"] = WORKFLOW.list_product_links(100)
            result["materials"] = WORKFLOW.list_materials(100)
            result["schedule_candidates"] = WORKFLOW.list_schedule(200)
            result["ai_usage"] = WORKFLOW.ai_usage()
            config = STORE.get_workspace_config()
            result["gallery_target"] = {"url": config["settings"].get("galleryUrl") or "", "configured": bool(config["settings"].get("galleryUrl"))}
            result["external_writes_enabled"] = False
            self.send_json(result)
            return
        if parsed.path == "/api/workflow/rebuild-status":
            with workflow_job_lock:
                self.send_json(dict(workflow_job_state))
            return
        if parsed.path == "/api/suggestions":
            params = parse_qs(parsed.query)
            status_value = params.get("status", ["todo"])[0]
            try:
                limit = min(300, max(1, int(params.get("limit", ["100"])[0])))
                cursor = params.get("before_created", [""])[0]
                cursor_id = params.get("before_id", [""])[0]
                self.send_json({**WORKFLOW.suggestion_page(status_value, limit, before_created=cursor,
                    before_id=cursor_id), "status": status_value})
            except ValueError as exc:
                self.send_json({"error": str(exc)}, 400)
            return
        if parsed.path == "/api/case":
            case_id = parse_qs(parsed.query).get("id", [""])[0]
            detail = WORKFLOW.case_detail(case_id)
            if not detail:
                self.send_json({"error": "case not found"}, 404)
            else:
                self.send_json({"case": detail})
            return
        if parsed.path == "/api/ai/usage":
            self.send_json({"enabled_automatically": False, "usage": WORKFLOW.ai_usage()})
            return
        if parsed.path == "/api/rules/versions":
            self.send_json({"active_version": active_analysis_version(), "algorithm_min_version": RULE_VERSION,
                            "versions": STORE.rule_versions()})
            return
        if parsed.path == "/api/evaluation":
            self.send_json(STORE.evaluation_report(active_analysis_version(), list(LABELS)))
            return
        if parsed.path == "/api/reviews":
            self.send_json({"reviews": STORE.review_history(), "legacy_audit": STORE.legacy_audit(),
                            "work_items": PHASE3.history(), "config_history": PHASE3.config_history()})
            return
        if parsed.path == "/api/work-item":
            try:
                sequence = int(parse_qs(parsed.query)["sequence"][0])
                analyses, reviews = STORE.analysis_map([sequence], active_analysis_version())
                if sequence not in analyses:
                    result = analyze_saved_event(sequence)
                    if result:
                        analyses, reviews = STORE.analysis_map([sequence], active_analysis_version())
                item = PHASE3.ensure(sequence, analyses[sequence], reviews.get(sequence)) if sequence in analyses else None
                if item is None:
                    raise KeyError(sequence)
                self.send_json({"work_item": item, "analysis": analyses[sequence],
                                "review": reviews.get(sequence)})
            except (KeyError, ValueError, TypeError):
                self.send_json({"error": "work item not found"}, 404)
            return
        if parsed.path == "/api/integrations":
            secrets = local_secrets(ENV_FILE)
            self.send_json({"deepseek_configured": bool(secrets.get("deepseekapi") or secrets.get("deepseek_api_key")),
                            "feishu_configured": bool(secrets.get("飞书app_id") and secrets.get("飞书appsecret")),
                            "feishu_app_id": secrets.get("飞书app_id") or "", "upload_enabled": True,"upload_mode":"confirmed_queue"})
            return
        if parsed.path == "/api/faq/source-status":
            self.send_json(PHASE3.quick_reply_status())
            return
        if parsed.path == "/api/context":
            try:
                sequence = int(parse_qs(parsed.query)["sequence"][0])
                self.send_json(public_context(sequence))
            except (KeyError, ValueError, TypeError):
                self.send_json({"error": "message not found"}, 404)
            return
        if parsed.path == "/api/review-queue":
            params = parse_qs(parsed.query)
            try:
                before = int(params["before"][0]) if "before" in params else None
                limit = int(params.get("limit", ["100"])[0])
                kind = params.get("kind", [""])[0]
                urgent = params.get("urgent", ["0"])[0] == "1"
                conflict = params.get("conflict", ["0"])[0] == "1"
                review_status = params.get("status", [""])[0]
                work_status = params.get("work_status", [""])[0]
                group = params.get("group", [""])[0]
                start = params.get("start", [""])[0]
                end = params.get("end", [""])[0]
                query = params.get("q", [""])[0]
                if (before is not None and before < 0) or kind not in ("", *LABELS) or review_status not in ("", "pending", "approved", "rejected") or \
                   work_status not in ("", "pending", "approved", "rejected", "handled", "needs_human") or \
                   max(map(len, (group, start, end, query))) > 100:
                    raise ValueError("invalid filter")
            except (ValueError, TypeError):
                self.send_json({"error": "invalid filter"}, 400)
                return
            self.send_json(attach_analysis(STORE.review_queue_page(active_analysis_version(), before=before,
                            limit=limit, kind=kind, urgent=urgent, conflict=conflict,
                            status=review_status, work_status=work_status, group=group,
                            start=start, end=end, query=query)))
            return
        if parsed.path == "/api/dev-audit/summary":
            self.send_json(STORE.development_audit_summary(active_analysis_version()))
            return
        if parsed.path == "/api/dev-audit":
            params = parse_qs(parsed.query)
            try:
                before = int(params["before"][0]) if "before" in params else None
                limit = int(params.get("limit", ["50"])[0])
                status_value = params.get("status", ["todo"])[0]
                kind = params.get("kind", [""])[0]
                query = params.get("q", [""])[0]
                if (before is not None and before < 0) or status_value not in ("todo", "audited", "all") or \
                   kind not in ("", *LABELS) or len(query) > 100:
                    raise ValueError("invalid development audit filter")
            except (ValueError, TypeError):
                self.send_json({"error": "invalid development audit filter"}, 400)
                return
            page = STORE.development_audit_page(active_analysis_version(), before=before, limit=limit,
                                                status=status_value, kind=kind, query=query,
                                                only_differences=parse_qs(parsed.query).get("differences", [""])[0] == "1")
            page["version"] = active_analysis_version()
            self.send_json(page)
            return
        if parsed.path == "/api/dev-audit/detail":
            try:
                sequence = int(parse_qs(parsed.query)["sequence"][0])
                material = development_audit_material(sequence)
                normalized = material["normalized"]
                media = [{"id": item.get("id"), "state": item.get("state"), "mime": item.get("mime"),
                          "error": item.get("error"), "vision_state": item.get("vision_state"),
                          "image_understanding": item.get("image_understanding")} for item in material["media"]]
                message = {key: normalized.get(key) for key in
                           ("id", "source_id", "group", "sender", "content", "timestamp", "time", "date", "attachments", "content_type")}
                message.update({"sequence": sequence, "media": media})
                self.send_json({"version": material["version"], "message": message,
                                "analysis": material["analysis"], "recommendation": material["rec"],
                                "predicted_faq_id": material["predicted_faq_id"],
                                "faq_candidates": material["faq_candidates"],
                                "context": public_context(sequence)["messages"],
                                "audit": STORE.development_audit(sequence, material["version"])})
            except (KeyError, ValueError, TypeError):
                self.send_json({"error": "development audit message not found"}, 404)
            return
        if parsed.path == "/api/coverage":
            day = parse_qs(parsed.query).get("date", [datetime.now(TZ).date().isoformat()])[0]
            try:
                datetime.strptime(day, "%Y-%m-%d")
            except ValueError:
                self.send_json({"error": "invalid date"}, 400)
                return
            self.send_json(STORE.coverage(day, rule_id=RULE_ID))
            return
        if parsed.path == "/api/messages":
            params = parse_qs(parsed.query)
            try:
                after = max(0, int(params.get("after", ["0"])[0]))
                limit = int(params.get("limit", ["200"])[0])
                before = int(params["before"][0]) if "before" in params else None
            except (ValueError, TypeError):
                self.send_json({"error": "invalid cursor or limit"}, 400)
                return
            if before is not None:
                self.send_json(attach_analysis(STORE.page_before(before, limit)))
                return
            start = params.get("start", [""])[0]
            end = params.get("end", [""])[0]
            if len(start) > 32 or len(end) > 32:
                self.send_json({"error": "invalid date range"}, 400)
                return
            self.send_json(attach_analysis(STORE.page(after, limit, start=start, end=end)))
            return
        if parsed.path == "/api/replies":
            params = parse_qs(parsed.query)
            try:
                limit = int(params.get("limit", ["100"])[0])
                before = int(params["before"][0]) if "before" in params else None
                if before is not None and before < 0:
                    raise ValueError("negative cursor")
            except (ValueError, TypeError):
                self.send_json({"error": "invalid cursor or limit"}, 400)
                return
            self.send_json(public_outbound_page(before, limit))
            return
        if parsed.path.startswith("/api/media/"):
            try:
                media_id = int(parsed.path.rsplit("/", 1)[-1])
            except ValueError:
                self.send_json({"error": "invalid media id"}, 400)
                return
            record = STORE.media_file(media_id)
            if not record:
                self.send_json({"error": "media not available"}, 404)
                return
            path = MEDIA_DIR / record["file_name"]
            if not path.is_file() or path.parent != MEDIA_DIR:
                self.send_json({"error": "media file missing"}, 404)
                return
            self.send_response(200)
            self.send_header("Content-Type", record["mime"])
            self.send_header("Content-Length", str(path.stat().st_size))
            self.send_header("Cache-Control", "private, max-age=3600")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            with path.open("rb") as file:
                while chunk := file.read(64 * 1024):
                    self.wfile.write(chunk)
            return
        return super().do_GET()

    def do_HEAD(self):
        if not self.local_only():
            self.send_error(403)
            return
        parsed = urlparse(self.path)
        if self.forbidden_static_path(parsed.path):
            self.send_error(404)
            return
        return super().do_HEAD()

    def do_POST(self):
        global sync_enabled
        if not self.local_only():
            self.send_json({"error": "local access only"}, 403)
            return
        parsed = urlparse(self.path)
        workflow_paths = ("/api/workflow/source", "/api/workflow/sync", "/api/workflow/import", "/api/workflow/material", "/api/workflow/profile", "/api/workflow/group", "/api/workflow/product", "/api/workflow/product-link", "/api/workflow/account", "/api/workflow/rebuild", "/api/workflow/case/split", "/api/workflow/case/merge", "/api/workflow/schedule/confirm", "/api/suggestion/review", "/api/faq-candidates", "/api/faq-candidate/status", "/api/faq-candidate/approve", "/api/case/ai-preview")
        workflow_paths = (*workflow_paths,"/api/processing/decision","/api/exports/prepare","/api/exports/run","/api/exports/approve-image")
        if parsed.path not in ("/api/sync", "/api/history", "/api/config", "/api/faq/import", "/api/integrations", "/api/feishu/check", "/api/review", "/api/dev-audit", "/api/work-item", "/api/work-item/ai/retry", "/api/analyze/reprocess", "/api/client-state-backup", "/api/rules/activate", *workflow_paths):
            self.send_json({"error": "not found"}, 404)
            return
        origin = self.headers.get("Origin", "").rstrip("/").lower()
        host = self.headers.get("Host", "").lower()
        if origin not in ("http://127.0.0.1:8799", "http://localhost:8799") or origin.removeprefix("http://") != host:
            self.send_json({"error": "same-origin request required"}, 403)
            return
        if parsed.path in ("/api/config", "/api/faq/import", "/api/integrations", "/api/feishu/check", "/api/review", "/api/dev-audit", "/api/work-item", "/api/work-item/ai/retry", "/api/analyze/reprocess", "/api/client-state-backup", "/api/rules/activate", *workflow_paths):
            try:
                length = int(self.headers.get("Content-Length", "0"))
                max_body = 3_200_000 if parsed.path == "/api/workflow/import" else 500000 if parsed.path in ("/api/client-state-backup", "/api/faq/import", "/api/exports/prepare") else 100000
                if length < 2 or length > max_body:
                    raise ValueError("invalid body size")
                payload = json.loads(self.rfile.read(length))
                if not isinstance(payload, dict):
                    raise ValueError("invalid payload")
                if parsed.path == "/api/rules/activate":
                    version = STORE.activate_rule_version(int(payload["version"]), RULE_VERSION)
                    analyze_saved_messages()
                    self.send_json({"ok": True, "active_version": version,
                                    "config": STORE.get_workspace_config(),
                                    "analysis": STORE.analysis_stats(version)})
                    return
                if parsed.path == "/api/processing/decision":
                    result=WORKFLOW.decide_processing(str(payload.get("case_id") or ""),str(payload.get("suggestion_id") or ""),
                        decision=str(payload.get("decision") or ""),plan_fingerprint=str(payload.get("plan_fingerprint") or ""),
                        draft=str(payload.get("draft") or ""),note=str(payload.get("note") or ""))
                    self.send_json({"ok":True,"case":result})
                    return
                if parsed.path == "/api/exports/prepare":
                    self.send_json({"ok":True,**OUTBOX.prepare(str(payload.get("case_id") or ""),
                        phashes=payload.get("phashes"),require_phash=True)})
                    return
                if parsed.path == "/api/exports/approve-image":
                    result=OUTBOX.approve_image_task(str(payload.get('case_id') or ''),str(payload.get('suggestion_id') or ''),
                        str(payload.get('plan_fingerprint') or ''),str(payload.get('task_key') or ''))
                    self.send_json({'ok':True,**result})
                    return
                if parsed.path == "/api/exports/run":
                    result=OUTBOX.dispatch(limit=int(payload.get("limit") or 5),job_id=str(payload.get("job_id") or ""),retry=payload.get("retry") is True)
                    self.send_json({"ok":True,**result},202)
                    return
                if parsed.path == "/api/workflow/source":
                    source_id = str(payload.get("source_id") or "")
                    result = WORKFLOW.set_source(source_id, enabled=payload.get("enabled"),
                        sync_enabled=payload.get("sync_enabled"), sync_interval_minutes=payload.get("sync_interval_minutes"),
                        source_account_id=payload.get("source_account_id"),url=payload.get("url"), sheet_id=payload.get("sheet_id"))
                    self.send_json({"ok": True, "source": result})
                    return
                if parsed.path == "/api/workflow/sync":
                    source_id = str(payload.get("source_id") or "")
                    action = str(payload.get("action") or "preview")
                    if action not in ("test", "preview", "sync"):
                        raise ValueError("只读操作应选择测试连接、预览或手动同步")
                    year = payload.get("year")
                    if year is not None and (not isinstance(year, int) or year < 2000 or year > 2100):
                        raise ValueError("年份映射范围无效")
                    month=payload.get('month')
                    if month is not None and (not isinstance(month,int) or not 1<=month<=12):
                        raise ValueError('月份必须为 1 至 12')
                    result = WORKFLOW.read_source(source_id, preview=action != "sync", year=year,
                        account_id=str(payload.get("source_account_id") or "") or None,month=month)
                    self.send_json(result)
                    return
                if parsed.path == "/api/workflow/import":
                    result = WORKFLOW.import_manual_source(str(payload.get("source_id") or ""),
                        str(payload.get("content") or ""), fmt=str(payload.get("format") or "json"),
                        material_type=str(payload.get("material_type") or "daily_copy"),
                        account_id=str(payload.get("source_account_id") or SOURCE_ACCOUNT_ID))
                    self.send_json({"ok": True, "report": result})
                    return
                if parsed.path == "/api/workflow/material":
                    result = WORKFLOW.set_material_state(str(payload.get("material_id") or ""),
                        state=str(payload.get("state") or "draft"), business_date=str(payload.get("business_date") or ""),
                        valid_until=str(payload.get("valid_until") or ""), group_scope=payload.get("group_scope") or [],
                        account_id=str(payload.get("source_account_id") or SOURCE_ACCOUNT_ID))
                    self.send_json({"ok": True, "material": result})
                    return
                if parsed.path == "/api/workflow/profile":
                    self.send_json({"ok": True, "profile": WORKFLOW.upsert_profile(payload)})
                    return
                if parsed.path == "/api/workflow/group":
                    self.send_json({"ok": True, "group": WORKFLOW.upsert_group(payload)})
                    return
                if parsed.path == "/api/workflow/product":
                    self.send_json({"ok": True, "product": WORKFLOW.upsert_product(payload)})
                    return
                if parsed.path == "/api/workflow/product-link":
                    self.send_json({"ok": True, "product_link": WORKFLOW.upsert_product_link(payload)})
                    return
                if parsed.path == "/api/workflow/account":
                    self.send_json({"ok": True, "account": WORKFLOW.upsert_source_account(payload)})
                    return
                if parsed.path == "/api/workflow/rebuild":
                    job = start_workflow_rebuild()
                    self.send_json({"ok": True, "job": job}, 202 if job.get("state") == "running" else 200)
                    return
                if parsed.path == "/api/workflow/case/split":
                    result = WORKFLOW.split_case_message(str(payload.get("case_id") or ""), int(payload.get("sequence")),
                        topic_key=str(payload.get("topic_key") or "manual_split"))
                    self.send_json({"ok": True, "result": result, "job": start_workflow_rebuild()}, 202)
                    return
                if parsed.path == "/api/workflow/case/merge":
                    result = WORKFLOW.merge_cases(str(payload.get("source_case_id") or ""), str(payload.get("target_case_id") or ""))
                    self.send_json({"ok": True, "result": result, "job": start_workflow_rebuild()}, 202)
                    return
                if parsed.path == "/api/workflow/schedule/confirm":
                    result = WORKFLOW.confirm_schedule(int(payload.get("year")), payload.get("group_ids"),
                        payload.get("source_id", "feishu-product-schedule"),
                        account_id=str(payload.get("source_account_id") or SOURCE_ACCOUNT_ID))
                    self.send_json({"ok": True, "report": result})
                    return
                if parsed.path == "/api/suggestion/review":
                    review = WORKFLOW.review_suggestion(str(payload.get("suggestion_id") or ""),
                        decision=str(payload.get("decision") or ""), corrections=payload.get("corrections") or {},
                        reason=str(payload.get("reason") or ""))
                    self.send_json({"ok": True, "review": review})
                    return
                if parsed.path == "/api/faq-candidates":
                    candidate=WORKFLOW.save_faq_candidate(payload)
                    self.send_json({"ok":True,"candidate":candidate})
                    return
                if parsed.path == "/api/faq-candidate/status":
                    status_value=str(payload.get("status") or "")
                    if status_value not in ("pending","rejected"):
                        raise ValueError("请选择保留待确认或暂不采用")
                    candidate=WORKFLOW.update_faq_candidate_status(str(payload.get("id") or ""),status=status_value)
                    self.send_json({"ok":True,"candidate":candidate})
                    return
                if parsed.path == "/api/faq-candidate/approve":
                    candidate=WORKFLOW.get_faq_candidate(str(payload.get("id") or ""))
                    if not candidate: raise KeyError(str(payload.get("id") or ""))
                    if candidate["status"]=="approved":
                        self.send_json({"ok":True,"candidate":candidate,"already_approved":True})
                        return
                    if candidate["scope"]=="product" and candidate.get("product_name")=="待识别商品":
                        raise ValueError("当前产品 FAQ 请先确认商品名称")
                    if candidate["scope"]=="product" and not candidate.get("valid_until"):
                        raise ValueError("当前产品 FAQ 请先填写失效日期，避免过期内容继续使用")
                    if candidate["scope"]=="product" and not candidate.get("valid_from"):
                        raise ValueError("当前产品 FAQ 请先填写开始日期")
                    if not candidate.get("group_scope"):
                        raise ValueError("请先在候选编辑中确认适用群范围，* 表示全部群")
                    old=STORE.get_workspace_config()
                    faqs=list(old.get("faqs") or [])
                    def norm(value): return re.sub(r"\s+","",str(value or "")).casefold()
                    existing=next((faq for faq in faqs if isinstance(faq,dict) and
                        norm(faq.get("question"))==norm(candidate["question"]) and
                        norm(faq.get("answer"))==norm(candidate["answer"]) and
                        faq.get("scope","general")==candidate["scope"] and
                        faq.get("product_name","")==candidate.get("product_name","") and
                        faq.get("valid_from","")==candidate.get("valid_from","") and
                        faq.get("valid_until","")==candidate.get("valid_until","") and
                        faq.get("source_account_id","")==candidate.get("source_account_id","") and
                        sorted(faq.get("group_scope") or [])==sorted(candidate.get("group_scope") or []) and
                        faq.get("enabled") and faq.get("approved")),None)
                    faq_id=str(existing.get("id")) if existing else "faq-"+candidate["id"].replace("faq-candidate-","")
                    if not existing:
                        keywords=[part.strip() for part in re.split(r"[；;\n]+",candidate.get("keywords") or "") if part.strip()]
                        faqs.append({"id":faq_id,"group":"当前产品" if candidate["scope"]=="product" else "通用",
                            "question":candidate["question"],"answer":candidate["answer"],"keywords":"，".join(keywords),
                            "source":"真实回复沉淀","enabled":True,"approved":True,"version":1,
                            "scope":candidate["scope"],"product_name":candidate.get("product_name") or "",
                            "valid_from":candidate.get("valid_from") or "","valid_until":candidate.get("valid_until") or "",
                            "source_candidate_id":candidate["id"],"source_evidence":candidate.get("evidence") or []})
                        faqs[-1].update(source_account_id=candidate.get("source_account_id") or SOURCE_ACCOUNT_ID,
                                        group_scope=candidate.get("group_scope") or [])
                    with STORE.write_lock:
                        saved=STORE.save_workspace_config(old.get("settings") or {},faqs)
                        candidate=WORKFLOW.update_faq_candidate_status(candidate["id"],status="approved",active_faq_id=faq_id)
                    PHASE3.config_change(old,saved)
                    self.send_json({"ok":True,"candidate":candidate,"active_faq_id":faq_id,
                        "duplicate_existing":bool(existing)})
                    return
                if parsed.path == "/api/case/ai-preview":
                    result = WORKFLOW.ai_preview(str(payload.get("case_id") or ""))
                    self.send_json({"ok": True, **result})
                    return
                if parsed.path == "/api/integrations":
                    app_id = payload.get("feishu_app_id")
                    app_secret = payload.get("feishu_app_secret") or ""
                    if not isinstance(app_id, str) or not isinstance(app_secret, str):
                        raise ValueError("invalid credentials")
                    update_feishu_credentials(ENV_FILE, app_id.strip(), app_secret.strip())
                    self.send_json({"ok": True, "feishu_configured": bool(local_secrets(ENV_FILE).get("飞书appsecret"))})
                    return
                if parsed.path == "/api/faq/import":
                    report = PHASE3.import_quick_replies(payload.get("entries"))
                    self.send_json({"ok": True, "report": report, "config": STORE.get_workspace_config()})
                    return
                if parsed.path == "/api/feishu/check":
                    gallery_url = str(STORE.get_workspace_config()["settings"].get("galleryUrl") or "")
                    self.send_json(check_feishu_table(ENV_FILE, gallery_url))
                    return
                if parsed.path == "/api/client-state-backup":
                    saved = {key: payload.get(key) for key in ("settings", "faqs", "audit", "decisions")}
                    if isinstance(saved["settings"], dict):
                        saved["settings"].pop("feishuAppSecret", None)
                        saved["settings"].pop("password", None)
                    self.send_json({"ok": True, **STORE.import_client_state_backup(saved)})
                    return
                if parsed.path == "/api/config":
                    settings = payload.get("settings")
                    faqs = payload.get("faqs")
                    if not isinstance(settings, dict) or not isinstance(faqs, list) or len(faqs) > 5000:
                        raise ValueError("invalid config")
                    for faq in faqs:
                        if not isinstance(faq,dict) or not isinstance(faq.get("question"),str) or not isinstance(faq.get("answer"),str):
                            raise ValueError("FAQ 问题和答案必须是文字")
                        if "group_scope" in faq and (not isinstance(faq["group_scope"],list) or not faq["group_scope"] or any(not isinstance(x,str) or not x.strip() or len(x)>160 for x in faq["group_scope"])):
                            raise ValueError("FAQ 适用群范围无效，* 表示全部群")
                        if faq.get("scope")=="product" and faq.get("enabled") and faq.get("approved"):
                            if not str(faq.get("product_name") or "").strip() or faq.get("product_name")=="待识别商品":
                                raise ValueError("启用产品 FAQ 前需要确认商品")
                            try:
                                start=datetime.strptime(str(faq.get("valid_from") or ""),"%Y-%m-%d")
                                end=datetime.strptime(str(faq.get("valid_until") or ""),"%Y-%m-%d")
                                if end<start: raise ValueError()
                            except ValueError: raise ValueError("启用产品 FAQ 前需要填写有效日期范围")
                    settings.pop("feishuAppSecret", None)
                    settings.pop("password", None)
                    old = STORE.get_workspace_config()
                    result = STORE.save_workspace_config(settings, faqs, only_if_empty=payload.get("only_if_empty") is True)
                    if old["initialized"] and not payload.get("only_if_empty"):
                        PHASE3.config_change(old, result)
                    analysis_config_keys = ("allowedShareDomains", "soldLastWeekProducts", "soldNextWeekProducts")
                    changed_analysis_config = any(old["settings"].get(key) != result["settings"].get(key)
                                                  for key in analysis_config_keys)
                    changed_rules = old["settings"].get("rules") != result["settings"].get("rules") or changed_analysis_config
                    if changed_rules:
                        STORE.create_rule_version(result["settings"], RULE_VERSION)
                        analyze_saved_messages()
                    result["active_rule_version"] = active_analysis_version()
                    self.send_json(result)
                    return
                if parsed.path == "/api/work-item":
                    sequence = int(payload.get("sequence"))
                    draft = payload.get("draft") or ""
                    note = payload.get("note") or ""
                    if not isinstance(draft, str) or not isinstance(note, str):
                        raise ValueError("invalid work item text")
                    item = PHASE3.save_decision(sequence, str(payload.get("status") or ""), draft, note)
                    self.send_json({"ok": True, "work_item": item})
                    return
                if parsed.path == "/api/work-item/ai/retry":
                    item = PHASE3.retry_ai(int(payload.get("sequence")))
                    self.send_json({"ok": True, "work_item": item})
                    return
                if parsed.path == "/api/review":
                    if payload.get("case_id"):
                        current=WORKFLOW.processing_detail(str(payload["case_id"]))
                        if not current or current["id"]!=payload.get("suggestion_id") or current["payload"].get("plan_fingerprint")!=payload.get("plan_fingerprint"):
                            raise ValueError("处理计划已更新，请刷新后复核分类")
                        if payload.get("rule_version")!=active_analysis_version(): raise ValueError("规则版本已变化，请刷新后复核分类")
                    sequence = int(payload.get("sequence"))
                    status_value = payload.get("status")
                    primary = payload.get("primary")
                    labels = payload.get("labels")
                    reason = payload.get("reason") or ""
                    if status_value not in ("pending", "approved", "rejected") or (primary is not None and primary not in LABELS) or \
                       (labels is not None and (not isinstance(labels, list) or any(label not in LABELS for label in labels))) or \
                       not isinstance(reason, str):
                        raise ValueError("invalid review")
                    decision = STORE.save_review(sequence, status_value, primary, labels, reason, rule_version=active_analysis_version())
                    self.send_json({"ok": True, "review": decision})
                    return
                if parsed.path == "/api/dev-audit":
                    sequence = int(payload.get("sequence"))
                    actual_primary = payload.get("actual_primary")
                    actual_labels = payload.get("actual_labels")
                    faq_verdict = payload.get("faq_verdict")
                    classification_only = faq_verdict is None
                    expected_faq_id = payload.get("expected_faq_id") or ""
                    note = payload.get("note") or ""
                    if actual_primary not in LABELS or not isinstance(actual_labels, list) or \
                       any(label not in LABELS for label in actual_labels) or \
                       (not classification_only and faq_verdict not in ("correct", "wrong", "missed", "not_needed")) or \
                       not isinstance(expected_faq_id, str) or len(expected_faq_id) > 200 or \
                       not isinstance(note, str) or len(note) > 1000:
                        raise ValueError("invalid development audit")
                    actual_labels = list(dict.fromkeys([*actual_labels, actual_primary]))
                    material = development_audit_material(sequence)
                    if payload.get("version") is not None and payload["version"] != material["version"]:
                        raise ValueError("规则版本已更新，请刷新样本后重新审核")
                    predicted_primary = str(material["analysis"].get("primary") or "unknown")
                    predicted_labels = set(material["analysis"].get("labels") or [])
                    predicted_labels.add(predicted_primary)
                    if (actual_primary != predicted_primary or set(actual_labels) != predicted_labels) and not note.strip():
                        raise ValueError("分类与机器判断不一致时，请填写修正原因")
                    predicted_faq_id = material["predicted_faq_id"]
                    if classification_only:
                        # Category-only reviews leave any earlier FAQ annotation untouched.
                        previous = STORE.development_audit(sequence, material["version"])
                        if previous:
                            faq_verdict = previous["faq_verdict"]
                            expected_faq_id = previous["expected_faq_id"] or ""
                            predicted_faq_id = previous["predicted_faq_id"] or ""
                            predicted_faqs = previous["predicted_faqs"]
                        else:
                            faq_verdict = "not_reviewed"
                            expected_faq_id = ""
                            predicted_faqs = material["faq_candidates"]
                    else:
                        predicted_faqs = material["faq_candidates"]
                        if faq_verdict in ("correct", "wrong") and not predicted_faq_id:
                            raise ValueError("当前分类没有实际引用 FAQ；请选择“漏匹配”或“无需 FAQ”")
                        if faq_verdict == "missed" and predicted_faq_id:
                            raise ValueError("机器已经引用 FAQ；若引用不合适，请选择“命中错误”")
                        if faq_verdict == "not_needed" and predicted_faq_id:
                            raise ValueError("机器已经引用 FAQ；若引用不合适，请选择“命中错误”")
                        faq_ids = {str(item.get("id") or "") for item in material["faqs"] if isinstance(item, dict)}
                        if expected_faq_id and expected_faq_id not in faq_ids:
                            raise ValueError("所选正确 FAQ 已不存在，请刷新后重新选择")
                        if faq_verdict == "correct" and not expected_faq_id:
                            expected_faq_id = predicted_faq_id
                        if faq_verdict == "missed" and not expected_faq_id:
                            raise ValueError("漏匹配时请选择应该命中的 FAQ")
                        if faq_verdict == "not_needed" and expected_faq_id:
                            raise ValueError("无需 FAQ 时请清空正确 FAQ")
                    audit = STORE.save_development_audit(
                        sequence, material["version"],
                        predicted_primary=predicted_primary,
                        predicted_labels=list(material["analysis"].get("labels") or []),
                        predicted_faq_id=predicted_faq_id,
                        predicted_faqs=predicted_faqs,
                        actual_primary=actual_primary, actual_labels=actual_labels, faq_verdict=faq_verdict,
                        expected_faq_id=expected_faq_id, note=note)
                    self.send_json({"ok": True, "audit": audit,
                                    "summary": STORE.development_audit_summary(material["version"])})
                    return
                count = analyze_saved_messages(force=True)
                self.send_json({"ok": True, "processed": count, "analysis": STORE.analysis_stats(active_analysis_version())})
                return
            except (ValueError, TypeError, json.JSONDecodeError) as exc:
                detail = str(exc) if parsed.path.startswith(("/api/exports", "/api/processing", "/api/review", "/api/work-item", "/api/workflow", "/api/suggestion", "/api/faq-candidate", "/api/case")) or parsed.path in ("/api/integrations", "/api/feishu/check", "/api/dev-audit") else "invalid request"
                self.send_json({"error": detail}, 400)
                return
            except KeyError:
                self.send_json({"error": "message not found"}, 404)
                return
            except requests.RequestException:
                self.send_json({"error": "外部服务暂不可连接；配置已保留"}, 502)
                return
        if parsed.path == "/api/history":
            if sync_enabled:
                self.send_json({"error": "请先暂停实时同步，再读取历史"}, 409)
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length > 2048 or length < 0:
                    raise ValueError("日期范围请求过大")
                body = json.loads(self.rfile.read(length)) if length else {}
                if not isinstance(body, dict):
                    raise ValueError("日期范围格式错误")
                today = datetime.now(TZ).date()
                start_date = datetime.strptime(body.get("start_date", today.isoformat()), "%Y-%m-%d").date()
                end_date = datetime.strptime(body.get("end_date", today.isoformat()), "%Y-%m-%d").date()
                if end_date < start_date or end_date > today or (end_date - start_date).days > 30:
                    raise ValueError("日期范围需在今天及以前，最多 31 天")
                start_at = datetime.combine(start_date, datetime.min.time(), TZ)
                end_at = datetime.combine(end_date, datetime.max.time(), TZ).replace(microsecond=0)
                result = fetch_history_once(start_at, end_at)
            except (ValueError, TypeError) as exc:
                self.send_json({"error": str(exc)}, 400)
                return
            except RuntimeError as exc:
                self.send_json({"error": str(exc)}, 409)
                return
            except Exception:
                with lock:
                    status["phase"] = "历史消息读取失败；消息同步仍已暂停"
                self.send_json({"error": "读取云中客当日历史失败；实时消息同步仍处于暂停状态"}, 502)
                return
            result.update({"ok": True, "message_count": STORE.summary()["message_count"], "sync_enabled": sync_enabled})
            self.send_json(result)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length < 1 or length > 1024:
                raise ValueError
            payload = json.loads(self.rfile.read(length))
            enabled = payload.get("enabled") if isinstance(payload, dict) else None
            if not isinstance(enabled, bool):
                raise ValueError
            save_sync_enabled(enabled)
        except (ValueError, OSError, json.JSONDecodeError):
            self.send_json({"error": "invalid sync setting"}, 400)
            return
        with lock:
            sync_enabled = enabled
            status["error"] = ""
            if enabled:
                status["phase"] = "正在连接云中客"
            elif status["connected"]:
                status["phase"] = "正在停止消息同步"
            else:
                status["phase"] = "消息同步已暂停"
        sync_changed.set()
        self.send_json({"ok": True, "sync_enabled": enabled})


def main():
    # Bind the local UI before historical maintenance so opening the workbench
    # does not wait for one recommendation row per stored message.
    server = ThreadingHTTPServer(("127.0.0.1", 8799), Handler)
    server.daemon_threads = True
    threading.Thread(target=repair_media_formats, name="media-format-repair", daemon=True).start()
    threading.Thread(target=analysis_backfill_worker, name="analysis-backfill", daemon=True).start()
    threading.Thread(target=ai_worker, args=(PHASE3,), name="deepseek-shadow-review", daemon=True).start()
    threading.Thread(target=connector_worker, name="yunzhongke-readonly", daemon=True).start()
    threading.Thread(target=history_recovery_worker, name="yunzhongke-history-recovery", daemon=True).start()
    threading.Thread(target=archive_media_worker, name="local-media-archive", daemon=True).start()
    threading.Thread(target=local_ocr_backfill_worker, name="local-image-ocr", daemon=True).start()
    threading.Thread(target=image_understanding_worker, name="selective-image-understanding", daemon=True).start()
    threading.Thread(target=feishu_material_sync_worker, name="feishu-readonly-sync", daemon=True).start()
    threading.Thread(target=processing_core_worker, name="local-information-processing", daemon=True).start()
    threading.Thread(target=cross_group_filter_worker, name="cross-group-filter", daemon=True).start()
    server.serve_forever()


if __name__ == "__main__":
    main()
