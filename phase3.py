"""Persistent shadow-mode work items and evidence-bound DeepSeek suggestions."""
from __future__ import annotations

import hashlib
import json
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from difflib import SequenceMatcher
from functools import lru_cache
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests

from analysis_engine import LABELS, RULE_VERSION
from workflow_store import resolve_schedule_context
from processing_core import POLICY_VERSION, eligible_faqs, scope_allows
from source_config import configured_source_account_id
from ingestion_store import source_is_revoked

SOURCE_ACCOUNT_ID = configured_source_account_id()


AI_LABELS = set(LABELS) - {"unknown"}
ROUTES = {"service": "售后问题", "warning": "撤回/风险内容", "mini": "撤回/风险内容", "burst": "撤回/风险内容",
          "feedback": "晒单图库", "order_image": "订单截图表", "requirement": "产品需求表",
          "product_link": "当天商品链接", "thanks": "强提醒感谢", "qa": "人工答疑",
          "closing": "无需回复", "ordinary": "普通交流", "unknown": "待判断"}
# Keep model calls off until a later, explicit product decision enables them.
AI_ENABLED = False


def local_secrets(path: Path) -> dict[str, str]:
    result = {}
    if not path.is_file():
        return result
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        parts = re.split(r"[：:=]", line, maxsplit=1)
        if len(parts) == 2:
            result[parts[0].strip().lower().replace(" ", "")] = parts[1].strip().strip("\"'")
    return result


def update_feishu_credentials(path: Path, app_id: str, app_secret: str):
    if not re.fullmatch(r"cli_[A-Za-z0-9]{8,80}", app_id):
        raise ValueError("飞书应用 ID 格式不正确")
    if app_secret and (len(app_secret) > 200 or "\n" in app_secret or "\r" in app_secret):
        raise ValueError("飞书应用凭证格式不正确")
    existing = path.read_text(encoding="utf-8-sig").splitlines() if path.exists() else []
    updates = {"飞书app_id": app_id}
    if app_secret:
        updates["飞书app secret"] = app_secret
    seen = set()
    lines = []
    for line in existing:
        parts = re.split(r"[：:=]", line, maxsplit=1)
        key = parts[0].strip().lower() if len(parts) == 2 else ""
        if key in updates:
            lines.append(key + "=" + updates[key])
            seen.add(key)
        else:
            lines.append(line)
    for key, value in updates.items():
        if key not in seen:
            lines.append(key + "=" + value)
    temporary = path.with_suffix(".env.tmp")
    temporary.write_text("\n".join(lines) + "\n", encoding="utf-8")
    temporary.replace(path)


def check_feishu_table(env_file: Path, gallery_url: str) -> dict:
    secrets = local_secrets(env_file)
    app_id, app_secret = secrets.get("飞书app_id"), secrets.get("飞书appsecret")
    if not app_id or not app_secret:
        raise ValueError("飞书应用凭证未配置")
    url = urlparse(gallery_url)
    if url.scheme != "https" or not url.hostname or not url.hostname.endswith(".feishu.cn"):
        raise ValueError("图库地址不是有效的飞书链接")
    match = re.fullmatch(r"/base/([A-Za-z0-9]+)", url.path.rstrip("/"))
    table = parse_qs(url.query).get("table", [""])[0]
    if not match or not re.fullmatch(r"tbl[A-Za-z0-9]+", table):
        raise ValueError("图库地址缺少多维表格应用或数据表 ID")
    auth = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
                         json={"app_id": app_id, "app_secret": app_secret}, timeout=15)
    auth.raise_for_status()
    token_data = auth.json()
    if token_data.get("code") != 0 or not token_data.get("tenant_access_token"):
        return {"ok": False, "stage": "auth", "code": token_data.get("code"),
                "message": str(token_data.get("msg") or "凭证验证失败")[:160]}
    response = requests.get("https://open.feishu.cn/open-apis/bitable/v1/apps/" + match[1] + "/tables/" + table + "/fields",
                            headers={"Authorization": "Bearer " + token_data["tenant_access_token"]},
                            params={"page_size": 100}, timeout=15)
    response.raise_for_status()
    body = response.json()
    if body.get("code") != 0:
        return {"ok": False, "stage": "table", "code": body.get("code"),
                "message": str(body.get("msg") or "无法读取目标表格")[:160]}
    fields = [str(item.get("field_name") or "") for item in ((body.get("data") or {}).get("items") or [])]
    return {"ok": True, "stage": "table", "field_names": fields, "field_count": len(fields),
            "app_token": match[1], "table_id": table, "upload_enabled": False}


FAQ_TOPICS = {
    "purchase_method": ("怎么买", "怎么拍", "如何拍", "不会拍", "不会操作", "拍不了", "拍不上", "不能拍",
                        "买不了", "怎么下单", "购买方法", "购买流程"),
    "payment": ("结算", "付款", "支付", "0元为什么", "为什么付款", "有运费", "运费"),
    "multi_item": ("两件", "多件", "拍几个", "拍多单", "几个", "几件", "多拍"),
    "tutorial_video": ("教程视频", "看教程视频", "看视频", "视频在哪", "视频在哪里", "视频在哪儿"),
    "price": ("价格不对", "价格", "价不对", "怎么买是", "价格显示", "价格不一样", "多少钱", "什么价格", "支付金额"),
    "coupon": ("优惠券", "没券", "没有券", "券没", "券已经", "领券", "领不了券", "优惠券没", "券不能用", "没有优惠", "没优惠", "领完", "已领过"),
    "link": ("链接", "连接", "求链接", "要链接", "上链接", "发链接", "产品在哪"),
    "availability": ("还有没有", "还有吗", "什么时候再来", "啥时候再来", "再来一波", "什么时候上", "催好物",
                     "催下一个", "抢完", "已发完", "卖完", "库存", "活动结束"),
    "group_invite": ("进群", "什么群", "怎么邀请", "邀请人", "邀请朋友", "拉好友"),
    "assistant": ("小助理", "加助理", "联系助理"),
    "mini_program": ("小程序", "发小程序", "小程序消息"),
    "promotion": ("广告", "发广告", "推广", "征婚", "婚姻介绍", "加微信", "微信联系方式", "借宝地",
                  "公众号文章", "公众号链接", "视频号", "二维码", "群外链接", "外部链接", "刷屏", "小程序",
                  "免费领取", "领福利", "代理招募", "返佣", "引流"),
    "burst": ("刷屏", "连续发", "乱发", "乱发东西", "短时间多条"),
    "shipping": ("发货", "物流", "快递", "什么时候发"),
}


def _faq_normalize(value: str) -> str:
    value = str(value or "").lower().replace("連結", "链接").replace("連接", "链接").replace("连接", "链接")
    value = value.replace("优惠卷", "优惠券").replace("没有卷", "没有券").replace("没卷", "没券").replace("卷没", "券没")
    value = value.replace("俩件", "两件").replace("二件", "两件")
    value = re.sub(r"https?://\S+", " ", value)
    return re.sub(r"[^0-9a-z\u3400-\u9fff]+", "", value)


_FAQ_TOPIC_ALIASES = {topic: tuple(_faq_normalize(alias) for alias in aliases)
                      for topic, aliases in FAQ_TOPICS.items()}


@lru_cache(maxsize=4096)
def _faq_topics(value: str) -> frozenset[str]:
    topics = {topic for topic, aliases in _FAQ_TOPIC_ALIASES.items() if any(alias in value for alias in aliases)}
    has_number = bool(re.search(r"\d+(?:\.\d+)?", value))
    has_price = bool(re.search(r"\d+(?:\.\d+)?(?:元|块)", value) or
                     (has_number and any(term in value for term in ("一件", "两件", "几件", "为什么", "为啥", "怎么是", "咋是"))))
    if has_price:
        topics.add("price")
    return frozenset(topics)


@lru_cache(maxsize=4096)
def _faq_bigrams(value: str) -> frozenset[str]:
    return frozenset(value[index:index + 2] for index in range(len(value) - 1)
                     if value[index:index + 2] not in {"怎么", "如何", "什么", "请问", "有没有", "在哪"})


def _faq_score(message: str, question: str, keywords: list[str], answer: str = "") -> float:
    fields = [value for value in [question, *keywords] if value]
    best = 0.0
    for field in fields:
        if len(field) >= 2 and field in message:
            best = max(best, min(.99, .82 + min(len(field), 12) * .014))
        elif len(message) >= 3 and message in field:
            best = max(best, min(.96, .80 + min(len(message), 12) * .013))
        else:
            common = SequenceMatcher(None, message, field, autojunk=False).find_longest_match(0, len(message), 0, len(field)).size
            if common >= 4:
                best = max(best, min(.96, .82 + min(common, 12) * .014))

    message_topics = _faq_topics(message)
    for field in fields:
        shared_topics = message_topics & _faq_topics(field)
        if shared_topics:
            longest_alias = max((len(alias) for topic in shared_topics for alias in _FAQ_TOPIC_ALIASES[topic]
                                 if alias in message or alias in field), default=2)
            best = max(best, min(.78, .56 + longest_alias * .025))

    # Answers can reveal the FAQ's subject when the imported title is generic.
    answer_topics = message_topics & _faq_topics(answer)
    if answer_topics:
        best = max(best, .48)

    message_grams = _faq_bigrams(message)
    for field in fields:
        grams = _faq_bigrams(field)
        if message_grams and grams:
            dice = 2 * len(message_grams & grams) / (len(message_grams) + len(grams))
            best = max(best, min(.86, dice * .86))
    answer_grams = _faq_bigrams(answer[:500])
    if message_grams and answer_grams:
        answer_dice = 2 * len(message_grams & answer_grams) / (len(message_grams) + len(answer_grams))
        best = max(best, min(.58, answer_dice * .58))
    return best


def faq_candidates(text: str, faqs: list[dict], limit: int = 3, *, product_names=None) -> list[dict]:
    """Find local FAQ candidates, including disabled entries, without authorizing their use."""
    message = _faq_normalize(text)
    if not message:
        return []
    result = []
    seen = set()
    for faq in faqs:
        if not isinstance(faq, dict) or not faq.get("answer"):
            continue
        product_name=str(faq.get("product_name") or "").strip()
        if faq.get("scope")=="product" and product_name and _faq_normalize(product_name) not in message and product_name not in (product_names or []):
            continue
        question = str(faq.get("question") or "").strip()
        terms = [term.strip() for term in re.split(r"[,，、;；\n\r]+", str(faq.get("keywords") or "")) if len(_faq_normalize(term)) >= 2]
        answer = str(faq["answer"])
        score = _faq_score(message, _faq_normalize(question), [_faq_normalize(term) for term in terms], _faq_normalize(answer))
        if score >= .52:
            identity = (_faq_normalize(question), _faq_normalize(answer[:300]))
            if identity in seen:
                continue
            seen.add(identity)
            result.append({"id": str(faq.get("id") or ""), "question": question,
                           "answer": answer, "group": str(faq.get("group") or ""),
                           "score": round(score, 3), "version": str(faq.get("version") or "local"),
                           "enabled": bool(faq.get("enabled")), "approved": bool(faq.get("approved"))})
    return sorted(result, key=lambda item: (-item["score"], item["question"]))[:max(1, min(10, limit))]


def minimize_for_ai(value: str) -> str:
    value = re.sub(r"https?://[^\s]+", lambda match: "[链接:" + (urlparse(match.group()).hostname or "未知")[:80] + "]", value)
    value = re.sub(r"(?<!\d)1[3-9]\d{9}(?!\d)", "[手机号]", value)
    return re.sub(r"(?<!\d)\d{12,}(?!\d)", "[编号]", value)


def valid_customer_service_url(url):
    parsed = urlparse(url)
    return parsed.scheme in ("http", "https") and bool(parsed.hostname) and not parsed.username and not parsed.password


def observed_group_service_contact(store, account_id, group_id, message_time):
    """Use an explicit same-group reply from the same day only as a draft source."""
    if not account_id or not group_id or not message_time: return {'state':'missing'}
    try:
        moment=datetime.fromisoformat(str(message_time).replace('Z','+00:00'))
        if moment.tzinfo is None: moment=moment.replace(tzinfo=timezone(timedelta(hours=8)))
        day=moment.astimezone(timezone(timedelta(hours=8))).date().isoformat()
    except (ValueError,TypeError): return {'state':'missing'}
    with store.connect() as db:
        rows=db.execute("""SELECT sequence,raw_json FROM events WHERE direction='outbound' AND source_account_id=?
            AND group_id=? AND source_timestamp>=? AND source_timestamp<=?
            AND parse_state<>'suppressed_duplicate_broadcast' ORDER BY source_timestamp DESC LIMIT 500""",
            (account_id,group_id,day+'T00:00:00+08:00',moment.astimezone(timezone(timedelta(hours=8))).isoformat(timespec='seconds'))).fetchall()
    found={}
    for row in rows:
        payload=json.loads(row['raw_json'] or '{}');raw=payload.get('data') or {}
        if not isinstance(raw,dict) or not raw.get('replyMessageId') or source_is_revoked(raw,payload.get('outer')): continue
        body=raw.get('content')
        if not isinstance(body,str) or not any(word in body for word in ('售后','客服','小助理')):continue
        urls={u.rstrip('.,;!?)，。！）') for u in re.findall(r'https?://[^\s<>]+',body)}
        urls={u for u in urls if valid_customer_service_url(u)}
        if len(urls)!=1 or not store.explicit_reply_target(int(row['sequence'])):continue
        url=next(iter(urls));found.setdefault(url,[]).append(int(row['sequence']))
    if len(found)!=1:return {'state':'ambiguous' if found else 'missing'}
    url=next(iter(found));return {'state':'resolved','url':url,'reply_sequences':found[url], 'date':day}


def is_service_contact_faq(faq):
    topic = str(faq.get("question") or "") + " " + str(faq.get("keywords") or "")
    if any(term in topic for term in ("不会拍", "拍不了", "下单协助", "购买教程")):
        return False
    return (any(term in topic for term in ("回购", "复购", "之前活动", "历史活动")) or
            any(term in topic for term in ("推售后", "联系客服", "售后客服", "客服入口", "售后链接", "联系售后")))


def faq_service_contact(faqs, *, historical=False, group_id=""):
    """Resolve a service FAQ only when it explicitly covers this group.

    A purchase-help link must never silently become the general service link.
    An unscoped service link is not a safe default because service contacts vary by group.
    Multiple distinct targets for one purpose remain unresolved.
    """
    general, previous = [], []
    for faq in faqs:
        if not faq.get("enabled") or not faq.get("approved"): continue
        if "group_scope" not in faq or not scope_allows(faq.get("group_scope"), group_id): continue
        if not is_service_contact_faq(faq): continue
        topic = str(faq.get("question") or "") + " " + str(faq.get("keywords") or "")
        history = any(term in topic for term in ("回购", "复购", "之前活动", "历史活动"))
        urls = re.findall(r"https?://[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]+", str(faq.get("answer") or ""))
        urls = list(dict.fromkeys(url.rstrip(".,;!?)") for url in urls if valid_customer_service_url(url)))
        if len(urls) != 1: continue
        (previous if history else general).append({"url": urls[0], "faq": faq})
    candidates = (previous or general) if historical else general
    if not candidates: return {"state": "missing", "faq_ids": []}
    if len({row["url"] for row in candidates}) != 1:
        return {"state": "ambiguous", "faq_ids": [row["faq"].get("id") for row in candidates]}
    selected = candidates[0]
    return {"state": "resolved", "url": selected["url"], "faq": selected["faq"],
            "faq_ids": [row["faq"].get("id") for row in candidates]}


def customer_service_contact(settings: dict, *, historical: bool = False):
    url = str(settings.get("aftercareUrl") or "").strip()
    parsed = urlparse(url)
    valid = valid_customer_service_url(url)
    template_key = "previousActivityTemplate" if historical else "customerServiceTemplate"
    template = str(settings.get(template_key) or "").strip() or ("请联系售后客服核实是否有复购链接：{{售后链接}}" if historical else "需要一对一协助，可联系售后客服：{{售后链接}}")
    return {"route": "aftercare_contact", "assignee": "aftercare", "action": "交售后客服一对一核实复购链接" if historical else "交售后客服一对一跟进客户需求",
            "draft": re.sub(r"\{\{\s*售后链接\s*\}\}", url, template) if valid else "",
            "blockers": [] if valid else ["尚未配置有效的售后客服链接"]}


def recommendation(primary: str, normalized: dict, analysis: dict, settings: dict, faqs: list,
                   media: list, context: list, business_context: dict | None = None) -> dict:
    content = str(normalized.get("content") or "")
    features = analysis.get("features") or {}
    customer_id = str(normalized.get("sender_id") or "")
    rules = settings.get("rules") if isinstance(settings.get("rules"), dict) else {}
    conversation_window = max(60, min(1800, int(rules.get("conversationSeconds") or 1800)))
    try:
        current_at = datetime.fromisoformat(str(normalized.get("timestamp") or ""))
    except ValueError:
        current_at = None
    conversation = []
    for item in context:
        if not item.get("content") or not (item.get("direction") == "outbound" or
                (item.get("direction") == "inbound" and item.get("sender_id") == customer_id)):
            continue
        try:
            item_at = datetime.fromisoformat(str(item.get("source_timestamp") or ""))
        except ValueError:
            continue
        if current_at and abs((current_at - item_at).total_seconds()) > conversation_window:
            continue
        conversation.append(item)
    conversation.append({"sequence": normalized.get("sequence"), "source_timestamp": normalized.get("timestamp") or "",
                         "content": content, "direction": "inbound", "sender_id": customer_id})
    conversation.sort(key=lambda item: (item.get("source_timestamp") or "", int(item.get("sequence") or 0)))
    customer_text = " ".join(str(item.get("content") or "") for item in conversation
                             if item.get("direction") == "inbound" and item.get("sender_id") == customer_id)
    conversation_text = "；".join(("群内回复：" if item.get("direction") == "outbound" else "客户：") +
                                 str(item.get("content") or "") for item in conversation)
    business_context = business_context or {}
    product_names = business_context.get("matched_products") or []
    group_id = str(normalized.get("group_id") or "")
    faqs = eligible_faqs(faqs, business_date=business_context.get("business_date") or str(normalized.get("timestamp") or "")[:10],
                         products=product_names, group_id=group_id,
                         account_id=str(normalized.get("source_account_id") or SOURCE_ACCOUNT_ID))
    # A legacy, unscoped service FAQ may still be useful as historical knowledge,
    # but its customer-service URL must not appear as a candidate for another group.
    faq_lookup_set = [faq for faq in faqs if not is_service_contact_faq(faq) or
                      ("group_scope" in faq and scope_allows(faq.get("group_scope"), group_id))]
    settings = dict(settings)
    service_contact = None
    schedule_only=bool(features.get("is_schedule_question") and re.search(r"几点|几时|(?:什么时候|啥时候).*(?:上架|上链接|发链接)",content))
    product_request=bool(features.get("is_product_request") and not schedule_only)
    needs_service = primary in ("service", "requirement") or "service" in (analysis.get("labels") or []) or (primary == "qa" and product_request and not features.get("is_daily_purchase"))
    if (needs_service and settings.get("useFaqServiceContacts") is True
            and not settings.get("aftercareUrl") and settings.get("aftercareScopeResolved") != "group"):
        service_contact = faq_service_contact(faqs, historical=bool(features.get("is_historical_activity")), group_id=group_id)
        if service_contact["state"] == "resolved":
            url = service_contact["url"]
            template = str(service_contact["faq"]["answer"]).replace(url, "{{售后链接}}")
            settings.update(aftercareUrl=url, aftercareTemplate=template,
                            customerServiceTemplate=template, previousActivityTemplate=template)
    def faq_lookup(value: str) -> str:
        return str(value or "").strip()
    def lookup_faqs(value: str, limit: int = 3):
        return faq_candidates(faq_lookup(value), faq_lookup_set, limit, product_names=product_names)
    result = {"action": "人工核验", "draft": "", "evidence": list(analysis.get("evidence") or []),
              "blockers": [], "faq": [], "checklist": [], "sendable": False}
    if service_contact:
        result["service_contact_source"] = {key: service_contact[key] for key in ("state", "faq_ids")}
        if service_contact["state"] == "resolved":
            result["evidence"].append("客服入口及话术来自已审核启用 FAQ：" + service_contact["faq"]["question"])
            result["selected_service_faq_id"] = service_contact["faq"].get("id")
        elif service_contact["state"] == "ambiguous":
            result["blockers"].append("同一用途存在多个不同客服链接，请先按群配置或明确 FAQ 适用范围")
    result["intent"] = primary
    result["assignee"] = "aftercare" if primary in ("service", "requirement") or (primary == "qa" and product_request and not features.get("is_daily_purchase")) else "group_human" if primary in ("qa", "product_link", "thanks") else "reviewer"
    result["assignee_name"] = {"aftercare": "售后客服 · 一对一服务", "group_human": "群内人工答复", "reviewer": "人工审核"}[result["assignee"]]
    result["response_state"] = analysis.get("response_state") or ("not_needed" if primary in ("closing", "ordinary") else "open")
    result["response_needed"] = False if primary in ("closing", "ordinary", "order_image", "feedback") else None if primary == "unknown" or analysis.get("response_state") == "replied" else True
    result["image_understanding"] = [item["image_understanding"] for item in media if item.get("image_understanding")]
    result["attachment_tasks"] = []
    result["evidence_media_ids"] = []
    for picture in media:
        if primary=="requirement" and not set(analysis.get("labels") or []).intersection({"order_image","feedback"}) and picture.get("usage") not in {"order","feedback"}:
            result["evidence_media_ids"].append(picture.get("id"))
            continue
        if features.get("image_usage") == "problem_evidence" or picture.get("usage") == "problem_evidence":
            result["evidence_media_ids"].append(picture.get("id"))
            continue
        visual = picture.get("image_understanding") or {}
        kind = visual.get("image_kind") if float(visual.get("confidence") or 0) >= .85 else None
        route = "order_image" if kind == "order_screenshot" else "feedback" if kind == "product_feedback" else None
        if not route and len(media) == 1:
            route = "order_image" if features.get("related_image_is_order") else "feedback" if features.get("related_image_is_feedback") or primary == "feedback" else None
        if route:
            target = str(settings.get("orderTableUrl" if route == "order_image" else "galleryUrl") or "")
            result["attachment_tasks"].append({"route": route, "media_id": picture.get("id"), "source_sequence": picture.get("event_sequence"),
                "action_key": "media:" + str(picture.get("id")) + ":" + route,
                "action": "核验订单截图后登记订单图表" if route == "order_image" else "将反馈图及配文核验后登记图库",
                "destination_url": target, "blockers": [] if target.startswith("https://") and picture.get("state") == "stored" else ["图片或目标表待核验"]})
    result["conversation_window_seconds"] = conversation_window
    order_verification=(settings.get("businessTargetSchemas") or {}).get("orderTableUrl") or {}
    order_schema_blocker=(order_verification.get("error_summary") or "订单图表字段尚未建好，需补充表结构") if (
        order_verification.get("url")==settings.get("orderTableUrl") and order_verification.get("schema_ready") is False) else ""
    if order_schema_blocker:
        for task in result["attachment_tasks"]:
            if task["route"]=="order_image": task["blockers"].append(order_schema_blocker)
    if primary == "closing":
        result.update(action="归档，无需回复", evidence=result["evidence"] or ["简短确认或礼貌收尾，没有新增诉求"])
    elif primary == "ordinary":
        result.update(action="记录交流，无需群回复", checklist=["如包含隐含问题，可改判为日常答疑"])
    elif primary == "thanks":
        result["action"] = "@豆妈感谢，适当回应"
        result["draft"] = str(settings.get("thanksTemplate") or "谢谢支持～")
        result["checklist"] = ["结合前后文确认感谢对象和语气"]
    elif primary == "requirement":
        result["action"] = "登记产品需求并交售后客服一对一跟进"
        result["destination_url"] = str(settings.get("demandTableUrl") or "")
        result["checklist"] = ["保留客户原话、图片和同一客户上下文", "确认具体商品或需求内容", "交售后客服核实复购、库存或个性化需要，不把承办方当作问题类别"]
        if not result["destination_url"].startswith("https://"):
            result["blockers"].append("尚未配置有效的产品需求表地址")
    elif primary == "order_image":
        result["action"] = "人工核对订单截图后登记到订单图表"
        result["destination_url"] = str(settings.get("orderTableUrl") or "")
        result["checklist"] = ["核实图片确为订单截图", "检查订单号、姓名、地址等敏感信息后再入表"]
        if not media:
            result["blockers"].append("未找到可核验的图片附件")
        elif any(item.get("state") != "stored" for item in media):
            result["blockers"].append("部分图片未完成本地归档")
        if not result["destination_url"].startswith("https://"):
            result["blockers"].append("尚未配置有效的订单图表地址")
        if order_schema_blocker: result["blockers"].append(order_schema_blocker)
    elif primary == "product_link":
        result["action"] = "提供当天商品的带链接文案"
        result["draft"] = str(settings.get("todayProductCopy") or "")
        result["checklist"] = ["确认该文案是今天当前在售商品", "核对链接和价格后再由人工审核"]
        if not result["draft"]:
            result["blockers"].append("尚未配置当天商品带链接文案")
    elif primary == "service":
        url = str(settings.get("aftercareUrl") or "").strip()
        historic = bool(features.get("is_historical_activity"))
        template_value = settings.get("previousActivityTemplate") if historic else settings.get("aftercareTemplate")
        template = str(template_value or "").strip()
        if not template:
            template = "请联系售后客服咨询之前活动的商品链接：{{售后链接}}" if historic else "请联系售后客服：{{售后链接}}"
        result["action"] = "请客户联系售后查询之前活动链接" if historic else "发送售后链接并人工提醒售后客服"
        if valid_customer_service_url(url):
            result["draft"] = re.sub(r"\{\{\s*售后链接\s*\}\}", url, template)
        else:
            result["blockers"].append("尚未配置有效的售后链接")
        result["faq"] = lookup_faqs(customer_text)
        result["checklist"] = ["确认售后问题和对应订单", "发送链接后人工提醒售后客服", "核对当前客服链接"]
    elif primary in ("warning", "mini", "burst"):
        result["action"] = "人工确认后要求撤回，并使用对应固定 FAQ 话术"
        lookup_text = " ".join([customer_text, *(analysis.get("evidence") or []), str(analysis.get("reason") or "")])
        result["faq"] = lookup_faqs(lookup_text)
        if result["faq"]:
            if primary == "mini":
                preferred_terms = ("小程序",)
            elif primary == "burst":
                preferred_terms = ("刷屏", "连续消息")
            elif features.get("is_public_article"):
                preferred_terms = ("公众号", "文章")
            elif features.get("is_video_channel"):
                preferred_terms = ("视频号",)
            elif features.get("is_qr_code"):
                preferred_terms = ("二维码", "扫码")
            else:
                preferred_terms = ("群外链接", "广告", "推广")
            matched = [item for item in result["faq"]
                       if any(term in item["question"] for term in preferred_terms)]
            if matched:
                result["faq"] = matched + [item for item in result["faq"] if item not in matched]
            result["evidence"].append("找到可核对的撤回处理话术候选；答案和审核状态见下方")
            approved = [item for item in result["faq"] if item["enabled"] and item["approved"] and item["score"] >= .60]
            # A moderation suggestion must not assert an action that this
            # workbench cannot verify or perform.
            approved = [item for item in approved if not re.search(r"(?:已|已经|现已|刚刚).{0,6}(?:踢|移出|封禁|拉黑)", str(item.get("answer") or ""))]
            preferred_approved = [item for item in approved
                                  if any(term in item["question"] for term in preferred_terms)]
            best = (preferred_approved or [{}])[0]
            if not preferred_approved:
                result["blockers"].append("未找到已审核启用的对应类别撤回 FAQ，不能用其他类别话术替代" if primary != "mini" else "未找到已审核启用的小程序专用撤回 FAQ，不能用其他类别话术替代")
            elif best.get("enabled") and best.get("approved") and best.get("score", 0) >= .60:
                result["draft"] = best["answer"]
                result["evidence"].append("待审草稿引用已审核启用 FAQ：" + best["question"])
            else:
                result["blockers"].append("对应撤回 FAQ 尚未审核启用，不能生成可批准话术")
        else:
            result["blockers"].append("未匹配到对应的已配置撤回 FAQ")
        allow = [value.strip().lower() for value in str((settings.get("rules") or {}).get("allowlist") or "").splitlines() if value.strip()]
        if str(normalized.get("sender") or "").lower() in allow or str(normalized.get("group") or "").lower() in allow:
            result["blockers"].append("命中群或成员白名单")
        result["checklist"] = ["确认内容属于小程序、公众号、广告、视频号、二维码、群外链接或刷屏", "要求撤回原消息", "人工复核固定 FAQ 后再处理"]
    elif primary == "feedback":
        result["action"] = "反馈图候选，人工核验入库"
        result["destination_url"] = str(settings.get("galleryUrl") or "")
        result["context_text"] = conversation_text[:1000]
        result["checklist"] = ["核对图片是否完整且清晰", "将同一客户的配文一起保存", "核实是否适合纳入飞书图库"]
        if not media:
            result["blockers"].append("未找到可核验的图片附件")
        elif any(item.get("state") != "stored" for item in media):
            result["blockers"].append("部分图片未完成本地归档")
        if not settings.get("galleryUrl"):
            result["blockers"].append("尚未配置图库地址")
    elif primary == "qa":
        daily_purchase = bool(features.get("is_daily_purchase"))
        result["action"] = "日常拍单疑问，人工答复" if daily_purchase else "结合客户上下文答疑，人工审核回复"
        result["context_text"] = conversation_text[:1000]
        result["faq"] = lookup_faqs(customer_text, limit=10)
        result["checklist"] = ["结合该客户同群 30 分钟内的上下文，不按单句孤立判断"]
        if daily_purchase:
            result["evidence"].append("按业务规则由人工核对拍单、价格或优惠，不自动套用 FAQ")
        elif any(word in conversation_text for word in ("价", "券", "优惠", "两件", "怎么拍", "拍不了", "下单")):
            result["checklist"].append("核对当时有效的价格、优惠及下单页面；必要时请成员提供截图")
        elif any(word in conversation_text for word in ("链接", "还有", "什么时候", "库存")):
            result["checklist"].append("核对当前链接、活动时间和库存后再答复")
        else:
            result["checklist"].append("在来源中核对商品事实；无法确认时明确转人工")
        best = result["faq"][0] if result["faq"] else None
        approved_match = next((candidate for candidate in result["faq"]
                               if candidate["score"] >= .65 and candidate["enabled"] and candidate["approved"]), None)
        if daily_purchase:
            pass
        elif approved_match:
            result["draft"] = approved_match["answer"]
            result["selected_faq_id"] = approved_match["id"]
            result["evidence"].append("待审草稿引用已审核启用 FAQ：" + approved_match["question"])
        elif not best:
            result["blockers"].append("本地 FAQ 中没有找到相关候选；需人工回答或补充 FAQ")
        elif not best["enabled"] or not best["approved"]:
            result["blockers"].append("命中待审核 FAQ「" + best["question"] + "」；请在 FAQ 管理中核对并启用后再引用")
        else:
            result["blockers"].append("命中 FAQ「" + best["question"] + "」，匹配度 " + str(round(best["score"] * 100)) + "%；需人工核对")
    else:
        result["action"] = "待判断，人工明确诉求"
        result["blockers"].append("初步分类证据不足")
    secondary = []
    if primary == "requirement" or (primary == "qa" and result["assignee"] == "aftercare"):
        contact = customer_service_contact(settings, historical=bool(features.get("is_historical_activity")))
        secondary.append(contact)
        result["draft"] = contact["draft"]
        if contact["draft"]:
            result["blockers"]=[reason for reason in result["blockers"] if not reason.startswith(("本地 FAQ 中没有找到相关候选", "命中 FAQ「", "命中待审核 FAQ「"))]
        result["actions"] = ([{"route": "requirement", "action": "登记产品需求表", "destination_url": result.get("destination_url"),
                               "blockers": [item for item in result["blockers"] if "需求表" in item]}] if primary == "requirement" else []) + [contact]
    analysis_labels = set(analysis.get("labels") or [])
    if primary != "service" and "service" in analysis_labels:
        historic = bool(features.get("is_historical_activity"))
        url = str(settings.get("aftercareUrl") or "").strip()
        template_value = settings.get("previousActivityTemplate") if historic else settings.get("aftercareTemplate")
        template = str(template_value or "").strip() or ("请联系售后客服咨询之前活动的商品链接：{{售后链接}}" if historic else "请联系售后：{{售后链接}}")
        secondary.append({"route": "service", "action": "发送售后链接并人工提醒售后客服" if not historic else "联系售后查询之前活动链接",
                          "draft": re.sub(r"\{\{\s*售后链接\s*\}\}", url, template) if valid_customer_service_url(url) else "",
                          "blockers": [] if valid_customer_service_url(url) else ["尚未配置有效的售后链接"]})
    if primary != "feedback" and "feedback" in analysis_labels:
        gallery_url = str(settings.get("galleryUrl") or "")
        secondary.append({"route": "feedback", "action": "将晒单图片和配文一起登记图库",
                          "destination_url": gallery_url, "context_text": conversation_text[:1000],
                          "blockers": [] if gallery_url else ["尚未配置图库地址"]})
    if primary != "order_image" and "order_image" in analysis_labels:
        order_url = str(settings.get("orderTableUrl") or "")
        secondary.append({"route": "order_image", "action": "人工核对后登记订单图表",
                          "destination_url": order_url,
                          "blockers": ([order_schema_blocker] if order_schema_blocker else []) + ([] if order_url.startswith("https://") else ["尚未配置有效的订单图表地址"])})
    if primary != "requirement" and "requirement" in analysis_labels and not (primary=="qa" and schedule_only):
        demand_url = str(settings.get("demandTableUrl") or "")
        secondary.append({"route": "requirement", "action": "将产品诉求登记到需求表",
                          "destination_url": demand_url, "context_text": conversation_text[:1000],
                          "blockers": [] if demand_url.startswith("https://") else ["尚未配置有效的产品需求表地址"]})
    if primary != "product_link" and "product_link" in analysis_labels:
        copy = str(settings.get("todayProductCopy") or "")
        secondary.append({"route": "product_link", "action": "提供当天商品的带链接文案", "draft": copy,
                          "blockers": [] if copy else ["尚未配置当天商品带链接文案"]})
    if primary != "thanks" and "thanks" in analysis_labels:
        secondary.append({"route": "thanks", "action": "适当回应感谢",
                          "draft": str(settings.get("thanksTemplate") or "谢谢支持～"), "blockers": []})
    if secondary:
        result["secondary_actions"] = secondary
    if service_contact and service_contact["state"] == "resolved":
        source = service_contact["faq"]
        result["faq"] = [{"id": str(source.get("id") or ""), "question": source["question"],
            "answer": source["answer"], "group": str(source.get("group") or ""),
            "version": str(source.get("version") or "local"), "enabled": True, "approved": True,
            "score": 1.0, "match_basis": "客服用途及适用范围"},
            *[row for row in result["faq"] if row.get("id") != source.get("id")]]
    if analysis.get("response_state") == "replied":
        result.update(action="来源已明确关联人工回复，请核对客户诉求是否解决", draft="", blockers=[],
                      answered_by_sequences=(analysis.get("features") or {}).get("confirmed_reply_sequences") or [])
    if primary in ("feedback", "qa", "service", "order_image", "requirement", "product_link", "thanks"):
        result["nearby_text"] = [str(item["content"])[:200] for item in conversation[:8]]
        result["context_text"] = conversation_text[:1000]
        result["conversation_id"] = analysis.get("conversation_id") or ""
        result["conversation_sequences"] = analysis.get("conversation_sequences") or []
    if business_context:
        result["business_context"] = business_context
    return result


class Phase3:
    def __init__(self, store, env_file: Path):
        self.store = store
        self.env_file = env_file
        self._active: set[int] = set()
        self._active_lock = threading.Lock()
        with self.store.write_lock, self.store.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS work_items (
                    event_sequence INTEGER PRIMARY KEY REFERENCES events(sequence),
                    route TEXT NOT NULL, labels_json TEXT NOT NULL,
                    recommendation_json TEXT NOT NULL, fingerprint TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending', edited_draft TEXT,
                    ai_state TEXT NOT NULL DEFAULT 'not_needed', ai_json TEXT,
                    ai_error TEXT, ai_ms INTEGER, ai_model TEXT,
                    ai_attempts INTEGER NOT NULL DEFAULT 0,
                    revision INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL, reviewed_at TEXT, reviewer TEXT
                );
                CREATE TABLE IF NOT EXISTS work_item_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_sequence INTEGER NOT NULL REFERENCES events(sequence),
                    action TEXT NOT NULL, before_json TEXT, after_json TEXT NOT NULL,
                    changed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS config_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL,
                    before_json TEXT NOT NULL, after_json TEXT NOT NULL, changed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS source_quick_replies (
                    source_key TEXT PRIMARY KEY, group_name TEXT NOT NULL, title TEXT NOT NULL,
                    body TEXT NOT NULL, media_kind TEXT NOT NULL, source_hash TEXT NOT NULL,
                    observed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS source_quick_reply_changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, source_key TEXT NOT NULL,
                    old_hash TEXT NOT NULL, new_hash TEXT NOT NULL, observed_at TEXT NOT NULL
                );
            """)
            columns = {row["name"] for row in db.execute("PRAGMA table_info(work_items)")}
            if "ai_attempts" not in columns:
                db.execute("ALTER TABLE work_items ADD COLUMN ai_attempts INTEGER NOT NULL DEFAULT 0")

    def config_change(self, old: dict, new: dict):
        old_settings, new_settings = old.get("settings") or {}, new.get("settings") or {}
        for kind, fields in (("售后", ("aftercareUrl", "aftercareTemplate", "customerServiceTemplate", "previousActivityTemplate", "aftercareName")),
                             ("流转目标", ("galleryUrl", "orderTableUrl", "demandTableUrl", "todayProductCopy", "thanksTemplate", "allowedShareDomains")),
                             ("FAQ", ())):
            before = {k: old_settings.get(k) for k in fields} if fields else old.get("faqs") or []
            after = {k: new_settings.get(k) for k in fields} if fields else new.get("faqs") or []
            if before != after:
                with self.store.write_lock, self.store.connect() as db:
                    db.execute("INSERT INTO config_history(kind,before_json,after_json,changed_at) VALUES (?,?,?,?)",
                               (kind, self.store.dumps(before), self.store.dumps(after), self.store.now()))

    def config_history(self, limit=30):
        with self.store.connect() as db:
            rows = db.execute("SELECT id,kind,changed_at FROM config_history ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(row) for row in rows]

    def import_quick_replies(self, entries: list[dict]) -> dict:
        if not isinstance(entries, list) or len(entries) > 5000:
            raise ValueError("快捷回复快照格式不正确")
        config = self.store.get_workspace_config()
        faqs = list(config["faqs"] or [])
        existing = {str(faq.get("source_id") or "") for faq in faqs if isinstance(faq, dict)}
        now = self.store.now()
        imported = media_only = changed = 0
        with self.store.write_lock, self.store.connect() as db:
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                group = str(entry.get("group") or "")[:100].strip()
                number = str(entry.get("n") or "")[:20].strip()
                title = str(entry.get("title") or "")[:250].strip()
                body = str(entry.get("answer") or "")[:4000].strip()
                media = str(entry.get("media") or "")[:20].strip()
                if not group or not number or (not title and not body and not media):
                    continue
                key = group + ":" + number
                digest = hashlib.sha256(self.store.dumps([group, title, body, media]).encode()).hexdigest()
                old = db.execute("SELECT source_hash FROM source_quick_replies WHERE source_key=?", (key,)).fetchone()
                if old and old["source_hash"] != digest:
                    changed += 1
                    db.execute("INSERT INTO source_quick_reply_changes(source_key,old_hash,new_hash,observed_at) VALUES (?,?,?,?)",
                               (key, old["source_hash"], digest, now))
                db.execute("""INSERT INTO source_quick_replies(source_key,group_name,title,body,media_kind,source_hash,observed_at)
                              VALUES (?,?,?,?,?,?,?) ON CONFLICT(source_key) DO UPDATE SET group_name=excluded.group_name,
                              title=excluded.title,body=excluded.body,media_kind=excluded.media_kind,
                              source_hash=excluded.source_hash,observed_at=excluded.observed_at""",
                           (key, group, title, body, media, digest, now))
                if body and key not in existing:
                    faqs.append({"id": "source-" + hashlib.sha256(key.encode()).hexdigest()[:16], "source_id": key,
                                 "source": "云中客只读快照", "group": group, "question": title or "快捷回复 " + number,
                                 "answer": body, "keywords": title, "enabled": False, "approved": False,
                                 "version": 1, "imported_at": now})
                    imported += 1
                    existing.add(key)
                elif not body:
                    media_only += 1
        if imported:
            self.store.save_workspace_config(config["settings"], faqs)
        return {"source_entries": len(entries), "faq_candidates_added": imported,
                "media_or_empty": media_only, "source_changes": changed,
                "faq_total": len(faqs), "observed_at": now}

    def quick_reply_status(self) -> dict:
        with self.store.connect() as db:
            row = db.execute("SELECT COUNT(*) n,COUNT(DISTINCT group_name) groups,MAX(observed_at) last_seen FROM source_quick_replies").fetchone()
            changed = db.execute("SELECT COUNT(*) FROM source_quick_reply_changes").fetchone()[0]
        return {"source_entries": row["n"], "groups": row["groups"], "last_seen": row["last_seen"], "source_changes": changed}

    def ensure(self, sequence: int, analysis: dict, review: dict | None = None,
               product_hint: dict | str | None = None) -> dict | None:
        event = self.store.analysis_event(sequence)
        if not event:
            return None
        normalized = json.loads(event["normalized_json"] or "null") or {}
        normalized["sequence"] = sequence
        normalized["source_account_id"] = str(event.get("source_account_id") or SOURCE_ACCOUNT_ID)
        normalized["group_id"] = str(event.get("group_id") or "")
        normalized.setdefault("sender_id", str(event.get("sender_id") or ""))
        context = self.store.analysis_context(event)
        try:
            event_at = datetime.fromisoformat(str(event.get("source_timestamp") or normalized.get("timestamp") or "").replace("Z", "+00:00"))
            if event_at.tzinfo is None: event_at = event_at.replace(tzinfo=timezone(timedelta(hours=8)))
            past_context = []
            for value in context:
                try:
                    at = datetime.fromisoformat(str(value.get("source_timestamp") or "").replace("Z", "+00:00"))
                    if at.tzinfo is None: at = at.replace(tzinfo=timezone(timedelta(hours=8)))
                    if at <= event_at: past_context.append(value)
                except (ValueError, TypeError): pass
            context = past_context
        except (ValueError, TypeError):
            context = []
        schedule_evidence = [normalized.get("content") or "", *[value.get("content") or "" for value in context
            if value.get("direction") == "inbound" and str(value.get("sender_id") or "") == normalized["sender_id"]]]
        business_context = resolve_schedule_context(self.store, normalized["source_account_id"],
            event.get("source_timestamp") or normalized.get("timestamp"), schedule_evidence, product_hint,group_id=normalized["group_id"])
        config = self.store.get_workspace_config()
        settings = dict(config["settings"] or {})
        # Once group-specific service profiles exist, never silently apply an
        # unrelated/global link. A legacy global URL is usable only when its
        # scope was explicitly confirmed as all groups.
        group_service = None
        workflow_ready = None
        dynamic_faqs = []
        matched_copy_refs=[]
        product_link_ambiguity = False
        product_link_unmatched = False
        with self.store.connect() as db:
            workflow_ready = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='business_groups'").fetchone()
            if workflow_ready:
                group_service = db.execute("""SELECT p.name,p.url,p.template FROM business_groups g
                    JOIN service_profiles p ON p.id=g.service_profile_id AND p.active=1
                    WHERE g.source_account_id=? AND g.group_id=? AND g.active=1""",
                    (str(event.get("source_account_id") or SOURCE_ACCOUNT_ID),str(event.get("group_id") or ""))).fetchone()
                group_id = str(event.get("group_id") or "")
                try:
                    business_date = datetime.fromisoformat(str(event.get("source_timestamp") or "")).astimezone(timezone(timedelta(hours=8))).date().isoformat()
                except (ValueError, TypeError):
                    business_date = ""
                if business_date and group_id:
                    rows = db.execute("""SELECT id,material_type,title,body,group_scope_json,source_ref_json,link_targets_json,version FROM daily_materials
                        WHERE source_account_id=? AND state='confirmed' AND business_date=? AND (valid_until IS NULL OR valid_until>=?)""",
                        (str(event.get("source_account_id") or SOURCE_ACCOUNT_ID),business_date,business_date)).fetchall()
                    eligible_copies=[]
                    matched_names=business_context.get("matched_products") or []
                    for material in rows:
                        if not scope_allows(material["group_scope_json"], group_id):
                            continue
                        source_ref=json.loads(material["source_ref_json"] or "{}")
                        product_name=str(source_ref.get("product_name") or "")
                        if product_name and product_name not in matched_names: continue
                        if material["material_type"] == "daily_faq":
                            dynamic_faqs.append({"id":material["id"],"question":material["title"],"answer":material["body"],
                                "keywords":material["title"],"group":"每日资料","enabled":True,"approved":True,"version":material["version"],
                                "scope":"product" if product_name else "general","product_name":product_name,
                                "valid_from":business_date,"valid_until":business_date})
                        elif material["material_type"]=="daily_copy":
                            body=str(material["body"] or "")
                            product_matches=bool(product_name or any(_faq_normalize(name) in _faq_normalize(body+str(material["title"])) for name in matched_names))
                            urls=re.findall(r"https?://[^\s<>]+",body)
                            if product_matches and urls and not re.search(r"【链接】|\[链接\]",body):
                                eligible_copies.append(material)
                    settings["todayProductCopy"]=""
                    if len(eligible_copies)==1:
                        settings["todayProductCopy"]=eligible_copies[0]["body"]
                        matched_copy_refs=[{"id":eligible_copies[0]["id"],"version":eligible_copies[0]["version"],"source":"confirmed_daily_copy"}]
                    links = db.execute("""SELECT product_id,product_name,url,copy,group_scope_json FROM product_links WHERE source_account_id=? AND state='confirmed' AND business_date=?""",
                        (str(event.get("source_account_id") or SOURCE_ACCOUNT_ID),business_date)).fetchall()
                    valid_links=[]
                    for link in links:
                        if scope_allows(link["group_scope_json"], group_id):
                            valid_links.append(link)
                    catalog={row["id"]:{"name":row["name"],"aliases":json.loads(row["aliases_json"] or "[]")}
                             for row in db.execute("SELECT id,name,aliases_json FROM products WHERE state='active'").fetchall()}
                    customer_text=" ".join([str(normalized.get("content") or ""), *(business_context.get("matched_products") or [])]).casefold()
                    def link_terms(link):
                        item=catalog.get(str(link["product_id"] or ""),{})
                        return [str(value).strip().casefold() for value in [link["product_name"],item.get("name"),*(item.get("aliases") or [])]
                                if str(value or "").strip() and len(str(value).strip())>=2]
                    explicit_terms={term for term in catalog.values() for term in [term_value.casefold() for term_value in [term["name"],*(term["aliases"] or [])]
                                      if str(term_value or "").strip() and len(str(term_value).strip())>=2]
                                    if term in customer_text}
                    explicit_mentions=bool(explicit_terms or matched_names)
                    matched_links=[link for link in valid_links if any(term in customer_text for term in link_terms(link))]
                    if explicit_mentions:
                        matched_links=[link for link in matched_links if any(term in customer_text for term in link_terms(link))]
                        if len(matched_links)==1:
                            link=matched_links[0]
                        elif len(matched_links)>1:
                            settings["todayProductCopy"] = ""
                            product_link_ambiguity = True
                            link=None
                        else:
                            if not matched_copy_refs:
                                settings["todayProductCopy"] = ""
                                product_link_unmatched = True
                            link=None
                    elif len(valid_links)==1:
                        link=valid_links[0]
                    elif len(matched_links)==1:
                        link=matched_links[0]
                    else:
                        link=None
                    if link:
                        settings["todayProductCopy"] = str(link["copy"] or (link["product_name"]+"\n"+link["url"]))
                    elif not explicit_mentions and len(valid_links)>1:
                        settings["todayProductCopy"] = ""
                        product_link_ambiguity = True
                    elif not explicit_mentions and not matched_copy_refs:
                        settings["todayProductCopy"] = ""
        if group_service:
            settings["aftercareName"] = group_service["name"]
            settings["aftercareUrl"] = group_service["url"]
            settings["aftercareTemplate"] = group_service["template"]
            settings["aftercareScopeResolved"] = "group"
        elif not workflow_ready:
            settings["aftercareScopeResolved"] = "legacy_global"
        elif settings.get("aftercareScope") != "all_groups":
            settings["aftercareUrl"] = ""
            settings["aftercareScopeResolved"] = "missing_group_mapping"
        else:
            settings["aftercareScopeResolved"] = "explicit_legacy_global"
        try:
            as_of_date=datetime.fromisoformat(str(event.get("source_timestamp") or "").replace("Z","+00:00")).astimezone(timezone(timedelta(hours=8))).date().isoformat()
        except (ValueError,TypeError):
            as_of_date=""
        faqs = [*(config["faqs"] or []), *dynamic_faqs]
        faqs=[faq for faq in faqs if not isinstance(faq,dict) or
            (as_of_date or faq.get("scope")!="product") and
            (not faq.get("valid_from") or as_of_date and str(faq.get("valid_from"))<=as_of_date) and
            (not faq.get("valid_until") or as_of_date and str(faq.get("valid_until"))>=as_of_date)]
        primary = review.get("primary_label") if review and review.get("status") != "pending" and review.get("primary_label") else analysis.get("primary", "unknown")
        labels = review.get("labels") if review and review.get("status") != "pending" and review.get("labels") else analysis.get("labels") or [primary]
        observed_contact=None
        if primary in ('service','requirement') and settings.get('aftercareScopeResolved')=='missing_group_mapping':
            observed_contact=observed_group_service_contact(self.store,normalized['source_account_id'],normalized['group_id'],event.get('source_timestamp'))
            if observed_contact.get('state')=='resolved':
                settings['aftercareUrl']=observed_contact['url']
                settings['aftercareScopeResolved']='observed_group_reply'
        media = self.store.media_for_event(sequence)
        linked_images = set((analysis.get("features") or {}).get("adjacent_image_sequences") or [])
        related_media_sequences = [item.get("sequence") for item in context
                                   if item.get("has_image") and item.get("direction") == "inbound"
                                   and item.get("sender_id") == normalized.get("sender_id") and item.get("sequence") in linked_images]
        existing_media_ids = {item.get("id") for item in media}
        for related_sequence in related_media_sequences:
            for related_media in self.store.media_for_event(int(related_sequence)):
                if related_media.get("id") not in existing_media_ids:
                    media.append(related_media)
                    existing_media_ids.add(related_media.get("id"))
        image_sources = [int(item["event_sequence"]) for item in media if item.get("event_sequence") is not None]
        image_analyses, _ = self.store.analysis_map(image_sources, int(analysis.get("version") or 0)) if image_sources else ({}, {})
        for item in media:
            source_features = (image_analyses.get(item.get("event_sequence")) or {}).get("features") or {}
            item["usage"] = source_features.get("image_usage")
            if review and review.get("rule_version")==analysis.get("version") and primary in ("order_image","feedback"):
                item["usage"]="order" if primary=="order_image" else "feedback"
        effective_analysis = {**analysis, "primary": primary, "labels": labels}
        suggestion = recommendation(primary, normalized, effective_analysis, settings, faqs, media, context,business_context)
        if observed_contact and observed_contact.get('state')=='resolved':
            suggestion['evidence'].append('客服链接来自本群同日明确关联的真人回复，使用前需核对是否仍有效')
            suggestion['service_contact_source']={'state':'observed_group_reply','reply_sequences':observed_contact['reply_sequences'],'date':observed_contact['date']}
        elif observed_contact and observed_contact.get('state')=='ambiguous':
            suggestion['blockers'].append('本群同日出现不同客服链接，需人工确定适用链接')
        suggestion["material_refs"]=matched_copy_refs
        if primary=="product_link" and not business_context.get("matched_products") and len(business_context.get("planned_products") or [])>1:
            suggestion["draft"]=""
            suggestion["blockers"]=list(dict.fromkeys([*(suggestion.get("blockers") or []),"当天涉及多个商品，先确认客户问的是哪一款，再选择购买文案"]))
        if primary=="product_link" and not suggestion.get("draft") and any(row.get("has_placeholder_link") for row in business_context.get("copy_records") or []):
            suggestion["blockers"].append("当天源文案使用【链接】占位符，需要补充真实购买链接")
        if product_link_ambiguity and primary == "product_link":
            suggestion["draft"] = ""
            suggestion["blockers"] = list(dict.fromkeys([*(suggestion.get("blockers") or []),
                "当前日期和群范围命中多个商品链接，请人工确认具体商品"] ))
        if product_link_unmatched and primary == "product_link":
            suggestion["draft"] = ""
            suggestion["blockers"] = list(dict.fromkeys([*(suggestion.get("blockers") or []),
                "客户提到的商品没有匹配到当前日期、群范围内的确认链接"] ))
        if settings.get("aftercareScopeResolved") == "missing_group_mapping" and suggestion.get("assignee") == "aftercare" and (suggestion.get("service_contact_source") or {}).get("state") != "resolved":
            suggestion["draft"] = ""
            suggestion["action"] = "转交售后一对一客服；请先为当前群配置对应客服链接"
            suggestion["blockers"] = list(dict.fromkeys([*(suggestion.get("blockers") or []),
                "当前群尚未配置客服映射；到“业务资料与群配置”选择本群客服链接"] ))
        suggestion["classification_source"] = "human" if review and review.get("status") != "pending" else "rules"
        suggestion["machine_intent"] = analysis.get("primary")
        machine_labels=set(analysis.get("labels") or [])
        reviewed_labels=set(labels or [])
        # An order screenshot is already a no-reply route. Some reviewers add
        # the orthogonal "closing" label to state that no response is needed;
        # that extra label does not change classification or disposition.
        if primary == analysis.get("primary") == "order_image":
            machine_labels.discard("closing")
            reviewed_labels.discard("closing")
        suggestion["human_rule_disagreement"] = suggestion["classification_source"] == "human" and (
            primary != analysis.get("primary") or reviewed_labels != machine_labels)
        needs_recheck = suggestion["human_rule_disagreement"] and (review or {}).get("rule_version") != analysis.get("version")
        if needs_recheck and review and review.get('status') == 'approved' and review.get('rule_version'):
            prior,_ = self.store.analysis_map([sequence],int(review['rule_version']))
            previous=prior.get(sequence) or {}
            if (previous.get('primary') == analysis.get('primary') and
                    set(previous.get('labels') or []) == set(analysis.get('labels') or [])):
                needs_recheck=False
                suggestion['evidence'].append('规则升级后机器分类未变化，沿用已确认的人工分类')
        suggestion["requires_classification_review"] = needs_recheck
        if needs_recheck:
            suggestion["action"] = "旧人工分类与当前规则存在分歧，请先复核本条分类和图片用途"
            suggestion["draft"] = ""
            suggestion["blockers"].append("旧人工分类尚未针对当前规则复核，暂停原分类下的处理建议")
        fingerprint = hashlib.sha256(self.store.dumps({"primary": primary, "labels": labels, "analysis": analysis.get("version"),
            "analysis_evidence": analysis.get("evidence"), "analysis_features": analysis.get("features"),
            "ai_policy": 11,
            "processing_policy": POLICY_VERSION,
            "business_context": business_context,
            "classification_review_version": (review or {}).get("rule_version"),
            "faq_policy": 7,
            "settings": {k: settings.get(k) for k in ("aftercareUrl", "aftercareTemplate", "customerServiceTemplate", "previousActivityTemplate", "aftercareScope", "aftercareScopeResolved", "useFaqServiceContacts", "warningTemplate",
                "galleryUrl", "orderTableUrl", "demandTableUrl", "businessTargetSchemas", "todayProductCopy", "todayProductScope", "thanksTemplate", "allowedShareDomains", "rules")},
            "faqs": [faq for faq in faqs if isinstance(faq, dict)],
            "media": [(m.get("id"), m.get("state")) for m in media]}).encode()).hexdigest()
        content = str(normalized.get("content") or "").strip()
        eligible = AI_ENABLED and primary not in ("closing",) and bool(content) and not re.fullmatch(r"\[(?:图片|视频)消息\]", content)
        ai_state = ("pending" if eligible else "disabled" if AI_ENABLED is False and primary not in ("closing",) and content else "not_needed")
        now = self.store.now()
        with self.store.write_lock, self.store.connect() as db:
            row = db.execute("SELECT * FROM work_items WHERE event_sequence=?", (sequence,)).fetchone()
            if not row:
                db.execute("""INSERT INTO work_items(event_sequence,route,labels_json,recommendation_json,fingerprint,ai_state,created_at,updated_at)
                              VALUES (?,?,?,?,?,?,?,?)""", (sequence, primary, self.store.dumps(labels), self.store.dumps(suggestion),
                              fingerprint, ai_state, now, now))
            elif row["fingerprint"] != fingerprint and row["status"] == "pending":
                db.execute("""UPDATE work_items SET route=?,labels_json=?,recommendation_json=?,fingerprint=?,
                              ai_state=?,ai_json=NULL,ai_error=NULL,ai_attempts=0,revision=revision+1,updated_at=? WHERE event_sequence=?""",
                           (primary, self.store.dumps(labels), self.store.dumps(suggestion), fingerprint,
                            ai_state, now, sequence))
        item = self.get(sequence, fingerprint)
        if item:
            for key in ("classification_source", "machine_intent", "human_rule_disagreement", "requires_classification_review"):
                item["recommendation"][key] = suggestion[key]
            if needs_recheck:
                item["recommendation"]["action"] = suggestion["action"]
                item["recommendation"]["draft"] = ""
                item["recommendation"]["blockers"] = list(dict.fromkeys([*(item["recommendation"].get("blockers") or []), *suggestion["blockers"]]))
        return item

    def get(self, sequence: int, current_fingerprint: str | None = None) -> dict | None:
        with self.store.connect() as db:
            row = db.execute("SELECT * FROM work_items WHERE event_sequence=?", (sequence,)).fetchone()
        if not row:
            return None
        item = dict(row)
        item["labels"] = json.loads(item.pop("labels_json"))
        item["recommendation"] = json.loads(item.pop("recommendation_json"))
        item["ai"] = json.loads(item.pop("ai_json") or "null")
        faq = item["recommendation"].get("faq") or []
        if item["ai"] and (item["route"] != "qa" or item["ai"].get("route") != "qa" or
                           not faq or faq[0].get("score", 0) < .85):
            item["ai"]["draft"] = ""
        item["stale"] = bool(current_fingerprint and item["fingerprint"] != current_fingerprint)
        item.pop("fingerprint")
        return item

    def save_decision(self, sequence: int, status: str, draft: str, note: str = "") -> dict:
        if status not in ("pending", "approved", "rejected", "handled", "needs_human"):
            raise ValueError("invalid work item status")
        if len(draft) > 4000 or len(note) > 1000:
            raise ValueError("review text too long")
        if status == "approved":
            version = self.store.active_rule_version(RULE_VERSION)
            analyses, reviews = self.store.analysis_map([sequence], version)
            current, review = analyses.get(sequence), reviews.get(sequence)
            if current and review and review.get("status") != "pending" and review.get("rule_version") != version:
                if (review.get("primary_label") and review["primary_label"] != current.get("primary")) or (review.get("labels") and set(review["labels"]) != set(current.get("labels") or [])):
                    raise ValueError("旧人工分类与当前规则不同，请先针对当前规则重新复核分类")
        with self.store.write_lock, self.store.connect() as db:
            row = db.execute("SELECT * FROM work_items WHERE event_sequence=?", (sequence,)).fetchone()
            if not row:
                raise KeyError(sequence)
            rec = json.loads(row["recommendation_json"])
            if status == "approved" and (rec.get("blockers") or not (draft.strip() or row["route"] in ("closing", "ordinary"))):
                raise ValueError("建议缺少可靠依据或必要配置，不能批准")
            if status == "approved" and row["ai_state"] == "done" and row["ai_json"]:
                ai_route = json.loads(row["ai_json"]).get("route")
                if ai_route != row["route"]:
                    review = db.execute("SELECT status FROM review_decisions WHERE event_sequence=?", (sequence,)).fetchone()
                    if not review or review["status"] != "approved":
                        raise ValueError("AI 与规则分类不一致，请先人工确认分类")
            now = self.store.now()
            db.execute("""UPDATE work_items SET status=?,edited_draft=?,reviewed_at=?,reviewer='本机审核员',
                          revision=revision+1,updated_at=? WHERE event_sequence=?""",
                       (status, draft.strip(), now if status != "pending" else None, now, sequence))
            after = {"status": status, "draft": draft.strip(), "note": note}
            db.execute("INSERT INTO work_item_log(event_sequence,action,before_json,after_json,changed_at) VALUES (?,?,?,?,?)",
                       (sequence, status, self.store.dumps({"status": row["status"], "draft": row["edited_draft"]}),
                        self.store.dumps(after), now))
        return self.get(sequence)

    def history(self, limit=100):
        with self.store.connect() as db:
            rows = db.execute("SELECT id,event_sequence,action,after_json,changed_at FROM work_item_log ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [{**dict(row), "detail": json.loads(row["after_json"])} for row in rows]

    def retry_ai(self, sequence: int):
        if not AI_ENABLED:
            raise ValueError("AI 模型调用已关闭；当前仅使用本地规则和 FAQ")
        with self.store.write_lock, self.store.connect() as db:
            row = db.execute("SELECT status FROM work_items WHERE event_sequence=?", (sequence,)).fetchone()
            if not row:
                raise KeyError(sequence)
            if row["status"] != "pending":
                raise ValueError("已审核待办不能重新调用模型")
            db.execute("UPDATE work_items SET ai_state='pending',ai_error=NULL,ai_json=NULL,ai_attempts=0,updated_at=? WHERE event_sequence=?",
                       (self.store.now(), sequence))
        return self.get(sequence)

    def stats(self):
        with self.store.connect() as db:
            rows = db.execute("SELECT route,status,ai_state,COUNT(*) n FROM work_items GROUP BY route,status,ai_state").fetchall()
            disagreements = db.execute("SELECT COUNT(*) FROM work_items WHERE ai_state='done' AND json_extract(ai_json,'$.route')<>route").fetchone()[0]
        result = {"total": 0, "routes": {}, "statuses": {}, "ai": {}, "ai_disagreements": disagreements}
        for row in rows:
            n = row["n"]
            result["total"] += n
            for key, value in (("routes", row["route"]), ("statuses", row["status"]), ("ai", row["ai_state"])):
                result[key][value] = result[key].get(value, 0) + n
        return result

    def ai_batch(self, limit=3):
        if not AI_ENABLED:
            return 0
        secrets = local_secrets(self.env_file)
        key = secrets.get("deepseekapi") or secrets.get("deepseek_api_key")
        if not key:
            return 0
        with self.store.connect() as db:
            rows = db.execute("""SELECT w.event_sequence FROM work_items w JOIN events e ON e.sequence=w.event_sequence
                                 WHERE w.ai_state='pending' AND w.status='pending'
                                 ORDER BY e.sequence ASC LIMIT ?""", (limit,)).fetchall()
        selected = []
        for row in rows:
            sequence = row["event_sequence"]
            with self._active_lock:
                if sequence in self._active:
                    continue
                self._active.add(sequence)
            selected.append(sequence)
        def run_one(sequence: int):
            try:
                self._ai_one(sequence, key)
            finally:
                with self._active_lock:
                    self._active.discard(sequence)
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(run_one, selected))
        return len(selected)

    def _ai_one(self, sequence: int, key: str):
        if not AI_ENABLED:
            return
        item = self.get(sequence)
        event = self.store.analysis_event(sequence)
        if not item or not event or item["ai_state"] != "pending":
            return
        normalized = json.loads(event["normalized_json"] or "null") or {}
        content = minimize_for_ai(str(normalized.get("content") or "")[:1200])
        context = self.store.analysis_context(event)
        previous = [minimize_for_ai(str(row.get("content") or "")[:200]) for row in context if row.get("sender_id") == event["sender_id"] and row.get("content")][:4]
        faq = item["recommendation"].get("faq") or []
        has_image = bool(self.store.media_for_event(sequence))
        payload = {"message": content, "nearby_same_sender": previous, "has_image": has_image,
                   "initial_route": item["route"],
                   "faq_candidates": [{"id": f["id"], "question": f["question"], "answer": f["answer"][:500]} for f in faq]}
        system = ("你是群消息审核台的辅助分类器。只返回 JSON 对象，字段为 route、labels、reason、draft、needs_human。"
                  "route 和 labels 只能使用 service/feedback/warning/mini/burst/qa/closing/ordinary/unknown。"
                  "允许多个标签，但要按下面的业务定义判断，不能跟随 initial_route 猜测。"
                  "service 仅指已下单后的发货、退款、退换、质量问题、售后客服；想买、求链接、想下单、已拍不属于售后。"
                  "feedback 仅指带图的真实使用评价或明确投稿意图；没有图片不能判为反馈图，单独的图片占位符不能推断图片内容。"
                  "qa 是明确问题，包括购买方法、商品链接、券和尺码；ordinary 是没有待答问题的普通交流、购买状态或商品意见。"
                  "closing 仅指好、行、收到、OK、谢谢等简短确认且无新诉求。warning/mini/burst 需要明确广告、未经允许的小程序或刷屏证据，不能仅凭链接存在判违规。"
                  "draft 只在 qa 且给定 FAQ 足以回答时引用其已批准答案；其他类别一律留空。"
                  "无法根据材料确认时 draft 留空且 needs_human=true。不要编造商品信息或链接。所有结果仅供人工审核。")
        started = time.monotonic()
        state, result, error = "done", None, None
        try:
            response = requests.post("https://api.deepseek.com/chat/completions",
                headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"},
                json={"model": "deepseek-flash", "messages": [{"role": "system", "content": system},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)}],
                    "response_format": {"type": "json_object"}, "temperature": 0, "max_tokens": 1000}, timeout=25)
            response.raise_for_status()
            body = response.json()
            content_text = str(body["choices"][0]["message"].get("content") or "").strip()
            if content_text.startswith("```"):
                content_text = re.sub(r"^```(?:json)?\s*|\s*```$", "", content_text, flags=re.I).strip()
            output = json.loads(content_text)
            route = str(output.get("route") or "unknown")
            labels = output.get("labels") or []
            if route not in LABELS or not isinstance(labels, list) or any(label not in LABELS for label in labels):
                raise ValueError("invalid AI labels")
            result = {"route": route, "labels": list(dict.fromkeys([route, *labels]))[:7],
                      "reason": str(output.get("reason") or "")[:500],
                      "draft": str(output.get("draft") or "")[:2000] if item["route"] == route == "qa" and
                               faq and faq[0].get("score", 0) >= .85 else "",
                      "needs_human": bool(output.get("needs_human", True)),
                      "usage": body.get("usage") or {}}
        except (requests.RequestException, ValueError, KeyError, TypeError, IndexError, json.JSONDecodeError) as exc:
            state, error = "error", type(exc).__name__
        with self.store.write_lock, self.store.connect() as db:
            retry_state = "pending" if state == "error" and item.get("ai_attempts", 0) < 2 else state
            db.execute("""UPDATE work_items SET ai_state=?,ai_json=?,ai_error=?,ai_ms=?,ai_model='deepseek-flash',
                          ai_attempts=ai_attempts+1,updated_at=?
                          WHERE event_sequence=? AND ai_state='pending'""",
                       (retry_state, self.store.dumps(result) if result else None, error,
                        round((time.monotonic() - started) * 1000), self.store.now(), sequence))


def ai_worker(phase3: Phase3, stop: threading.Event | None = None):
    while not (stop and stop.is_set()):
        try:
            count = phase3.ai_batch(4)
        except Exception:
            count = 0
        time.sleep(.5 if count else 5)
