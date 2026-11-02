"""Pure local processing policy shared by the work queue and calibration tools."""
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone

# Bump when task disposition or classification-equivalence rules change.
POLICY_VERSION = 12
TZ = timezone(timedelta(hours=8))
STATUS_LABELS = {
    "needs_configuration": "待补资料", "human": "待人工处理", "ready": "建议待确认",
    "answered": "已有真实回复", "no_reply": "无需回复", "confirmed": "已确认建议",
    "handled": "已人工处理", "waiting": "等待资料", "stale": "建议已更新，需重新确认",
}


def text_key(value):
    return re.sub(r"[^0-9a-z\u3400-\u9fff]+", "", str(value or "").casefold())


def scope_allows(scopes, group_id):
    if isinstance(scopes, str):
        try:
            scopes = json.loads(scopes)
        except (ValueError, TypeError):
            return False
    return isinstance(scopes, list) and ("*" in scopes or bool(group_id and group_id in scopes))


def eligible_faqs(faqs, *, business_date="", products=None, group_id="", account_id="", include_disabled=False):
    """Scope first, rank later. A title in a copy is not a customer question."""
    product_keys = {text_key(name) for name in (products or []) if text_key(name)}
    result = []
    for faq in faqs:
        if not isinstance(faq, dict) or not str(faq.get("answer") or "").strip():
            continue
        if not include_disabled and not (faq.get("enabled") and faq.get("approved")):
            continue
        if faq.get("source_account_id") and str(faq["source_account_id"]) != account_id:
            continue
        if "group_scope" in faq and not scope_allows(faq["group_scope"], group_id):
            continue
        start, end = str(faq.get("valid_from") or ""), str(faq.get("valid_until") or "")
        if (start or end) and (not business_date or start and business_date < start or end and business_date > end):
            continue
        if faq.get("scope") == "product":
            name = text_key(faq.get("product_name"))
            if not start or not end or not name or name == text_key("待识别商品") or name not in product_keys:
                continue
        result.append(faq)
    return result


def answer_conflicts(answer, reply):
    """Reject explicit contradictions before looking at character similarity."""
    left, right = text_key(answer), text_key(reply)
    pairs = (("可以", "不可以"), ("支持", "不支持"), ("包邮", "不包邮"),
             ("有货", "没货"), ("有货", "无货"), ("能机洗", "不能机洗"),
             ("能水洗", "不能水洗"), ("起球", "不起球"))
    for positive, negative in pairs:
        def polarity(text):
            if negative in text:
                return -1
            if positive in text:
                return 1
            return 0
        if polarity(left) * polarity(right) == -1:
            return True
    # Only compare amounts with the same explicit unit. Arbitrary numbers such
    # as a date or an order count must not be compared to a price.
    units = (r"(?:元|块|亓)", r"(?:克|g)(?![a-z])", r"斤", r"天", r"小时")
    for unit in units:
        pattern = r"(\d+(?:\.\d+)?)\s*" + unit
        a = {float(x) for x in re.findall(pattern, str(answer).lower())}
        b = {float(x) for x in re.findall(pattern, str(reply).lower())}
        if a and b and not a.intersection(b):
            return True
    # URLs are factual targets; a similar sentence with a different target is
    # not an exact supporting answer.
    a = set(re.findall(r"https?://[^\s<>]+", str(answer)))
    b = set(re.findall(r"https?://[^\s<>]+", str(reply)))
    return bool(a and b and not a.intersection(b))


def select_daily_copy(page, product_terms, question="", *, operators=None):
    """Keep operator-authored source rows; numbered script roles are not facts."""
    operators = set(operators or ["豆妈"])
    terms = [text_key(x) for x in product_terms if len(text_key(x)) >= 2]
    question_key = text_key(question)
    grams = {question_key[i:i + 2] for i in range(len(question_key) - 1)}
    records = []
    in_product = False
    for index, row in enumerate((page or {}).get("values") or []):
        if len(row) < 3:
            continue
        author, body = str(row[1] or "").strip(), str(row[2] or "").strip()
        if author not in operators or not body or body.startswith("[{'id':"):
            continue
        body_key = text_key(body)
        direct = any(term in body_key for term in terms)
        if direct:
            in_product = True
        # Link pushes without a matched product start a different campaign.
        elif len(row) > 3 and str(row[3] or "").strip() == "链接":
            in_product = False
        if not (direct or in_product):
            continue
        overlap = sum(1 for gram in grams if gram in body_key) / max(1, len(grams))
        records.append({"row": index + 1, "author": author, "body": body[:3000],
                        "relevance": round(overlap, 3), "direct_product": direct,
                        "has_placeholder_link": bool(re.search(r"【(?:拍\d件)?链接】|\[链接\]", body)),
                        "source_type": "daily_sheet_operator_copy", "usable_as_fact": False})
    records.sort(key=lambda x: (-x["relevance"], not x["direct_product"], x["row"]))
    return records[:8]


def _task(key, route, title, sequences, *, draft="", blockers=None, target="", media_id=None, note=""):
    return {"key": key, "route": route, "title": title, "source_sequences": sorted(set(sequences)),
            "draft": draft, "blockers": list(dict.fromkeys(blockers or [])), "destination_url": target,
            "media_id": media_id, "note": note, "execution": "local_suggestion_only"}


def extend_reply_coverage(messages, replies, analyses):
    """Extend an explicit customer reply to nearby, evidenced question fragments.

    Quoted messages remain exact. Nearby unrelated questions and later followups
    are never covered just because the same customer received a reply.
    """
    by_sequence={int(m["sequence"]):m for m in messages}
    def stamp(value):
        try:
            at=datetime.fromisoformat(str(value or "").replace("Z","+00:00"))
            return at.replace(tzinfo=TZ) if at.tzinfo is None else at
        except (ValueError,TypeError): return None
    def topics(text):
        text=str(text or "")
        groups={
            "price":("价格","多少钱","元","价不对"),
            "quantity":("两件","二件","一件","两单","几件","几个","两个","拍2件"),
            "purchase":("怎么拍","能么拍","怎么买","如何拍","怎么下单","下单","流程","教程","视频","购物车"),
            "selection":("款式","同样","开衫","任选","选项"),
            "coupon":("券","劵","优惠"),
            "availability":("还有","啥时候","什么时候","急需","想买","想要","复购","活动","再来一波"),
            "shipping":("发货","物流","快递","送达"),
            "size":("尺码","码数","大码","多大","多少斤"),
            "care":("机洗","水洗","手洗","起球"),
            "material":("材质","成分","扎不扎","薄款"),
            "service":("退款","换货","售后","客服","小助理"),
        }
        result={name for name,terms in groups.items() if any(term in text for term in terms)}
        if re.search(r"(?:为什么|怎么|咋|还是).*?\d",text): result.add("price")
        return result
    for reply in replies:
        if not reply.get("reply_to_case") or reply.get("reply_link_type")=="quoted": continue
        original=set(int(seq) for seq in reply.get("reply_to_sequences") or [])
        reply_at=stamp(reply.get("timestamp"))
        if not original or not reply_at: continue
        proof={}
        for anchor_sequence in original:
            anchor=by_sequence.get(anchor_sequence) or {};anchor_at=stamp(anchor.get("timestamp"))
            if not anchor_at: continue
            parent=(analyses.get(anchor_sequence,{}).get("features") or {}).get("followup_question_sequence")
            for sequence,message in by_sequence.items():
                if sequence in original: continue
                at=stamp(message.get("timestamp"))
                if not at or at>anchor_at or at>=reply_at or (anchor_at-at).total_seconds()>120: continue
                content=str(message.get("content") or "").strip()
                primary=message.get("primary")
                if primary in {"closing","ordinary","feedback","order_image"}: continue
                if parent==sequence:
                    proof[sequence]="明确关联的前序问题"
                elif re.fullmatch(r"(?:@豆妈[\s\u2005\u2006]*|豆妈[!！\s]*)+",content):
                    proof[sequence]="同一提问的提醒片段"
                elif re.fullmatch(r"\[(?:图片|附件).*\]",content) and (message.get("media") or []):
                    proof[sequence]="同一问题的图片依据"
                else:
                    question_topics=topics(content)
                    if question_topics and question_topics.issubset(topics(reply.get("content"))):
                        proof[sequence]="真实回复覆盖同一提问主题"
        if proof:
            reply["direct_reply_to_sequences"]=sorted(original)
            reply["reply_to_sequences"]=sorted(original|set(proof))
            reply["coverage_basis"]={str(seq):reason for seq,reason in proof.items()}


def build_processing_plan(payload, turns, settings=None):
    """A case has one conversation, but reply and image tasks stay independent."""
    case_id = payload["case_id"]
    answered = set(payload.get("answered_message_sequences") or [])
    labels, tasks, unresolved, drafts, reply_blockers = [], {}, [], [], []
    assignees = []
    customer_messages = payload.get("messages") or []
    settings = settings or {}
    captions = "\n".join(str(m.get("content") or "") for m in customer_messages
                          if not m.get("revoked") and m.get("primary") not in {"closing", "ordinary"})[:2500]
    messages_by_sequence={int(m["sequence"]):m for m in customer_messages}
    missing_image_turns=[]
    image_task_sources={}
    def moment(value):
        try:
            stamp=datetime.fromisoformat(str(value or "").replace("Z","+00:00"))
            return stamp.replace(tzinfo=TZ) if stamp.tzinfo is None else stamp
        except (ValueError,TypeError): return None
    def image_caption(media, sequence):
        source=next((m for m in customer_messages if m["sequence"]==(media.get("event_sequence") or sequence)),None)
        if not source: return ""
        base=moment(source.get("timestamp")); result=[]
        for message in customer_messages:
            if message.get("revoked"): continue
            content=str(message.get("content") or "").strip(); at=moment(message.get("timestamp"))
            if not content or re.fullmatch(r"\[(?:图片|视频|附件).*\]",content): continue
            if base and at and abs((at-base).total_seconds())<=120: result.append(content)
        return "\n".join(result)[:2500]
    def is_complaint_evidence(media, sequence):
        source=messages_by_sequence.get(int(media.get("event_sequence") or sequence)) or {}
        base=moment(source.get("timestamp"))
        if not base: return True
        for message in customer_messages:
            at=moment(message.get("timestamp"))
            content=str(message.get("content") or "")
            if at and abs((at-base).total_seconds())<=120 and re.search(
                r"破了|破损|坏了|损坏|碎了|裂了|漏液|漏了|被拆|拆过|开封过|退货|退款|换货|少件|异物",content):
                return True
        return False
    for turn in turns:
        sequence, primary = int(turn["sequence"]), turn.get("primary") or "unknown"
        if (messages_by_sequence.get(sequence) or {}).get("revoked"):
            continue
        rec = turn.get("recommendation") or {}
        for media in turn.get("media") or []:
            if not media.get("id") or primary != "service":
                continue
            source=messages_by_sequence.get(int(media.get('event_sequence') or sequence)) or {}
            if not turn.get('service_evidence_confirmed') and not (moment(source.get('timestamp')) and is_complaint_evidence(media,sequence)):
                key=f"media:{media['id']}:image_review"
                tasks[key]=_task(key,'image_review','复核图片用途',[sequence],media_id=media['id'],
                                 blockers=['附近售后内容不足以确认这张图片是售后证据'],note=image_caption(media,sequence))
                continue
            target=str(settings.get("aftercareEvidenceUrl") or "")
            blockers=[]
            if media.get("state") != "stored": blockers.append("图片尚未完整接收，需要补收或人工核对原消息")
            if not target.startswith("https://"): blockers.append("售后证据表地址待补")
            key=f"media:{media['id']}:service_evidence"
            tasks[key]=_task(key,"service_evidence","登记售后证据图",[sequence],blockers=blockers,
                             target=target,media_id=media['id'],note=image_caption(media,sequence))
        if rec.get("requires_classification_review"):
            key=f"{case_id}:classification:{sequence}"
            tasks[key]=_task(key,"classification","复核本条历史人工分类与当前规则",[sequence],
                blockers=["旧人工分类需要按当前规则复核"],note="选择采用当前规则，或保留历史人工分类并确认已复核。")
        for label in turn.get("labels") or [primary]:
            if label not in labels:
                labels.append(label)
        if primary in {"requirement", "service"} and rec.get("assignee") == "aftercare":
            assignees.append("aftercare")
        if primary not in {"closing", "ordinary", "feedback", "order_image"} and sequence not in answered:
            unresolved.append(sequence)
            assignees.append(rec.get("assignee") or "reviewer")
            if rec.get("draft") and rec["draft"] not in drafts:
                drafts.append(rec["draft"])
            reply_blockers.extend(rec.get("blockers") or [])
        actions = [*(rec.get("attachment_tasks") or []), *(rec.get("secondary_actions") or [])]
        if primary in {"requirement", "feedback", "order_image"}:
            actions.append({"route": primary, "action": rec.get("action"),
                            "destination_url": rec.get("destination_url"), "blockers": [x for x in rec.get("blockers") or []
                                if primary!="requirement" or "需求表" in x]})
        for action in actions:
            route = action.get("route") or ""
            if route == "aftercare_contact":
                assignees.append("aftercare")
                key=f"{case_id}:aftercare"
                already_answered=sequence in answered
                item=_task(key,"aftercare_contact",
                    "售后客服一对一跟进" if already_answered else "提供售后链接并安排一对一跟进",
                    [sequence],draft="" if already_answered else str(action.get("draft") or ""),
                    blockers=action.get("blockers") or [],
                    note=("已有明确关联的真人群回复；此项仅供内部提醒售后跟进，避免重复向群内发送。回复原文见对话上下文。"
                          if already_answered else str(action.get("action") or "将客户需求交售后一对一服务。")))
                if key in tasks:
                    tasks[key]["source_sequences"]=sorted(set(tasks[key]["source_sequences"]+[sequence]))
                else:
                    tasks[key]=item
                continue
            if route not in {"requirement", "feedback", "order_image"}:
                continue
            media_values = turn.get("media") or []
            if action.get("media_id"):
                media_values = [m for m in media_values if m.get("id") == action["media_id"]]
            if route == "requirement":
                media_values = [None]
            for media in media_values:
                if media and (media.get("usage") == "problem_evidence" or primary == "qa" and not action.get("media_id")):
                    continue
                if media and route == "feedback" and is_complaint_evidence(media,sequence):
                    continue
                if media and action.get("media_id") is None and primary not in {route, "closing", "ordinary"}:
                    continue
                media_id = media.get("id") if media else None
                key = f"media:{media_id}:{route}" if media_id else f"{case_id}:{route}"
                blockers = list(action.get("blockers") or [])
                if media and media.get("state") != "stored":
                    blockers.append("图片尚未完整接收，需要补收或人工核对原消息")
                target = str(action.get("destination_url") or "")
                if not target.startswith("https://"):
                    blockers.append({"feedback": "图库地址待补", "order_image": "订单图表地址待补", "requirement": "需求表地址待补"}[route])
                item = _task(key, route, {"feedback": "反馈图及配文入图库", "order_image": "核验并登记订单图", "requirement": "登记产品需求"}[route],
                             [sequence], blockers=blockers, target=target, media_id=media_id, note=image_caption(media,sequence) if media else captions)
                if key in tasks:
                    tasks[key]["source_sequences"] = sorted(set(tasks[key]["source_sequences"] + [sequence]))
                else:
                    tasks[key] = item
                if media_id:
                    image_task_sources[key]=int(media.get("event_sequence") or sequence)
        if primary in {"feedback", "order_image"} and not turn.get("media"):
            missing_image_turns.append((sequence,primary))
    # A complaint image must not simultaneously inherit a visual model's
    # stale feedback/order route; review is required to change its purpose.
    evidence_ids={t.get('media_id') for t in tasks.values() if t.get('route') in {'service_evidence','image_review'}}
    for task_key in list(tasks):
        task=tasks[task_key]
        if task.get('media_id') in evidence_ids and task.get('route') in {'feedback','order_image'}:
            del tasks[task_key]
            image_task_sources.pop(task_key,None)
    # A caption may precede its image. Resolve against all tasks in this same
    # customer conversation after collecting them; a genuinely missing image
    # message must not be hidden by another, successfully received picture.
    for sequence,primary in missing_image_turns:
        source=messages_by_sequence.get(sequence) or {}
        content=str(source.get("content") or "").strip()
        at=moment(source.get("timestamp"))
        paired=[]
        if content and not re.fullmatch(r"\[(?:图片|视频|附件).*\]",content) and at:
            for key,media_sequence in image_task_sources.items():
                task=tasks[key]
                image_at=moment((messages_by_sequence.get(media_sequence) or {}).get("timestamp"))
                if task["route"]==primary and image_at and abs((at-image_at).total_seconds())<=120:
                    paired.append(task)
        if paired:
            for task in paired:
                task["source_sequences"]=sorted(set(task["source_sequences"]+[sequence]))
        else:
            key=f"{case_id}:{primary}:missing_media"
            if key in tasks:
                tasks[key]["source_sequences"]=sorted(set(tasks[key]["source_sequences"]+[sequence]))
            else:
                tasks[key]=_task(key,primary,"补收图片后核验用途",[sequence],blockers=["没有收到可核验的图片附件"])
    if unresolved:
        assignee = "aftercare" if "aftercare" in assignees else "group_human" if "group_human" in assignees else "reviewer"
        route = "aftercare_contact" if assignee == "aftercare" else "warning" if any(x in labels for x in ("warning", "mini", "burst")) else "reply"
        title = "提供本群一对一客服链接并人工跟进" if route == "aftercare_contact" else "核对违规内容并按 FAQ 要求撤回" if route == "warning" else "结合客户上下文答复"
        tasks[f"{case_id}:reply"] = _task(f"{case_id}:reply", route, title, unresolved,
            draft="\n\n".join(drafts), blockers=reply_blockers,
            note="；".join(str(m.get("content") or "") for m in customer_messages if int(m["sequence"]) in unresolved)[:1200])
    else:
        assignee = "aftercare" if "aftercare" in assignees else "reviewer"
    values = list(tasks.values())
    blockers = list(dict.fromkeys(x for task in values for x in task["blockers"]))
    hard_blockers=[x for x in blockers if any(term in x for term in ("配置","映射","地址","真实购买链接","占位符","图片尚未","图片未完成","没有收到","未找到可核验的图片","旧人工分类","表结构","字段待补建","表编辑权限"))]
    if any("旧人工分类" not in item for item in hard_blockers):
        state = "needs_configuration"
    elif blockers or unresolved and not drafts:
        state = "human"
    elif values:
        state = "ready"
    elif payload.get("gold_reply_sequences"):
        state = "answered"
    else:
        state = "no_reply"
    return {"policy_version": POLICY_VERSION, "status": state, "status_label": STATUS_LABELS[state],
            "assignee": assignee, "response_needed": bool(unresolved), "unanswered_sequences": unresolved,
            "answered_sequences": sorted(answered), "labels": labels, "tasks": values, "blockers": blockers,"hard_blockers":hard_blockers,
            "reference_reply_sequences": payload.get("gold_reply_sequences") or [],
            "external_execution_enabled": False}
