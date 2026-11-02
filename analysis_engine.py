"""Explainable, read-only first-pass classification of Yunzhongke group messages."""
from __future__ import annotations

import re
from datetime import datetime, timedelta
import hashlib
import json
from urllib.parse import parse_qs, urlparse
from source_config import configured_source_account_id


RULE_VERSION = 34
SOURCE_ACCOUNT_ID = configured_source_account_id()
LABELS = {
    "service": "售后问题",
    "feedback": "反馈图候选",
    "warning": "疑似推广",
    "mini": "小程序",
    "burst": "疑似刷屏",
    "order_image": "订单截图",
    "requirement": "产品需求",
    "product_link": "当天商品链接",
    "thanks": "强提醒感谢",
    "qa": "日常问题",
    "closing": "无需回复",
    "ordinary": "普通交流",
    "unknown": "待判断",
}
SERVICE_TERMS = ("售后", "退款", "退货", "退换", "换货", "换尺码", "没发货", "还没发货", "未发货", "物流", "快递", "没收到", "未收到", "质量问题", "坏了", "破损", "漏发", "少发", "客服", "异物", "变质", "发霉", "有虫", "虫子", "商品召回", "商家召回", "拦截退回", "包裹退回", "退款寄件", "退货寄件", "放心吃吗", "放心使用吗")
FEEDBACK_TERMS = ("反馈", "晒图", "晒单", "用了", "使用了", "试用", "效果", "好用", "体验", "划算", "收到货", "拿到", "用了几天", "用了两天", "用了三天", "用了一个月", "好吃", "很喜欢", "不错", "满意", "新鲜", "多汁", "皮薄", "脆甜", "都是豆妈家的")
PROMOTION_TERMS = ("加我", "加微信", "咨询微信", "私聊我", "扫码", "加群", "引流", "推广", "免费领取", "领福利", "代理招募", "返佣", "征婚", "婚姻介绍", "互换群", "借宝地", "广告", "公众号文章", "视频号", "二维码", "群外链接", "带点客户", "帮忙宣传", "有需要可以加", "加他", "工厂直销", "厂家直销", "库存压力大", "击穿行业底价", "买贵补差", "价保全年", "数量有限", "速来抢购", "转发有好礼", "现金提现", "边玩边赚", "等你来拿", "极速版")
ORDER_IMAGE_TERMS = ("订单图", "订单截图", "下单截图", "付款截图", "支付截图", "订单页面", "订单信息", "订单号", "订单编号", "我的订单", "订单详情", "付款成功", "已付款", "交易单号")
HISTORIC_ACTIVITY_TERMS = ("之前活动链接", "以前活动链接", "上次活动链接", "之前卖过", "上周卖过", "之前买过", "以前买过", "上周活动", "老活动链接", "复购链接", "返场链接")
PRODUCT_REQUEST_TERMS = ("想要", "想买", "也要", "找一找", "再来一波", "啥时候再来", "之前你们卖", "这件还有", "还有没有", "还有吗", "要产品", "要这个产品", "求产品", "求同款", "求个", "希望有", "希望上", "希望以后上", "能不能上", "什么时候上", "几点上链接", "什么时候有", "什么时候还有", "啥时候有", "啥时候还有", "还会有吗", "能上吗", "有类似的吗", "上新", "还能买吗", "回购", "复购")
PRODUCT_AVAILABILITY_TERMS = ("牙膏", "面霜", "卫衣", "烧卖", "商品", "产品", "羊毛衫", "颈膜", "衣服", "水果", "护肤品", "洗发水", "洗衣液", "洗衣服", "面膜", "口红", "零食", "食品", "食品袋", "萝卜干", "萝卜", "笋丁", "辣白菜", "嫩姜芽", "酱牛肉", "袜子", "鞋", "裤子", "纸巾", "垃圾袋", "奶粉", "牛奶", "护手霜", "身体乳", "卸妆油", "意大利面", "意面", "羊肚菌", "防晒霜", "保暖衣", "凝胶", "黄芪阿胶口服液", "次抛")
DAILY_PURCHASE_TERMS = ("怎么拍", "如何拍", "拍单", "怎么下单", "拍不了", "拍不上", "不能拍", "买不了", "领券", "优惠券", "优惠", "没券", "券没了", "价格", "多少钱", "运费", "几个手机", "拍几件", "几件", "几单", "多拍", "两件怎么", "付款", "支付", "链接点不开", "无法支付", "下单失败", "下单")
TODAY_PRODUCT_LINK_TERMS = ("今天哪里拍", "今天在哪拍", "今天怎么拍", "今天的链接", "今天链接", "当天商品哪里拍", "当天的商品哪里拍", "当天哪里下单", "今天这个在哪买", "找不到链接", "链接找不到", "链接在哪", "链接在哪里")
GRATITUDE_TERMS = ("谢谢", "感谢", "辛苦了", "太感谢", "感恩")
AFTERCARE_CONTEXT_TERMS = ("售后", "退款", "退货", "退换", "换货", "物流", "快递", "没收到", "未收到", "质量问题", "坏了", "破损", "漏发", "少发", "客服")
CLOSING = {"买啦", "买了", "买好了", "已排", "好", "好啊", "好呀", "好的", "好的呀", "好哒", "好滴", "好呢", "好嘞", "好咧", "行", "行吧", "行的", "嗯", "嗯嗯", "恩", "哦", "哦哦", "收到", "收到了", "收到啦", "我收到了", "知道了", "明白", "明白了", "了解", "是的", "谢谢", "好的谢谢", "谢了", "辛苦了", "好了", "可以了", "已经可以了", "已拍", "拍了", "我拍了", "已经拍了", "已下单", "下单了", "我下单了", "我也下单了", "我也买了", "已经下单", "已付款", "看到了", "坐等收货", "等收货", "坐等", "不好意思", "发错了不好意思", "宝宝玩手机", "👌好", "ok", "okay", "👌", "👍"}
URL_RE = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)
QUESTION_TERMS = ("怎么", "咋", "如何", "请问", "能否", "可以吗", "有没有", "是什么", "什么", "多久", "多少", "哪里", "哪儿", "在哪", "什么时候", "啥时候", "为什么", "是不是", "会不会", "扎不扎", "就行吧", "吗")
REQUEST_TERMS = ("上链接", "发链接", "链接在哪", "链接在哪里", "链接再发", "再发", "发个", "发一下", "帮我", "求链接", "求推荐", "看看", "找一找", "想要", "我想选", "想选", "怎么选", "我不会", "不会操作", "不会弄", "怎么联系", "不懂怎么", "拍不了", "买不了", "不能拍", "能么拍", "无法下单", "不是这个价", "不给买", "没券了", "券没了", "还有吗", "还有没有", "再来一波", "什么时候再来", "啥时候再来", "怎么结算", "价格不一样", "只买到一件", "流程")
ORDER_ISSUE_TERMS = ("只买到一件", "不是这个价", "下单就不是", "打开是", "不给买", "不能同时", "没有优惠", "券已经", "优惠券没", "领不了券", "价格不一样")
def _text(value: object) -> str:
    if isinstance(value, str):
        return value.strip()[:4000]
    if isinstance(value, dict):
        for key in ("text", "msg", "message", "content", "description", "title"):
            if key in value:
                return _text(value[key])
    if isinstance(value, list):
        return "\n".join(filter(None, (_text(item) for item in value)))[:4000]
    return ""


def analyze(raw: dict, normalized: dict, context: list[dict], media: list[dict], settings: dict | None = None) -> dict:
    """Classify one message using the sender's short conversation context."""
    raw = raw if isinstance(raw, dict) else {}
    normalized = normalized if isinstance(normalized, dict) else {}
    settings = settings if isinstance(settings, dict) else {}
    rules = settings.get("rules") if isinstance(settings.get("rules"), dict) else {}
    body = str(normalized.get("content") or _text(raw.get("content"))).strip()
    semantic_body = re.sub(r"<[^>]*>", " ", body).strip()
    compact = re.sub(r"[\s，。,.!！~～…]+", "", re.sub(r"\[[^\]]{1,8}\]", "", re.sub(r"\[ok\]", "ok", semantic_body, flags=re.I))).lower()
    stamp = normalized.get("timestamp")
    try:
        at = datetime.fromisoformat(stamp) if stamp else None
    except (TypeError, ValueError):
        at = None
    group_id = str(normalized.get("group_id") or "")
    sender_id = str(normalized.get("sender_id") or "")
    nearby = []
    same_sender = []
    recent_outbound = []
    burst_count = 1
    context_sequences = []
    conversation_window = max(60, min(1800, int(rules.get("conversationSeconds") or 1800)))
    for event in context:
        if group_id and event.get("group_id") != group_id:
            continue
        try:
            previous_at = datetime.fromisoformat(event["source_timestamp"])
        except (KeyError, TypeError, ValueError):
            continue
        delta = (at - previous_at).total_seconds() if at else 999999
        if abs(delta) > conversation_window:
            continue
        if event.get("sequence") is not None:
            context_sequences.append(int(event["sequence"]))
        nearby.append(event)
        if event.get("direction") == "outbound":
            recent_outbound.append(event)
        elif sender_id and event.get("sender_id") == sender_id and event.get("direction") == "inbound":
            same_sender.append(event)
            if abs(delta) <= max(5, min(600, int(rules.get("burstSeconds") or 30))):
                burst_count += 1
    same_sender.sort(key=lambda item: (item.get("source_timestamp") or "", int(item.get("sequence") or 0)))
    conversation_events = [item for item in nearby if item.get("direction") == "outbound" or
                           (sender_id and item.get("sender_id") == sender_id and item.get("direction") == "inbound")]
    current_event = {"sequence": normalized.get("sequence"), "source_timestamp": stamp or "",
                     "content": semantic_body, "direction": "inbound", "sender_id": sender_id}
    conversation_events.append(current_event)
    conversation_events.sort(key=lambda item: (item.get("source_timestamp") or "", int(item.get("sequence") or 0)))
    conversation_text = " ".join(str(item.get("content") or "").strip() for item in conversation_events if item.get("content"))
    customer_events = [item for item in conversation_events if item.get("direction") == "inbound" and item.get("sender_id") == sender_id]
    customer_text = " ".join(str(item.get("content") or "").strip() for item in customer_events if item.get("content"))
    # Keep the session intact for display, but attach semantics only to the
    # current turn and neighbouring image captions. Old risks/requests must not
    # become a new task on every later message from this customer.
    def gap(item):
        try:
            return abs((at - datetime.fromisoformat(item["source_timestamp"])).total_seconds()) if at else 999999
        except (ValueError, TypeError, KeyError):
            return 999999
    adjacent_sender = sorted((item for item in same_sender if gap(item) <= 120), key=gap)[:4]
    current_image_hint = bool(normalized.get("image_urls") or normalized.get("attachments"))
    captions = [str(item.get("content") or "") for item in adjacent_sender
                if not item.get("has_image") and not str(item.get("content") or "").startswith("[图片")]
    conversation_body = re.sub(r"<[^>]*>", " ", " ".join([semantic_body, *captions]) if current_image_hint else semantic_body).strip()
    reorder = bool(re.search(r"退.{0,12}(?:重|从新|重新|再).{0,5}(?:拍|选|下单)", conversation_body))
    reorder = reorder or (any(term in conversation_body for term in ("还没发货", "未发货", "没发货")) and any(term in conversation_body for term in ("想换", "换一件", "换一个", "重新选", "换款")))
    explicit_after_sale = any(term in conversation_body for term in
                             ("质量", "破损", "坏了", "漏发", "少发", "物流", "退款到账", "没收到", "退货", "售后", "异物", "变质", "发霉", "有虫", "商品召回", "商家召回", "放心吃吗"))
    product_constraint = bool(re.search(r"(?:只有|只找到|找不到).{0,8}(?:颜色|尺码|码数|款式)|(?:尺码|码数).{0,8}(?:大一些|小一些)", semantic_body))
    strong_mention = bool(re.search(r"@\s*[\u2005\u2006\u2009\u3000 ]*豆妈|(?:感谢|谢谢).{0,4}豆妈|豆妈[，,、\s]*豆妈", semantic_body))
    product_concern = any(term in semantic_body for term in ("不敢吃", "吃了就上火", "容易上火", "副作用", "过敏", "孕妇能", "哺乳期能"))
    product_usage_issue = any(term in semantic_body for term in ("涂不开", "化不开", "腻住", "不吸收", "搓泥", "刺痛", "辣眼", "熏眼", "起皮", "太干", "用着不舒服"))
    quantity_question = bool(re.search(r"(?:有|每袋|一袋|每包|一包).{0,8}(?:几|多少)(?:个|片|张|件|袋|包|盒|支|瓶|套)", semantic_body))
    stock_order_issue = bool(re.search(
        r"(?:没有?货|无货|没库存|没有库存|卖完了|卖没了|喜欢的款式.{0,6}(?:没了|没有了)|"
        r"款式.{0,6}(?:没了|没有了|卖完了)|显示.{0,5}(?:没货|无货|无库存)|"
        r"(?:下单|拍单|购买).{0,5}(?:不了|不行|失败|不成功)|不能(?:下单|拍|买)|拍不了|买不了)",
        semantic_body))
    link_usage_issue = bool(re.search(r"(?:复制到.{0,12}(?:不行|不能|打不开)|(?:链接|连接).{0,8}(?:点不开|打不开|不能用|不行了)|(?:点不开|打不开|不能用).{0,8}(?:链接|连接))", semantic_body))
    current_question = product_constraint or product_concern or product_usage_issue or quantity_question or stock_order_issue or link_usage_issue or "?" in semantic_body or "？" in semantic_body or any(term in semantic_body for term in QUESTION_TERMS)
    current_request = stock_order_issue or link_usage_issue or any(term in semantic_body for term in REQUEST_TERMS)
    followup_phrase = bool(re.search(r"(?:我看了.{0,5}没有|我正在处理|还是.{0,8}(?:[0-9]|不行|没有)|仍然|依然|我想买|我想拍|急需下单|(?:券|卷).{0,4}领了|重新拍|这么回事|怎么回事|涂不开|化不开|腻住|不吸收|不舒服|复制到.{0,10}(?:不行|不能|打不开))", semantic_body))
    previous_question = None
    if followup_phrase:
        for item in reversed(same_sender):
            if not at or datetime.fromisoformat(item["source_timestamp"]) >= at:
                continue
            previous_text = str(item.get("content") or "")
            if item.get("has_image"):
                continue
            previous_compact = re.sub(r"[\s，。,.!！~～…]+", "", re.sub(r"\[[^\]]{1,8}\]", "", previous_text)).lower()
            if previous_compact in CLOSING:
                break
            if (any(term in previous_text for term in (*QUESTION_TERMS, *REQUEST_TERMS, "咋", "价", "涂不开", "化不开", "腻住", "不吸收", "搓泥", "刺痛", "不舒服"))
                    or "？" in previous_text or "?" in previous_text):
                previous_question = item
                break

    context_sequences = list(dict.fromkeys(context_sequences))
    conversation_sequences = list(dict.fromkeys(
        [int(item["sequence"]) for item in same_sender if item.get("sequence") is not None] +
        ([int(normalized["sequence"])] if normalized.get("sequence") is not None else [])
    ))
    raw_type = " ".join(str(raw.get(k) or "") for k in ("type", "msg_type", "wxtype", "content_type", "sub_type")).lower()
    source_sticker = any(token in raw_type for token in ("sendbiaoqing", "sticker", "emotion", "emoji"))
    raw_blob = json.dumps(raw, ensure_ascii=False, default=str).lower()
    image_signal = bool(normalized.get("image_urls")) or bool(normalized.get("attachments")) or "image" in raw_type or "图片" in raw_type or raw_type.strip() == "3"
    audio_signal = ("audio" in raw_type or "voice" in raw_type or "语音" in raw_type or
                    any(str(item.get("mime") or "").lower().startswith("audio/") for item in media))
    audio_placeholder = audio_signal and (not body or body.strip() in {"[语音消息]", "[语音]", "语音消息"})
    image_state = "none"
    if image_signal:
        states = {item.get("state") for item in media}
        image_state = "stored" if "stored" in states else "failed" if "failed" in states else "pending" if states else "missing_url"
    current_ocr_text = " ".join(str(item.get("ocr_text") or "") for item in media)
    adjacent_images = [item for item in adjacent_sender if item.get("has_image")]
    session_feedback_images = sorted((item for item in same_sender
        if item.get("has_image") and gap(item) <= min(conversation_window, 600)),
        key=lambda item: (item.get("source_timestamp") or "", int(item.get("sequence") or 0)))[-4:]
    contextual_image_question = bool(adjacent_images and re.search(
        r"(?:我的|我这|我有).{0,8}(?:这么多|那么多|这么少|这么贵|好几个|这么多东西)", semantic_body))
    current_question = current_question or contextual_image_question
    image_ocr_text = " ".join([current_ocr_text, *(str(item.get("ocr_text") or "") for item in adjacent_images)])
    vision_results = [item.get("image_understanding") for item in media if isinstance(item.get("image_understanding"), dict)]
    if not image_signal:
        vision_results += [item["image_understanding"] for item in adjacent_images if isinstance(item.get("image_understanding"), dict)]
    trusted_vision = [item for item in vision_results if float(item.get("confidence") or 0) >= .85 and item.get("evidence")]
    vision_kinds = {item.get("image_kind") for item in trusted_vision}
    ocr_order_terms = [term for term in ("订单", "实付款", "交易单号", "付款成功", "已付款", "收货地址", "待收货", "待发货", "买家已付款") if term in image_ocr_text]
    image_is_order = "order_screenshot" in vision_kinds or len(ocr_order_terms) >= 2 or any(term in image_ocr_text for term in ORDER_IMAGE_TERMS)
    vision_emoji = image_signal and any(item.get("image_kind") == "emoji" and not item.get("needs_human") for item in trusted_vision) and not image_is_order
    mini = raw.get("is_mini_parameter") in (True, 1, "1") or any(term in raw_type for term in ("mini", "applet", "小程序")) or "不支持显示的消息类型【小程序】" in body
    raw_links = URL_RE.findall((conversation_body + (" " + image_ocr_text if not image_is_order else "")).replace("\\/", "/"))
    links = [urlparse(url).hostname or "" for url in raw_links]
    hosts = {host.lower().removeprefix("www.") for host in links if host}
    public_article = any(host == "mp.weixin.qq.com" for host in hosts) or any(token in raw_type or token in raw_blob for token in ("article", "appmsg", "公众号")) or any(term in (conversation_body + image_ocr_text) for term in ("公众号文章", "公众号链接"))
    video_channel = any(host in ("channels.weixin.qq.com", "finder.video.qq.com") or host.endswith(".channels.weixin.qq.com") for host in hosts) or any(token in raw_type or token in raw_blob or token in (conversation_body + image_ocr_text) for token in ("video_account", "channels", "视频号"))
    qr_code = any(token in raw_type or token in raw_blob for token in ("qrcode", "qr_code", "qr-code")) or any(term in (conversation_body + image_ocr_text).lower() for term in ("二维码", "收款码", "扫码", "qr code"))
    allowed_domains = settings.get("allowedShareDomains") or rules.get("allowedShareDomains") or ""
    if isinstance(allowed_domains, list):
        allowed_domain_values = [str(value).strip().lower() for value in allowed_domains]
    else:
        allowed_domain_values = [value.strip().lower() for value in re.split(r"[,，;；\s]+", str(allowed_domains)) if value.strip()]
    def is_allowed_host(host: str) -> bool:
        host = host.lower().removeprefix("www.")
        return any(host == domain.removeprefix("www.") or host.endswith("." + domain.removeprefix("www.")) for domain in allowed_domain_values)
    non_group_link = bool(hosts) and not all(is_allowed_host(host) for host in hosts)
    promotional_card_hosts = []
    if "card_link" in raw_type or "[卡片链接消息]" in body:
        for url in raw_links:
            query_keys = {key.lower() for key in parse_qs(urlparse(url).query)}
            if query_keys.intersection({"advid", "promote", "pacid"}):
                host = urlparse(url).hostname or "未知域名"
                if host not in promotional_card_hosts:
                    promotional_card_hosts.append(host)
    if ("card_link" in raw_type or "href=" in body) and not links:
        links = ["未解析卡片链接"]
        non_group_link = True
    tags = []
    evidence = []
    uncertainty = []
    for visual in trusted_vision:
        evidence.append("图片视觉初筛：" + str(visual.get("image_kind")) + "；" + "；".join(visual.get("evidence") or []))
        if visual.get("needs_human"):
            uncertainty.append("视觉模型要求人工复核图片")
    if vision_results and not trusted_vision:
        uncertainty.append("图片视觉初筛置信度不足，交人工判断")
    def mark(tag: str, why: str):
        if tag not in tags:
            tags.append(tag)
        evidence.append(why)
    service_probe = semantic_body
    if image_signal or len(re.sub(r"[^\u4e00-\u9fffA-Za-z0-9]", "", semantic_body)) <= 12:
        service_probe += " " + " ".join(str(item.get("content") or "") for item in same_sender if gap(item) <= 600)
    service_hits = [term for term in SERVICE_TERMS if term in service_probe] if not reorder or explicit_after_sale else []
    if re.search(r"(?:尺码|码数|尺寸).{0,10}(?:不合适|不合身|偏小|偏大|太小|太大)|(?:不合身|不合适).{0,6}(?:尺码|码数|尺寸)", semantic_body):
        service_hits.append("收到商品后尺码不合适")
    if re.search(r"(?:商家|卖家|店家).{0,12}(?:发错|寄错|漏发|少发)|(?:我买的|买的|收到的|穿了|洗了).{0,16}(?:起毛球|起球)", semantic_body):
        service_hits.append("已购商品发错或出现质量问题")
    service_context_terms = ("售后", "客服", "售后链接", "售后小助手", "一对一", "退款", "退货", "质量问题", "换货", "物流", "快递", "异物", "变质", "发霉", "商家召回", "拦截退回")
    service_context_found = any(
        term in str(item.get("content") or "")
        for item in (*same_sender, *recent_outbound)
        for term in service_context_terms
    )
    service_followup = service_context_found and bool(re.search(
        r"(?:加了|加上了|已经加|联系了|联系过|添加了).{0,12}(?:还没|没有|未|暂时没).{0,8}(?:回复|回|处理|理)|"
        r"(?:还没|没有|未|暂时没).{0,8}(?:回复|回|处理|理).{0,8}(?:客服|售后)?",
        semantic_body))
    if service_followup:
        service_hits.append("售后/客服跟进")
    promotion_hits = [term for term in PROMOTION_TERMS if term in conversation_body]
    commercial_sale_ad = bool(
        re.search(r"(?:大处理|急售|转让|甩卖|清仓|优惠力度大|低价转让|库存处理|工厂处理|出售)", semantic_body)
        and re.search(r"(?:联系电话|联系我|电话|手机|有需要|需要的|[1-9]\d{6,})", semantic_body)
    )
    if commercial_sale_ad:
        promotion_hits.append("带联系方式的商品转让/清仓广告")
    image_promotion_hits = [term for term in (*PROMOTION_TERMS, "公众号", "关注微信", "加客服", "添加客服", "微信号", "扫码添加", "招商加盟") if term in image_ocr_text]
    # Promotional wording inside a normal shopping screenshot is weak evidence.
    social_ad_terms = ("扫码", "加微信", "加我", "微信号", "添加客服", "招商加盟", "代理招募", "二维码", "关注公众号")
    strong_image_ad = [term for term in social_ad_terms if term in image_ocr_text]
    visual_ad = any(item.get("image_kind") == "ad" and float(item.get("confidence") or 0) >= .9
                    and len(item.get("evidence") or []) >= 2 for item in trusted_vision)
    image_ad_confirmed = visual_ad or bool(strong_image_ad) or (len(set(image_promotion_hits)) >= 2 and not image_is_order)
    # A product package can itself contain a scan instruction. One isolated
    # OCR word on a visually identified product is not proof of a shared QR ad.
    package_scan_only = ("product_feedback" in vision_kinds and set(image_promotion_hits) == {"扫码"}
                         and not visual_ad and not any(term in conversation_body.lower() for term in ("二维码", "收款码", "扫码", "qr code"))
                         and not any(token in raw_type or token in raw_blob for token in ("qrcode", "qr_code", "qr-code")))
    if package_scan_only:
        image_ad_confirmed = False
        qr_code = False
    if image_is_order and not visual_ad:
        image_ad_confirmed = False
        qr_code = False
    if visual_ad:
        promotion_hits.append("视觉识别到广告/引流图片")
    if image_ad_confirmed:
        promotion_hits.extend("图片文字:" + term for term in image_promotion_hits[:5])
    elif image_promotion_hits and not image_is_order:
        uncertainty.append("图片只有促销词线索，不能据此认定广告；需结合图片类型核验")
    if non_group_link:
        promotion_hits.append("群外链接")
    if "微信" in conversation_body and re.search(r"1[3-9]\d{9}", conversation_body) and "联系电话" not in conversation_body:
        promotion_hits.append("微信联系方式")
    feedback_hits = [term for term in FEEDBACK_TERMS if term in conversation_body]
    if image_signal:
        feedback_hits.extend(term for term in ("晒单", "使用反馈", "买家秀") if term in image_ocr_text and term not in feedback_hits)
    same_sender_image_context = image_signal or bool(adjacent_images)
    # Image content type and message intent are distinct. A text turn never
    # becomes a receipt just because the customer sent one nearby.
    image_issue = None
    if image_signal and not source_sticker and not vision_emoji:
        for item in reversed(same_sender):
            if not at or datetime.fromisoformat(item["source_timestamp"]) >= at or item.get("has_image"):
                continue
            previous_text = str(item.get("content") or "")
            previous_compact = re.sub(r"[\s，。,.!！~～…]+", "", re.sub(r"\[[^\]]{1,8}\]", "", previous_text)).lower()
            if previous_compact in CLOSING:
                break
            if re.search(r"(?:为什么|怎么|咋).{0,16}(?:[0-9]|价格|金额|价钱)|(?:价格|金额|实付|价钱).{0,8}(?:不对|不一样|高了|贵了)|不是这个价|拍贵了", previous_text):
                image_issue = item
                break
            if "?" in previous_text or "？" in previous_text or any(term in previous_text for term in QUESTION_TERMS):
                break
    # Trusted order-page evidence in OCR/vision is more specific than a nearby
    # price question. Keep an explicit order screenshot on the order-image
    # route even when the customer is using it to explain a prior issue.
    image_usage = "problem_evidence" if image_issue and not image_is_order else "message_attachment"
    order_image = image_signal and (image_is_order or (not image_issue and any(term in conversation_body for term in ORDER_IMAGE_TERMS)))
    if same_sender_image_context and not current_question and re.search(r"(?:收到(?:了|货)?|收货|到货)[^，。！？?]{1,}", conversation_body):
        feedback_hits.append("图片及收货配文")
    if image_signal and "product_feedback" in vision_kinds:
        feedback_hits.append("视觉识别为实物反馈图")
    schedule_last = settings.get("soldLastWeekProducts") or ""
    schedule_next = settings.get("soldNextWeekProducts") or ""
    if isinstance(schedule_last, str):
        schedule_last = [value.strip() for value in re.split(r"[\r\n,，;；]+", schedule_last) if value.strip()]
    if isinstance(schedule_next, str):
        schedule_next = [value.strip() for value in re.split(r"[\r\n,，;；]+", schedule_next) if value.strip()]
    schedule_match = any(str(product).lower() in conversation_body.lower() for product in schedule_last)
    next_week_match = any(str(product).lower() in conversation_body.lower() for product in schedule_next)
    asks_for_product = any(term in conversation_body for term in ("有吗", "还有", "链接", "哪里拍", "哪拍", "怎么拍", "买", "下单", "什么时候"))
    resend_previous_link = bool(re.search(r"(?:链接.{0,8}(?:有没有.{0,6}(?:再发|重发)|再发我|重新发我|再给我)|(?:之前|以前|上次|买过|复购|回购).{0,10}链接|(?:牙膏|面膜|牛奶|卫衣|商品|产品|卸妆油).{0,8}(?:回购|复购|再买))", semantic_body))
    historical_activity = (any(term in conversation_body for term in (*HISTORIC_ACTIVITY_TERMS, "之前你们卖", "之前卖的", "上次卖的")) or
                           (schedule_match and not next_week_match and asks_for_product) or resend_previous_link)
    explicit_link_request = bool(re.search(r"(?:把.{0,20}链接.{0,8}(?:发|给)|(?:发|给)(?:一下|下|我)?.{0,10}链接|链接.{0,8}(?:再?发|给|还有吗|在哪|在哪里)|求.{0,4}链接|(?:还有|有没有|哪里有).{0,8}链接.{0,4}(?:吗|么|呢|弹一下)?)", semantic_body))
    explicit_link_request = explicit_link_request or bool(re.search(
        r"(?:(?:这个|那个|该)?(?:商品)?链接.{0,8}(?:在哪|在哪里|哪儿)|链接.{0,8}(?:在哪|在哪里|哪儿))",
        semantic_body))
    product_location_request = bool(re.search(
        r"[\u3400-\u9fffA-Za-z0-9]{1,18}(?:在哪里拍|在哪儿拍|在哪拍|哪里拍|哪拍|在哪里买|在哪儿买|在哪买|哪儿买|哪买|链接在哪|链接在哪里)",
        semantic_body))
    mention_tail = re.sub(r"@[\w\u4e00-\u9fff]+", "", semantic_body)
    mention_tail = re.sub(r"[\s，。,.!！?？~～…\[\]0-9]", "", mention_tail)
    link_followup_nudge = bool(strong_mention and len(mention_tail) <= 3 and any(
        "链接" in str(item.get("content") or "") and gap(item) <= 600 for item in same_sender))
    compact_link_request = re.sub(r"\s+", "", re.sub(r"@[\w\u4e00-\u9fff]+", "", semantic_body))
    short_link_request = bool(re.fullmatch(r"(?:[\u4e00-\u9fffA-Za-z0-9]{1,12}(?:的)?(?:商品|产品)?链接|链接)(?:呢|呀|啊)?", compact_link_request)) and not historical_activity
    explicit_today_link = ((any(term in conversation_body for term in TODAY_PRODUCT_LINK_TERMS) and
                            any(term in conversation_body for term in ("今天", "当天", "找不到链接", "链接找不到"))) or
                           bool(re.search(r"(?:今天|当天).{0,12}(?:哪里拍|哪拍|链接|怎么拍|哪儿买)", conversation_body)))
    generic_today_link = bool(re.search(r"(?:^上链接$|链接再发|再发.{0,3}链接|求链接|(?:没|未|没有|找不).{0,4}找到.{0,4}链接)", conversation_body.strip())) or (
        (explicit_link_request or short_link_request or product_location_request or link_followup_nudge) and not historical_activity)
    product_reference_context = "product_reference" in vision_kinds and (image_signal or bool(adjacent_images))
    product_reference_text = " ".join([semantic_body, *(str(item.get("content") or "") for item in same_sender if gap(item) <= 600)])
    product_reference_request = product_reference_context and bool(re.search(
        r"(?:链接|哪里拍|哪儿拍|有吗|有么|有没有|啥时候|什么时候|想要|也要|求(?:一个|个|同款)?|还能买|想买|求购|要买)",
        product_reference_text))
    today_product_link = explicit_today_link or (generic_today_link and not product_reference_request and not historical_activity)
    daily_purchase = bool(image_issue or stock_order_issue or link_usage_issue) or any(term in conversation_body for term in DAILY_PURCHASE_TERMS)
    availability_text = re.sub(r"@\s*[\u2005\u2006\u2009\u3000 ]*豆妈", " ", semantic_body)
    generic_availability = bool(re.search(r"(?:有没有|有没|还有没有|还会有|没有)[\s:：,，]*[\u3400-\u9fffA-Za-z0-9]{2,14}(?:吗|么|呢|呀|啊|了)?[？?]?$|(?:还有|有)[\u3400-\u9fffA-Za-z0-9]{2,14}(?:吗|么|呢|呀|啊)[？?]?$", availability_text))
    availability_exclusions = ("优惠", "价格", "运费", "链接", "客服", "售后", "照片", "图片", "时间", "问题", "方法", "效果", "功效", "活动", "库存", "券", "可能", "人", "感觉", "回事", "感觉吗")
    if any(term in availability_text for term in availability_exclusions):
        generic_availability = False
    named_item_availability = bool(re.search(
        r"[\u3400-\u9fffA-Za-z0-9]{2,18}(?:有吗|有么|有没|有没有|还有吗|还能买吗|还可以买|有活动吗|有没有活动)",
        availability_text))
    product_availability = (any(term in semantic_body for term in PRODUCT_AVAILABILITY_TERMS) and
                            bool(re.search(r"(?:有没有|有没|还有没有|还会有|还有吗|有.{0,8}(?:吗|么)|还能买)", semantic_body))) or generic_availability or named_item_availability
    relist_request = bool(re.search(r"(?:面霜|牙膏|卫衣|烧卖|商品|产品|链接).{0,6}(?:上|补)(?:一下|一个)?|(?:上|补)(?:一下|一个).{0,6}(?:面霜|牙膏|卫衣|烧卖|商品|产品)", semantic_body))
    relist_request = relist_request or bool(re.search(r"(?:再|还).{0,4}(?:整|来|安排|上).{0,16}(?:活动|团购|链接)", semantic_body))
    direct_product_want = bool(re.search(
        r"(?:我也要|我想要|也想要|还想要|想要).{0,12}(?:" + "|".join(re.escape(term) for term in PRODUCT_AVAILABILITY_TERMS) + r")",
        semantic_body))
    addressed_text = re.sub(r"@\s*[\u2005\u2006\u2009\u3000 ]*豆妈", " ", semantic_body)
    addressed_product_want = strong_mention and (
        any(term in addressed_text for term in PRODUCT_AVAILABILITY_TERMS) or
        bool(re.search(r"(?:^|[\s，,])(?:要|想要|想买|求|来个|来一份).{1,12}$", addressed_text)))
    generic_product_want = bool(re.search(
        r"^(?:我)?(?:要|想要|来一个|来个).{0,14}(?:男士|女士|大码|小码|同款|这个|那个|的)$",
        re.sub(r"[\s，。,.!！?？~～…]+", "", semantic_body)))
    future_product_notice = bool(re.search(
        r"(?:下次|以后|后续|之后).{0,18}(?:有|出|上|卖|做|安排).{0,18}(?:活动|新品|商品|产品|团购)"
        r"|(?:下次|以后|后续|之后).{0,18}(?:活动|新品|商品|产品|团购).{0,12}(?:通知|提醒|告诉|告知|呼我|说一声)"
        r"|(?:活动|新品|商品|产品|团购).{0,12}(?:通知|提醒|告诉|告知|呼我|说一声).{0,8}(?:一下|我|下)?"
        r"|(?:下次|以后|后续|之后).{0,12}有了.{0,12}(?:说|通知|提醒|告诉|呼我)",
        semantic_body))
    planned_product_question = bool(re.search(r"(?:有|有没有).{0,12}(?:安排|计划|打算)|(?:安排|计划|打算).{0,10}(?:上|卖|做|商品|产品)", semantic_body))
    usage_question = bool(re.search(
        r"(?:用|用了|使用).{0,16}(?:还用|还要|还需要|要不要|能不能用).{0,16}(?:吗|么|呢|？)",
        semantic_body))
    ingredient_question = bool(re.search(r"(?:有没有|有没|有|是否|会不会).{0,6}(?:额外)?(?:添加|加)(?:糖|盐|香精|防腐剂|色素)", semantic_body))
    product_depletion = bool(
        any(term in semantic_body for term in PRODUCT_AVAILABILITY_TERMS)
        and re.search(r"(?:没了|用完了|快没了|吃完了|用光了|快用完了|需要补|想补货|想再买)", semantic_body)
    )
    product_request = (not (usage_question or ingredient_question) and (product_reference_request or product_depletion or generic_product_want or any(term in conversation_body for term in PRODUCT_REQUEST_TERMS) or product_availability or
                       relist_request or future_product_notice or direct_product_want or addressed_product_want or
                       planned_product_question or resend_previous_link))
    feedback_caption_text = conversation_body if (image_signal or adjacent_images) else semantic_body
    positive_caption = bool(re.search(r"(?:好吃|很喜欢|超爱吃|喜欢|不错|满意|好用|新鲜|多汁|皮薄|脆甜|豆妈家的|真好|高大上|好看|漂亮)|(?:包装|外观).{0,8}(?:高大上|好看|漂亮|不错)", feedback_caption_text))
    received_caption = (not re.search(r"(?:找到了|找到啦|找着了)", feedback_caption_text) and
                        bool(re.search(r"(?:收到(?:了|货)?|收货|到货|拿到|到了)", feedback_caption_text)))
    caption_feedback = positive_caption or received_caption
    experiential_repurchase = positive_caption and bool(re.search(
        r"(?:用后|用完|使用后|以后|之后).{0,8}(?:再)?(?:复购|回购)", feedback_caption_text))
    if experiential_repurchase:
        direct_repurchase_request = bool(re.search(
            r"(?:复购|回购).{0,8}(?:链接|怎么拍|哪里拍|有吗|还有吗)|(?:求|发|给).{0,6}(?:复购|回购).{0,6}链接",
            feedback_caption_text))
        if not direct_repurchase_request:
            product_request = False
    feedback_image_context = bool(image_signal or (caption_feedback and (adjacent_images or session_feedback_images)))
    if caption_feedback and feedback_image_context:
        feedback_hits.append("同一客户前序图片后的正向商品反馈配文")
    adjacent_feedback_image = any(
        isinstance(item.get("image_understanding"), dict)
        and item["image_understanding"].get("image_kind") == "product_feedback"
        and float(item["image_understanding"].get("confidence") or 0) >= .65
        for item in (adjacent_images or session_feedback_images))
    if adjacent_feedback_image and re.search(r"(?:收到|到货|到了)", semantic_body):
        feedback_hits.append("同一客户前序反馈图后的收货配文")
    feedback_visual_candidate = any(
        isinstance(item, dict) and item.get("image_kind") == "product_feedback"
        and float(item.get("confidence") or 0) >= .60 for item in vision_results)
    if feedback_visual_candidate and image_signal and not image_is_order and not (product_request or historical_activity) and not (promotion_hits or public_article or video_channel or qr_code or non_group_link):
        feedback_hits.append("视觉模型将图片识别为晒单候选，留待人工核验")
        uncertainty.append("晒单图片视觉判断仅作为候选，需人工确认后再入库")
    elif feedback_visual_candidate and re.search(r"(?:好吃|很喜欢|喜欢|不错|满意|好用|新鲜|多汁|皮薄|收到|到货|到了)", conversation_body):
        feedback_hits.append("低置信度晒单图与同客户配文相互印证，留待人工确认")
    schedule_question = bool(re.search(r"(?:几点|啥时候|什么时候).{0,12}(?:上链接|再来|上架|开团|返场)", semantic_body))
    if product_request or historical_activity:
        order_image = False
    current_gratitude = any(term in semantic_body for term in GRATITUDE_TERMS)
    is_video_or_article_ad = public_article or video_channel
    ad_or_external_share = bool(promotion_hits or promotional_card_hosts or is_video_or_article_ad or qr_code or non_group_link)
    if ad_or_external_share or mini:
        # Promotional content may contain words such as "有需要" and even a
        # question. It belongs to moderation, not the product-demand queue.
        product_request = False
        today_product_link = False
    if service_hits and rules.get("aftercare", True):
        mark("service", "售后线索：" + "、".join(service_hits[:3]))
    if mini and rules.get("warning", True):
        mark("mini", "来源标记为小程序")
    if promotion_hits and rules.get("warning", True):
        mark("warning", "发现推广或群外分享线索：" + "、".join(promotion_hits[:3]))
    if public_article and rules.get("warning", True):
        mark("warning", "识别到公众号文章链接")
    if video_channel and rules.get("warning", True):
        mark("warning", "识别到视频号分享")
    if qr_code and rules.get("warning", True):
        mark("warning", "识别到二维码或扫码内容")
    if non_group_link and rules.get("warning", True):
        mark("warning", "包含未列入群内链接白名单的分享链接" + ("：" + "、".join(sorted(hosts)[:3]) if hosts else ""))
    if promotional_card_hosts and rules.get("warning", True):
        mark("warning", "卡片链接含广告/推广跟踪参数，域名需人工核验：" + "、".join(promotional_card_hosts[:3]))
    allowlist = {line.strip().lower() for line in str(rules.get("allowlist") or "").splitlines() if line.strip()}
    if (str(normalized.get("group") or "").lower() in allowlist or group_id.lower() in allowlist or
            str(normalized.get("sender") or "").lower() in allowlist or sender_id.lower() in allowlist):
        if "warning" in tags or "mini" in tags:
            uncertainty.append("命中白名单，推广风险需人工核实")
    threshold = max(3, min(100, int(rules.get("burstCount") or 8)))
    if burst_count >= threshold and sender_id and group_id and rules.get("burst", True):
        mark("burst", f"同一客户上下文中 {int(rules.get('burstSeconds') or 30)} 秒内至少 {burst_count} 条")
    if image_issue and not image_is_order:
        mark("qa", "本图是前序价格/拍单问题的补充证据，关联消息 #" + str(image_issue.get("sequence") or ""))
    elif order_image and not service_hits:
        mark("order_image", "当前消息为订单截图；关联配文保留在同一会话")
    elif (not service_hits and not image_issue and feedback_image_context and feedback_hits and not (product_request or historical_activity)) and rules.get("feedback", True):
        mark("feedback", "图片与同一客户上下文中的晒单/使用反馈线索匹配：" + "、".join(feedback_hits[:3]))
    elif (not service_hits and not image_issue and adjacent_images and
          re.search(r"(?:收到了|收到货).{0,16}(?:喜欢|爱吃|好吃|不错|满意|好用)", semantic_body) and
          not (product_request or historical_activity) and rules.get("feedback", True)):
        mark("feedback", "同一客户前序图片后的正向收货/使用反馈配文")
    elif image_signal and not ad_or_external_share:
        uncertainty.append("图片文字未命中可自动分类线索，需人工判断是订单图、晒单还是广告图")
    request_hits = [term for term in REQUEST_TERMS if term in conversation_body]
    request_hits += [term for term in ORDER_ISSUE_TERMS if term in conversation_body and term not in request_hits]
    current_is_question = current_question
    is_question = current_question or product_constraint or "?" in conversation_body or "？" in conversation_body or any(term in conversation_body for term in QUESTION_TERMS)
    if request_hits:
        is_question = True
    if today_product_link:
        mark("product_link", "询问当天商品购买链接或表示找不到链接")
    elif historical_activity and not (ad_or_external_share or mini):
        mark("requirement", "咨询历史商品或复购，交售后客服一对一核实")
    elif product_request and not today_product_link and (not daily_purchase or historical_activity) and not (ad_or_external_share or mini):
        mark("requirement", "提出其他商品需求，需登记到需求表")
    requirement_intent = not (ad_or_external_share or mini) and (historical_activity or (product_request and not daily_purchase and not today_product_link))
    if (is_question and (schedule_question or not (requirement_intent or today_product_link)) or previous_question) and not (ad_or_external_share or mini or service_hits) and rules.get("qa", True):
        mark("qa", "包含提问或请求线索" + ("：" + "、".join(request_hits[:2]) if request_hits else ""))
    if previous_question and not (ad_or_external_share or mini or service_hits):
        mark("qa", "本条是前序问题的继续反馈，关联消息 #" + str(previous_question.get("sequence") or ""))
    if strong_mention and current_gratitude and not current_is_question and not (current_request or today_product_link or product_request or historical_activity or ad_or_external_share or mini):
        mark("thanks", "@豆妈且上下文表达感谢，建议适当回应")
    acknowledgement_text = re.sub(r"@[\w\u4e00-\u9fff]+", " ", semantic_body)
    acknowledgement_key = re.sub(r"[\s，。,.!?！？~～…]+", "", re.sub(r"\[[^\]]{1,8}\]", "", acknowledgement_text)).lower()
    typo_purchase_ack = bool(re.fullmatch(r"(?:豆妈)?(?:你好)?(?:已|己|巳)拍(?:了|啦)?(?:谢谢|感谢)?|(?:谢谢|感谢)(?:豆妈)?(?:已|己|巳)拍(?:了|啦)?", acknowledgement_key))
    purchase_hope_ack = bool(re.fullmatch(r"(?:我也|我)?(?:已|己|巳)拍(?:了|啦)?(?:希望|但愿)(?:喜欢|合适|好用)", acknowledgement_key))
    short_purchase_ack = bool(re.fullmatch(r"(?:我|我也)?(?:(?:已|己|巳)(?:0元)?(?:拍|下单|买)(?:了|啦|咯)?|(?:拍|下单|买)(?:了|啦|咯))(?:谢谢|感谢)?", acknowledgement_key))
    price_purchase_ack = bool(re.fullmatch(r"\d+(?:\.\d+)?(?:元|块|💰)?(?:已)?(?:拍(?:下|了)?|下单|买了)(?:[\u4e00-\u9fffA-Za-z0-9]{0,12})?(?:谢谢|感谢)?", compact))
    purchase_completion = bool(re.fullmatch(
        r"(?:(?:今天|刚刚|我也|我|囤货|好|好的)?(?:已经|已)?)(?:拍|买|下单)(?:了)?[一二两三四五六七八九十0-9]*(?:件|份|个|单|套)(?:[\u4e00-\u9fffA-Za-z0-9]{0,12})?(?:谢谢|感谢|辛苦了)?",
        acknowledgement_key))
    found_and_bought = bool(re.search(r"(?:找到了|找到啦|找着了).{0,10}(?:已拍|拍了|买了|下单)", acknowledgement_key))
    bought_all = acknowledgement_key in {"都买了", "都拍了", "全买了", "全拍了", "都下单了"}
    bought_and_praised = bool(re.search(r"(?:已拍|拍了|买了|下单了).{0,12}(?:太|很|真)?划算", acknowledgement_key))
    apology_ack = bool(re.search(r"^(?:不好意思|抱歉).{0,24}(?:发错了|宝宝|孩子|小朋友|玩手机|误发)|^发错了(?:不好意思|抱歉)?$", acknowledgement_key))
    polite_ack = (len(acknowledgement_key) <= 24 and acknowledgement_key.startswith(("收到", "谢谢", "感谢", "辛苦", "好的", "好"))
                  and not current_question and not current_request and not service_hits and not promotion_hits and not image_signal)
    reorder_ack = price_purchase_ack or purchase_completion or found_and_bought or bought_all or bought_and_praised or bool(re.fullmatch(r"(?:我|那我|好的我)?(?:重新|重|再)(?:拍|下单)(?:一下|了|一单)?", compact))
    browse_up_ack = bool(re.search(r"(?:爬楼|翻楼|往上看).{0,10}(?:买|拍|找)", acknowledgement_key) or
                         re.search(r"(?:找到了|找到啦|找着了).{0,10}(?:再上面|在上面|再下面|在下面|上面|下面|楼上|楼下)", acknowledgement_key))
    waiting_ack = bool(re.fullmatch(r"(?:嗯+)?(?:好|好的|好哒)(?:蹲守|坐等|等着|等待)", acknowledgement_key))
    short_ack = (waiting_ack or short_purchase_ack or typo_purchase_ack or reorder_ack or apology_ack or polite_ack or bool(re.fullmatch(r"(?:我也|我|囤货|好|好的)?(?:已经|已)?(?:拍了?|买了|下单了?)[一二两三四五六七八九十0-9]*(?:件|单|个|套)?(?:谢谢|感谢|辛苦了)?", acknowledgement_key)) or acknowledgement_key in CLOSING or browse_up_ack or (len(acknowledgement_key) <= 20 and acknowledgement_key.startswith(("收到了", "已经可以了", "可以了", "已拍", "已拍了", "已经拍下", "好已拍", "好的已拍", "好已下单", "好的已下单", "已下单", "我拍了", "我下单了", "我也下单了", "我也买了"))))
    if strong_mention and not short_ack and not (requirement_intent or today_product_link or ad_or_external_share or mini or service_hits):
        mark("qa", "@豆妈强提醒，结合该客户上下文人工处理")
    actionable_context = bool(service_hits or ad_or_external_share or order_image or feedback_hits or today_product_link or historical_activity or product_request or is_question)
    current_request_hits = [term for term in REQUEST_TERMS if term in semantic_body]
    current_service = any(term in semantic_body for term in SERVICE_TERMS) and (not reorder or explicit_after_sale)
    current_promotion = any(term in semantic_body for term in PROMOTION_TERMS)
    current_order_language = any(term in semantic_body for term in ORDER_IMAGE_TERMS
                                 if term not in ("已付款", "付款成功"))
    current_daily_terms = DAILY_PURCHASE_TERMS
    if short_ack:
        current_daily_terms = tuple(term for term in DAILY_PURCHASE_TERMS if term not in ("下单", "支付", "付款"))
    current_special_content = bool(
        current_request_hits or current_is_question or current_service or current_promotion or
        current_order_language or (image_signal and any(term in semantic_body for term in ORDER_IMAGE_TERMS)) or
        any(term in semantic_body for term in (*FEEDBACK_TERMS, *PRODUCT_REQUEST_TERMS,
                                               *current_daily_terms, *TODAY_PRODUCT_LINK_TERMS,
                                               "二维码", "扫码", "公众号文章", "视频号")) or
        URL_RE.search(semantic_body) or mini or image_signal
    )
    recently_answered = False
    if at:
        for item in recent_outbound:
            if sender_id not in {str(value) for value in item.get("reply_target_ids") or []}:
                continue
            try:
                reply_at = datetime.fromisoformat(str(item.get("source_timestamp") or ""))
            except (TypeError, ValueError):
                continue
            if timedelta(0) <= at-reply_at <= timedelta(minutes=5):
                recently_answered = True
                break
    answered_acknowledgement = recently_answered and acknowledgement_key in (CLOSING | {"好的坐等", "好坐等", "好呢坐等", "好哒坐等", "好的我等", "好我等", "收到我等", "好的等链接", "好等链接"})
    closing_ack = (answered_acknowledgement or short_ack and not current_special_content) and not (strong_mention and current_gratitude and not current_request and not today_product_link and not product_request and not historical_activity)
    closing_ack = closing_ack or (reorder_ack and not current_question and not image_signal and not current_service and not current_promotion)
    if short_ack and not image_signal and not actionable_context and not strong_mention and rules.get("closing", True):
        mark("closing", "简短确认且没有新增问题或诉求")
    if image_signal and image_state != "stored":
        uncertainty.append("图片内容不可用，等待归档或人工查看")
    primary_order = ("warning", "mini", "burst", "service", "order_image", "feedback", "product_link", "requirement", "thanks", "qa", "closing")
    primary = next((tag for tag in primary_order if tag in tags), None)
    if schedule_question and primary == "requirement":
        primary = "qa"
    current_thanks = current_gratitude and not current_is_question
    if strong_mention and is_question and not current_thanks and primary not in ("warning", "mini", "burst", "order_image", "service", "requirement", "product_link"):
        primary = "qa"
    if (vision_emoji or (source_sticker and image_state == "stored")) and not ad_or_external_share and not current_question and not current_request:
        tags = ["closing"]
        evidence = ["来源明确标记为表情包且无广告/提问线索，无需回复" if source_sticker else "视觉识别为表情/礼貌收尾图片，没有新增诉求"]
        uncertainty = []
        primary = "closing"
    if (closing_ack or purchase_hope_ack and not current_question and not image_signal and not current_service and not current_promotion) and rules.get("closing", True) and (not feedback_hits or reorder_ack or purchase_hope_ack):
        # A short acknowledgement closes the current turn. Do not repeat an older
        # question, service issue, or request as a new action for this message.
        tags = ["closing"]
        evidence = ["当前消息是简短确认或已拍状态，没有提出新问题；无需回复"]
        uncertainty = []
        primary = "closing"
    if not primary:
        if audio_placeholder:
            accidental_voice = any(
                gap(item) <= 180 and re.search(r"(?:宝宝|孩子|小朋友).{0,8}(?:玩手机|误发|发的)|(?:玩手机|误发|发错了)", str(item.get("content") or ""))
                for item in same_sender)
            if accidental_voice:
                primary = "ordinary"
                tags = ["ordinary"]
                evidence.append("同一客户随后说明这组语音为误触发；语音内容未转写，不猜测其正文")
            else:
                primary = "unknown"
                tags = ["unknown"]
                uncertainty.append("收到语音但没有可用转写，需人工听取后分类")
        elif image_signal or (compact in CLOSING) or (links and non_group_link):
            primary = "unknown"
        elif body:
            primary = "ordinary"
            tags.append("ordinary")
        else:
            primary = "unknown"
            uncertainty.append("缺少可解析正文")
    # Explicit source references can close an answer task. Mere proximity of
    # a group broadcast is only a candidate, never proof that this user was answered.
    source_ids = {str(value) for value in (normalized.get("source_id"), normalized.get("id"), raw.get("id"), raw.get("msgid"), *(normalized.get("source_reference_ids") or [])) if value is not None and str(value)}
    if current_is_question or product_request:
        source_ids.update(str(reference) for item in adjacent_images for reference in item.get("source_reference_ids") or [])
    confirmed_replies = []
    for item in recent_outbound:
        reply_reference = str(item.get("reply_to_source_id") or "")
        # Only an explicit source-message reference is confirmed here. A
        # recipient-only association is inferred later at case level, where
        # the system can check whether there is exactly one eligible issue.
        if reply_reference and reply_reference in source_ids:
            confirmed_replies.append(int(item["sequence"]))
    assignee = "aftercare" if primary in ("service", "requirement") or (primary == "qa" and product_request and not daily_purchase) else "group_human" if primary in ("qa", "product_link", "thanks") else "reviewer"
    needs_review = primary in ("unknown", "warning", "mini", "burst", "order_image", "requirement", "product_link", "thanks", "feedback", "service", "qa") or bool(uncertainty)
    confidence = "low" if primary == "unknown" or ("warning" in tags and allowlist) else "medium" if uncertainty else "high"
    session_sequences = sorted(set(conversation_sequences))
    session_start = int(normalized.get("conversation_anchor_sequence") or
                        (session_sequences[0] if session_sequences else int(normalized.get("sequence") or 0)))
    source_account_id = str(normalized.get("source_account_id") or SOURCE_ACCOUNT_ID)
    conversation_id = hashlib.sha256((source_account_id + "|" + group_id + "|" + sender_id + "|" + str(session_start)).encode("utf-8")).hexdigest()[:16]
    return {
        "version": RULE_VERSION,
        "primary": primary,
        "labels": tags or [primary],
        "label_names": [LABELS[tag] for tag in (tags or [primary])],
        "confidence": confidence,
        "needs_review": needs_review,
        "evidence": evidence,
        "uncertainty": uncertainty,
        "features": {"has_image": image_signal, "image_state": image_state, "has_link": bool(links), "link_hosts": sorted(set(links))[:5],
                     "is_mini": mini, "is_public_article": public_article, "is_video_channel": video_channel,
                     "is_qr_code": qr_code, "is_non_group_link": non_group_link, "is_ad_or_external_share": ad_or_external_share,
                     "is_order_image": order_image, "has_related_image": same_sender_image_context,
                     "related_image_is_order": image_is_order, "image_usage": image_usage,
                     "image_issue_sequence": image_issue.get("sequence") if image_issue else None,
                     "is_source_sticker": source_sticker, "has_untranscribed_audio": audio_placeholder,
                     "related_image_is_feedback": "product_feedback" in vision_kinds,
                     "has_image_ocr": bool(image_ocr_text), "is_image_promotion": image_ad_confirmed,
                     "image_vision_kinds": sorted(vision_kinds), "image_vision_available": bool(vision_results),
                     "image_vision_results": vision_results,
                     "adjacent_image_sequences": sorted({int(item["sequence"]) for item in adjacent_images if item.get("sequence")} |
                         ({int(item["sequence"]) for item in session_feedback_images if item.get("sequence")} if caption_feedback else set())),
                     "is_reorder_question": reorder, "is_schedule_question": schedule_question, "followup_question_sequence": previous_question.get("sequence") if previous_question else None, "confirmed_reply_sequences": confirmed_replies,
                     "image_ocr_terms": image_promotion_hits[:5],
                     "is_historical_activity": historical_activity,
                     "matches_last_week_sale": schedule_match, "matches_next_week_sale": next_week_match,
                     "is_today_product_link": today_product_link, "is_daily_purchase": daily_purchase,
                     "is_product_request": product_request, "is_future_product_notice": future_product_notice,
                     "strong_mention": strong_mention,
                     "human_answer_required": bool(daily_purchase or strong_mention),
                     "same_sender_window_count": burst_count, "recent_outbound_count": len(recent_outbound),
                     "conversation_message_count": len(session_sequences), "conversation_window_seconds": conversation_window},
        "assignee": assignee,
        "assignee_name": {"aftercare": "售后客服 · 一对一服务", "group_human": "群内人工答复", "reviewer": "人工审核"}[assignee],
        "response_state": "replied" if confirmed_replies else "not_needed" if primary in ("closing", "ordinary", "order_image", "feedback") else "open",
        "response_needed": None if confirmed_replies or primary == "unknown" else primary not in ("closing", "ordinary", "order_image", "feedback"),
        "context_sequences": context_sequences[:80],
        "conversation_id": conversation_id,
        "conversation_sequences": session_sequences[:80],
        "reason": "；".join((evidence + uncertainty)[:5]) or "未发现需要处理的明确线索",
    }
