"""Inspect business targets and explicitly initialize an empty order-image table.

This module never creates records, uploads files, or deletes existing fields.
"""
from __future__ import annotations

import re
from urllib.parse import parse_qs, urlparse

import requests

API = "https://open.feishu.cn/open-apis"
ORDER_FIELDS = [
    {"field_name": "记录标识", "type": 1},
    {"field_name": "发生时间", "type": 5, "property": {"date_formatter": "yyyy-MM-dd HH:mm", "auto_fill": False}},
    {"field_name": "来源账号", "type": 1},
    {"field_name": "群名", "type": 1},
    {"field_name": "群ID", "type": 1},
    {"field_name": "用户名", "type": 1},
    {"field_name": "用户ID", "type": 1},
    {"field_name": "商品", "type": 1},
    {"field_name": "订单图", "type": 17},
    {"field_name": "配文", "type": 1},
    {"field_name": "消息ID", "type": 1},
    {"field_name": "客户对话ID", "type": 1},
    {"field_name": "处理状态", "type": 3, "property": {"options": [
        {"name": "待核验"}, {"name": "已确认"}, {"name": "已归档"}, {"name": "暂不采用"}]}},
    {"field_name": "登记时间", "type": 5, "property": {"date_formatter": "yyyy-MM-dd HH:mm", "auto_fill": False}},
    {"field_name": "来源键", "type": 1},
]
DEMAND_FIELDS = {"group_name": ("群名", 1), "customer_name": ("用户名", 1),
    "product": ("需求产品", 1), "occurred_at": ("时间", 5),
    "category": ("类目", 3), "status": ("状态", 3), "source_key": ("SourceID", 1)}


class TargetError(ValueError):
    def __init__(self, code):
        self.code = str(code)
        super().__init__("飞书表操作失败，错误码：" + self.code)


def parse_target(url):
    parsed = urlparse(str(url or ""))
    match = re.fullmatch(r"/base/([A-Za-z0-9]+)", parsed.path.rstrip("/"))
    table = parse_qs(parsed.query).get("table", [""])[0]
    if (parsed.scheme != "https" or not (parsed.hostname or "").endswith(".feishu.cn")
            or parsed.username or parsed.password or parsed.port or not match
            or not re.fullmatch(r"tbl[A-Za-z0-9]+", table)):
        raise ValueError("请提供带 table 参数的飞书多维表格地址")
    return match[1], table


def order_schema_plan(fields):
    """Preflight every existing field before performing any remote mutation."""
    by_name = {field["field_name"]: field for field in fields}
    for expected in ORDER_FIELDS:
        actual = by_name.get(expected["field_name"])
        if actual and actual["type"] != expected["type"]:
            raise ValueError("已有字段类型不一致，未修改：" + expected["field_name"])
    primary = [field for field in fields if field.get("is_primary")]
    if len(primary) != 1 or primary[0]["type"] != 1:
        raise ValueError("主字段不是唯一文本字段，未修改表结构")
    rename = None
    if primary[0]["field_name"] != "记录标识":
        if primary[0]["field_name"] != "文本" or "记录标识" in by_name:
            raise ValueError("主字段不是空白表默认字段，未修改表结构")
        rename = {"field_id": primary[0]["field_id"], "field_name": "记录标识", "type": 1}
    creates = [field for field in ORDER_FIELDS if field["field_name"] not in by_name
               and field["field_name"] != "记录标识"]
    return {"rename": rename, "create": creates}


def demand_field_mapping(fields):
    by_name = {field["field_name"]: field for field in fields}
    mapping, issues = {}, []
    for key, (name, kind) in DEMAND_FIELDS.items():
        field = by_name.get(name)
        if not field or field["type"] != kind:
            issues.append("缺少兼容字段：" + name)
        else:
            mapping[key] = {"field_name": name, "field_id": field["field_id"], "type": kind}
    return {"fields": mapping, "issues": issues}


class TargetClient:
    def __init__(self, token, url, session=None):
        app, table = parse_target(url)
        self.url = url
        self.root = API + f"/bitable/v1/apps/{app}/tables/{table}"
        self.headers = {"Authorization": "Bearer " + token}
        self.session = session or requests.Session()

    def request(self, method, suffix, **kwargs):
        response = self.session.request(method, self.root + suffix, headers=self.headers,
                                        timeout=(5, 25), **kwargs)
        body = response.json()
        if body.get("code") != 0:
            # Do not include request headers or credentials in exceptions/reports.
            raise TargetError(body.get("code"))
        response.raise_for_status()
        return body.get("data") or {}

    def list_items(self, suffix, max_pages=100):
        items, page_token, seen = [], "", set()
        for _ in range(max_pages):
            params = {"page_size": 100}
            if page_token: params["page_token"] = page_token
            data = self.request("GET", suffix, params=params)
            items.extend(data.get("items") or [])
            if not data.get("has_more"): return items
            page_token = data.get("page_token") or ""
            if not page_token or page_token in seen: raise ValueError("飞书表分页游标无效")
            seen.add(page_token)
        raise ValueError("飞书表数据超过检查上限，未继续操作")

    def inspect(self):
        fields = self.list_items("/fields")
        records = self.request("GET", "/records", params={"page_size": 1})
        return {"url": self.url, "fields": fields, "record_count": records.get("total"),
                "readable": True, "records_written": 0}

    def initialize_empty_order_table(self):
        fields = self.list_items("/fields")
        plan = order_schema_plan(fields)
        if plan["rename"] or plan["create"]:
            records = self.list_items("/records")
            if any(any(value not in (None, "", [], {}) for value in (row.get("fields") or {}).values()) for row in records):
                raise ValueError("订单图表已有业务内容，未修改结构；请先核对现有字段")
            if plan["rename"]:
                change = plan["rename"]
                self.request("PUT", "/fields/" + change["field_id"],
                             json={"field_name": change["field_name"], "type": change["type"]})
            for field in plan["create"]:
                self.request("POST", "/fields", json=field)
        result = self.inspect()
        remaining = order_schema_plan(result["fields"])
        if remaining["rename"] or remaining["create"]: raise ValueError("订单图表结构校验未通过")
        return {**result, "schema_ready": True, "changes": plan}
