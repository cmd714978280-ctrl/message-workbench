"""Explicit one-shot setup; no background external execution is enabled."""
from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime
from pathlib import Path

from feishu_business_targets import TargetClient, TargetError, demand_field_mapping, order_schema_plan
from ingestion_store import IngestionStore
from workflow_store import WorkflowStore, BUSINESS_TZ


def main():
    parser = argparse.ArgumentParser(description="检查飞书业务表并保存工作台配置")
    parser.add_argument("--demand-url", required=True)
    parser.add_argument("--order-url", required=True)
    parser.add_argument("--apply", action="store_true", help="保存本地配置，停用旧测试文案源，启用 FAQ 客服入口")
    parser.add_argument("--initialize-order-schema", action="store_true", help="明确授权仅补建空白订单图表的字段")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    store = IngestionStore(root / "data/messages.sqlite3")
    workflow = WorkflowStore(store, root.parent / ".env")
    token = workflow._token()
    demand = TargetClient(token, args.demand_url)
    order = TargetClient(token, args.order_url)
    demand_info = demand.inspect()
    demand_mapping = demand_field_mapping(demand_info["fields"])
    if demand_mapping["issues"]: raise ValueError("需求表字段待核对：" + "；".join(demand_mapping["issues"]))
    order_info = order.inspect()
    plan = order_schema_plan(order_info["fields"])
    stamp = datetime.now(BUSINESS_TZ).strftime("%Y%m%d-%H%M%S")
    report_path = root / "data/reports" / ("business-targets-" + stamp + ".json")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report = {"date": datetime.now(BUSINESS_TZ).isoformat(), "demand": demand_info,
              "demand_mapping": demand_mapping, "order": order_info, "order_schema_plan": plan,
              "local_config_applied": False, "records_written": 0, "files_uploaded": 0}
    # Persist a reviewable preflight before any authorized schema write.
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if args.initialize_order_schema:
        try:
            report["order"] = order.initialize_empty_order_table()
        except TargetError as exc:
            report["order"] = order.inspect()
            report["order"].update(schema_ready=False, error_code=exc.code,
                error_summary="飞书应用缺少该表编辑权限，订单图表字段待补建" if exc.code=="91403" else "订单图表字段初始化失败，请核对飞书权限和字段方案")
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if args.apply:
        backup = root / "data/backups" / ("business-targets-config-" + stamp)
        backup.mkdir(parents=True, exist_ok=False)
        with store.connect() as source, sqlite3.connect(backup / "messages.sqlite3") as target: source.backup(target)
        config = store.get_workspace_config()
        settings = dict(config["settings"])
        verification = dict(settings.get("businessTargetSchemas") or {})
        for key, info in (("demandTableUrl", demand_info), ("orderTableUrl", report["order"])):
            settings[key] = info["url"]
            verification[key] = {"url": info["url"], "checked_at": report["date"], "readable": True,
                "schema_ready": not demand_mapping["issues"] if key == "demandTableUrl" else not any(order_schema_plan(info["fields"]).values()),
                "fields": [{"field_id": row["field_id"], "field_name": row["field_name"], "type": row["type"]} for row in info["fields"]]}
            if not verification[key]["schema_ready"]:
                verification[key].update(error_code=info.get("error_code") or "schema_incomplete",
                    error_summary=info.get("error_summary") or "订单图表字段待补建", schema_plan=order_schema_plan(info["fields"]))
            if key == "demandTableUrl": verification[key]["mapping"] = demand_mapping["fields"]
        settings["businessTargetSchemas"] = verification
        settings["useFaqServiceContacts"] = True
        store.save_workspace_config(settings, config["faqs"])
        workflow.set_source("feishu-copy-test", enabled=False, sync_enabled=False)
        report["local_config_applied"] = True
        report["local_backup"] = str(backup)
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"report": str(report_path), "local_config_applied": report["local_config_applied"],
        "demand_fields": len(demand_info["fields"]), "order_fields": len(report["order"]["fields"]),
        "records_written": 0, "files_uploaded": 0}, ensure_ascii=False))


if __name__ == "__main__": main()
