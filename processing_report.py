"""Export calibration metrics using saved human annotations; never calls a model."""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from analysis_engine import LABELS, RULE_VERSION
from ingestion_store import IngestionStore


def build_report(store, version=RULE_VERSION, baseline_version=None):
    baseline_version = baseline_version if baseline_version is not None else version - 1
    summary = store.development_audit_summary(version)
    with store.connect() as db:
        rows = db.execute("""SELECT d.*,a.result_json,b.result_json baseline_result,e.normalized_json FROM development_audits d
            JOIN events e ON e.sequence=d.event_sequence AND e.display=1
            JOIN analysis_results a ON a.event_sequence=d.event_sequence AND a.rule_version=?
            LEFT JOIN analysis_results b ON b.event_sequence=d.event_sequence AND b.rule_version=?
            WHERE d.rule_version=(SELECT MAX(h.rule_version) FROM development_audits h
              WHERE h.event_sequence=d.event_sequence AND h.rule_version<=?) ORDER BY d.event_sequence""", (version, baseline_version, version)).fetchall()
        vision = {r["state"]: r["n"] for r in db.execute("SELECT state,COUNT(*) n FROM image_understanding GROUP BY state")}
    fixed, regressions, differences = [], [], []
    baseline = Counter()
    baseline_correct = 0
    for row in rows:
        current = json.loads(row["result_json"])
        actual = row["actual_primary"]
        prior = json.loads(row["baseline_result"] or "{}")
        old_prediction = prior.get("primary") or row["predicted_primary"]
        baseline[old_prediction] += 1
        old_correct, new_correct = old_prediction == actual, current["primary"] == actual
        baseline_correct += int(old_correct)
        entry = {"sequence": row["event_sequence"], "before": old_prediction, "after": current["primary"], "human": actual,
                 "annotation_version": row["rule_version"], "note": row["note"],
                 "excerpt": str(json.loads(row["normalized_json"] or "{}").get("content") or "")[:120]}
        if not old_correct and new_correct:
            fixed.append(entry)
        if old_correct and not new_correct:
            regressions.append(entry)
        if not new_correct or set(current.get("labels") or []) != set(json.loads(row["actual_labels_json"] or "[]")):
            differences.append(entry)
    return {"summary": summary, "baseline_version": baseline_version, "baseline_correct": baseline_correct,
            "baseline_accuracy": round(baseline_correct/len(rows),4) if rows else None,
            "fixed_primary": fixed, "regressions_primary": regressions,
            "differences": differences, "vision_cache": vision,
            "limitations": "同一批人工标注参与了规则修正，此处是回归对照，不是独立泛化准确率；人工标签与责任路由分开评估。"}


def export_report(store, directory: Path, version=RULE_VERSION):
    report = build_report(store, version)
    directory.mkdir(parents=True, exist_ok=True)
    (directory/f"processing-v{version}.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    s = report["summary"]
    lines = [f"# 信息处理 v{version} 调试对照", "", report["limitations"], "",
             f"人工标签 {s['reviewed']} 条；当前规则主分类一致 {s['category_correct']} 条（{(s['category_accuracy'] or 0)*100:.1f}%）；分类与标签完全一致 {s['full_label_correct']} 条。",
             f"在同一套最新人工标签上，v{report['baseline_version']} 主类一致 {report['baseline_correct']} 条；本轮修正 {len(report['fixed_primary'])} 条，新差异 {len(report['regressions_primary'])} 条。", "",
             "| 分类 | 人工审核条数 | 人工占比 | 当前机器条数 |", "|---|---:|---:|---:|"]
    for category, count in sorted(s["human_categories"].items(), key=lambda item: -item[1]):
        lines.append(f"| {LABELS.get(category,category)} | {count} | {count/s['reviewed']*100:.1f}% | {s['machine_categories'].get(category,0)} |")
    lines += ["", "## 待调试差异", "", "| 消息序号 | 上版机器 | 当前机器 | 人工结论 |", "|---|---|---|---|"]
    for item in report["differences"]:
        lines.append(f"| #{item['sequence']} | {LABELS.get(item['before'])} | {LABELS.get(item['after'])} | {LABELS.get(item['human'])} |")
    path = directory/f"processing-v{version}.md"
    path.write_text("\n".join(lines)+"\n", encoding="utf-8")
    return report, path


if __name__ == "__main__":
    root = Path(__file__).resolve().parent
    report, path = export_report(IngestionStore(root/"data/messages.sqlite3"), root/"data/reports")
    print(json.dumps({"report": str(path), "summary": report["summary"], "fixed": len(report["fixed_primary"]),
                      "regressions": len(report["regressions_primary"]), "vision": report["vision_cache"]}, ensure_ascii=False))
