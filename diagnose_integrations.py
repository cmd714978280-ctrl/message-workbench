"""Minimal integration checks. Never prints secrets or message content."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import requests

from phase3 import check_feishu_table, local_secrets


def main():
    env = Path(__file__).resolve().parent.parent / ".env"
    secrets = local_secrets(env)
    key = secrets.get("deepseekapi") or secrets.get("deepseek_api_key")
    if not key:
        print("DeepSeek: missing key")
    else:
        try:
            response = requests.post("https://api.deepseek.com/chat/completions",
                headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"},
                json={"model": "deepseek-flash", "messages": [{"role": "user", "content": "只返回 JSON：{\"ok\":true}"}],
                      "response_format": {"type": "json_object"}, "max_tokens": 40}, timeout=20)
            print("DeepSeek: HTTP", response.status_code)
            if response.ok:
                body = response.json()
                print("DeepSeek: response present", bool(body.get("choices")))
        except (requests.RequestException, ValueError, KeyError, json.JSONDecodeError) as exc:
            print("DeepSeek:", type(exc).__name__)
    try:
        with sqlite3.connect(Path(__file__).resolve().parent / "data" / "messages.sqlite3") as db:
            row = db.execute("SELECT settings_json FROM workspace_config WHERE id=1").fetchone()
        gallery_url = (json.loads(row[0]).get("galleryUrl") if row else "") or ""
        result = check_feishu_table(env, gallery_url)
        print("Feishu: readable", result.get("ok"), "stage", result.get("stage"),
              "code", result.get("code"), "fields", result.get("field_count"))
    except (requests.RequestException, ValueError) as exc:
        print("Feishu:", type(exc).__name__, str(exc)[:120] if isinstance(exc, ValueError) else "network")


if __name__ == "__main__":
    main()
