"""Runtime configuration for Yunzhongke connector identity.

The rule ID is operational configuration and can change without a code edit.
The source-account ID is a stable namespace for message/workflow history and
must not be derived from the rule ID.
"""
from __future__ import annotations

import os
import re
from pathlib import Path


def _key(value: str) -> str:
    return re.sub(r"[\s_\-]", "", value).casefold()


def _read_config(path: Path | None = None) -> dict[str, str]:
    env_path = Path(path) if path else Path(__file__).resolve().parent.parent / ".env"
    values: dict[str, str] = {}
    if not env_path.is_file():
        return values
    for line in env_path.read_text(encoding="utf-8-sig").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        parts = re.split(r"[：:=]", stripped, maxsplit=1)
        if len(parts) == 2:
            values[_key(parts[0])] = parts[1].strip().strip("\"'")
    return values


def _value(path: Path | None, *keys: str) -> str:
    for key in keys:
        value = os.environ.get(key, "").strip()
        if value:
            return value
    values = _read_config(path)
    for key in keys:
        value = values.get(_key(key), "").strip()
        if value:
            return value
    return ""


def configured_rule_id(path: Path | None = None, *, required: bool = True) -> str:
    value = _value(path, "YUNZHONGKE_RULE_ID", "云中客规则ID")
    if value and re.fullmatch(r"[A-Za-z0-9_-]{1,64}", value):
        return value
    if required:
        raise RuntimeError("请在工作区根目录 .env 配置 YUNZHONGKE_RULE_ID")
    return ""


def configured_source_account_id(path: Path | None = None) -> str:
    value = _value(path, "YUNZHONGKE_SOURCE_ACCOUNT_ID", "云中客来源账号ID")
    if value and re.fullmatch(r"[A-Za-z0-9_.:-]{1,160}", value):
        return value
    # This is a stable fallback namespace, not a connector rule ID.
    return "yunzhongke:primary"
