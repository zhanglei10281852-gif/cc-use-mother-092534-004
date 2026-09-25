"""时间工具：统一使用带时区的 ISO 8601 字符串。"""
from __future__ import annotations

from datetime import datetime, timezone


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(value: datetime | None = None) -> str:
    moment = value or utc_now()
    if moment.tzinfo is None:
        raise ValueError("时间戳必须带时区")
    return moment.isoformat()


def parse_iso(value: str | datetime) -> datetime:
    moment = datetime.fromisoformat(value) if isinstance(value, str) else value
    if moment.tzinfo is None:
        raise ValueError("时间戳必须带时区")
    return moment
