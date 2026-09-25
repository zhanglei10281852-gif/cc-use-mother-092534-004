"""测试公共工具：固定时钟与服务工厂。"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from memory_governance import MemoryGovernanceService  # noqa: E402

T0 = datetime(2026, 9, 25, 1, 0, 0, tzinfo=timezone.utc)


class FixedClock:
    def __init__(self, start: datetime = T0) -> None:
        self.moment = start

    def __call__(self) -> datetime:
        return self.moment

    def advance(self, **kwargs) -> datetime:
        self.moment += timedelta(**kwargs)
        return self.moment


def make_service() -> tuple[MemoryGovernanceService, FixedClock]:
    clock = FixedClock()
    return MemoryGovernanceService(clock=clock), clock
