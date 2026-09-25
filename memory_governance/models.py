"""领域对象、常量与错误类型，语义与 domain/contract.json 对齐。"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone

# ---------------------------------------------------------------- 记忆类别
CATEGORY_USER_UPLOAD = "user_upload"  # 用户上传内容
CATEGORY_TOOL_RESULT = "tool_result"  # 工具返回
CATEGORY_MODEL_NOTE = "model_note"  # 模型整理笔记
CATEGORY_SUMMARY = "summary"  # 派生摘要
CATEGORY_INDEX_FRAGMENT = "index_fragment"  # 搜索索引片段
RAW_CATEGORIES = frozenset({CATEGORY_USER_UPLOAD, CATEGORY_TOOL_RESULT, CATEGORY_MODEL_NOTE})
DERIVED_CATEGORIES = frozenset({CATEGORY_SUMMARY, CATEGORY_INDEX_FRAGMENT})
ALL_CATEGORIES = RAW_CATEGORIES | DERIVED_CATEGORIES

# ---------------------------------------------------------------- 生命周期状态
ACTIVE = "active"
SUPERSEDED = "superseded"
HELD = "held"
EXPORTING = "exporting"
PENDING_ERASURE = "pending_erasure"
ERASING = "erasing"
ERASED = "erased"
FAILED = "failed"
# 任务仍可读取的状态；pending_erasure 起读取权限已收回
READABLE_STATES = frozenset({ACTIVE, HELD, EXPORTING})

# ---------------------------------------------------------------- 事件类型
EVT_MEMORY_INGESTED = "memory.ingested"
EVT_MEMORY_DERIVED = "memory.derived"
EVT_MEMORY_CORRECTED = "memory.corrected"
EVT_HOLD_APPLIED = "hold.applied"
EVT_HOLD_RELEASED = "hold.released"
EVT_EXPORT_REQUESTED = "export.requested"
EVT_EXPORT_COMPLETED = "export.completed"
EVT_ERASURE_REQUESTED = "erasure.requested"
EVT_ERASURE_STEP_COMPLETED = "erasure.step_completed"
EVT_ERASURE_COMPLETED = "erasure.completed"
EVT_TASK_GRANTED = "task.granted"
EVT_TASK_REVOKED = "task.revoked"
EVT_TASK_ACCESS_RECORDED = "task.access_recorded"
EVT_RULE_PUBLISHED = "retention.rule_published"

# ---------------------------------------------------------------- 删除阶段
STAGE_REVOKE = "revoke"  # 收回读取权限（可恢复）
STAGE_QUARANTINE = "quarantine"  # 原文移入隔离区（可恢复）
STAGE_PURGE = "purge"  # 销毁隔离区原文（不可恢复）
STAGE_FREEZE = "freeze"  # 法律保全冻结，仅记录原因
ERASURE_STAGES = (STAGE_REVOKE, STAGE_QUARANTINE, STAGE_PURGE)

STEP_PENDING = "pending"
STEP_DONE = "done"
STEP_SKIPPED = "skipped"  # 被法律保全冻结
STEP_FAILED = "failed"
STEP_CANCELLED = "cancelled"  # 隔离恢复后取消

REQUEST_PENDING = "pending"
REQUEST_RUNNING = "running"
REQUEST_COMPLETED = "completed"
REQUEST_COMPLETED_WITH_HOLDS = "completed_with_holds"
REQUEST_FAILED = "failed"

ACCESS_READ = "read"
ACCESS_DECISION = "decision"  # 该次读取影响了决策

REQUIREMENT_CONTRACT = "contract"  # 合同要求
REQUIREMENT_REGULATION = "regulation"  # 法规要求
REQUIREMENTS = frozenset({REQUIREMENT_CONTRACT, REQUIREMENT_REGULATION})


class GovernanceError(Exception):
    """治理服务基础错误。"""


class NotFoundError(GovernanceError):
    """对象不存在。"""


class ValidationError(GovernanceError):
    """输入违反领域约束。"""


class PermissionDeniedError(GovernanceError):
    """越权访问。"""


class StateConflictError(GovernanceError):
    """当前状态不允许该操作。"""


def ensure_aware(value: datetime) -> datetime:
    """合同 time_policy 要求所有时间必须带时区。"""
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValidationError("时间必须包含时区（ISO 8601 with timezone）")
    return value


def to_iso(value: datetime) -> str:
    """统一归一到 UTC 的 ISO 8601 文本，保证可字典序比较。"""
    return ensure_aware(value).astimezone(timezone.utc).isoformat()


def parse_iso(text: str) -> datetime:
    return datetime.fromisoformat(text)


def hash_content(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class MemoryItem:
    """一条记忆的单个版本；旧版本行永不覆盖，只追加新版本。"""

    tenant_id: str
    item_id: str
    version: int
    category: str
    purpose: str
    sensitive_categories: tuple[str, ...]
    content: str | None  # 清除后为 None，仅留哈希作证
    content_hash: str
    state: str
    decision_influencing: bool
    user_retention_days: int | None
    contract_retention_days: int | None
    retain_until: str | None
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class Auditor:
    """审计员授权范围：可审计的租户、可查看原文的目的、是否允许看原文。"""

    auditor_id: str
    tenant_ids: frozenset[str]
    purposes: frozenset[str] = frozenset()
    can_view_content: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "tenant_ids", frozenset(self.tenant_ids))
        object.__setattr__(self, "purposes", frozenset(self.purposes))
