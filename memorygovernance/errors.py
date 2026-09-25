"""服务层异常类型。"""
from __future__ import annotations


class GovernanceError(Exception):
    """所有治理服务异常的基类。"""


class ValidationError(GovernanceError):
    """入参不满足领域约束。"""


class NotFoundError(GovernanceError):
    """租户内找不到指定对象。"""


class ConflictError(GovernanceError):
    """对象状态与请求操作冲突。"""


class ImmutableFactError(ConflictError):
    """试图覆盖已影响决策的旧事实。"""


class PolicyBlockedError(GovernanceError):
    """操作被合同保留或法律保全阻断。"""
