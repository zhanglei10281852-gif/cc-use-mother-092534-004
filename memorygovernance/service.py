"""记忆分级治理领域服务。

所有写操作都满足：
* 写入即固化租户、数据主体、来源、目的、敏感类别；
* 派生记忆通过 ``derivation_edges`` 绑定到全部直接来源的具体版本；
* 事实与规则只追加新版本，永不覆盖；
* 删除沿派生依赖图闭包执行，分「隔离（可恢复）→ 清除（不可恢复）」两阶段，
  合同保留期内暂缓、法律保全节点冻结并说明原因；
* 同一租户+主体的删除请求幂等，中断后续跑只处理未完成步骤；
* 清除证明只含标识、散列与计数。
"""
from __future__ import annotations

import hashlib
import json
import uuid
from collections import deque
from datetime import timedelta
from typing import Any, Iterable

from .errors import (
    ConflictError,
    ImmutableFactError,
    NotFoundError,
    PolicyBlockedError,
    ValidationError,
)
from .store import MemoryStore
from .timeutil import parse_iso, to_iso, utc_now

VALID_SOURCE_TYPES = {"user_upload", "tool_return", "model_note", "derivation"}
TERMINAL_STEP_STATUSES = {"quarantined", "purged", "restored"}


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:16]}"


def _sha256(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


class MemoryGovernanceService:
    def __init__(self, store: MemoryStore) -> None:
        self.store = store

    # ===================================================================
    # 写入：摄取、派生与更正
    # ===================================================================

    def ingest_memory(
        self,
        tenant_id: str,
        *,
        item_id: str,
        subject_id: str,
        kind: str,
        content: str,
        purpose: str,
        sensitivity_category: str,
        source_type: str,
        source_ref: str,
        actor_id: str,
        user_expires_at: str | None = None,
        derived_from: Iterable[tuple[str, int]] | None = None,
        occurred_at: str | None = None,
    ) -> dict[str, Any]:
        """摄取一条记忆。``derived_from`` 为 ``(上游 item_id, 版本号)`` 时写入派生边。"""
        self._require_text(tenant_id, "tenant_id")
        self._require_text(item_id, "item_id")
        self._require_text(subject_id, "subject_id")
        self._require_text(purpose, "purpose")
        self._require_text(sensitivity_category, "sensitivity_category")
        self._require_text(source_ref, "source_ref")
        if source_type not in VALID_SOURCE_TYPES:
            raise ValidationError(f"未知来源类型：{source_type}")
        if not isinstance(content, str):
            raise ValidationError("content 必须是字符串")
        if user_expires_at is not None:
            parse_iso(user_expires_at)
        now = occurred_at or to_iso()
        derived_from = list(derived_from or [])

        try:
            if self._get_item(tenant_id, item_id) is not None:
                raise ConflictError(f"记忆已存在：{item_id}")
            upstreams: list[dict[str, Any]] = []
            for upstream_id, upstream_version in derived_from:
                upstream = self._get_item(tenant_id, upstream_id)
                if upstream is None:
                    raise ValidationError(f"派生来源不存在：{upstream_id}")
                version = self._get_version(tenant_id, upstream_id, upstream_version)
                if version is None:
                    raise ValidationError(
                        f"派生来源版本不存在：{upstream_id}@v{upstream_version}"
                    )
                upstreams.append(version)

            self.store.execute(
                "insert into memory_items (item_id, tenant_id, subject_id, kind, "
                "sensitivity_category, purpose, source_type, source_ref, user_expires_at, "
                "current_version_no, state, created_at, updated_at) "
                "values (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'active', ?, ?)",
                (
                    item_id, tenant_id, subject_id, kind, sensitivity_category, purpose,
                    source_type, source_ref, user_expires_at, now, now,
                ),
            )
            digest = _sha256(content)
            self.store.execute(
                "insert into memory_versions (tenant_id, item_id, version_no, content, "
                "content_sha256, created_at) values (?, ?, 1, ?, ?, ?)",
                (tenant_id, item_id, content, digest, now),
            )
            for upstream in upstreams:
                self.store.execute(
                    "insert into derivation_edges (tenant_id, edge_id, upstream_item_id, "
                    "upstream_version_no, downstream_item_id, created_at) "
                    "values (?, ?, ?, ?, ?, ?)",
                    (
                        tenant_id, _new_id("edge"), upstream["item_id"],
                        upstream["version_no"], item_id, now,
                    ),
                )
            event_type = "memory.derived" if derived_from else "memory.ingested"
            self.store.append_event(
                tenant_id, _new_id("evt"), event_type, item_id, now, actor_id,
                {
                    "subject_id": subject_id,
                    "kind": kind,
                    "source_type": source_type,
                    "purpose": purpose,
                    "sensitivity_category": sensitivity_category,
                    "content_sha256": digest,
                    "derived_from": [
                        {"item_id": u["item_id"], "version_no": u["version_no"]}
                        for u in upstreams
                    ],
                },
            )
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise
        return self._get_item(tenant_id, item_id)  # type: ignore[return-value]

    def correct_memory(
        self,
        tenant_id: str,
        item_id: str,
        *,
        new_content: str,
        reason: str,
        actor_id: str,
        occurred_at: str | None = None,
    ) -> dict[str, Any]:
        """追加更正版本。旧版本永不被修改或覆盖。"""
        self._require_text(reason, "reason")
        now = occurred_at or to_iso()
        try:
            item = self._require_item(tenant_id, item_id)
            if item["state"] in ("recoverable", "erased"):
                raise ConflictError(f"记忆处于{item['state']}状态，不能更正：{item_id}")
            next_version = item["current_version_no"] + 1
            old = self._require_version(tenant_id, item_id, item["current_version_no"])
            self.store.execute(
                "update memory_versions set superseded_at=? "
                "where tenant_id=? and item_id=? and version_no=?",
                (now, tenant_id, item_id, old["version_no"]),
            )
            digest = _sha256(new_content)
            self.store.execute(
                "insert into memory_versions (tenant_id, item_id, version_no, content, "
                "content_sha256, correction_reason, created_at) values (?, ?, ?, ?, ?, ?, ?)",
                (tenant_id, item_id, next_version, new_content, digest, reason, now),
            )
            self.store.execute(
                "update memory_items set current_version_no=?, updated_at=? "
                "where tenant_id=? and item_id=?",
                (next_version, now, tenant_id, item_id),
            )
            self.store.append_event(
                tenant_id, _new_id("evt"), "memory.corrected", item_id, now, actor_id,
                {
                    "from_version_no": old["version_no"],
                    "to_version_no": next_version,
                    "reason": reason,
                    "old_content_sha256": old["content_sha256"],
                    "new_content_sha256": digest,
                    "old_version_decision_used": bool(old["decision_used"]),
                },
            )
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise
        return {
            "item_id": item_id,
            "version_no": next_version,
            "content_sha256": digest,
            "superseded_version_no": next_version - 1,
        }

    def record_decision_use(
        self,
        tenant_id: str,
        *,
        decision_id: str,
        item_id: str,
        version_no: int,
        task_id: str,
        summary: str,
        actor_id: str,
        decided_at: str | None = None,
    ) -> dict[str, Any]:
        """登记某版本事实曾影响决策；该版本此后不可覆盖。"""
        moment = decided_at or to_iso()
        try:
            self._require_item(tenant_id, item_id)
            version = self._require_version(tenant_id, item_id, version_no)
            if self.store.query_one(
                "select 1 from decision_uses where tenant_id=? and decision_id=?",
                (tenant_id, decision_id),
            ):
                raise ConflictError(f"决策记录已存在：{decision_id}")
            self.store.execute(
                "insert into decision_uses (tenant_id, decision_id, item_id, version_no, "
                "task_id, summary, decided_at, recorded_at) values (?, ?, ?, ?, ?, ?, ?, ?)",
                (tenant_id, decision_id, item_id, version_no, task_id, summary, moment, to_iso()),
            )
            self.store.execute(
                "update memory_versions set decision_used=1 "
                "where tenant_id=? and item_id=? and version_no=?",
                (tenant_id, item_id, version_no),
            )
            self.store.append_event(
                tenant_id, _new_id("evt"), "decision.used", item_id, moment, actor_id,
                {"decision_id": decision_id, "task_id": task_id, "version_no": version_no},
            )
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise
        return {"decision_id": decision_id, "item_id": item_id, "version_no": version_no}

    def overwrite_decision_used_version(self, *args: Any, **kwargs: Any) -> None:
        """显式表达禁区：任何调用都被拒绝（旧事实只能追加更正）。"""
        raise ImmutableFactError("已影响决策的版本不可覆盖，只能追加新版本")

    # ===================================================================
    # 保留期限：规则（追加版本）与裁决
    # ===================================================================

    def set_contract_retention(
        self,
        tenant_id: str,
        *,
        rule_id: str,
        contract_minimum_days: int,
        actor_id: str,
        subject_id: str | None = None,
        kind: str | None = None,
    ) -> dict[str, Any]:
        """登记/修订合同最低保留天数。规则同样只追加新版本。"""
        if contract_minimum_days < 0:
            raise ValidationError("合同最低保留天数不能为负")
        now = to_iso()
        try:
            previous = self.store.query_one(
                "select * from retention_rules where tenant_id=? and rule_id=? and active=1",
                (tenant_id, rule_id),
            )
            version_no = 1
            if previous is not None:
                version_no = previous["version_no"] + 1
                self.store.execute(
                    "update retention_rules set active=0, superseded_at=? "
                    "where tenant_id=? and rule_id=? and active=1",
                    (now, tenant_id, rule_id),
                )
            self.store.execute(
                "insert into retention_rules (tenant_id, rule_id, subject_id, kind, "
                "contract_minimum_days, version_no, active, created_at) "
                "values (?, ?, ?, ?, ?, ?, 1, ?)",
                (tenant_id, rule_id, subject_id, kind, contract_minimum_days, version_no, now),
            )
            self.store.append_event(
                tenant_id, _new_id("evt"), "retention.configured", rule_id, now, actor_id,
                {
                    "rule_id": rule_id,
                    "subject_id": subject_id,
                    "kind": kind,
                    "contract_minimum_days": contract_minimum_days,
                    "version_no": version_no,
                },
            )
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise
        return {
            "rule_id": rule_id,
            "version_no": version_no,
            "contract_minimum_days": contract_minimum_days,
        }

    def resolve_retention(self, tenant_id: str, item_id: str, at: str | None = None) -> dict[str, Any]:
        """裁决单条记忆的有效保留期限：max(用户选择, 合同下限)，保全另行阻断。"""
        moment = parse_iso(at) if at else utc_now()
        item = self._require_item(tenant_id, item_id)
        created_at = parse_iso(item["created_at"])

        rule = self._matching_retention_rule(tenant_id, item["subject_id"], item["kind"])
        contract_min_expires_at = None
        if rule is not None:
            contract_min_expires_at = to_iso(
                created_at + timedelta(days=rule["contract_minimum_days"])
            )

        candidates = [item["user_expires_at"], contract_min_expires_at]
        present = [parse_iso(v) for v in candidates if v]
        effective = to_iso(max(present)) if present else None

        holds = self.active_holds_for(tenant_id, item["subject_id"], item_id)
        return {
            "item_id": item_id,
            "user_expires_at": item["user_expires_at"],
            "contract_rule_id": rule["rule_id"] if rule else None,
            "contract_min_expires_at": contract_min_expires_at,
            "effective_expires_at": effective,
            "held": bool(holds),
            "active_holds": [
                {"hold_id": h["hold_id"], "reason": h["reason"], "created_by": h["created_by"]}
                for h in holds
            ],
            "deletable_at": (
                to_iso(moment)
                if not holds and (effective is None or parse_iso(effective) <= moment)
                else None
            ),
        }

    def _matching_retention_rule(
        self, tenant_id: str, subject_id: str, kind: str
    ) -> dict[str, Any] | None:
        rules = self.store.query_all(
            "select * from retention_rules where tenant_id=? and active=1", (tenant_id,)
        )

        def specificity(rule: dict[str, Any]) -> int:
            # 主体+类型最具体；命中其一次之；两者皆空的规则是全局基线（0 分，仍适用）。
            return (
                2 if rule["subject_id"] == subject_id and rule["kind"] == kind else
                1 if rule["subject_id"] == subject_id or rule["kind"] == kind else
                0 if rule["subject_id"] is None and rule["kind"] is None else -1
            )

        applicable = [r for r in rules if specificity(r) >= 0]
        if not applicable:
            return None
        return max(applicable, key=specificity)

    # ===================================================================
    # 法律保全
    # ===================================================================

    def apply_hold(
        self,
        tenant_id: str,
        *,
        hold_id: str,
        subject_id: str,
        reason: str,
        created_by: str,
        item_id: str | None = None,
    ) -> dict[str, Any]:
        self._require_text(reason, "reason")  # 保全必须给出原因
        now = to_iso()
        try:
            if self.store.query_one(
                "select 1 from legal_holds where tenant_id=? and hold_id=?",
                (tenant_id, hold_id),
            ):
                raise ConflictError(f"保全已存在：{hold_id}")
            if item_id is not None:
                self._require_item(tenant_id, item_id)
            self.store.execute(
                "insert into legal_holds (tenant_id, hold_id, subject_id, item_id, reason, "
                "created_by, created_at) values (?, ?, ?, ?, ?, ?, ?)",
                (tenant_id, hold_id, subject_id, item_id, reason, created_by, now),
            )
            # 已在删除链中的节点立即回到 held 并冻结对应步骤。
            if item_id is not None:
                self._freeze_item_for_hold(tenant_id, item_id, hold_id, reason, now)
            else:
                rows = self.store.query_all(
                    "select item_id from memory_items where tenant_id=? and subject_id=? "
                    "and state in ('pending_erasure', 'recoverable')",
                    (tenant_id, subject_id),
                )
                for row in rows:
                    self._freeze_item_for_hold(tenant_id, row["item_id"], hold_id, reason, now)
            self.store.append_event(
                tenant_id, _new_id("evt"), "hold.applied", hold_id, now, created_by,
                {"subject_id": subject_id, "item_id": item_id, "reason": reason},
            )
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise
        return {"hold_id": hold_id, "subject_id": subject_id, "item_id": item_id}

    def release_hold(
        self, tenant_id: str, hold_id: str, *, released_by: str, note: str = ""
    ) -> dict[str, Any]:
        now = to_iso()
        try:
            hold = self.store.query_one(
                "select * from legal_holds where tenant_id=? and hold_id=?",
                (tenant_id, hold_id),
            )
            if hold is None:
                raise NotFoundError(f"保全不存在：{hold_id}")
            if hold["released_at"] is not None:
                raise ConflictError(f"保全已解除：{hold_id}")
            self.store.execute(
                "update legal_holds set released_at=?, release_note=? "
                "where tenant_id=? and hold_id=?",
                (now, note, tenant_id, hold_id),
            )
            # 解除冻结：仍在隔离区的节点回到 recoverable，其余回到 pending_erasure，
            # 之后重跑删除链即可继续未完成步骤。
            self.store.execute(
                "update erasure_steps set status='pending', frozen_reason=null, hold_id=null "
                "where tenant_id=? and hold_id=? and status='frozen'",
                (tenant_id, hold_id),
            )
            self.store.execute(
                "update memory_items set "
                "state=case when quarantined_at is not null then 'recoverable' "
                "           else 'pending_erasure' end "
                "where tenant_id=? and state='held' and erasure_request_id is not null "
                "and not exists ("
                "  select 1 from legal_holds h where h.tenant_id=memory_items.tenant_id "
                "  and h.released_at is null and ("
                "    h.item_id=memory_items.item_id or "
                "    (h.item_id is null and h.subject_id=memory_items.subject_id)))",
                (tenant_id,),
            )
            self.store.append_event(
                tenant_id, _new_id("evt"), "hold.released", hold_id, now, released_by,
                {"note": note},
            )
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise
        return {"hold_id": hold_id, "released_at": now}

    def active_holds_for(
        self, tenant_id: str, subject_id: str, item_id: str
    ) -> list[dict[str, Any]]:
        return self.store.query_all(
            "select * from legal_holds where tenant_id=? and released_at is null and "
            "(item_id=? or (item_id is null and subject_id=?)) "
            "order by created_at",
            (tenant_id, item_id, subject_id),
        )

    def _freeze_item_for_hold(
        self, tenant_id: str, item_id: str, hold_id: str, reason: str, now: str
    ) -> None:
        self.store.execute(
            "update erasure_steps set status='frozen', hold_id=?, frozen_reason=? "
            "where tenant_id=? and item_id=? and status in ('pending', 'blocked')",
            (hold_id, reason, tenant_id, item_id),
        )
        self.store.execute(
            "update memory_items set state='held' "
            "where tenant_id=? and item_id=? and state in ('pending_erasure', 'recoverable')",
            (tenant_id, item_id),
        )

    # ===================================================================
    # 任务访问授权（任务引用）
    # ===================================================================

    def grant_task_access(
        self, tenant_id: str, *, task_id: str, item_id: str, granted_at: str | None = None
    ) -> dict[str, Any]:
        self._require_item(tenant_id, item_id)
        now = granted_at or to_iso()
        existing = self.store.query_one(
            "select * from task_grants where tenant_id=? and task_id=? and item_id=?",
            (tenant_id, task_id, item_id),
        )
        if existing and existing["revoked_at"] is None:
            return {"grant_id": existing["grant_id"], "task_id": task_id, "item_id": item_id}
        if existing:
            self.store.execute(
                "update task_grants set revoked_at=null, revoke_reason=null "
                "where tenant_id=? and grant_id=?",
                (tenant_id, existing["grant_id"]),
            )
            grant_id = existing["grant_id"]
        else:
            grant_id = _new_id("grant")
            self.store.execute(
                "insert into task_grants (tenant_id, grant_id, task_id, item_id, granted_at) "
                "values (?, ?, ?, ?, ?)",
                (tenant_id, grant_id, task_id, item_id, now),
            )
        self.store.commit()
        return {"grant_id": grant_id, "task_id": task_id, "item_id": item_id}

    def revoke_task_access(
        self, tenant_id: str, *, task_id: str, item_id: str, reason: str = ""
    ) -> None:
        now = to_iso()
        self.store.execute(
            "update task_grants set revoked_at=?, revoke_reason=? "
            "where tenant_id=? and task_id=? and item_id=? and revoked_at is null",
            (now, reason, tenant_id, task_id, item_id),
        )
        self.store.commit()

    # ===================================================================
    # 依赖图
    # ===================================================================

    def downstream_closure(self, tenant_id: str, root_item_ids: Iterable[str]) -> list[str]:
        """沿派生边求下游闭包：摘要、索引片段等全部派生节点。"""
        roots = list(root_item_ids)
        seen: set[str] = set()
        queue: deque[str] = deque(roots)
        while queue:
            current = queue.popleft()
            if current in seen:
                continue
            seen.add(current)
            rows = self.store.query_all(
                "select downstream_item_id from derivation_edges "
                "where tenant_id=? and upstream_item_id=?",
                (tenant_id, current),
            )
            for row in rows:
                if row["downstream_item_id"] not in seen:
                    queue.append(row["downstream_item_id"])
        return sorted(seen)

    # ===================================================================
    # 导出
    # ===================================================================

    def export_subject_data(
        self, tenant_id: str, subject_id: str, *, requested_by: str
    ) -> dict[str, Any]:
        """沿依赖图导出主体的全部记忆（含派生节点）及任务引用。"""
        roots = [
            row["item_id"]
            for row in self.store.query_all(
                "select item_id from memory_items where tenant_id=? and subject_id=?",
                (tenant_id, subject_id),
            )
        ]
        closure = self.downstream_closure(tenant_id, roots)
        items = []
        for item_id in closure:
            item = self._get_item(tenant_id, item_id)
            if item is None:
                continue
            versions = self.store.query_all(
                "select version_no, content, content_sha256, correction_reason, "
                "decision_used, created_at, superseded_at from memory_versions "
                "where tenant_id=? and item_id=? order by version_no",
                (tenant_id, item_id),
            )
            items.append({"item": item, "versions": versions})
        task_refs = self.store.query_all(
            "select g.task_id, g.item_id, g.granted_at, g.revoked_at from task_grants g "
            "where g.tenant_id=? order by g.task_id, g.item_id",
            (tenant_id,),
        )
        task_refs = [r for r in task_refs if r["item_id"] in set(closure)]
        now = to_iso()
        self.store.append_event(
            tenant_id, _new_id("evt"), "export.requested", subject_id, now, requested_by,
            {"item_count": len(items), "item_ids": closure},
        )
        self.store.commit()
        return {
            "tenant_id": tenant_id,
            "subject_id": subject_id,
            "exported_at": now,
            "items": items,
            "task_references": task_refs,
        }

    # ===================================================================
    # 删除：幂等请求、分阶段执行、中断续跑
    # ===================================================================

    def request_erasure(
        self,
        tenant_id: str,
        *,
        subject_id: str,
        requested_by: str,
        reason: str | None = None,
        recovery_grace_days: int = 30,
    ) -> dict[str, Any]:
        """发起删除。重复请求返回同一条清除链，绝不创建第二条。"""
        if recovery_grace_days < 0:
            raise ValidationError("可恢复宽限天数不能为负")
        try:
            prior = self.store.query_one(
                "select * from erasure_requests where tenant_id=? and subject_id=? "
                "order by requested_at desc, rowid desc limit 1",
                (tenant_id, subject_id),
            )
            # 已终结或进行中的请求一律返回原链；只有用户主动撤销（恢复）后才允许重新发起。
            if prior is not None and prior["status"] != "withdrawn":
                return {"request": prior, "idempotent": True}

            roots = [
                row["item_id"]
                for row in self.store.query_all(
                    "select item_id from memory_items where tenant_id=? and subject_id=? "
                    "and state not in ('erased')",
                    (tenant_id, subject_id),
                )
            ]
            closure = self.downstream_closure(tenant_id, roots)
            request_id = _new_id("erasure")
            now = to_iso()
            self.store.execute(
                "insert into erasure_requests (tenant_id, request_id, subject_id, "
                "requested_by, reason, status, requested_at, recovery_grace_days) "
                "values (?, ?, ?, ?, ?, 'pending', ?, ?)",
                (tenant_id, request_id, subject_id, requested_by, reason, now,
                 recovery_grace_days),
            )
            for item_id in closure:
                item = self._require_item(tenant_id, item_id)
                if item["state"] == "erased":
                    continue
                for stage in ("quarantine", "purge"):
                    self.store.execute(
                        "insert into erasure_steps (tenant_id, step_id, request_id, item_id, "
                        "subject_id, stage, status, created_at) values (?, ?, ?, ?, ?, ?, 'pending', ?)",
                        (tenant_id, _new_id("step"), request_id, item_id, subject_id, stage, now),
                    )
                self.store.execute(
                    "update memory_items set state='pending_erasure', erasure_request_id=?, "
                    "updated_at=? where tenant_id=? and item_id=?",
                    (request_id, now, tenant_id, item_id),
                )
            self.store.append_event(
                tenant_id, _new_id("evt"), "erasure.requested", request_id, now, requested_by,
                {"subject_id": subject_id, "reason": reason, "closure": closure},
            )
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise
        request = self.store.query_one(
            "select * from erasure_requests where tenant_id=? and request_id=?",
            (tenant_id, request_id),
        )
        return {"request": request, "idempotent": False}

    def run_erasure(
        self,
        tenant_id: str,
        request_id: str,
        *,
        now: str | None = None,
    ) -> dict[str, Any]:
        """推进删除链。只处理未完成步骤，可在中断后反复调用直到完成。"""
        moment = parse_iso(now) if now else utc_now()
        now_iso = to_iso(moment)
        try:
            request = self.store.query_one(
                "select * from erasure_requests where tenant_id=? and request_id=?",
                (tenant_id, request_id),
            )
            if request is None:
                raise NotFoundError(f"删除请求不存在：{request_id}")
            if request["status"] in ("completed", "withdrawn"):
                return self._erasure_snapshot(tenant_id, request_id)

            self.store.execute(
                "update erasure_requests set status='in_progress' "
                "where tenant_id=? and request_id=?",
                (tenant_id, request_id),
            )

            steps = self.store.query_all(
                "select * from erasure_steps where tenant_id=? and request_id=? order by rowid",
                (tenant_id, request_id),
            )
            quarantine_steps = {s["item_id"]: s for s in steps if s["stage"] == "quarantine"}
            actions: list[dict[str, str]] = []

            # --- 阶段一：隔离（可恢复） ---
            for item_id, step in quarantine_steps.items():
                if step["status"] in TERMINAL_STEP_STATUSES:
                    continue
                item = self._require_item(tenant_id, item_id)
                holds = self.active_holds_for(tenant_id, item["subject_id"], item_id)
                if holds:
                    hold = holds[0]
                    self._mark_step(
                        step, "frozen", now_iso,
                        hold_id=hold["hold_id"],
                        frozen_reason=f"法律保全 {hold['hold_id']}：{hold['reason']}",
                    )
                    self.store.execute(
                        "update memory_items set state='held' where tenant_id=? and item_id=?",
                        (tenant_id, item_id),
                    )
                    actions.append({"item_id": item_id, "action": "frozen", "hold_id": hold["hold_id"]})
                    continue
                retention = self.resolve_retention(tenant_id, item_id, at=now_iso)
                # 用户主动删除以请求本身为准；合同最低保留与法律保全仍然生效。
                contract_min = retention["contract_min_expires_at"]
                if contract_min is not None and parse_iso(contract_min) > moment:
                    # 合同保留期未满：暂缓，保留原因，后续重跑自动继续。
                    self.store.execute(
                        "update erasure_steps set status='blocked', attempt=attempt+1, "
                        "frozen_reason=?, last_error=? where tenant_id=? and step_id=?",
                        (
                            f"合同最低保留期未满（到期 {contract_min}）",
                            f"等待至 {contract_min}", tenant_id, step["step_id"],
                        ),
                    )
                    actions.append({"item_id": item_id, "action": "awaiting_retention",
                                    "until": contract_min})
                    continue
                self._quarantine_item(
                    tenant_id, item_id, request_id, moment,
                    request["recovery_grace_days"], step,
                )
                self._mark_step(step, "quarantined", now_iso)
                actions.append({"item_id": item_id, "action": "quarantined"})

            # --- 阶段二：宽限期满后物理清除（不可恢复） ---
            for step in steps:
                if step["stage"] != "purge" or step["status"] in ("purged", "restored"):
                    continue
                item = self._require_item(tenant_id, step["item_id"])
                if step["status"] == "frozen" or item["state"] == "held":
                    continue
                if item["state"] != "recoverable":
                    continue  # 尚未隔离或保留期未满
                purge_after = parse_iso(item["purge_after"])
                if purge_after > moment:
                    actions.append({
                        "item_id": item["item_id"],
                        "action": "in_recovery_window",
                        "purge_after": item["purge_after"],
                    })
                    continue
                holds = self.active_holds_for(tenant_id, item["subject_id"], item["item_id"])
                if holds:
                    hold = holds[0]
                    self.store.execute(
                        "update erasure_steps set status='frozen', hold_id=?, frozen_reason=? "
                        "where tenant_id=? and step_id=?",
                        (hold["hold_id"],
                         f"法律保全 {hold['hold_id']}：{hold['reason']}",
                         tenant_id, step["step_id"]),
                    )
                    self.store.execute(
                        "update memory_items set state='held' where tenant_id=? and item_id=?",
                        (tenant_id, item["item_id"]),
                    )
                    actions.append({"item_id": item["item_id"], "action": "frozen",
                                    "hold_id": hold["hold_id"]})
                    continue
                self._purge_item(tenant_id, item["item_id"], now_iso)
                self.store.execute(
                    "update erasure_steps set status='purged', attempt=attempt+1, "
                    "completed_at=? where tenant_id=? and step_id=?",
                    (now_iso, tenant_id, step["step_id"]),
                )
                actions.append({"item_id": item["item_id"], "action": "purged"})

            snapshot = self._finalize_request(tenant_id, request_id, now_iso)
            snapshot["actions"] = actions
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise
        return snapshot

    def restore_erasure(
        self, tenant_id: str, request_id: str, *, restored_by: str
    ) -> dict[str, Any]:
        """宽限期内撤销删除：从隔离区还原全部内容。"""
        now = to_iso()
        try:
            request = self.store.query_one(
                "select * from erasure_requests where tenant_id=? and request_id=?",
                (tenant_id, request_id),
            )
            if request is None:
                raise NotFoundError(f"删除请求不存在：{request_id}")
            if request["status"] == "completed":
                raise PolicyBlockedError("已物理清除，不可恢复")
            step_rows = self.store.query_all(
                "select distinct item_id from erasure_steps where tenant_id=? and request_id=?",
                (tenant_id, request_id),
            )
            request_item_ids = [row["item_id"] for row in step_rows]
            restored_items: set[str] = set()
            for item_id in request_item_ids:
                quarantined = self.store.query_all(
                    "select * from quarantined_content where tenant_id=? and item_id=?",
                    (tenant_id, item_id),
                )
                for row in quarantined:
                    self.store.execute(
                        "update memory_versions set content=? where tenant_id=? and item_id=? "
                        "and version_no=?",
                        (row["content"], tenant_id, row["item_id"], row["version_no"]),
                    )
                if quarantined:
                    restored_items.add(item_id)
                self.store.execute(
                    "delete from quarantined_content where tenant_id=? and item_id=?",
                    (tenant_id, item_id),
                )
                self.store.execute(
                    "update memory_items set state='active', quarantined_at=null, "
                    "purge_after=null, erasure_request_id=null, updated_at=? "
                    "where tenant_id=? and item_id=?",
                    (now, tenant_id, item_id),
                )
                self.store.execute(
                    "update erasure_steps set status='restored', completed_at=? "
                    "where tenant_id=? and request_id=? and item_id=? and status not in "
                    "('purged', 'frozen')",
                    (now, tenant_id, request_id, item_id),
                )
                if quarantined:
                    self.store.append_event(
                        tenant_id, _new_id("evt"), "erasure.restored", item_id, now, restored_by,
                        {"request_id": request_id},
                    )
            self.store.execute(
                "update erasure_requests set status='withdrawn', completed_at=? "
                "where tenant_id=? and request_id=?",
                (now, tenant_id, request_id),
            )
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise
        return {"request_id": request_id, "status": "withdrawn",
                "restored_items": sorted(restored_items)}

    def _quarantine_item(
        self,
        tenant_id: str,
        item_id: str,
        request_id: str,
        moment,
        grace_days: int,
        step: dict[str, Any] | None = None,
    ) -> None:
        now_iso = to_iso(moment)
        versions = self.store.query_all(
            "select * from memory_versions where tenant_id=? and item_id=?",
            (tenant_id, item_id),
        )
        for version in versions:
            if version["content"] is not None:
                self.store.execute(
                    "insert into quarantined_content (tenant_id, item_id, version_no, "
                    "content, content_sha256, moved_at) values (?, ?, ?, ?, ?, ?)",
                    (tenant_id, item_id, version["version_no"], version["content"],
                     version["content_sha256"], now_iso),
                )
        # 物理清除后证明仍需版本散列：隔离时把散列固化到步骤明细（不含原文）。
        if step is not None:
            hashes = [
                {"version_no": v["version_no"], "content_sha256": v["content_sha256"]}
                for v in versions
            ]
            self.store.execute(
                "update erasure_steps set detail_json=? where tenant_id=? and step_id=?",
                (json.dumps(hashes, ensure_ascii=False), tenant_id, step["step_id"]),
            )
        # 工作副本中的原文被抽离，只留散列与元数据。
        self.store.execute(
            "update memory_versions set content=null where tenant_id=? and item_id=?",
            (tenant_id, item_id),
        )
        # 任务引用在隔离时即撤销。
        self.store.execute(
            "update task_grants set revoked_at=?, revoke_reason=? "
            "where tenant_id=? and item_id=? and revoked_at is null",
            (now_iso, f"erasure {request_id}", tenant_id, item_id),
        )
        purge_after = to_iso(moment + timedelta(days=grace_days))
        self.store.execute(
            "update memory_items set state='recoverable', quarantined_at=?, purge_after=?, "
            "updated_at=? where tenant_id=? and item_id=?",
            (now_iso, purge_after, now_iso, tenant_id, item_id),
        )
        self.store.append_event(
            tenant_id, _new_id("evt"), "erasure.quarantined", item_id, now_iso, "system",
            {"request_id": request_id, "purge_after": purge_after,
             "version_count": len(versions)},
        )

    def _purge_item(self, tenant_id: str, item_id: str, now_iso: str) -> None:
        self.store.execute(
            "delete from quarantined_content where tenant_id=? and item_id=?",
            (tenant_id, item_id),
        )
        # 决策引用中的摘要原文一并清除，引用关系计数保留在证明中。
        self.store.execute(
            "delete from decision_uses where tenant_id=? and item_id=?",
            (tenant_id, item_id),
        )
        self.store.execute(
            "delete from memory_versions where tenant_id=? and item_id=?",
            (tenant_id, item_id),
        )
        self.store.execute(
            "update memory_items set state='erased', purged_at=?, updated_at=?, "
            "user_expires_at=null where tenant_id=? and item_id=?",
            (now_iso, now_iso, tenant_id, item_id),
        )
        self.store.append_event(
            tenant_id, _new_id("evt"), "erasure.purged", item_id, now_iso, "system", {}
        )

    def _mark_step(
        self, step: dict[str, Any], status: str, now_iso: str, **fields: Any
    ) -> None:
        assignments = ["status=?", "attempt=attempt+1"]
        params: list[Any] = [status]
        if status in TERMINAL_STEP_STATUSES:
            assignments.append("completed_at=?")
            params.append(now_iso)
            # 进入终结态时清掉历史尝试留下的暂缓/错误说明，避免证明出现过期原因。
            assignments.extend(["frozen_reason=null", "hold_id=null", "last_error=null"])
        for key, value in fields.items():
            assignments.append(f"{key}=?")
            params.append(value)
        params.extend([step["tenant_id"], step["step_id"]])
        self.store.execute(
            f"update erasure_steps set {', '.join(assignments)} where tenant_id=? and step_id=?",
            params,
        )

    def _finalize_request(
        self, tenant_id: str, request_id: str, now_iso: str
    ) -> dict[str, Any]:
        steps = self.store.query_all(
            "select * from erasure_steps where tenant_id=? and request_id=?",
            (tenant_id, request_id),
        )
        by_item: dict[str, dict[str, str]] = {}
        for step in steps:
            by_item.setdefault(step["item_id"], {})[step["stage"]] = step["status"]

        purged, frozen, recoverable, waiting = [], [], [], []
        for item_id, pair in by_item.items():
            q, p = pair.get("quarantine"), pair.get("purge")
            if p == "purged":
                purged.append(item_id)
            elif q == "frozen" or p == "frozen":
                frozen.append(item_id)
            elif q == "quarantined":
                recoverable.append(item_id)
            elif q == "blocked":
                waiting.append(item_id)

        request = self.store.query_one(
            "select * from erasure_requests where tenant_id=? and request_id=?",
            (tenant_id, request_id),
        )
        assert request is not None
        new_status = request["status"]
        certificate = None
        if waiting:
            new_status = "pending"
        elif recoverable:
            new_status = "recoverable"
        elif frozen and purged:
            new_status = "completed_frozen"
        elif frozen:
            new_status = "frozen_partial"
        else:
            new_status = "completed"
        # 证明只在链终结（全部清除，或清除与冻结并存）时签发；
        # 冻结中、可恢复中等中间态不产生最终证明。终结态未变化时复用既有证明。
        terminal_unchanged = (
            new_status == request["status"] and request["certificate_id"]
        )
        if new_status in ("completed", "completed_frozen") and not terminal_unchanged:
            certificate = self._issue_certificate(tenant_id, request, now_iso, steps, by_item)

        terminal = new_status in ("completed", "completed_frozen")
        self.store.execute(
            "update erasure_requests set status=?, completed_at=? "
            + (", certificate_id=?" if certificate else "")
            + " where tenant_id=? and request_id=?",
            (
                new_status,
                now_iso if terminal else None,
                *([certificate["certificate_id"]] if certificate else []),
                tenant_id, request_id,
            ),
        )
        if certificate:
            self.store.append_event(
                tenant_id, _new_id("evt"), "erasure.completed", request_id, now_iso, "system",
                {"status": new_status, "certificate_id": certificate["certificate_id"]},
            )
        return self._erasure_snapshot(tenant_id, request_id)

    def _issue_certificate(
        self,
        tenant_id: str,
        request: dict[str, Any],
        now_iso: str,
        steps: list[dict[str, Any]],
        by_item: dict[str, dict[str, str]],
    ) -> dict[str, Any]:
        certificate_id = _new_id("cert")
        entries = []
        frozen_count = purged_count = 0
        for item_id, pair in sorted(by_item.items()):
            item = self._require_item(tenant_id, item_id)
            q_step = next(s for s in steps if s["item_id"] == item_id and s["stage"] == "quarantine")
            is_frozen = "frozen" in pair.values()
            final_status = "frozen" if is_frozen else "purged"
            if is_frozen:
                frozen_count += 1
            else:
                purged_count += 1
            # 版本散列来自隔离前固化的信息：步骤 detail 或当前版本表（冻结节点版本仍在）。
            if is_frozen:
                version_rows = self.store.query_all(
                    "select version_no, content_sha256 from memory_versions "
                    "where tenant_id=? and item_id=? order by version_no",
                    (tenant_id, item_id),
                )
                version_hashes = [
                    {"version_no": r["version_no"], "content_sha256": r["content_sha256"]}
                    for r in version_rows
                ]
            else:
                # 已物理清除：散列来自隔离时固化的步骤明细，原文在任何地方都不再保留。
                q_detail = self.store.query_one(
                    "select detail_json from erasure_steps where tenant_id=? and request_id=? "
                    "and item_id=? and stage='quarantine'",
                    (tenant_id, request["request_id"], item_id),
                )
                version_hashes = (
                    json.loads(q_detail["detail_json"])
                    if q_detail and q_detail["detail_json"]
                    else self._hashes_from_events(tenant_id, item_id)
                )
            entries.append({
                "item_id": item_id,
                "kind": item["kind"],
                "sensitivity_category": item["sensitivity_category"],
                "subject_id": item["subject_id"],
                "final_status": final_status,
                "version_count": len(version_hashes),
                "version_hashes": version_hashes,
                "quarantined_at": item["quarantined_at"],
                "purged_at": item["purged_at"],
                "frozen_reason": q_step["frozen_reason"],
                "hold_id": q_step["hold_id"],
            })

        revoked_task_refs = self.store.query_one(
            "select count(*) as c from task_grants where tenant_id=? "
            "and revoke_reason=?",
            (tenant_id, f"erasure {request['request_id']}"),
        )["c"]
        counts = {
            "items_total": len(by_item),
            "items_purged": purged_count,
            "items_frozen": frozen_count,
            "versions_purged": sum(e["version_count"] for e in entries if e["final_status"] == "purged"),
            "task_references_revoked": revoked_task_refs,
        }
        self.store.execute(
            "insert into erasure_certificates (tenant_id, certificate_id, request_id, "
            "subject_id, issued_at, counts_json) values (?, ?, ?, ?, ?, ?)",
            (tenant_id, certificate_id, request["request_id"], request["subject_id"],
             now_iso, json.dumps(counts, ensure_ascii=False)),
        )
        self.store.execute(
            "delete from certificate_entries where tenant_id=? and certificate_id=?",
            (tenant_id, certificate_id),
        )
        for entry in entries:
            self.store.execute(
                "insert into certificate_entries (tenant_id, certificate_id, item_id, kind, "
                "sensitivity_category, subject_id, final_status, version_count, "
                "version_hashes_json, quarantined_at, purged_at, frozen_reason, hold_id) "
                "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (tenant_id, certificate_id, entry["item_id"], entry["kind"],
                 entry["sensitivity_category"], entry["subject_id"], entry["final_status"],
                 entry["version_count"],
                 json.dumps(entry["version_hashes"], ensure_ascii=False),
                 entry["quarantined_at"], entry["purged_at"],
                 entry["frozen_reason"], entry["hold_id"]),
            )
        return {"certificate_id": certificate_id, "counts": counts, "entries": entries}

    def _hashes_from_events(self, tenant_id: str, item_id: str) -> list[dict[str, Any]]:
        hashes: dict[int, str] = {}
        for event in self.store.list_events(tenant_id, item_id):
            payload = json.loads(event["payload_json"] or "{}")
            digest = payload.get("content_sha256")
            if digest:
                hashes.setdefault(len(hashes) + 1, digest)
            digest = payload.get("new_content_sha256")
            if digest:
                hashes[payload.get("to_version_no", len(hashes) + 1)] = digest
        return [
            {"version_no": version_no, "content_sha256": digest}
            for version_no, digest in sorted(hashes.items())
        ]

    def _erasure_snapshot(self, tenant_id: str, request_id: str) -> dict[str, Any]:
        request = self.store.query_one(
            "select * from erasure_requests where tenant_id=? and request_id=?",
            (tenant_id, request_id),
        )
        steps = self.store.query_all(
            "select step_id, item_id, stage, status, frozen_reason, hold_id, attempt, "
            "last_error, completed_at from erasure_steps where tenant_id=? and request_id=? "
            "order by rowid",
            (tenant_id, request_id),
        )
        certificate = None
        if request and request["certificate_id"]:
            certificate = self.get_certificate(tenant_id, request["certificate_id"])
        return {"request": request, "steps": steps, "certificate": certificate}

    def get_certificate(self, tenant_id: str, certificate_id: str) -> dict[str, Any]:
        cert = self.store.query_one(
            "select * from erasure_certificates where tenant_id=? and certificate_id=?",
            (tenant_id, certificate_id),
        )
        if cert is None:
            raise NotFoundError(f"清除证明不存在：{certificate_id}")
        entries = self.store.query_all(
            "select item_id, kind, sensitivity_category, subject_id, final_status, "
            "version_count, version_hashes_json, quarantined_at, purged_at, "
            "frozen_reason, hold_id from certificate_entries "
            "where tenant_id=? and certificate_id=? order by item_id",
            (tenant_id, certificate_id),
        )
        for entry in entries:
            entry["version_hashes"] = json.loads(entry.pop("version_hashes_json"))
        # 证明载荷中绝不允许出现原文：再做一道断言式检查。
        serialized = json.dumps({"certificate": cert, "entries": entries}, ensure_ascii=False)
        live = self.store.query_all(
            "select content from memory_versions where tenant_id=? and content is not null",
            (tenant_id,),
        )
        for row in live:
            if row["content"] and row["content"] in serialized:
                raise PolicyBlockedError("证明中不得包含记忆原文")
        return {
            "certificate_id": cert["certificate_id"],
            "request_id": cert["request_id"],
            "subject_id": cert["subject_id"],
            "issued_at": cert["issued_at"],
            "counts": json.loads(cert["counts_json"]),
            "entries": entries,
        }

    # ===================================================================
    # 历史时点审计
    # ===================================================================

    def audit_task_readable(
        self,
        tenant_id: str,
        *,
        task_id: str,
        at: str,
        content_access: bool = False,
        scope_item_ids: Iterable[str] | None = None,
    ) -> dict[str, Any]:
        """还原 ``at`` 时刻某任务可读取的记忆清单。

        * 默认只返回元数据与散列，``content_access=True``（由授权层确认审计人具备
          内容权限）才返回可读原文；已清除的内容对任何人都不可恢复。
        * ``scope_item_ids`` 是审计人自身的数据可见范围，范围外的节点不出现，
          审计接口不能成为越权读取的通道。
        """
        moment = parse_iso(at)
        scope = set(scope_item_ids) if scope_item_ids is not None else None
        grants = self.store.query_all(
            "select * from task_grants where tenant_id=? and task_id=? "
            "and granted_at <= ? order by item_id",
            (tenant_id, task_id, to_iso(moment)),
        )
        readable = []
        for grant in grants:
            if grant["revoked_at"] is not None and parse_iso(grant["revoked_at"]) <= moment:
                continue
            item_id = grant["item_id"]
            if scope is not None and item_id not in scope:
                continue
            item = self._get_item(tenant_id, item_id)
            if item is None or parse_iso(item["created_at"]) > moment:
                continue
            # 删除/隔离在该时点之后才发生，时点上仍可读。
            if item["quarantined_at"] and parse_iso(item["quarantined_at"]) <= moment:
                continue
            versions = self.store.query_all(
                "select version_no, content, content_sha256, decision_used, created_at, "
                "superseded_at from memory_versions where tenant_id=? and item_id=? "
                "and created_at <= ? order by version_no",
                (tenant_id, item_id, to_iso(moment)),
            )
            if not versions:
                # 版本行已被物理清除：审计时点早于清除时，用步骤明细中的散列
                # 还原「当时存在过什么」，原文对任何人都不再可得。
                fallback = self.store.query_one(
                    "select detail_json from erasure_steps where tenant_id=? and item_id=? "
                    "and stage='quarantine' order by created_at limit 1",
                    (tenant_id, item_id),
                )
                hash_rows = json.loads(fallback["detail_json"]) if fallback and fallback["detail_json"] else []
                versions = [
                    {
                        "version_no": h["version_no"],
                        "content": None,
                        "content_sha256": h["content_sha256"],
                        "decision_used": 0,
                        "created_at": item["created_at"],
                        "superseded_at": None,
                    }
                    for h in hash_rows
                ]
            current = None
            version_view = []
            for version in versions:
                superseded = version["superseded_at"] and parse_iso(version["superseded_at"]) <= moment
                version_view.append({
                    "version_no": version["version_no"],
                    "content_sha256": version["content_sha256"],
                    "decision_used": bool(version["decision_used"]),
                    "current_at_time": not superseded,
                    "content": (
                        version["content"] if content_access and version["content"] is not None
                        else None
                    ),
                    "content_available": version["content"] is not None,
                    "redacted": not content_access or version["content"] is None,
                })
                if not superseded:
                    current = version["version_no"]
            readable.append({
                "item_id": item_id,
                "kind": item["kind"],
                "sensitivity_category": item["sensitivity_category"],
                "source_type": item["source_type"],
                "purpose": item["purpose"],
                "current_version_at_time": current,
                "versions": version_view,
            })
        return {
            "tenant_id": tenant_id,
            "task_id": task_id,
            "as_of": to_iso(moment),
            "content_disclosed": content_access,
            "readable_count": len(readable),
            "readable": readable,
        }

    # ===================================================================
    # 内部查询
    # ===================================================================

    def _get_item(self, tenant_id: str, item_id: str) -> dict[str, Any] | None:
        return self.store.query_one(
            "select * from memory_items where tenant_id=? and item_id=?",
            (tenant_id, item_id),
        )

    def _require_item(self, tenant_id: str, item_id: str) -> dict[str, Any]:
        item = self._get_item(tenant_id, item_id)
        if item is None:
            raise NotFoundError(f"记忆不存在：{item_id}")
        return item

    def _get_version(
        self, tenant_id: str, item_id: str, version_no: int
    ) -> dict[str, Any] | None:
        return self.store.query_one(
            "select * from memory_versions where tenant_id=? and item_id=? and version_no=?",
            (tenant_id, item_id, version_no),
        )

    def _require_version(
        self, tenant_id: str, item_id: str, version_no: int
    ) -> dict[str, Any]:
        version = self._get_version(tenant_id, item_id, version_no)
        if version is None:
            raise NotFoundError(f"记忆版本不存在：{item_id}@v{version_no}")
        return version

    @staticmethod
    def _require_text(value: str, field: str) -> None:
        if not value or not str(value).strip():
            raise ValidationError(f"{field} 不能为空")
