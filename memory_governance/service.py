"""记忆分级治理服务。

覆盖合同规则：
- P-04-01 版本绑定：派生数据必须记录全部直接来源，边绑定到具体版本；
- P-04-02 并发边界：法律保全优先于普通删除，且必须给出原因；
- P-04-03 恢复约束：已影响决策的旧版本只追加更正，不覆盖历史；
- P-04-04 审计结果：重复删除请求复用同一条清除链，不重复创建。
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable

from . import models as m
from .models import Auditor, MemoryItem
from .store import Store

_SOURCE_KINDS = {"upload", "tool", "model", "system"}


class MemoryGovernanceService:
    """记忆摄取、派生、更正、保留、保全、导出、删除与审计的统一入口。"""

    def __init__(self, db_path: str = ":memory:", clock: Callable[[], datetime] | None = None) -> None:
        self.store = Store(db_path)
        self.store.init_schema()
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    # ================================================================ 工具
    def _now(self) -> datetime:
        return m.ensure_aware(self._clock())

    def _resolve_time(self, occurred_at: datetime | None) -> datetime:
        return m.ensure_aware(occurred_at) if occurred_at is not None else self._now()

    def _event(
        self,
        tenant_id: str,
        event_type: str,
        aggregate_id: str,
        actor_id: str,
        payload: dict[str, Any],
        occurred_at: datetime,
    ) -> str:
        return self.store.append_event(tenant_id, event_type, aggregate_id, occurred_at, actor_id, payload)

    @staticmethod
    def _item_payload(item: MemoryItem) -> dict[str, Any]:
        """事件与清单只携带元数据和哈希，绝不携带原文。"""
        return {
            "item_id": item.item_id,
            "version": item.version,
            "category": item.category,
            "purpose": item.purpose,
            "sensitive_categories": list(item.sensitive_categories),
            "content_hash": item.content_hash,
            "decision_influencing": item.decision_influencing,
            "user_retention_days": item.user_retention_days,
            "contract_retention_days": item.contract_retention_days,
            "retain_until": item.retain_until,
            "created_at": item.created_at,
        }

    # ================================================================ 写入
    def ingest(
        self,
        *,
        tenant_id: str,
        item_id: str,
        category: str,
        purpose: str,
        content: str,
        source_kind: str,
        origin: str,
        actor_id: str,
        sensitive_categories: Iterable[str] = (),
        user_retention_days: int | None = None,
        occurred_at: datetime | None = None,
    ) -> MemoryItem:
        """写入原始记忆，同时登记来源、租户、目的、敏感类别与保留期限。"""
        if category not in m.RAW_CATEGORIES:
            raise m.ValidationError(f"ingest 仅接受原始类别 {sorted(m.RAW_CATEGORIES)}，派生数据请用 derive")
        if source_kind not in _SOURCE_KINDS:
            raise m.ValidationError(f"未知来源类型：{source_kind}")
        if user_retention_days is not None and user_retention_days <= 0:
            raise m.ValidationError("用户选择的保留天数必须为正数")
        if self.store.current_item(tenant_id, item_id) is not None:
            raise m.ValidationError(f"业务标识已存在：{item_id}；更正请使用 correct 追加新版本")
        occurred = self._resolve_time(occurred_at)
        categories = tuple(sorted(set(sensitive_categories)))
        contract_days, retain_until = self._compute_retention(
            tenant_id, purpose, category, categories, user_retention_days, occurred
        )
        now_iso = m.to_iso(occurred)
        item = MemoryItem(
            tenant_id=tenant_id,
            item_id=item_id,
            version=1,
            category=category,
            purpose=purpose,
            sensitive_categories=categories,
            content=content,
            content_hash=m.hash_content(content),
            state=m.ACTIVE,
            decision_influencing=False,
            user_retention_days=user_retention_days,
            contract_retention_days=contract_days,
            retain_until=retain_until,
            created_at=now_iso,
            updated_at=now_iso,
        )
        with self.store.transaction():
            self.store.insert_item(item)
            self.store.insert_source(tenant_id, item_id, 1, source_kind, origin, actor_id, now_iso)
            self._event(
                tenant_id,
                m.EVT_MEMORY_INGESTED,
                item_id,
                actor_id,
                self._item_payload(item) | {"source_kind": source_kind, "origin": origin},
                occurred,
            )
        return item

    def derive(
        self,
        *,
        tenant_id: str,
        item_id: str,
        category: str,
        purpose: str,
        content: str,
        sources: Iterable[str | tuple[str, int]],
        actor_id: str,
        relation: str = "summarizes",
        sensitive_categories: Iterable[str] = (),
        user_retention_days: int | None = None,
        occurred_at: datetime | None = None,
    ) -> MemoryItem:
        """写入派生记忆（摘要、索引片段），并绑定全部直接来源的版本。"""
        if category not in m.DERIVED_CATEGORIES:
            raise m.ValidationError(f"derive 仅接受派生类别 {sorted(m.DERIVED_CATEGORIES)}")
        resolved: list[MemoryItem] = []
        for src in sources:
            src_id, src_version = (src, None) if isinstance(src, str) else src
            if src_version is None:
                current = self.store.current_item(tenant_id, src_id)
                if current is None:
                    raise m.NotFoundError(f"来源不存在：{src_id}")
                src_version = current.version
            src_item = self.store.get_item(tenant_id, src_id, src_version)
            if src_item is None:
                raise m.NotFoundError(f"来源不存在：{src_id}#v{src_version}")
            resolved.append(src_item)
        if not resolved:
            raise m.ValidationError("派生数据必须能够追溯到全部直接来源（P-04-01 版本绑定）")
        if self.store.current_item(tenant_id, item_id) is not None:
            raise m.ValidationError(f"业务标识已存在：{item_id}")
        # 派生记忆的敏感类别不得低于任一来源，防止借派生稀释分级
        inherited: set[str] = set()
        for src_item in resolved:
            inherited |= set(src_item.sensitive_categories)
        categories = tuple(sorted(set(sensitive_categories) | inherited))
        occurred = self._resolve_time(occurred_at)
        contract_days, retain_until = self._compute_retention(
            tenant_id, purpose, category, categories, user_retention_days, occurred
        )
        now_iso = m.to_iso(occurred)
        item = MemoryItem(
            tenant_id=tenant_id,
            item_id=item_id,
            version=1,
            category=category,
            purpose=purpose,
            sensitive_categories=categories,
            content=content,
            content_hash=m.hash_content(content),
            state=m.ACTIVE,
            decision_influencing=False,
            user_retention_days=user_retention_days,
            contract_retention_days=contract_days,
            retain_until=retain_until,
            created_at=now_iso,
            updated_at=now_iso,
        )
        with self.store.transaction():
            self.store.insert_item(item)
            edges = []
            for src_item in resolved:
                self.store.insert_edge(
                    tenant_id, src_item.item_id, src_item.version, item_id, 1, relation, now_iso
                )
                edges.append(
                    {"item_id": src_item.item_id, "version": src_item.version, "relation": relation}
                )
            self._event(
                tenant_id,
                m.EVT_MEMORY_DERIVED,
                item_id,
                actor_id,
                self._item_payload(item) | {"sources": edges},
                occurred,
            )
        return item

    def correct(
        self,
        *,
        tenant_id: str,
        item_id: str,
        content: str,
        actor_id: str,
        reason: str,
        occurred_at: datetime | None = None,
    ) -> MemoryItem:
        """追加更正版本：旧版本转入 superseded 并完整保留，绝不覆盖。"""
        current = self.store.current_item(tenant_id, item_id)
        if current is None:
            raise m.NotFoundError(f"记忆不存在：{item_id}")
        if current.state == m.HELD:
            raise m.StateConflictError("法律保全期间节点已冻结，不能更正")
        if current.state in (m.PENDING_ERASURE, m.ERASING, m.ERASED):
            raise m.StateConflictError(f"记忆处于删除流程（{current.state}），不能更正")
        if not reason or not reason.strip():
            raise m.ValidationError("更正必须说明原因")
        occurred = self._resolve_time(occurred_at)
        now_iso = m.to_iso(occurred)
        contract_days, retain_until = self._compute_retention(
            tenant_id,
            current.purpose,
            current.category,
            current.sensitive_categories,
            current.user_retention_days,
            occurred,
        )
        new_item = MemoryItem(
            tenant_id=tenant_id,
            item_id=item_id,
            version=current.version + 1,
            category=current.category,
            purpose=current.purpose,
            sensitive_categories=current.sensitive_categories,
            content=content,
            content_hash=m.hash_content(content),
            state=m.ACTIVE,
            decision_influencing=False,
            user_retention_days=current.user_retention_days,
            contract_retention_days=contract_days,
            retain_until=retain_until,
            created_at=now_iso,
            updated_at=now_iso,
        )
        with self.store.transaction():
            # P-04-03：旧版本即使影响过决策也只标记 superseded，行与原文保留
            self.store.update_item_state(tenant_id, item_id, current.version, m.SUPERSEDED, now_iso)
            self.store.insert_item(new_item)
            self._event(
                tenant_id,
                m.EVT_MEMORY_CORRECTED,
                item_id,
                actor_id,
                {
                    "item_id": item_id,
                    "old_version": current.version,
                    "reason": reason,
                    "old_content_hash": current.content_hash,
                    "old_decision_influencing": current.decision_influencing,
                    "new_item": self._item_payload(new_item),
                },
                occurred,
            )
        return new_item

    def stale_derivatives(self, *, tenant_id: str, item_id: str) -> list[dict[str, Any]]:
        """仍绑定在旧版本上的派生记忆，提示需要重新派生。"""
        current = self.store.current_item(tenant_id, item_id)
        if current is None:
            raise m.NotFoundError(f"记忆不存在：{item_id}")
        stale = []
        for edge in self.store.edges_from(tenant_id, item_id):
            if edge["src_version"] < current.version:
                dst = self.store.get_item(tenant_id, edge["dst_item_id"], edge["dst_version"])
                stale.append(
                    {
                        "item_id": edge["dst_item_id"],
                        "version": edge["dst_version"],
                        "category": dst.category if dst else None,
                        "pinned_source_version": edge["src_version"],
                        "current_source_version": current.version,
                    }
                )
        return stale

    # ================================================================ 保留
    def publish_retention_rule(
        self,
        *,
        tenant_id: str,
        rule_id: str,
        requirement: str,
        retention_days: int | None,
        actor_id: str,
        scope: dict[str, Any] | None = None,
        occurred_at: datetime | None = None,
    ) -> dict[str, Any]:
        """发布保留规则新版本；已发布版本永不覆盖（version_policy）。"""
        if requirement not in m.REQUIREMENTS:
            raise m.ValidationError(f"未知要求类型：{requirement}")
        if retention_days is not None and retention_days <= 0:
            raise m.ValidationError("保留天数必须为正数或 None（无限期）")
        occurred = self._resolve_time(occurred_at)
        version = self.store.latest_rule_version(tenant_id, rule_id) + 1
        scope = scope or {}
        with self.store.transaction():
            self.store.insert_rule(
                tenant_id, rule_id, version, requirement, retention_days, scope, actor_id, m.to_iso(occurred)
            )
            self._event(
                tenant_id,
                m.EVT_RULE_PUBLISHED,
                rule_id,
                actor_id,
                {
                    "rule_id": rule_id,
                    "version": version,
                    "requirement": requirement,
                    "retention_days": retention_days,
                    "scope": scope,
                },
                occurred,
            )
            self._recompute_tenant_retention(tenant_id, occurred)
        return {"rule_id": rule_id, "version": version, "requirement": requirement, "retention_days": retention_days}

    def _matching_rules(
        self, tenant_id: str, purpose: str, category: str, sensitive_categories: tuple[str, ...]
    ) -> list[dict[str, Any]]:
        matched = []
        for rule in self.store.latest_rules(tenant_id):
            scope = json.loads(rule["scope_json"])
            if scope.get("purpose") not in (None, purpose):
                continue
            if scope.get("category") not in (None, category):
                continue
            sensitive = scope.get("sensitive_category")
            if sensitive is not None and sensitive not in sensitive_categories:
                continue
            matched.append(rule)
        return matched

    def _compute_retention(
        self,
        tenant_id: str,
        purpose: str,
        category: str,
        sensitive_categories: tuple[str, ...],
        user_days: int | None,
        base: datetime,
    ) -> tuple[int | None, str | None]:
        """保留期限 = 用户选择与合同/法规要求的并集取最长；无限期规则优先。"""
        days: list[int] = []
        indefinite = False
        for rule in self._matching_rules(tenant_id, purpose, category, sensitive_categories):
            if rule["retention_days"] is None:
                indefinite = True
            else:
                days.append(rule["retention_days"])
        contract_days = None if indefinite else (max(days) if days else None)
        if indefinite:
            effective_days = None
        else:
            candidates = [d for d in (user_days, contract_days) if d is not None]
            effective_days = max(candidates) if candidates else None
        retain_until = m.to_iso(base + timedelta(days=effective_days)) if effective_days is not None else None
        return contract_days, retain_until

    def _recompute_tenant_retention(self, tenant_id: str, at: datetime) -> None:
        now_iso = m.to_iso(at)
        for item in self.store.list_items(tenant_id):
            if item.state == m.ERASED:
                continue
            contract_days, retain_until = self._compute_retention(
                tenant_id,
                item.purpose,
                item.category,
                item.sensitive_categories,
                item.user_retention_days,
                m.parse_iso(item.created_at),
            )
            self.store.update_item_retention(
                tenant_id, item.item_id, item.version, contract_days, retain_until, now_iso
            )

    def effective_retention(self, *, tenant_id: str, item_id: str) -> dict[str, Any]:
        """保留期限由用户选择、合同要求与法律保全共同决定；保全期间不到期。"""
        item = self.store.current_item(tenant_id, item_id)
        if item is None:
            raise m.NotFoundError(f"记忆不存在：{item_id}")
        holds = self.store.active_holds_for_item(tenant_id, item_id)
        basis: list[str] = []
        if item.user_retention_days is not None:
            basis.append(f"用户选择保留 {item.user_retention_days} 天")
        if item.contract_retention_days is not None:
            basis.append(f"合同/法规要求保留 {item.contract_retention_days} 天")
        hold_reasons = [h["reason"] for h in holds]
        for reason in hold_reasons:
            basis.append(f"法律保全中：{reason}")
        return {
            "item_id": item_id,
            "version": item.version,
            "user_retention_days": item.user_retention_days,
            "contract_retention_days": item.contract_retention_days,
            "hold_active": bool(holds),
            "hold_reasons": hold_reasons,
            "retain_until": None if holds else item.retain_until,
            "basis": basis,
        }

    def expired_items(self, *, tenant_id: str, at: datetime | None = None) -> list[MemoryItem]:
        """已到期且未被保全的当前版本，供生命周期巡检使用。"""
        moment = m.to_iso(self._resolve_time(at))
        expired = []
        seen: set[str] = set()
        for item in self.store.list_items(tenant_id):
            if item.item_id in seen:
                continue
            seen.add(item.item_id)
            current = self.store.current_item(tenant_id, item.item_id)
            if current is None or current.state != m.ACTIVE or current.retain_until is None:
                continue
            if current.retain_until <= moment and not self.store.active_holds_for_item(tenant_id, item.item_id):
                expired.append(current)
        return expired

    # ================================================================ 法律保全
    def apply_hold(
        self,
        *,
        tenant_id: str,
        hold_id: str,
        item_ids: Iterable[str],
        reason: str,
        actor_id: str,
        occurred_at: datetime | None = None,
    ) -> dict[str, Any]:
        """冻结节点并说明原因；保全优先于进行中的删除（P-04-02）。"""
        if not reason or not reason.strip():
            raise m.ValidationError("法律保全必须给出原因（P-04-02 并发边界）")
        if self.store.get_hold(tenant_id, hold_id) is not None:
            raise m.ValidationError(f"保全标识已存在：{hold_id}")
        item_ids = sorted(set(item_ids))
        for item_id in item_ids:
            if not self.store.list_versions(tenant_id, item_id):
                raise m.NotFoundError(f"记忆不存在：{item_id}")
        occurred = self._resolve_time(occurred_at)
        now_iso = m.to_iso(occurred)
        affected_requests: set[str] = set()
        with self.store.transaction():
            self.store.insert_hold(tenant_id, hold_id, reason, actor_id, now_iso)
            for item_id in item_ids:
                self.store.insert_hold_link(tenant_id, hold_id, item_id, now_iso)
                for version in self.store.list_versions(tenant_id, item_id):
                    if version.state in (m.ACTIVE, m.SUPERSEDED):
                        self.store.update_item_state(tenant_id, item_id, version.version, m.HELD, now_iso)
                # 冻结该条目尚未执行的删除步骤
                for step in self.store.actionable_steps_for_item(tenant_id, item_id):
                    self.store.skip_step(
                        tenant_id, step["request_id"], step["step_id"], f"法律保全冻结：{reason}", now_iso
                    )
                    affected_requests.add(step["request_id"])
                    self._event(
                        tenant_id,
                        m.EVT_ERASURE_STEP_COMPLETED,
                        item_id,
                        actor_id,
                        {
                            "request_id": step["request_id"],
                            "step_id": step["step_id"],
                            "item_id": item_id,
                            "stage": step["stage"],
                            "result": m.STEP_SKIPPED,
                            "reason": f"法律保全冻结：{reason}",
                        },
                        occurred,
                    )
            self._event(
                tenant_id,
                m.EVT_HOLD_APPLIED,
                hold_id,
                actor_id,
                {"hold_id": hold_id, "item_ids": item_ids, "reason": reason},
                occurred,
            )
        for request_id in sorted(affected_requests):
            self._finalize_request(tenant_id, request_id, occurred)
        return self.store.get_hold(tenant_id, hold_id)

    def release_hold(
        self,
        *,
        tenant_id: str,
        hold_id: str,
        actor_id: str,
        reason: str,
        occurred_at: datetime | None = None,
    ) -> dict[str, Any]:
        """解除保全：被冻结的条目恢复可读，删除需重新发起。"""
        hold = self.store.get_hold(tenant_id, hold_id)
        if hold is None:
            raise m.NotFoundError(f"保全不存在：{hold_id}")
        if hold["released_at"] is not None:
            raise m.StateConflictError(f"保全已解除：{hold_id}")
        occurred = self._resolve_time(occurred_at)
        now_iso = m.to_iso(occurred)
        links = self.store.hold_links(tenant_id, hold_id)
        with self.store.transaction():
            self.store.release_hold(tenant_id, hold_id, actor_id, now_iso, reason)
            for link in links:
                self._restore_versions(tenant_id, link["item_id"], now_iso)
            self._event(
                tenant_id,
                m.EVT_HOLD_RELEASED,
                hold_id,
                actor_id,
                {"hold_id": hold_id, "item_ids": [l["item_id"] for l in links], "release_reason": reason},
                occurred,
            )
        return self.store.get_hold(tenant_id, hold_id)

    def _restore_versions(self, tenant_id: str, item_id: str, now_iso: str) -> None:
        """解除冻结：隔离中的原文取回，状态按版本新旧恢复为 active/superseded。"""
        versions = self.store.list_versions(tenant_id, item_id)
        latest = versions[-1].version if versions else 0
        quarantined = {row["version"]: row["content"] for row in self.store.quarantine_for(tenant_id, item_id)}
        for version in versions:
            if version.state not in (m.HELD, m.PENDING_ERASURE, m.ERASING):
                continue
            if version.version in quarantined:
                self.store.restore_item_content(
                    tenant_id, item_id, version.version, quarantined[version.version], now_iso
                )
            restored = m.ACTIVE if version.version == latest else m.SUPERSEDED
            self.store.update_item_state(tenant_id, item_id, version.version, restored, now_iso)
        if quarantined:
            self.store.delete_quarantine(tenant_id, item_id)

    # ================================================================ 导出
    def request_export(
        self,
        *,
        tenant_id: str,
        requester_id: str,
        idempotency_key: str,
        item_ids: Iterable[str] | None = None,
        occurred_at: datetime | None = None,
    ) -> dict[str, Any]:
        """登记导出请求；相同幂等键复用同一请求。"""
        existing = self.store.export_by_key(tenant_id, idempotency_key)
        if existing is not None:
            return self._export_view(existing, idempotent_replay=True)
        occurred = self._resolve_time(occurred_at)
        closure = self._closure(tenant_id, item_ids)
        export_id = "exp-" + uuid.uuid4().hex[:12]
        with self.store.transaction():
            self.store.insert_export(
                tenant_id,
                export_id,
                idempotency_key,
                requester_id,
                {"item_ids": sorted(closure)},
                m.to_iso(occurred),
            )
            self._event(
                tenant_id,
                m.EVT_EXPORT_REQUESTED,
                export_id,
                requester_id,
                {"export_id": export_id, "idempotency_key": idempotency_key, "item_ids": sorted(closure)},
                occurred,
            )
        return self._export_view(self.store.get_export(tenant_id, export_id))

    def build_export(
        self, *, tenant_id: str, export_id: str, occurred_at: datetime | None = None
    ) -> dict[str, Any]:
        """沿依赖图导出本体、派生、来源与任务引用；面向数据主体本人，含原文。"""
        export = self.store.get_export(tenant_id, export_id)
        if export is None:
            raise m.NotFoundError(f"导出请求不存在：{export_id}")
        occurred = self._resolve_time(occurred_at)
        now_iso = m.to_iso(occurred)
        item_ids = json.loads(export["scope_json"])["item_ids"]
        items: list[dict[str, Any]] = []
        with self.store.transaction():
            currents = []
            for item_id in item_ids:
                current = self.store.current_item(tenant_id, item_id)
                if current is not None and current.state == m.ACTIVE:
                    self.store.update_item_state(tenant_id, item_id, current.version, m.EXPORTING, now_iso)
                    currents.append(current)
            for item_id in item_ids:
                for version in self.store.list_versions(tenant_id, item_id):
                    entry = self._item_payload(version) | {"state": version.state}
                    if version.content is None:
                        entry["content"] = None
                        entry["unavailable_reason"] = "erased"
                    else:
                        entry["content"] = version.content
                        entry["unavailable_reason"] = None
                    items.append(entry)
            for current in currents:
                self.store.update_item_state(tenant_id, current.item_id, current.version, m.ACTIVE, now_iso)
            if export["state"] != "completed":
                self.store.complete_export(tenant_id, export_id, now_iso)
                self._event(
                    tenant_id,
                    m.EVT_EXPORT_COMPLETED,
                    export_id,
                    export["requester_id"],
                    {
                        "export_id": export_id,
                        "counts": {"items": len(item_ids), "versions": len(items)},
                    },
                    occurred,
                )
        sources = [row for item_id in item_ids for row in self.store.sources_for(tenant_id, item_id)]
        task_refs = [row for item_id in item_ids for row in self.store.task_refs_for_item(tenant_id, item_id)]
        return {
            "export_id": export_id,
            "tenant_id": tenant_id,
            "generated_at": now_iso,
            "manifest": {
                "items_total": len(item_ids),
                "versions_total": len(items),
                "unavailable": sorted({e["item_id"] for e in items if e["unavailable_reason"]}),
            },
            "items": items,
            "derivation_edges": self.store.edges_among(tenant_id, item_ids),
            "source_records": sources,
            "task_references": task_refs,
        }

    def _export_view(self, export: dict[str, Any], idempotent_replay: bool = False) -> dict[str, Any]:
        return {
            "export_id": export["export_id"],
            "tenant_id": export["tenant_id"],
            "state": export["state"],
            "idempotency_key": export["idempotency_key"],
            "item_ids": json.loads(export["scope_json"])["item_ids"],
            "idempotent_replay": idempotent_replay,
        }

    # ================================================================ 删除
    def request_erasure(
        self,
        *,
        tenant_id: str,
        requester_id: str,
        idempotency_key: str,
        item_ids: Iterable[str] | None = None,
        occurred_at: datetime | None = None,
    ) -> dict[str, Any]:
        """登记删除请求并生成清除链；相同幂等键复用同一请求（P-04-04）。"""
        existing = self.store.erasure_by_key(tenant_id, idempotency_key)
        if existing is not None:
            return self._request_view(existing, idempotent_replay=True)
        occurred = self._resolve_time(occurred_at)
        now_iso = m.to_iso(occurred)
        closure = self._closure(tenant_id, item_ids)
        request_id = "erq-" + uuid.uuid4().hex[:12]
        steps: list[tuple[str, str, str, str | None]] = []  # (item_id, stage, state, reason)
        for item_id, _depth in sorted(closure.items(), key=lambda kv: (-kv[1], kv[0])):
            current = self.store.current_item(tenant_id, item_id)
            if current is None or current.state == m.ERASED:
                continue
            holds = self.store.active_holds_for_item(tenant_id, item_id)
            if holds:
                reason = "法律保全冻结：" + "；".join(h["reason"] for h in holds)
                steps.append((item_id, m.STAGE_FREEZE, m.STEP_SKIPPED, reason))
            else:
                for stage in m.ERASURE_STAGES:
                    steps.append((item_id, stage, m.STEP_PENDING, None))
        with self.store.transaction():
            self.store.insert_erasure_request(
                tenant_id,
                request_id,
                idempotency_key,
                requester_id,
                {"item_ids": sorted(closure)},
                m.REQUEST_PENDING,
                now_iso,
            )
            for seq, (item_id, stage, state, reason) in enumerate(steps):
                self.store.insert_step(
                    tenant_id, request_id, f"{request_id}-{seq:04d}", seq, item_id, stage, state, reason, now_iso
                )
            self._event(
                tenant_id,
                m.EVT_ERASURE_REQUESTED,
                request_id,
                requester_id,
                {
                    "request_id": request_id,
                    "idempotency_key": idempotency_key,
                    "item_ids": sorted(closure),
                    "steps": len(steps),
                },
                occurred,
            )
        return self._request_view(self.store.get_erasure_request(tenant_id, request_id))

    def _closure(self, tenant_id: str, item_ids: Iterable[str] | None) -> dict[str, int]:
        """依赖图闭包：本体 + 下游派生（摘要、索引片段等），值为下游深度。"""
        if item_ids is None:
            roots = sorted({item.item_id for item in self.store.list_items(tenant_id)})
        else:
            roots = sorted(set(item_ids))
        closure: dict[str, int] = {}
        queue: list[tuple[str, int]] = []
        for item_id in roots:
            if self.store.current_item(tenant_id, item_id) is None:
                raise m.NotFoundError(f"记忆不存在：{item_id}")
            closure[item_id] = 0
            queue.append((item_id, 0))
        while queue:
            current_id, depth = queue.pop(0)
            for edge in self.store.edges_from(tenant_id, current_id):
                dst = edge["dst_item_id"]
                if dst not in closure or closure[dst] < depth + 1:
                    closure[dst] = depth + 1
                    queue.append((dst, depth + 1))
        return closure

    def run_erasure(
        self,
        *,
        tenant_id: str,
        request_id: str,
        max_steps: int | None = None,
        step_hook: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """执行清除链；max_steps 用于模拟中断，剩余步骤留待恢复。"""
        return self._execute(tenant_id, request_id, max_steps=max_steps, step_hook=step_hook)

    def resume_erasure(
        self,
        *,
        tenant_id: str,
        request_id: str,
        max_steps: int | None = None,
        step_hook: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """中断或失败后继续执行未完成步骤；已完成步骤自动跳过。"""
        return self._execute(tenant_id, request_id, max_steps=max_steps, step_hook=step_hook)

    def _execute(
        self,
        tenant_id: str,
        request_id: str,
        *,
        max_steps: int | None,
        step_hook: Callable[[dict[str, Any]], None] | None,
    ) -> dict[str, Any]:
        request = self.store.get_erasure_request(tenant_id, request_id)
        if request is None:
            raise m.NotFoundError(f"删除请求不存在：{request_id}")
        if request["state"] in (m.REQUEST_COMPLETED, m.REQUEST_COMPLETED_WITH_HOLDS):
            return self._request_view(request)  # 幂等：终态请求直接返回
        if request["state"] == m.REQUEST_PENDING:
            with self.store.transaction():
                self.store.update_erasure_state(tenant_id, request_id, m.REQUEST_RUNNING, m.to_iso(self._now()))
        executed = 0
        while max_steps is None or executed < max_steps:
            step = self.store.next_actionable_step(tenant_id, request_id)
            if step is None:
                break
            try:
                if step_hook is not None:
                    step_hook(step)
                self._execute_step(tenant_id, request_id, step)
            except Exception as exc:  # 步骤失败：记录后可由 resume 继续
                now_iso = m.to_iso(self._now())
                with self.store.transaction():
                    self.store.update_step(
                        tenant_id,
                        request_id,
                        step["step_id"],
                        state=m.STEP_FAILED,
                        updated_at=now_iso,
                        last_error=str(exc),
                    )
                    self.store.update_erasure_state(tenant_id, request_id, m.REQUEST_FAILED, now_iso)
                raise m.StateConflictError(f"删除步骤 {step['step_id']} 失败：{exc}") from exc
            executed += 1
        return self._finalize_request(tenant_id, request_id, self._now())

    def _execute_step(self, tenant_id: str, request_id: str, step: dict[str, Any]) -> None:
        occurred = self._now()
        now_iso = m.to_iso(occurred)
        item_id = step["item_id"]
        with self.store.transaction():
            holds = self.store.active_holds_for_item(tenant_id, item_id)
            if holds:
                # P-04-02：执行期间出现保全，冻结该条目剩余全部步骤
                reason = "法律保全冻结：" + "；".join(h["reason"] for h in holds)
                for rest in self.store.actionable_steps_for_item(tenant_id, item_id):
                    if rest["request_id"] == request_id:
                        self.store.skip_step(tenant_id, request_id, rest["step_id"], reason, now_iso)
                self._event(
                    tenant_id,
                    m.EVT_ERASURE_STEP_COMPLETED,
                    item_id,
                    "system",
                    {
                        "request_id": request_id,
                        "step_id": step["step_id"],
                        "item_id": item_id,
                        "stage": step["stage"],
                        "result": m.STEP_SKIPPED,
                        "reason": reason,
                    },
                    occurred,
                )
                return
            stage = step["stage"]
            detail: dict[str, Any] = {}
            if stage == m.STAGE_REVOKE:
                for version in self.store.list_versions(tenant_id, item_id):
                    if version.state in (m.ACTIVE, m.SUPERSEDED, m.EXPORTING):
                        self.store.update_item_state(
                            tenant_id, item_id, version.version, m.PENDING_ERASURE, now_iso
                        )
            elif stage == m.STAGE_QUARANTINE:
                for version in self.store.list_versions(tenant_id, item_id):
                    if version.content is not None:
                        self.store.insert_quarantine(
                            tenant_id, item_id, version.version, version.content, request_id, now_iso
                        )
                        self.store.clear_item_content(tenant_id, item_id, version.version, now_iso)
                    if version.state == m.PENDING_ERASURE:
                        self.store.update_item_state(tenant_id, item_id, version.version, m.ERASING, now_iso)
            elif stage == m.STAGE_PURGE:
                removed = self.store.delete_quarantine(tenant_id, item_id)
                refs_removed = self.store.delete_task_refs_for_item(tenant_id, item_id)
                for version in self.store.list_versions(tenant_id, item_id):
                    if version.state in (m.ERASING, m.PENDING_ERASURE):
                        self.store.update_item_state(tenant_id, item_id, version.version, m.ERASED, now_iso)
                detail = {"quarantine_removed": removed, "task_references_removed": refs_removed}
            else:
                raise m.ValidationError(f"未知删除阶段：{stage}")
            self.store.update_step(
                tenant_id,
                request_id,
                step["step_id"],
                state=m.STEP_DONE,
                updated_at=now_iso,
                detail_json=json.dumps(detail, ensure_ascii=False) if detail else None,
                completed_at=now_iso,
            )
            self._event(
                tenant_id,
                m.EVT_ERASURE_STEP_COMPLETED,
                item_id,
                "system",
                {
                    "request_id": request_id,
                    "step_id": step["step_id"],
                    "item_id": item_id,
                    "stage": stage,
                    "result": m.STEP_DONE,
                },
                occurred,
            )

    def _finalize_request(self, tenant_id: str, request_id: str, at: datetime) -> dict[str, Any]:
        request = self.store.get_erasure_request(tenant_id, request_id)
        steps = self.store.steps_for_request(tenant_id, request_id)
        states = {step["state"] for step in steps}
        now_iso = m.to_iso(at)
        if states & {m.STEP_PENDING, m.STEP_FAILED}:
            new_state = m.REQUEST_FAILED if m.STEP_FAILED in states else m.REQUEST_RUNNING
        elif m.STEP_SKIPPED in states:
            new_state = m.REQUEST_COMPLETED_WITH_HOLDS
        else:
            new_state = m.REQUEST_COMPLETED
        terminal = new_state in (m.REQUEST_COMPLETED, m.REQUEST_COMPLETED_WITH_HOLDS)
        if request["state"] != new_state:
            with self.store.transaction():
                self.store.update_erasure_state(tenant_id, request_id, new_state, now_iso)
                if terminal:
                    self._event(
                        tenant_id,
                        m.EVT_ERASURE_COMPLETED,
                        request_id,
                        "system",
                        {
                            "request_id": request_id,
                            "state": new_state,
                            "counts": {
                                "steps_done": sum(1 for s in steps if s["state"] == m.STEP_DONE),
                                "steps_skipped": sum(1 for s in steps if s["state"] == m.STEP_SKIPPED),
                                "steps_cancelled": sum(1 for s in steps if s["state"] == m.STEP_CANCELLED),
                            },
                        },
                        at,
                    )
        return self._request_view(self.store.get_erasure_request(tenant_id, request_id))

    def restore_quarantined(
        self,
        *,
        tenant_id: str,
        item_id: str,
        actor_id: str,
        occurred_at: datetime | None = None,
    ) -> MemoryItem:
        """隔离阶段可恢复：取回原文并取消该条目剩余删除步骤。"""
        current = self.store.current_item(tenant_id, item_id)
        if current is None:
            raise m.NotFoundError(f"记忆不存在：{item_id}")
        if current.state != m.ERASING:
            raise m.StateConflictError("仅隔离（erasing）阶段可以恢复，清除完成后不可恢复")
        occurred = self._resolve_time(occurred_at)
        now_iso = m.to_iso(occurred)
        affected_requests: set[str] = set()
        with self.store.transaction():
            self._restore_versions(tenant_id, item_id, now_iso)
            for step in self.store.actionable_steps_for_item(tenant_id, item_id):
                self.store.update_step(
                    tenant_id,
                    step["request_id"],
                    step["step_id"],
                    state=m.STEP_CANCELLED,
                    updated_at=now_iso,
                    reason="隔离恢复，取消剩余清除",
                )
                affected_requests.add(step["request_id"])
                self._event(
                    tenant_id,
                    m.EVT_ERASURE_STEP_COMPLETED,
                    item_id,
                    actor_id,
                    {
                        "request_id": step["request_id"],
                        "step_id": step["step_id"],
                        "item_id": item_id,
                        "stage": step["stage"],
                        "result": m.STEP_CANCELLED,
                        "reason": "隔离恢复，取消剩余清除",
                    },
                    occurred,
                )
        for request_id in sorted(affected_requests):
            self._finalize_request(tenant_id, request_id, occurred)
        restored = self.store.current_item(tenant_id, item_id)
        assert restored is not None
        return restored

    def get_erasure_request(self, *, tenant_id: str, request_id: str) -> dict[str, Any]:
        request = self.store.get_erasure_request(tenant_id, request_id)
        if request is None:
            raise m.NotFoundError(f"删除请求不存在：{request_id}")
        return self._request_view(request)

    def _request_view(self, request: dict[str, Any], idempotent_replay: bool = False) -> dict[str, Any]:
        steps = self.store.steps_for_request(request["tenant_id"], request["request_id"])
        counts: dict[str, int] = {}
        for step in steps:
            counts[step["state"]] = counts.get(step["state"], 0) + 1
        return {
            "request_id": request["request_id"],
            "tenant_id": request["tenant_id"],
            "state": request["state"],
            "idempotency_key": request["idempotency_key"],
            "requester_id": request["requester_id"],
            "item_ids": json.loads(request["scope_json"])["item_ids"],
            "created_at": request["created_at"],
            "step_counts": counts,
            "idempotent_replay": idempotent_replay,
        }

    def erasure_proof(self, *, tenant_id: str, request_id: str) -> dict[str, Any]:
        """删除证明清单：只有哈希、状态与计数，不含任何原文。"""
        request = self.store.get_erasure_request(tenant_id, request_id)
        if request is None:
            raise m.NotFoundError(f"删除请求不存在：{request_id}")
        steps = self.store.steps_for_request(tenant_id, request_id)
        item_ids = json.loads(request["scope_json"])["item_ids"]
        entries = []
        refs_removed = 0
        for item_id in item_ids:
            versions = self.store.list_versions(tenant_id, item_id)
            item_steps = [s for s in steps if s["item_id"] == item_id]
            done = [s["stage"] for s in item_steps if s["state"] == m.STEP_DONE]
            skipped = [s for s in item_steps if s["state"] == m.STEP_SKIPPED]
            for step in item_steps:
                if step["detail_json"]:
                    refs_removed += json.loads(step["detail_json"]).get("task_references_removed", 0)
            entries.append(
                {
                    "item_id": item_id,
                    "versions": [v.version for v in versions],
                    "category": versions[-1].category if versions else None,
                    "content_hashes": {str(v.version): v.content_hash for v in versions},
                    "final_state": versions[-1].state if versions else "unknown",
                    "stages_completed": done,
                    "hold_reason": skipped[0]["reason"] if skipped else None,
                }
            )
        counts = {
            "items_total": len(item_ids),
            "items_erased": sum(1 for e in entries if e["final_state"] == m.ERASED),
            "items_frozen": sum(1 for e in entries if e["hold_reason"]),
            "steps_done": sum(1 for s in steps if s["state"] == m.STEP_DONE),
            "steps_skipped": sum(1 for s in steps if s["state"] == m.STEP_SKIPPED),
            "steps_cancelled": sum(1 for s in steps if s["state"] == m.STEP_CANCELLED),
            "steps_pending": sum(1 for s in steps if s["state"] in (m.STEP_PENDING, m.STEP_FAILED)),
            "task_references_removed": refs_removed,
        }
        return {
            "request_id": request_id,
            "tenant_id": tenant_id,
            "state": request["state"],
            "idempotency_key": request["idempotency_key"],
            "generated_at": m.to_iso(self._now()),
            "counts": counts,
            "entries": entries,
            "content_included": False,
        }

    # ================================================================ 任务授权与读取
    def grant_task(
        self,
        *,
        tenant_id: str,
        task_id: str,
        purposes: Iterable[str],
        sensitive_categories: Iterable[str] = (),
        actor_id: str,
        occurred_at: datetime | None = None,
    ) -> dict[str, Any]:
        """登记任务可读范围：目的白名单 + 可触达的敏感类别。"""
        occurred = self._resolve_time(occurred_at)
        purposes = sorted(set(purposes))
        categories = sorted(set(sensitive_categories))
        with self.store.transaction():
            self.store.put_grant(tenant_id, task_id, purposes, categories, actor_id, m.to_iso(occurred))
            self._event(
                tenant_id,
                m.EVT_TASK_GRANTED,
                task_id,
                actor_id,
                {"task_id": task_id, "purposes": purposes, "sensitive_categories": categories},
                occurred,
            )
        return {"task_id": task_id, "purposes": purposes, "sensitive_categories": categories}

    def revoke_task(
        self, *, tenant_id: str, task_id: str, actor_id: str, occurred_at: datetime | None = None
    ) -> None:
        grant = self.store.get_grant(tenant_id, task_id)
        if grant is None:
            raise m.NotFoundError(f"任务授权不存在：{task_id}")
        occurred = self._resolve_time(occurred_at)
        with self.store.transaction():
            self.store.revoke_grant(tenant_id, task_id, m.to_iso(occurred))
            self._event(tenant_id, m.EVT_TASK_REVOKED, task_id, actor_id, {"task_id": task_id}, occurred)

    def record_task_access(
        self,
        *,
        tenant_id: str,
        task_id: str,
        item_id: str,
        access_kind: str = m.ACCESS_READ,
        actor_id: str,
        occurred_at: datetime | None = None,
    ) -> dict[str, Any]:
        """登记一次任务读取；decision 类型会把该版本标记为"曾影响决策"。"""
        grant = self.store.get_grant(tenant_id, task_id)
        if grant is None or grant["revoked_at"] is not None:
            raise m.PermissionDeniedError(f"任务无有效授权：{task_id}")
        item = self.store.current_item(tenant_id, item_id)
        if item is None:
            raise m.NotFoundError(f"记忆不存在：{item_id}")
        if item.purpose not in json.loads(grant["purposes_json"]):
            raise m.PermissionDeniedError(f"目的 {item.purpose} 不在任务授权范围内")
        allowed = set(json.loads(grant["sensitive_categories_json"]))
        if not set(item.sensitive_categories) <= allowed:
            raise m.PermissionDeniedError("敏感类别超出任务授权范围")
        if item.state not in m.READABLE_STATES:
            raise m.StateConflictError(f"状态 {item.state} 不可读取")
        if access_kind not in (m.ACCESS_READ, m.ACCESS_DECISION):
            raise m.ValidationError(f"未知访问类型：{access_kind}")
        occurred = self._resolve_time(occurred_at)
        now_iso = m.to_iso(occurred)
        with self.store.transaction():
            self.store.insert_task_reference(
                tenant_id, task_id, item_id, item.version, access_kind, now_iso
            )
            if access_kind == m.ACCESS_DECISION:
                self.store.set_decision_influencing(tenant_id, item_id, item.version, now_iso)
            self._event(
                tenant_id,
                m.EVT_TASK_ACCESS_RECORDED,
                item_id,
                actor_id,
                {"task_id": task_id, "item_id": item_id, "version": item.version, "access_kind": access_kind},
                occurred,
            )
        return {"task_id": task_id, "item_id": item_id, "version": item.version, "access_kind": access_kind}

    # ================================================================ 审计
    def audit_read_set(
        self, *, tenant_id: str, task_id: str, as_of: datetime, auditor: Auditor
    ) -> dict[str, Any]:
        """按历史时点还原任务当时可读的记忆集合；越权内容一律脱敏。"""
        moment = m.to_iso(m.ensure_aware(as_of))
        if tenant_id not in auditor.tenant_ids:
            raise m.PermissionDeniedError(f"审计员 {auditor.auditor_id} 无权访问租户 {tenant_id}")
        items, grants = self._replay(tenant_id, moment)
        grant = grants.get(task_id)
        entries: list[dict[str, Any]] = []
        if grant is not None:
            latest: dict[str, tuple[int, dict[str, Any]]] = {}
            for (item_id, version), record in items.items():
                if item_id not in latest or version > latest[item_id][0]:
                    latest[item_id] = (version, record)
            for item_id, (version, record) in sorted(latest.items()):
                if record["state"] not in m.READABLE_STATES:
                    continue
                if record["purpose"] not in grant["purposes"]:
                    continue
                if not set(record["sensitive_categories"]) <= set(grant["sensitive_categories"]):
                    continue
                entry = {
                    "item_id": item_id,
                    "version": version,
                    "category": record["category"],
                    "purpose": record["purpose"],
                    "sensitive_categories": record["sensitive_categories"],
                    "state": record["state"],
                    "content_hash": record["content_hash"],
                    "decision_influencing": record["decision_influencing"],
                }
                live = self.store.get_item(tenant_id, item_id, version)
                authorized = auditor.can_view_content and record["purpose"] in auditor.purposes
                if authorized and live is not None and live.content is not None:
                    entry["content"] = live.content
                    entry["redacted"] = False
                else:
                    entry["content"] = None
                    entry["redacted"] = True
                    entry["redaction_reason"] = (
                        "原文已清除" if (live is None or live.content is None) else "审计员无权查看原文"
                    )
                entries.append(entry)
        return {
            "tenant_id": tenant_id,
            "task_id": task_id,
            "as_of": moment,
            "readable_count": len(entries),
            "entries": entries,
        }

    def _replay(
        self, tenant_id: str, as_of_iso: str
    ) -> tuple[dict[tuple[str, int], dict[str, Any]], dict[str, dict[str, Any]]]:
        """重放截至时点的事件，重建各版本状态与任务授权。"""
        items: dict[tuple[str, int], dict[str, Any]] = {}
        grants: dict[str, dict[str, Any]] = {}

        def put(payload: dict[str, Any]) -> None:
            items[(payload["item_id"], payload["version"])] = {
                "category": payload["category"],
                "purpose": payload["purpose"],
                "sensitive_categories": list(payload["sensitive_categories"]),
                "content_hash": payload["content_hash"],
                "decision_influencing": bool(payload.get("decision_influencing", False)),
                "state": m.ACTIVE,
            }

        for event in self.store.events_until(tenant_id, as_of_iso):
            payload = json.loads(event["payload_json"])
            kind = event["event_type"]
            if kind in (m.EVT_MEMORY_INGESTED, m.EVT_MEMORY_DERIVED):
                put(payload)
            elif kind == m.EVT_MEMORY_CORRECTED:
                old_key = (payload["item_id"], payload["old_version"])
                if old_key in items:
                    items[old_key]["state"] = m.SUPERSEDED
                put(payload["new_item"])
            elif kind == m.EVT_HOLD_APPLIED:
                for item_id in payload["item_ids"]:
                    for (iid, _ver), record in items.items():
                        if iid == item_id and record["state"] in (m.ACTIVE, m.SUPERSEDED):
                            record["state"] = m.HELD
            elif kind == m.EVT_HOLD_RELEASED:
                for item_id in payload["item_ids"]:
                    self._replay_restore(items, item_id)
            elif kind == m.EVT_ERASURE_STEP_COMPLETED:
                if payload.get("result") != m.STEP_DONE:
                    continue
                target = {
                    m.STAGE_REVOKE: m.PENDING_ERASURE,
                    m.STAGE_QUARANTINE: m.ERASING,
                    m.STAGE_PURGE: m.ERASED,
                }.get(payload["stage"])
                if target is not None:
                    for (iid, _ver), record in items.items():
                        if iid == payload["item_id"]:
                            record["state"] = target
            elif kind == m.EVT_TASK_GRANTED:
                grants[payload["task_id"]] = {
                    "purposes": list(payload["purposes"]),
                    "sensitive_categories": list(payload["sensitive_categories"]),
                }
            elif kind == m.EVT_TASK_REVOKED:
                grants.pop(payload["task_id"], None)
            elif kind == m.EVT_TASK_ACCESS_RECORDED:
                if payload["access_kind"] == m.ACCESS_DECISION:
                    key = (payload["item_id"], payload["version"])
                    if key in items:
                        items[key]["decision_influencing"] = True
        return items, grants

    @staticmethod
    def _replay_restore(items: dict[tuple[str, int], dict[str, Any]], item_id: str) -> None:
        versions = [ver for (iid, ver) in items if iid == item_id]
        if not versions:
            return
        latest = max(versions)
        for ver in versions:
            record = items[(item_id, ver)]
            if record["state"] in (m.HELD, m.PENDING_ERASURE, m.ERASING):
                record["state"] = m.ACTIVE if ver == latest else m.SUPERSEDED
