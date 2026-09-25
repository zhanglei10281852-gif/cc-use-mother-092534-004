"""SQLite 存储层：表结构、实体读写与仅追加事件日志。

事件日志是审计时点还原的事实来源，只追加不修改；业务表保存当前状态。
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator

from .models import MemoryItem, to_iso

SCHEMA = """
CREATE TABLE IF NOT EXISTS memory_item (
  tenant_id TEXT NOT NULL,
  item_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  category TEXT NOT NULL,
  purpose TEXT NOT NULL,
  sensitive_categories TEXT NOT NULL,
  content TEXT,
  content_hash TEXT NOT NULL,
  state TEXT NOT NULL,
  decision_influencing INTEGER NOT NULL DEFAULT 0,
  user_retention_days INTEGER,
  contract_retention_days INTEGER,
  retain_until TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (tenant_id, item_id, version)
);
CREATE TABLE IF NOT EXISTS source_record (
  tenant_id TEXT NOT NULL,
  item_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  source_kind TEXT NOT NULL,
  origin TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  captured_at TEXT NOT NULL,
  PRIMARY KEY (tenant_id, item_id, version, source_kind, origin)
);
CREATE TABLE IF NOT EXISTS derivation_edge (
  tenant_id TEXT NOT NULL,
  edge_id TEXT NOT NULL,
  src_item_id TEXT NOT NULL,
  src_version INTEGER NOT NULL,
  dst_item_id TEXT NOT NULL,
  dst_version INTEGER NOT NULL,
  relation TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (tenant_id, edge_id),
  UNIQUE (tenant_id, src_item_id, src_version, dst_item_id, dst_version, relation)
);
CREATE TABLE IF NOT EXISTS retention_rule (
  tenant_id TEXT NOT NULL,
  rule_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  requirement TEXT NOT NULL,
  retention_days INTEGER,
  scope_json TEXT NOT NULL,
  published_by TEXT NOT NULL,
  published_at TEXT NOT NULL,
  PRIMARY KEY (tenant_id, rule_id, version)
);
CREATE TABLE IF NOT EXISTS legal_hold (
  tenant_id TEXT NOT NULL,
  hold_id TEXT NOT NULL,
  reason TEXT NOT NULL,
  applied_by TEXT NOT NULL,
  applied_at TEXT NOT NULL,
  released_by TEXT,
  released_at TEXT,
  release_reason TEXT,
  PRIMARY KEY (tenant_id, hold_id)
);
CREATE TABLE IF NOT EXISTS hold_link (
  tenant_id TEXT NOT NULL,
  hold_id TEXT NOT NULL,
  item_id TEXT NOT NULL,
  linked_at TEXT NOT NULL,
  PRIMARY KEY (tenant_id, hold_id, item_id)
);
CREATE TABLE IF NOT EXISTS erasure_request (
  tenant_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  requester_id TEXT NOT NULL,
  scope_json TEXT NOT NULL,
  state TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (tenant_id, request_id),
  UNIQUE (tenant_id, idempotency_key)
);
CREATE TABLE IF NOT EXISTS erasure_step (
  tenant_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  step_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  item_id TEXT NOT NULL,
  stage TEXT NOT NULL,
  state TEXT NOT NULL,
  reason TEXT,
  detail_json TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  completed_at TEXT,
  PRIMARY KEY (tenant_id, request_id, step_id)
);
CREATE TABLE IF NOT EXISTS quarantine (
  tenant_id TEXT NOT NULL,
  item_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  content TEXT NOT NULL,
  request_id TEXT NOT NULL,
  quarantined_at TEXT NOT NULL,
  PRIMARY KEY (tenant_id, item_id, version)
);
CREATE TABLE IF NOT EXISTS export_request (
  tenant_id TEXT NOT NULL,
  export_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  requester_id TEXT NOT NULL,
  scope_json TEXT NOT NULL,
  state TEXT NOT NULL,
  created_at TEXT NOT NULL,
  completed_at TEXT,
  PRIMARY KEY (tenant_id, export_id),
  UNIQUE (tenant_id, idempotency_key)
);
CREATE TABLE IF NOT EXISTS task_grant (
  tenant_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  purposes_json TEXT NOT NULL,
  sensitive_categories_json TEXT NOT NULL,
  granted_by TEXT NOT NULL,
  granted_at TEXT NOT NULL,
  revoked_at TEXT,
  PRIMARY KEY (tenant_id, task_id)
);
CREATE TABLE IF NOT EXISTS task_reference (
  tenant_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  item_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  access_kind TEXT NOT NULL,
  accessed_at TEXT NOT NULL,
  PRIMARY KEY (tenant_id, task_id, item_id, version, access_kind)
);
CREATE TABLE IF NOT EXISTS event_log (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  tenant_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,
  occurred_at TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  UNIQUE (tenant_id, event_id)
);
"""


def _row_to_item(row: sqlite3.Row) -> MemoryItem:
    return MemoryItem(
        tenant_id=row["tenant_id"],
        item_id=row["item_id"],
        version=row["version"],
        category=row["category"],
        purpose=row["purpose"],
        sensitive_categories=tuple(json.loads(row["sensitive_categories"])),
        content=row["content"],
        content_hash=row["content_hash"],
        state=row["state"],
        decision_influencing=bool(row["decision_influencing"]),
        user_retention_days=row["user_retention_days"],
        contract_retention_days=row["contract_retention_days"],
        retain_until=row["retain_until"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class Store:
    """薄存储层：只负责 SQL，不含业务规则。"""

    def __init__(self, path: str = ":memory:") -> None:
        self._conn = sqlite3.connect(path)
        self._conn.row_factory = sqlite3.Row

    def init_schema(self) -> None:
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    @contextmanager
    def transaction(self) -> Iterator[None]:
        try:
            yield
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    # ------------------------------------------------------------ 事件日志
    def append_event(
        self,
        tenant_id: str,
        event_type: str,
        aggregate_id: str,
        occurred_at: datetime,
        actor_id: str,
        payload: dict[str, Any],
    ) -> str:
        event_id = uuid.uuid4().hex
        self._conn.execute(
            "INSERT INTO event_log(tenant_id, event_id, event_type, aggregate_id, occurred_at, actor_id, payload_json)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                tenant_id,
                event_id,
                event_type,
                aggregate_id,
                to_iso(occurred_at),
                actor_id,
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
            ),
        )
        return event_id

    def events_until(self, tenant_id: str, as_of_iso: str) -> list[dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT * FROM event_log WHERE tenant_id = ? AND occurred_at <= ? ORDER BY seq",
            (tenant_id, as_of_iso),
        )
        return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------ 记忆条目
    def insert_item(self, item: MemoryItem) -> None:
        self._conn.execute(
            "INSERT INTO memory_item VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                item.tenant_id,
                item.item_id,
                item.version,
                item.category,
                item.purpose,
                json.dumps(list(item.sensitive_categories), ensure_ascii=False),
                item.content,
                item.content_hash,
                item.state,
                int(item.decision_influencing),
                item.user_retention_days,
                item.contract_retention_days,
                item.retain_until,
                item.created_at,
                item.updated_at,
            ),
        )

    def get_item(self, tenant_id: str, item_id: str, version: int) -> MemoryItem | None:
        cur = self._conn.execute(
            "SELECT * FROM memory_item WHERE tenant_id = ? AND item_id = ? AND version = ?",
            (tenant_id, item_id, version),
        )
        row = cur.fetchone()
        return _row_to_item(row) if row else None

    def current_item(self, tenant_id: str, item_id: str) -> MemoryItem | None:
        cur = self._conn.execute(
            "SELECT * FROM memory_item WHERE tenant_id = ? AND item_id = ? ORDER BY version DESC LIMIT 1",
            (tenant_id, item_id),
        )
        row = cur.fetchone()
        return _row_to_item(row) if row else None

    def list_versions(self, tenant_id: str, item_id: str) -> list[MemoryItem]:
        cur = self._conn.execute(
            "SELECT * FROM memory_item WHERE tenant_id = ? AND item_id = ? ORDER BY version",
            (tenant_id, item_id),
        )
        return [_row_to_item(row) for row in cur.fetchall()]

    def list_items(self, tenant_id: str) -> list[MemoryItem]:
        cur = self._conn.execute(
            "SELECT * FROM memory_item WHERE tenant_id = ? ORDER BY item_id, version",
            (tenant_id,),
        )
        return [_row_to_item(row) for row in cur.fetchall()]

    def update_item_state(self, tenant_id: str, item_id: str, version: int, state: str, updated_at: str) -> None:
        self._conn.execute(
            "UPDATE memory_item SET state = ?, updated_at = ? WHERE tenant_id = ? AND item_id = ? AND version = ?",
            (state, updated_at, tenant_id, item_id, version),
        )

    def update_item_retention(
        self,
        tenant_id: str,
        item_id: str,
        version: int,
        contract_days: int | None,
        retain_until: str | None,
        updated_at: str,
    ) -> None:
        self._conn.execute(
            "UPDATE memory_item SET contract_retention_days = ?, retain_until = ?, updated_at = ?"
            " WHERE tenant_id = ? AND item_id = ? AND version = ?",
            (contract_days, retain_until, updated_at, tenant_id, item_id, version),
        )

    def clear_item_content(self, tenant_id: str, item_id: str, version: int, updated_at: str) -> None:
        self._conn.execute(
            "UPDATE memory_item SET content = NULL, updated_at = ? WHERE tenant_id = ? AND item_id = ? AND version = ?",
            (updated_at, tenant_id, item_id, version),
        )

    def restore_item_content(self, tenant_id: str, item_id: str, version: int, content: str, updated_at: str) -> None:
        self._conn.execute(
            "UPDATE memory_item SET content = ?, updated_at = ? WHERE tenant_id = ? AND item_id = ? AND version = ?",
            (content, updated_at, tenant_id, item_id, version),
        )

    def set_decision_influencing(self, tenant_id: str, item_id: str, version: int, updated_at: str) -> None:
        self._conn.execute(
            "UPDATE memory_item SET decision_influencing = 1, updated_at = ?"
            " WHERE tenant_id = ? AND item_id = ? AND version = ?",
            (updated_at, tenant_id, item_id, version),
        )

    # ------------------------------------------------------------ 来源记录
    def insert_source(
        self,
        tenant_id: str,
        item_id: str,
        version: int,
        source_kind: str,
        origin: str,
        actor_id: str,
        captured_at: str,
    ) -> None:
        self._conn.execute(
            "INSERT INTO source_record VALUES (?, ?, ?, ?, ?, ?, ?)",
            (tenant_id, item_id, version, source_kind, origin, actor_id, captured_at),
        )

    def sources_for(self, tenant_id: str, item_id: str) -> list[dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT * FROM source_record WHERE tenant_id = ? AND item_id = ? ORDER BY version",
            (tenant_id, item_id),
        )
        return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------ 派生边
    def insert_edge(
        self,
        tenant_id: str,
        src_item_id: str,
        src_version: int,
        dst_item_id: str,
        dst_version: int,
        relation: str,
        created_at: str,
    ) -> str:
        edge_id = uuid.uuid4().hex
        self._conn.execute(
            "INSERT INTO derivation_edge VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (tenant_id, edge_id, src_item_id, src_version, dst_item_id, dst_version, relation, created_at),
        )
        return edge_id

    def edges_from(self, tenant_id: str, item_id: str) -> list[dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT * FROM derivation_edge WHERE tenant_id = ? AND src_item_id = ?",
            (tenant_id, item_id),
        )
        return [dict(row) for row in cur.fetchall()]

    def edges_among(self, tenant_id: str, item_ids: list[str]) -> list[dict[str, Any]]:
        if not item_ids:
            return []
        marks = ",".join("?" for _ in item_ids)
        cur = self._conn.execute(
            f"SELECT * FROM derivation_edge WHERE tenant_id = ? AND src_item_id IN ({marks}) AND dst_item_id IN ({marks})",
            (tenant_id, *item_ids, *item_ids),
        )
        return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------ 保留规则
    def insert_rule(
        self,
        tenant_id: str,
        rule_id: str,
        version: int,
        requirement: str,
        retention_days: int | None,
        scope: dict[str, Any],
        published_by: str,
        published_at: str,
    ) -> None:
        self._conn.execute(
            "INSERT INTO retention_rule VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                tenant_id,
                rule_id,
                version,
                requirement,
                retention_days,
                json.dumps(scope, ensure_ascii=False, sort_keys=True),
                published_by,
                published_at,
            ),
        )

    def latest_rule_version(self, tenant_id: str, rule_id: str) -> int:
        cur = self._conn.execute(
            "SELECT COALESCE(MAX(version), 0) FROM retention_rule WHERE tenant_id = ? AND rule_id = ?",
            (tenant_id, rule_id),
        )
        return int(cur.fetchone()[0])

    def latest_rules(self, tenant_id: str) -> list[dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT r.* FROM retention_rule r"
            " JOIN (SELECT rule_id, MAX(version) AS mv FROM retention_rule WHERE tenant_id = ? GROUP BY rule_id) t"
            " ON r.rule_id = t.rule_id AND r.version = t.mv AND r.tenant_id = ?",
            (tenant_id, tenant_id),
        )
        return [dict(row) for row in cur.fetchall()]

    def rule_versions(self, tenant_id: str, rule_id: str) -> list[dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT * FROM retention_rule WHERE tenant_id = ? AND rule_id = ? ORDER BY version",
            (tenant_id, rule_id),
        )
        return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------ 法律保全
    def insert_hold(self, tenant_id: str, hold_id: str, reason: str, applied_by: str, applied_at: str) -> None:
        self._conn.execute(
            "INSERT INTO legal_hold(tenant_id, hold_id, reason, applied_by, applied_at) VALUES (?, ?, ?, ?, ?)",
            (tenant_id, hold_id, reason, applied_by, applied_at),
        )

    def get_hold(self, tenant_id: str, hold_id: str) -> dict[str, Any] | None:
        cur = self._conn.execute(
            "SELECT * FROM legal_hold WHERE tenant_id = ? AND hold_id = ?",
            (tenant_id, hold_id),
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def release_hold(self, tenant_id: str, hold_id: str, released_by: str, released_at: str, release_reason: str) -> None:
        self._conn.execute(
            "UPDATE legal_hold SET released_by = ?, released_at = ?, release_reason = ?"
            " WHERE tenant_id = ? AND hold_id = ?",
            (released_by, released_at, release_reason, tenant_id, hold_id),
        )

    def insert_hold_link(self, tenant_id: str, hold_id: str, item_id: str, linked_at: str) -> None:
        self._conn.execute(
            "INSERT INTO hold_link VALUES (?, ?, ?, ?)",
            (tenant_id, hold_id, item_id, linked_at),
        )

    def hold_links(self, tenant_id: str, hold_id: str) -> list[dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT * FROM hold_link WHERE tenant_id = ? AND hold_id = ?",
            (tenant_id, hold_id),
        )
        return [dict(row) for row in cur.fetchall()]

    def active_holds_for_item(self, tenant_id: str, item_id: str) -> list[dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT h.* FROM legal_hold h JOIN hold_link l ON h.tenant_id = l.tenant_id AND h.hold_id = l.hold_id"
            " WHERE l.tenant_id = ? AND l.item_id = ? AND h.released_at IS NULL",
            (tenant_id, item_id),
        )
        return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------ 删除请求与步骤
    def insert_erasure_request(
        self,
        tenant_id: str,
        request_id: str,
        idempotency_key: str,
        requester_id: str,
        scope: dict[str, Any],
        state: str,
        now_iso: str,
    ) -> None:
        self._conn.execute(
            "INSERT INTO erasure_request VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                tenant_id,
                request_id,
                idempotency_key,
                requester_id,
                json.dumps(scope, ensure_ascii=False, sort_keys=True),
                state,
                now_iso,
                now_iso,
            ),
        )

    def get_erasure_request(self, tenant_id: str, request_id: str) -> dict[str, Any] | None:
        cur = self._conn.execute(
            "SELECT * FROM erasure_request WHERE tenant_id = ? AND request_id = ?",
            (tenant_id, request_id),
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def erasure_by_key(self, tenant_id: str, idempotency_key: str) -> dict[str, Any] | None:
        cur = self._conn.execute(
            "SELECT * FROM erasure_request WHERE tenant_id = ? AND idempotency_key = ?",
            (tenant_id, idempotency_key),
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def update_erasure_state(self, tenant_id: str, request_id: str, state: str, updated_at: str) -> None:
        self._conn.execute(
            "UPDATE erasure_request SET state = ?, updated_at = ? WHERE tenant_id = ? AND request_id = ?",
            (state, updated_at, tenant_id, request_id),
        )

    def insert_step(
        self,
        tenant_id: str,
        request_id: str,
        step_id: str,
        seq: int,
        item_id: str,
        stage: str,
        state: str,
        reason: str | None,
        now_iso: str,
    ) -> None:
        self._conn.execute(
            "INSERT INTO erasure_step(tenant_id, request_id, step_id, seq, item_id, stage, state, reason, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (tenant_id, request_id, step_id, seq, item_id, stage, state, reason, now_iso, now_iso),
        )

    def steps_for_request(self, tenant_id: str, request_id: str) -> list[dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT * FROM erasure_step WHERE tenant_id = ? AND request_id = ? ORDER BY seq",
            (tenant_id, request_id),
        )
        return [dict(row) for row in cur.fetchall()]

    def next_actionable_step(self, tenant_id: str, request_id: str) -> dict[str, Any] | None:
        cur = self._conn.execute(
            "SELECT * FROM erasure_step WHERE tenant_id = ? AND request_id = ? AND state IN ('pending', 'failed')"
            " ORDER BY seq LIMIT 1",
            (tenant_id, request_id),
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def update_step(
        self,
        tenant_id: str,
        request_id: str,
        step_id: str,
        *,
        state: str,
        updated_at: str,
        reason: str | None = None,
        detail_json: str | None = None,
        last_error: str | None = None,
        completed_at: str | None = None,
    ) -> None:
        self._conn.execute(
            "UPDATE erasure_step SET state = ?, reason = ?, detail_json = COALESCE(?, detail_json),"
            " last_error = ?, completed_at = ?, updated_at = ?, attempts = attempts + 1"
            " WHERE tenant_id = ? AND request_id = ? AND step_id = ?",
            (state, reason, detail_json, last_error, completed_at, updated_at, tenant_id, request_id, step_id),
        )

    def actionable_steps_for_item(self, tenant_id: str, item_id: str) -> list[dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT * FROM erasure_step WHERE tenant_id = ? AND item_id = ? AND state IN ('pending', 'failed')",
            (tenant_id, item_id),
        )
        return [dict(row) for row in cur.fetchall()]

    def skip_step(self, tenant_id: str, request_id: str, step_id: str, reason: str, updated_at: str) -> None:
        self._conn.execute(
            "UPDATE erasure_step SET state = 'skipped', reason = ?, updated_at = ?"
            " WHERE tenant_id = ? AND request_id = ? AND step_id = ?",
            (reason, updated_at, tenant_id, request_id, step_id),
        )

    # ------------------------------------------------------------ 隔离区
    def insert_quarantine(
        self, tenant_id: str, item_id: str, version: int, content: str, request_id: str, quarantined_at: str
    ) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO quarantine VALUES (?, ?, ?, ?, ?, ?)",
            (tenant_id, item_id, version, content, request_id, quarantined_at),
        )

    def quarantine_for(self, tenant_id: str, item_id: str) -> list[dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT * FROM quarantine WHERE tenant_id = ? AND item_id = ?",
            (tenant_id, item_id),
        )
        return [dict(row) for row in cur.fetchall()]

    def delete_quarantine(self, tenant_id: str, item_id: str) -> int:
        cur = self._conn.execute(
            "DELETE FROM quarantine WHERE tenant_id = ? AND item_id = ?",
            (tenant_id, item_id),
        )
        return cur.rowcount

    # ------------------------------------------------------------ 导出
    def insert_export(
        self,
        tenant_id: str,
        export_id: str,
        idempotency_key: str,
        requester_id: str,
        scope: dict[str, Any],
        now_iso: str,
    ) -> None:
        self._conn.execute(
            "INSERT INTO export_request(tenant_id, export_id, idempotency_key, requester_id, scope_json, state, created_at)"
            " VALUES (?, ?, ?, ?, ?, 'requested', ?)",
            (
                tenant_id,
                export_id,
                idempotency_key,
                requester_id,
                json.dumps(scope, ensure_ascii=False, sort_keys=True),
                now_iso,
            ),
        )

    def get_export(self, tenant_id: str, export_id: str) -> dict[str, Any] | None:
        cur = self._conn.execute(
            "SELECT * FROM export_request WHERE tenant_id = ? AND export_id = ?",
            (tenant_id, export_id),
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def export_by_key(self, tenant_id: str, idempotency_key: str) -> dict[str, Any] | None:
        cur = self._conn.execute(
            "SELECT * FROM export_request WHERE tenant_id = ? AND idempotency_key = ?",
            (tenant_id, idempotency_key),
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def complete_export(self, tenant_id: str, export_id: str, completed_at: str) -> None:
        self._conn.execute(
            "UPDATE export_request SET state = 'completed', completed_at = ? WHERE tenant_id = ? AND export_id = ?",
            (completed_at, tenant_id, export_id),
        )

    # ------------------------------------------------------------ 任务授权与引用
    def put_grant(
        self,
        tenant_id: str,
        task_id: str,
        purposes: list[str],
        sensitive_categories: list[str],
        granted_by: str,
        granted_at: str,
    ) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO task_grant(tenant_id, task_id, purposes_json, sensitive_categories_json, granted_by, granted_at)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (
                tenant_id,
                task_id,
                json.dumps(sorted(purposes), ensure_ascii=False),
                json.dumps(sorted(sensitive_categories), ensure_ascii=False),
                granted_by,
                granted_at,
            ),
        )

    def get_grant(self, tenant_id: str, task_id: str) -> dict[str, Any] | None:
        cur = self._conn.execute(
            "SELECT * FROM task_grant WHERE tenant_id = ? AND task_id = ?",
            (tenant_id, task_id),
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def revoke_grant(self, tenant_id: str, task_id: str, revoked_at: str) -> None:
        self._conn.execute(
            "UPDATE task_grant SET revoked_at = ? WHERE tenant_id = ? AND task_id = ?",
            (revoked_at, tenant_id, task_id),
        )

    def insert_task_reference(
        self,
        tenant_id: str,
        task_id: str,
        item_id: str,
        version: int,
        access_kind: str,
        accessed_at: str,
    ) -> None:
        self._conn.execute(
            "INSERT OR IGNORE INTO task_reference VALUES (?, ?, ?, ?, ?, ?)",
            (tenant_id, task_id, item_id, version, access_kind, accessed_at),
        )

    def task_refs_for_item(self, tenant_id: str, item_id: str) -> list[dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT * FROM task_reference WHERE tenant_id = ? AND item_id = ?",
            (tenant_id, item_id),
        )
        return [dict(row) for row in cur.fetchall()]

    def task_refs_for_task(self, tenant_id: str, task_id: str) -> list[dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT * FROM task_reference WHERE tenant_id = ? AND task_id = ?",
            (tenant_id, task_id),
        )
        return [dict(row) for row in cur.fetchall()]

    def delete_task_refs_for_item(self, tenant_id: str, item_id: str) -> int:
        cur = self._conn.execute(
            "DELETE FROM task_reference WHERE tenant_id = ? AND item_id = ?",
            (tenant_id, item_id),
        )
        return cur.rowcount
