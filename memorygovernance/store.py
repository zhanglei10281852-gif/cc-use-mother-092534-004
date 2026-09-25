"""SQLite 存储层。

只负责持久化与基础查询，所有治理规则在 :mod:`memorygovernance.service` 中裁决。
全部表都带 ``tenant_id``，对象标识在租户内唯一；时间列统一存带时区 ISO 8601。
"""
from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from pathlib import Path
from typing import Any

SCHEMA = """
create table if not exists memory_items (
    item_id              text not null,
    tenant_id            text not null,
    subject_id           text not null,
    kind                 text not null,
    sensitivity_category text not null,
    purpose              text not null,
    source_type          text not null,
    source_ref           text not null,
    user_expires_at      text,
    current_version_no   integer not null default 1,
    state                text not null default 'active',
    created_at           text not null,
    updated_at           text not null,
    quarantined_at       text,
    purge_after          text,
    purged_at            text,
    erasure_request_id   text,
    primary key (tenant_id, item_id)
);

create table if not exists memory_versions (
    tenant_id    text not null,
    item_id      text not null,
    version_no   integer not null,
    content      text,
    content_sha256 text not null,
    correction_reason text,
    decision_used integer not null default 0,
    created_at   text not null,
    superseded_at text,
    primary key (tenant_id, item_id, version_no)
);

create table if not exists derivation_edges (
    tenant_id text not null,
    edge_id text not null,
    upstream_item_id text not null,
    upstream_version_no integer not null,
    downstream_item_id text not null,
    created_at text not null,
    primary key (tenant_id, edge_id),
    unique (tenant_id, upstream_item_id, upstream_version_no, downstream_item_id)
);

create table if not exists retention_rules (
    tenant_id text not null,
    rule_id text not null,
    subject_id text,
    kind text,
    contract_minimum_days integer,
    version_no integer not null,
    active integer not null default 1,
    created_at text not null,
    superseded_at text,
    primary key (tenant_id, rule_id, version_no)
);

create table if not exists legal_holds (
    tenant_id text not null,
    hold_id text not null,
    subject_id text not null,
    item_id text,
    reason text not null,
    created_by text not null,
    created_at text not null,
    released_at text,
    release_note text,
    primary key (tenant_id, hold_id)
);

create table if not exists task_grants (
    tenant_id text not null,
    grant_id text not null,
    task_id text not null,
    item_id text not null,
    granted_at text not null,
    revoked_at text,
    revoke_reason text,
    primary key (tenant_id, grant_id),
    unique (tenant_id, task_id, item_id)
);

create table if not exists decision_uses (
    tenant_id text not null,
    decision_id text not null,
    item_id text not null,
    version_no integer not null,
    task_id text not null,
    summary text not null,
    decided_at text not null,
    recorded_at text not null,
    primary key (tenant_id, decision_id)
);

create table if not exists erasure_requests (
    tenant_id text not null,
    request_id text not null,
    subject_id text not null,
    requested_by text not null,
    reason text,
    status text not null,
    requested_at text not null,
    recovery_grace_days integer not null,
    completed_at text,
    certificate_id text,
    primary key (tenant_id, request_id)
);

-- 同一租户+主体只允许一条未终结的清除链，保证幂等。
create unique index if not exists ux_active_erasure
    on erasure_requests(tenant_id, subject_id)
    where status in ('pending', 'in_progress', 'recoverable', 'frozen_partial');

create table if not exists erasure_steps (
    tenant_id text not null,
    step_id text not null,
    request_id text not null,
    item_id text not null,
    subject_id text not null,
    stage text not null,           -- quarantine / purge
    status text not null,          -- pending / quarantined / purged / frozen / restored
    frozen_reason text,
    hold_id text,
    attempt integer not null default 0,
    last_error text,
    detail_json text,
    created_at text not null,
    completed_at text,
    primary key (tenant_id, step_id),
    unique (tenant_id, request_id, item_id, stage)
);

create table if not exists quarantined_content (
    tenant_id text not null,
    item_id text not null,
    version_no integer not null,
    content text not null,
    content_sha256 text not null,
    moved_at text not null,
    primary key (tenant_id, item_id, version_no)
);

create table if not exists erasure_certificates (
    tenant_id text not null,
    certificate_id text not null,
    request_id text not null,
    subject_id text not null,
    issued_at text not null,
    counts_json text not null,
    primary key (tenant_id, certificate_id)
);

create table if not exists certificate_entries (
    tenant_id text not null,
    certificate_id text not null,
    item_id text not null,
    kind text not null,
    sensitivity_category text not null,
    subject_id text not null,
    final_status text not null,    -- purged / frozen
    version_count integer not null,
    version_hashes_json text not null,
    quarantined_at text,
    purged_at text,
    frozen_reason text,
    hold_id text
);

create table if not exists event_log (
    tenant_id text not null,
    event_id text not null,
    event_type text not null,
    aggregate_id text not null,
    occurred_at text not null,
    actor_id text not null,
    payload_json text,
    primary key (tenant_id, event_id)
);
"""


class MemoryStore:
    """对 sqlite 连接的薄封装，查询方法返回 dict 行。"""

    def __init__(self, dsn: str | Path = ":memory:") -> None:
        self.connection = sqlite3.connect(
            str(dsn), detect_types=sqlite3.PARSE_DECLTYPES, isolation_level=None
        )
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("pragma foreign_keys = on")
        self.connection.execute("pragma journal_mode = wal")
        self.connection.executescript(SCHEMA)
        self.connection.execute("begin")

    # --- 基础工具 -------------------------------------------------------

    def execute(self, sql: str, params: Iterable[Any] = ()) -> sqlite3.Cursor:
        return self.connection.execute(sql, tuple(params))

    def query_one(self, sql: str, params: Iterable[Any] = ()) -> dict | None:
        row = self.connection.execute(sql, tuple(params)).fetchone()
        return dict(row) if row is not None else None

    def query_all(self, sql: str, params: Iterable[Any] = ()) -> list[dict]:
        rows = self.connection.execute(sql, tuple(params)).fetchall()
        return [dict(row) for row in rows]

    def commit(self) -> None:
        self.connection.execute("commit")
        self.connection.execute("begin")

    def rollback(self) -> None:
        self.connection.execute("rollback")
        self.connection.execute("begin")

    def close(self) -> None:
        self.connection.execute("commit")
        self.connection.close()

    # --- 事件日志 -------------------------------------------------------

    def append_event(
        self,
        tenant_id: str,
        event_id: str,
        event_type: str,
        aggregate_id: str,
        occurred_at: str,
        actor_id: str,
        payload: dict | None = None,
    ) -> None:
        self.execute(
            "insert into event_log values (?, ?, ?, ?, ?, ?, ?)",
            (
                tenant_id,
                event_id,
                event_type,
                aggregate_id,
                occurred_at,
                actor_id,
                json.dumps(payload, ensure_ascii=False) if payload is not None else None,
            ),
        )

    def list_events(self, tenant_id: str, aggregate_id: str | None = None) -> list[dict]:
        if aggregate_id is None:
            return self.query_all(
                "select * from event_log where tenant_id=? order by occurred_at, rowid",
                (tenant_id,),
            )
        return self.query_all(
            "select * from event_log where tenant_id=? and aggregate_id=? order by occurred_at, rowid",
            (tenant_id, aggregate_id),
        )
