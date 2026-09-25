"""记忆分级治理服务的端到端测试。"""
from __future__ import annotations

import unittest
from datetime import timedelta

from memorygovernance import (
    ImmutableFactError,
    MemoryGovernanceService,
    MemoryStore,
    NotFoundError,
)
from memorygovernance.timeutil import to_iso

TENANT = "tenant-a"
SUBJECT = "user-1"
T0 = "2026-09-01T00:00:00+00:00"


def days(base: str, n: int) -> str:
    from memorygovernance.timeutil import parse_iso
    return to_iso(parse_iso(base) + timedelta(days=n))


def build_service() -> MemoryGovernanceService:
    return MemoryGovernanceService(MemoryStore(":memory:"))


def seed_chain(svc: MemoryGovernanceService, tenant=TENANT, subject=SUBJECT):
    """用户上传 -> 模型摘要 -> 搜索索引片段 的三级派生链。"""
    raw = svc.ingest_memory(
        tenant, item_id="raw-1", subject_id=subject, kind="conversation",
        content="用户上传的原始内容", purpose="对话支持",
        sensitivity_category="personal", source_type="user_upload",
        source_ref="upload-1", actor_id="operator", occurred_at=T0,
        user_expires_at=days(T0, 7),
    )
    summary = svc.ingest_memory(
        tenant, item_id="summary-1", subject_id=subject, kind="summary",
        content="模型整理的摘要", purpose="检索增强",
        sensitivity_category="derived_personal", source_type="model_note",
        source_ref="note-1", actor_id="model", occurred_at=days(T0, 1),
        derived_from=[("raw-1", 1)],
    )
    index = svc.ingest_memory(
        tenant, item_id="index-1", subject_id=subject, kind="index_fragment",
        content="索引片段", purpose="全文检索",
        sensitivity_category="derived_personal", source_type="tool_return",
        source_ref="indexer-9", actor_id="indexer", occurred_at=days(T0, 2),
        derived_from=[("summary-1", 1)],
    )
    return raw, summary, index


class IngestionLineageTests(unittest.TestCase):
    def test_write_captures_provenance_tenant_purpose_sensitivity(self) -> None:
        svc = build_service()
        seed_chain(svc)
        item = svc._get_item(TENANT, "raw-1")
        self.assertEqual(item["tenant_id"], TENANT)
        self.assertEqual(item["purpose"], "对话支持")
        self.assertEqual(item["sensitivity_category"], "personal")
        self.assertEqual(item["source_type"], "user_upload")

    def test_derivation_edges_bind_specific_versions(self) -> None:
        svc = build_service()
        seed_chain(svc)
        self.assertEqual(
            svc.downstream_closure(TENANT, ["raw-1"]),
            ["index-1", "raw-1", "summary-1"],
        )
        edges = svc.store.query_all("select * from derivation_edges")
        self.assertEqual(
            {(e["upstream_item_id"], e["upstream_version_no"], e["downstream_item_id"])
             for e in edges},
            {("raw-1", 1, "summary-1"), ("summary-1", 1, "index-1")},
        )

    def test_derivation_rejects_unknown_source(self) -> None:
        svc = build_service()
        with self.assertRaises(Exception):
            svc.ingest_memory(
                TENANT, item_id="x", subject_id=SUBJECT, kind="summary",
                content="c", purpose="p", sensitivity_category="s",
                source_type="derivation", source_ref="r", actor_id="a",
                derived_from=[("missing", 1)],
            )

    def test_export_traverses_graph_and_task_refs(self) -> None:
        svc = build_service()
        seed_chain(svc)
        svc.grant_task_access(TENANT, task_id="task-9", item_id="summary-1")
        bundle = svc.export_subject_data(TENANT, SUBJECT, requested_by="user-1")
        exported = {i["item"]["item_id"] for i in bundle["items"]}
        self.assertEqual(exported, {"raw-1", "summary-1", "index-1"})
        refs = {(r["task_id"], r["item_id"]) for r in bundle["task_references"]}
        self.assertIn(("task-9", "summary-1"), refs)


class VersioningTests(unittest.TestCase):
    def test_correction_appends_and_never_overwrites(self) -> None:
        svc = build_service()
        seed_chain(svc)
        result = svc.correct_memory(
            TENANT, "raw-1", new_content="更正后的内容",
            reason="用户指出事实有误", actor_id="operator",
            occurred_at=days(T0, 3),
        )
        self.assertEqual(result["version_no"], 2)
        old = svc._get_version(TENANT, "raw-1", 1)
        new = svc._get_version(TENANT, "raw-1", 2)
        self.assertEqual(old["content"], "用户上传的原始内容")
        self.assertIsNotNone(old["superseded_at"])
        self.assertEqual(new["content"], "更正后的内容")
        item = svc._get_item(TENANT, "raw-1")
        self.assertEqual(item["current_version_no"], 2)
        # 派生边仍指向当时影响过的 v1，不被悄悄改写。
        edge = svc.store.query_one(
            "select * from derivation_edges where downstream_item_id='summary-1'"
        )
        self.assertEqual(edge["upstream_version_no"], 1)

    def test_decision_used_version_is_protected(self) -> None:
        svc = build_service()
        seed_chain(svc)
        svc.record_decision_use(
            TENANT, decision_id="d-1", item_id="raw-1", version_no=1,
            task_id="task-1", summary="依据 v1 做出了授信决定", actor_id="reviewer",
        )
        version = svc._get_version(TENANT, "raw-1", 1)
        self.assertEqual(version["decision_used"], 1)
        # 追加更正仍然允许；旧版本原样保留。
        svc.correct_memory(TENANT, "raw-1", new_content="新事实", reason="更正",
                           actor_id="operator")
        self.assertEqual(
            svc._get_version(TENANT, "raw-1", 1)["content"],
            "用户上传的原始内容",
        )
        with self.assertRaises(ImmutableFactError):
            svc.overwrite_decision_used_version()


class RetentionTests(unittest.TestCase):
    def test_effective_retention_is_max_of_user_and_contract(self) -> None:
        svc = build_service()
        seed_chain(svc)
        svc.set_contract_retention(
            TENANT, rule_id="rule-conv", contract_minimum_days=30,
            actor_id="legal", kind="conversation",
        )
        verdict = svc.resolve_retention(TENANT, "raw-1", at=days(T0, 10))
        self.assertEqual(verdict["user_expires_at"], days(T0, 7))
        self.assertEqual(verdict["contract_min_expires_at"], days(T0, 30))
        self.assertEqual(verdict["effective_expires_at"], days(T0, 30))
        self.assertIsNone(verdict["deletable_at"])

    def test_erasure_waits_for_retention_then_proceeds_on_resume(self) -> None:
        svc = build_service()
        seed_chain(svc)
        svc.set_contract_retention(
            TENANT, rule_id="rule-all", contract_minimum_days=30,
            actor_id="legal",
        )
        request = svc.request_erasure(
            TENANT, subject_id=SUBJECT, requested_by="user-1",
            recovery_grace_days=0,
        )["request"]
        early = svc.run_erasure(TENANT, request["request_id"], now=days(T0, 10))
        statuses = {s["item_id"]: s["status"] for s in early["steps"] if s["stage"] == "quarantine"}
        self.assertTrue(all(s == "blocked" for s in statuses.values()))
        self.assertIsNone(early["certificate"])
        # 中断后于全部节点的保留期均满后重跑：只处理未完成步骤。
        late = svc.run_erasure(TENANT, request["request_id"], now=days(T0, 35))
        self.assertEqual(late["request"]["status"], "completed")
        self.assertEqual(late["certificate"]["counts"]["items_purged"], 3)


class LegalHoldTests(unittest.TestCase):
    def test_held_node_freezes_with_reason_others_purge(self) -> None:
        svc = build_service()
        seed_chain(svc)
        request = svc.request_erasure(
            TENANT, subject_id=SUBJECT, requested_by="user-1",
            recovery_grace_days=0,
        )["request"]
        svc.apply_hold(
            TENANT, hold_id="hold-1", subject_id=SUBJECT, item_id="summary-1",
            reason="诉讼保全（案号 LAW-2026-777）", created_by="legal-officer",
        )
        result = svc.run_erasure(TENANT, request["request_id"], now=days(T0, 10))
        self.assertEqual(result["request"]["status"], "completed_frozen")
        cert = result["certificate"]
        self.assertEqual(cert["counts"]["items_purged"], 2)
        self.assertEqual(cert["counts"]["items_frozen"], 1)
        frozen = [e for e in cert["entries"] if e["final_status"] == "frozen"]
        self.assertEqual(frozen[0]["item_id"], "summary-1")
        self.assertIn("LAW-2026-777", frozen[0]["frozen_reason"])
        # 被冻结节点原文仍在；其余已不可读。
        self.assertIsNotNone(svc._get_version(TENANT, "summary-1", 1)["content"])
        self.assertEqual(svc._get_item(TENANT, "raw-1")["state"], "erased")

    def test_hold_during_recovery_freezes_and_release_allows_purge(self) -> None:
        svc = build_service()
        seed_chain(svc)
        request = svc.request_erasure(
            TENANT, subject_id=SUBJECT, requested_by="user-1",
            recovery_grace_days=30,
        )["request"]
        svc.run_erasure(TENANT, request["request_id"], now=days(T0, 5))
        self.assertEqual(svc._get_item(TENANT, "raw-1")["state"], "recoverable")
        # 宽限期内到来的保全：节点冻结。
        svc.apply_hold(
            TENANT, hold_id="hold-2", subject_id=SUBJECT,
            reason="监管调查", created_by="regulator",
        )
        held = svc.run_erasure(TENANT, request["request_id"], now=days(T0, 40))
        self.assertEqual(held["request"]["status"], "frozen_partial")
        self.assertEqual(svc._get_item(TENANT, "raw-1")["state"], "held")
        # 解除后重跑同一条链即可继续。
        svc.release_hold(TENANT, "hold-2", released_by="regulator", note="调查结束")
        done = svc.run_erasure(TENANT, request["request_id"], now=days(T0, 41))
        self.assertEqual(done["request"]["status"], "completed")
        self.assertEqual(done["certificate"]["counts"]["items_purged"], 3)


class IdempotencyTests(unittest.TestCase):
    def test_duplicate_request_returns_same_chain(self) -> None:
        svc = build_service()
        seed_chain(svc)
        first = svc.request_erasure(TENANT, subject_id=SUBJECT, requested_by="user-1")
        second = svc.request_erasure(TENANT, subject_id=SUBJECT, requested_by="user-1")
        self.assertFalse(first["idempotent"])
        self.assertTrue(second["idempotent"])
        self.assertEqual(
            first["request"]["request_id"], second["request"]["request_id"]
        )
        step_count = svc.store.query_one(
            "select count(*) as c from erasure_steps"
        )["c"]
        self.assertEqual(step_count, 6)  # 3 个节点 × 2 阶段，无第二条链


class StagedErasureTests(unittest.TestCase):
    def test_recoverable_stage_then_purge_and_certificate_without_content(self) -> None:
        svc = build_service()
        seed_chain(svc)
        svc.grant_task_access(TENANT, task_id="task-9", item_id="index-1")
        request = svc.request_erasure(
            TENANT, subject_id=SUBJECT, requested_by="user-1",
            recovery_grace_days=30,
        )["request"]

        stage1 = svc.run_erasure(TENANT, request["request_id"], now=days(T0, 5))
        self.assertEqual(stage1["request"]["status"], "recoverable")
        # 工作副本原文已抽离，任务引用已撤销。
        self.assertIsNone(svc._get_version(TENANT, "raw-1", 1)["content"])
        grant = svc.store.query_one(
            "select * from task_grants where task_id='task-9'"
        )
        self.assertIsNotNone(grant["revoked_at"])
        # 隔离区仍可恢复。
        backup = svc.store.query_one(
            "select content from quarantined_content where item_id='raw-1'"
        )
        self.assertEqual(backup["content"], "用户上传的原始内容")

        stage2 = svc.run_erasure(TENANT, request["request_id"], now=days(T0, 40))
        self.assertEqual(stage2["request"]["status"], "completed")
        cert = stage2["certificate"]
        self.assertEqual(cert["counts"]["items_purged"], 3)
        self.assertEqual(cert["counts"]["versions_purged"], 3)
        self.assertEqual(cert["counts"]["task_references_revoked"], 1)
        # 证明中只有标识与散列，没有原文。
        raw_entry = next(e for e in cert["entries"] if e["item_id"] == "raw-1")
        self.assertEqual(raw_entry["final_status"], "purged")
        self.assertTrue(all(h["content_sha256"] for h in raw_entry["version_hashes"]))
        serialized = repr(cert)
        self.assertNotIn("用户上传的原始内容", serialized)
        self.assertNotIn("模型整理的摘要", serialized)
        # 隔离区与版本行都已物理清除。
        self.assertEqual(
            svc.store.query_one("select count(*) as c from quarantined_content")["c"], 0
        )
        self.assertEqual(
            svc.store.query_one("select count(*) as c from memory_versions")["c"], 0
        )

    def test_restore_during_grace_window(self) -> None:
        svc = build_service()
        seed_chain(svc)
        request = svc.request_erasure(
            TENANT, subject_id=SUBJECT, requested_by="user-1",
            recovery_grace_days=30,
        )["request"]
        svc.run_erasure(TENANT, request["request_id"], now=days(T0, 5))
        outcome = svc.restore_erasure(
            TENANT, request["request_id"], restored_by="user-1"
        )
        self.assertEqual(outcome["status"], "withdrawn")
        self.assertEqual(svc._get_item(TENANT, "raw-1")["state"], "active")
        self.assertEqual(
            svc._get_version(TENANT, "summary-1", 1)["content"], "模型整理的摘要"
        )

    def test_resume_after_interruption_is_step_safe(self) -> None:
        svc = build_service()
        seed_chain(svc)
        request = svc.request_erasure(
            TENANT, subject_id=SUBJECT, requested_by="user-1",
            recovery_grace_days=0,
        )["request"]
        # 连续两次「中断后重跑」是等价且安全的：完成后的运行返回同一快照。
        first = svc.run_erasure(TENANT, request["request_id"], now=days(T0, 5))
        second = svc.run_erasure(TENANT, request["request_id"], now=days(T0, 5))
        self.assertEqual(first["certificate"]["certificate_id"],
                         second["certificate"]["certificate_id"])
        purged_steps = svc.store.query_all(
            "select attempt from erasure_steps where status='purged'"
        )
        self.assertTrue(all(row["attempt"] == 1 for row in purged_steps))


class AuditTests(unittest.TestCase):
    def test_point_in_time_reconstruction_respects_versions_and_redaction(self) -> None:
        svc = build_service()
        seed_chain(svc)
        svc.grant_task_access(TENANT, task_id="task-1", item_id="raw-1", granted_at=T0)
        svc.correct_memory(
            TENANT, "raw-1", new_content="更正后的内容", reason="用户更正",
            actor_id="operator", occurred_at=days(T0, 4),
        )

        before = svc.audit_task_readable(
            TENANT, task_id="task-1", at=days(T0, 3),
        )
        self.assertEqual(before["readable_count"], 1)
        view = before["readable"][0]
        self.assertEqual(view["current_version_at_time"], 1)
        self.assertTrue(all(v["redacted"] for v in view["versions"]))
        self.assertIsNone(view["versions"][0]["content"])

        after = svc.audit_task_readable(
            TENANT, task_id="task-1", at=days(T0, 5), content_access=True,
        )
        view = after["readable"][0]
        self.assertEqual(view["current_version_at_time"], 2)
        self.assertEqual(
            [v["content"] for v in view["versions"] if v["current_at_time"]],
            ["更正后的内容"],
        )

    def test_audit_scope_cannot_cross_authorization_boundary(self) -> None:
        svc = build_service()
        seed_chain(svc)
        svc.grant_task_access(TENANT, task_id="task-1", item_id="raw-1", granted_at=T0)
        # 审计人只有 summary-1 的可见范围：即使任务被授权读取 raw-1 也看不到。
        scoped = svc.audit_task_readable(
            TENANT, task_id="task-1", at=days(T0, 5),
            scope_item_ids=["summary-1"],
        )
        self.assertEqual(scoped["readable_count"], 0)
        allowed = svc.audit_task_readable(
            TENANT, task_id="task-1", at=days(T0, 5),
            scope_item_ids=["raw-1"], content_access=True,
        )
        self.assertEqual(allowed["readable_count"], 1)

    def test_audit_after_purge_lists_history_but_never_revives_content(self) -> None:
        svc = build_service()
        seed_chain(svc)
        svc.grant_task_access(TENANT, task_id="task-1", item_id="raw-1", granted_at=T0)
        request = svc.request_erasure(
            TENANT, subject_id=SUBJECT, requested_by="user-1",
            recovery_grace_days=0,
        )["request"]
        svc.run_erasure(TENANT, request["request_id"], now=days(T0, 5))

        # 清除之后的时点：不可读。
        self.assertEqual(
            svc.audit_task_readable(TENANT, task_id="task-1", at=days(T0, 10))["readable_count"],
            0,
        )
        # 清除之前的时点：清单与散列可还原，但即使带内容权限原文也不可恢复。
        historical = svc.audit_task_readable(
            TENANT, task_id="task-1", at=days(T0, 3), content_access=True,
        )
        self.assertEqual(historical["readable_count"], 1)
        version = historical["readable"][0]["versions"][0]
        self.assertIsNone(version["content"])
        self.assertFalse(version["content_available"])
        self.assertEqual(len(version["content_sha256"]), 64)


class TenantIsolationTests(unittest.TestCase):
    def test_identifiers_and_graphs_are_tenant_scoped(self) -> None:
        svc = build_service()
        seed_chain(svc, tenant="tenant-a")
        seed_chain(svc, tenant="tenant-b")
        self.assertEqual(
            svc.downstream_closure("tenant-a", ["raw-1"]),
            ["index-1", "raw-1", "summary-1"],
        )
        with self.assertRaises(NotFoundError):
            svc.resolve_retention("tenant-c", "raw-1")
        ra = svc.request_erasure("tenant-a", subject_id=SUBJECT, requested_by="u")
        rb = svc.request_erasure("tenant-b", subject_id=SUBJECT, requested_by="u")
        self.assertNotEqual(
            ra["request"]["request_id"], rb["request"]["request_id"]
        )


if __name__ == "__main__":
    unittest.main()
