"""审计：按历史时点还原任务可读集合，越权内容不脱敏不返回（P-04-07）。"""
from __future__ import annotations

import unittest

from helpers import make_service
from memory_governance import Auditor, PermissionDeniedError, models


class AuditTimelineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.service, self.clock = make_service()
        svc = self.service
        svc.grant_task(tenant_id="t-1", task_id="task-1", purposes=["chat"], actor_id="ops")
        self.t_grant = self.clock.moment
        self.clock.advance(hours=1)
        svc.ingest(
            tenant_id="t-1", item_id="mem-a", category=models.CATEGORY_USER_UPLOAD,
            purpose="chat", content="甲-原始", source_kind="upload", origin="web", actor_id="ops",
        )
        self.t_a = self.clock.moment
        self.clock.advance(hours=1)
        svc.ingest(
            tenant_id="t-1", item_id="mem-b", category=models.CATEGORY_TOOL_RESULT,
            purpose="chat", content="乙-原始", source_kind="tool", origin="api", actor_id="ops",
        )
        self.t_b = self.clock.moment
        self.clock.advance(hours=1)
        svc.record_task_access(
            tenant_id="t-1", task_id="task-1", item_id="mem-a",
            access_kind=models.ACCESS_DECISION, actor_id="ops",
        )
        self.t_decision = self.clock.moment
        self.clock.advance(hours=1)
        svc.correct(
            tenant_id="t-1", item_id="mem-a", content="甲-更正",
            actor_id="ops", reason="用户更正",
        )
        self.t_corrected = self.clock.moment
        self.clock.advance(hours=1)
        request = svc.request_erasure(
            tenant_id="t-1", requester_id="user-1", idempotency_key="del-b", item_ids=["mem-b"]
        )
        svc.run_erasure(tenant_id="t-1", request_id=request["request_id"])
        self.t_erased = self.clock.moment

    def audit(self, as_of, auditor):
        return self.service.audit_read_set(
            tenant_id="t-1", task_id="task-1", as_of=as_of, auditor=auditor
        )

    def test_point_in_time_read_set(self) -> None:
        auditor = Auditor("aud-1", {"t-1"})
        before_correction = self.audit(self.t_b, auditor)
        self.assertEqual(
            {(e["item_id"], e["version"]) for e in before_correction["entries"]},
            {("mem-a", 1), ("mem-b", 1)},
        )
        after_correction = self.audit(self.t_corrected, auditor)
        self.assertEqual(
            {(e["item_id"], e["version"]) for e in after_correction["entries"]},
            {("mem-a", 2), ("mem-b", 1)},
        )
        after_erasure = self.audit(self.t_erased, auditor)
        self.assertEqual(
            {e["item_id"] for e in after_erasure["entries"]}, {"mem-a"}
        )

    def test_decision_influence_visible_in_history(self) -> None:
        auditor = Auditor("aud-1", {"t-1"})
        view = self.audit(self.t_decision, auditor)
        entry = next(e for e in view["entries"] if e["item_id"] == "mem-a")
        self.assertEqual(entry["version"], 1)
        self.assertTrue(entry["decision_influencing"])

    def test_no_grant_no_read_set(self) -> None:
        auditor = Auditor("aud-1", {"t-1"})
        view = self.service.audit_read_set(
            tenant_id="t-1", task_id="task-1", as_of=self.t_grant, auditor=auditor
        )
        self.assertEqual(view["readable_count"], 0)  # 授权时尚无任何记忆


class AuditRedactionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.service, self.clock = make_service()
        svc = self.service
        svc.grant_task(tenant_id="t-1", task_id="task-1", purposes=["chat"], actor_id="ops")
        svc.ingest(
            tenant_id="t-1", item_id="mem-1", category=models.CATEGORY_USER_UPLOAD,
            purpose="chat", content="敏感原文", source_kind="upload", origin="web", actor_id="ops",
        )
        self.t_before_erasure = self.clock.moment
        self.clock.advance(hours=1)

    def test_unprivileged_auditor_gets_metadata_only(self) -> None:
        view = self.service.audit_read_set(
            tenant_id="t-1", task_id="task-1", as_of=self.t_before_erasure,
            auditor=Auditor("aud-1", {"t-1"}),
        )
        entry = view["entries"][0]
        self.assertTrue(entry["redacted"])
        self.assertIsNone(entry["content"])
        self.assertEqual(entry["redaction_reason"], "审计员无权查看原文")
        self.assertEqual(entry["content_hash"], models.hash_content("敏感原文"))

    def test_privileged_auditor_sees_content_within_purpose(self) -> None:
        view = self.service.audit_read_set(
            tenant_id="t-1", task_id="task-1", as_of=self.t_before_erasure,
            auditor=Auditor("aud-2", {"t-1"}, purposes={"chat"}, can_view_content=True),
        )
        self.assertEqual(view["entries"][0]["content"], "敏感原文")

    def test_purged_content_unavailable_even_to_privileged_auditor(self) -> None:
        request = self.service.request_erasure(
            tenant_id="t-1", requester_id="user-1", idempotency_key="del-1", item_ids=["mem-1"]
        )
        self.service.run_erasure(tenant_id="t-1", request_id=request["request_id"])
        view = self.service.audit_read_set(
            tenant_id="t-1", task_id="task-1", as_of=self.t_before_erasure,
            auditor=Auditor("aud-2", {"t-1"}, purposes={"chat"}, can_view_content=True),
        )
        entry = view["entries"][0]  # 时点还原仍可见当时存在，但原文已销毁
        self.assertIsNone(entry["content"])
        self.assertEqual(entry["redaction_reason"], "原文已清除")

    def test_cross_tenant_auditor_denied(self) -> None:
        with self.assertRaises(PermissionDeniedError):
            self.service.audit_read_set(
                tenant_id="t-1", task_id="task-1", as_of=self.t_before_erasure,
                auditor=Auditor("aud-3", {"other-tenant"}),
            )


class TaskGrantScopeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.service, self.clock = make_service()
        svc = self.service
        svc.grant_task(tenant_id="t-1", task_id="task-1", purposes=["chat"], actor_id="ops")
        svc.ingest(
            tenant_id="t-1", item_id="chat-1", category=models.CATEGORY_USER_UPLOAD,
            purpose="chat", content="对话内容", source_kind="upload", origin="web", actor_id="ops",
        )
        svc.ingest(
            tenant_id="t-1", item_id="ana-1", category=models.CATEGORY_USER_UPLOAD,
            purpose="analytics", content="分析内容", source_kind="upload", origin="web",
            actor_id="ops",
        )
        svc.ingest(
            tenant_id="t-1", item_id="med-1", category=models.CATEGORY_USER_UPLOAD,
            purpose="chat", content="健康内容", source_kind="upload", origin="web",
            actor_id="ops", sensitive_categories=["health"],
        )

    def test_access_outside_purpose_denied(self) -> None:
        with self.assertRaises(PermissionDeniedError):
            self.service.record_task_access(
                tenant_id="t-1", task_id="task-1", item_id="ana-1", actor_id="ops"
            )

    def test_access_outside_sensitive_scope_denied(self) -> None:
        with self.assertRaises(PermissionDeniedError):
            self.service.record_task_access(
                tenant_id="t-1", task_id="task-1", item_id="med-1", actor_id="ops"
            )

    def test_read_set_filtered_by_grant(self) -> None:
        view = self.service.audit_read_set(
            tenant_id="t-1", task_id="task-1", as_of=self.clock.moment,
            auditor=Auditor("aud-1", {"t-1"}),
        )
        self.assertEqual({e["item_id"] for e in view["entries"]}, {"chat-1"})

    def test_revoked_task_reads_nothing(self) -> None:
        self.service.revoke_task(tenant_id="t-1", task_id="task-1", actor_id="ops")
        view = self.service.audit_read_set(
            tenant_id="t-1", task_id="task-1", as_of=self.clock.moment,
            auditor=Auditor("aud-1", {"t-1"}),
        )
        self.assertEqual(view["readable_count"], 0)


if __name__ == "__main__":
    unittest.main()
