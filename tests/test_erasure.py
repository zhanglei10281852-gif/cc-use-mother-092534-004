"""删除：依赖图闭包、幂等、可恢复阶段、中断恢复与证明清单（P-04-04、P-04-06）。"""
from __future__ import annotations

import unittest

from helpers import make_service
from memory_governance import StateConflictError, models


def build_graph(service) -> None:
    """raw-1 → sum-1 → idx-1 的派生链，外加一条任务引用。"""
    service.ingest(
        tenant_id="t-1", item_id="raw-1", category=models.CATEGORY_USER_UPLOAD,
        purpose="chat", content="原始上传内容", source_kind="upload", origin="web",
        actor_id="operator-01",
    )
    service.derive(
        tenant_id="t-1", item_id="sum-1", category=models.CATEGORY_SUMMARY,
        purpose="chat", content="派生摘要", sources=["raw-1"], actor_id="operator-01",
    )
    service.derive(
        tenant_id="t-1", item_id="idx-1", category=models.CATEGORY_INDEX_FRAGMENT,
        purpose="search", content="索引片段", sources=["sum-1"], actor_id="operator-01",
        relation="indexes",
    )
    service.grant_task(tenant_id="t-1", task_id="task-1", purposes=["chat"], actor_id="operator-01")
    service.record_task_access(
        tenant_id="t-1", task_id="task-1", item_id="raw-1", actor_id="operator-01"
    )


class ErasureClosureTest(unittest.TestCase):
    def setUp(self) -> None:
        self.service, self.clock = make_service()
        build_graph(self.service)

    def test_closure_covers_derivatives_and_task_references(self) -> None:
        request = self.service.request_erasure(
            tenant_id="t-1", requester_id="user-1", idempotency_key="del-1", item_ids=["raw-1"]
        )
        self.assertEqual(request["item_ids"], ["idx-1", "raw-1", "sum-1"])
        request = self.service.run_erasure(tenant_id="t-1", request_id=request["request_id"])
        self.assertEqual(request["state"], "completed")
        for item_id in ("raw-1", "sum-1", "idx-1"):
            item = self.service.store.current_item("t-1", item_id)
            self.assertEqual(item.state, models.ERASED)
            self.assertIsNone(item.content)
        self.assertEqual(self.service.store.task_refs_for_item("t-1", "raw-1"), [])

    def test_duplicate_request_reuses_same_chain(self) -> None:
        first = self.service.request_erasure(
            tenant_id="t-1", requester_id="user-1", idempotency_key="del-1", item_ids=["raw-1"]
        )
        second = self.service.request_erasure(
            tenant_id="t-1", requester_id="user-1", idempotency_key="del-1", item_ids=["raw-1"]
        )
        self.assertTrue(second["idempotent_replay"])  # P-04-04
        self.assertEqual(first["request_id"], second["request_id"])
        steps = self.service.store.steps_for_request("t-1", first["request_id"])
        self.assertEqual(len(steps), 9)  # 三条记忆 × 三个阶段，没有第二条清除链

    def test_completed_request_is_idempotent(self) -> None:
        request = self.service.request_erasure(
            tenant_id="t-1", requester_id="user-1", idempotency_key="del-1", item_ids=["raw-1"]
        )
        self.service.run_erasure(tenant_id="t-1", request_id=request["request_id"])
        again = self.service.run_erasure(tenant_id="t-1", request_id=request["request_id"])
        self.assertEqual(again["state"], "completed")

    def test_proof_manifest_has_hashes_and_counts_without_content(self) -> None:
        request = self.service.request_erasure(
            tenant_id="t-1", requester_id="user-1", idempotency_key="del-1", item_ids=["raw-1"]
        )
        self.service.run_erasure(tenant_id="t-1", request_id=request["request_id"])
        proof = self.service.erasure_proof(tenant_id="t-1", request_id=request["request_id"])
        self.assertFalse(proof["content_included"])
        self.assertEqual(proof["counts"]["items_total"], 3)
        self.assertEqual(proof["counts"]["items_erased"], 3)
        self.assertEqual(proof["counts"]["steps_done"], 9)
        self.assertEqual(proof["counts"]["task_references_removed"], 1)
        raw_entry = next(e for e in proof["entries"] if e["item_id"] == "raw-1")
        self.assertEqual(
            raw_entry["content_hashes"]["1"], models.hash_content("原始上传内容")
        )
        for entry in proof["entries"]:
            self.assertNotIn("content", entry)  # 证明清单不含原文


class ErasureStagesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.service, self.clock = make_service()
        self.service.ingest(
            tenant_id="t-1", item_id="mem-1", category=models.CATEGORY_USER_UPLOAD,
            purpose="chat", content="待清除的原文", source_kind="upload", origin="web",
            actor_id="operator-01",
        )
        self.request = self.service.request_erasure(
            tenant_id="t-1", requester_id="user-1", idempotency_key="del-1", item_ids=["mem-1"]
        )

    def test_stages_are_recoverable_until_purge(self) -> None:
        self.service.run_erasure(
            tenant_id="t-1", request_id=self.request["request_id"], max_steps=2
        )
        item = self.service.store.current_item("t-1", "mem-1")
        self.assertEqual(item.state, models.ERASING)
        self.assertIsNone(item.content)  # 原文已隔离
        quarantined = self.service.store.quarantine_for("t-1", "mem-1")
        self.assertEqual(quarantined[0]["content"], "待清除的原文")
        restored = self.service.restore_quarantined(
            tenant_id="t-1", item_id="mem-1", actor_id="reviewer-02"
        )
        self.assertEqual(restored.state, models.ACTIVE)
        self.assertEqual(restored.content, "待清除的原文")
        steps = self.service.store.steps_for_request("t-1", self.request["request_id"])
        self.assertEqual(steps[-1]["state"], "cancelled")

    def test_restore_after_purge_rejected(self) -> None:
        self.service.run_erasure(tenant_id="t-1", request_id=self.request["request_id"])
        with self.assertRaises(StateConflictError):
            self.service.restore_quarantined(
                tenant_id="t-1", item_id="mem-1", actor_id="reviewer-02"
            )

    def test_resume_after_interruption(self) -> None:
        partial = self.service.run_erasure(
            tenant_id="t-1", request_id=self.request["request_id"], max_steps=1
        )
        self.assertEqual(partial["state"], "running")
        self.assertEqual(partial["step_counts"].get("pending"), 2)
        done = self.service.resume_erasure(tenant_id="t-1", request_id=self.request["request_id"])
        self.assertEqual(done["state"], "completed")
        self.assertEqual(self.service.store.current_item("t-1", "mem-1").state, models.ERASED)

    def test_resume_after_step_failure(self) -> None:
        def fail_once(step):
            raise RuntimeError("存储暂时不可用")

        with self.assertRaises(StateConflictError):
            self.service.run_erasure(
                tenant_id="t-1", request_id=self.request["request_id"], step_hook=fail_once
            )
        failed = self.service.get_erasure_request(
            tenant_id="t-1", request_id=self.request["request_id"]
        )
        self.assertEqual(failed["state"], "failed")
        done = self.service.resume_erasure(tenant_id="t-1", request_id=self.request["request_id"])
        self.assertEqual(done["state"], "completed")
        steps = self.service.store.steps_for_request("t-1", self.request["request_id"])
        self.assertTrue(all(s["state"] == "done" for s in steps))
        self.assertGreaterEqual(steps[0]["attempts"], 2)  # 失败步骤被重试


if __name__ == "__main__":
    unittest.main()
