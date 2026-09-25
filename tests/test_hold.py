"""法律保全：冻结优先于删除且必须说明原因（P-04-02）。"""
from __future__ import annotations

import unittest

from helpers import make_service
from memory_governance import StateConflictError, ValidationError, models


class HoldTest(unittest.TestCase):
    def setUp(self) -> None:
        self.service, self.clock = make_service()
        self.service.ingest(
            tenant_id="t-1",
            item_id="mem-1",
            category=models.CATEGORY_USER_UPLOAD,
            purpose="chat",
            content="被保全的原文",
            source_kind="upload",
            origin="web",
            actor_id="operator-01",
        )

    def apply_hold(self, **overrides):
        params = dict(
            tenant_id="t-1",
            hold_id="hold-1",
            item_ids=["mem-1"],
            reason="诉讼保全 2026-09",
            actor_id="reviewer-02",
        )
        params.update(overrides)
        return self.service.apply_hold(**params)

    def test_hold_requires_reason(self) -> None:
        with self.assertRaises(ValidationError):
            self.apply_hold(reason="")

    def test_hold_freezes_item(self) -> None:
        self.apply_hold()
        item = self.service.store.current_item("t-1", "mem-1")
        self.assertEqual(item.state, models.HELD)

    def test_held_item_erasure_only_freezes_with_reason(self) -> None:
        self.apply_hold()
        request = self.service.request_erasure(
            tenant_id="t-1", requester_id="user-1", idempotency_key="del-1", item_ids=["mem-1"]
        )
        request = self.service.run_erasure(tenant_id="t-1", request_id=request["request_id"])
        self.assertEqual(request["state"], "completed_with_holds")
        item = self.service.store.current_item("t-1", "mem-1")
        self.assertEqual(item.content, "被保全的原文")  # 保全节点不被清除
        proof = self.service.erasure_proof(tenant_id="t-1", request_id=request["request_id"])
        self.assertEqual(proof["counts"]["items_frozen"], 1)
        self.assertIn("诉讼保全", proof["entries"][0]["hold_reason"])

    def test_hold_applied_mid_erasure_wins(self) -> None:
        request = self.service.request_erasure(
            tenant_id="t-1", requester_id="user-1", idempotency_key="del-1", item_ids=["mem-1"]
        )
        self.service.run_erasure(tenant_id="t-1", request_id=request["request_id"], max_steps=1)
        item = self.service.store.current_item("t-1", "mem-1")
        self.assertEqual(item.state, models.PENDING_ERASURE)
        self.apply_hold()
        steps = self.service.store.steps_for_request("t-1", request["request_id"])
        self.assertEqual([s["state"] for s in steps], ["done", "skipped", "skipped"])
        self.assertIn("诉讼保全", steps[1]["reason"])
        # 解除后内容恢复可读，删除需重新发起
        self.service.release_hold(
            tenant_id="t-1", hold_id="hold-1", actor_id="reviewer-02", reason="保全到期"
        )
        item = self.service.store.current_item("t-1", "mem-1")
        self.assertEqual(item.state, models.ACTIVE)
        self.assertEqual(item.content, "被保全的原文")
        again = self.service.request_erasure(
            tenant_id="t-1", requester_id="user-1", idempotency_key="del-2", item_ids=["mem-1"]
        )
        again = self.service.run_erasure(tenant_id="t-1", request_id=again["request_id"])
        self.assertEqual(again["state"], "completed")
        self.assertEqual(self.service.store.current_item("t-1", "mem-1").state, models.ERASED)

    def test_duplicate_hold_id_rejected(self) -> None:
        self.apply_hold()
        with self.assertRaises(ValidationError):
            self.apply_hold()

    def test_release_twice_rejected(self) -> None:
        self.apply_hold()
        self.service.release_hold(
            tenant_id="t-1", hold_id="hold-1", actor_id="reviewer-02", reason="结案"
        )
        with self.assertRaises(StateConflictError):
            self.service.release_hold(
                tenant_id="t-1", hold_id="hold-1", actor_id="reviewer-02", reason="重复解除"
            )


if __name__ == "__main__":
    unittest.main()
