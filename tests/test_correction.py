"""更正与版本：旧事实只追加不覆盖（P-04-03）。"""
from __future__ import annotations

import unittest

from helpers import make_service
from memory_governance import StateConflictError, ValidationError, models


class CorrectionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.service, self.clock = make_service()
        self.service.ingest(
            tenant_id="t-1",
            item_id="mem-1",
            category=models.CATEGORY_MODEL_NOTE,
            purpose="chat",
            content="旧事实：用户偏好咖啡",
            source_kind="model",
            origin="conversation",
            actor_id="operator-01",
        )

    def correct(self, **overrides):
        params = dict(
            tenant_id="t-1",
            item_id="mem-1",
            content="新事实：用户偏好茶",
            actor_id="operator-01",
            reason="用户更正",
        )
        params.update(overrides)
        return self.service.correct(**params)

    def test_correction_appends_version_without_overwriting(self) -> None:
        new_item = self.correct()
        self.assertEqual(new_item.version, 2)
        self.assertEqual(new_item.state, models.ACTIVE)
        old = self.service.store.get_item("t-1", "mem-1", 1)
        self.assertEqual(old.state, models.SUPERSEDED)
        self.assertEqual(old.content, "旧事实：用户偏好咖啡")  # 旧版本原文保留
        current = self.service.store.current_item("t-1", "mem-1")
        self.assertEqual(current.content, "新事实：用户偏好茶")

    def test_correction_requires_reason(self) -> None:
        with self.assertRaises(ValidationError):
            self.correct(reason="  ")

    def test_decision_influencing_old_version_preserved(self) -> None:
        self.service.grant_task(
            tenant_id="t-1", task_id="task-1", purposes=["chat"], actor_id="operator-01"
        )
        self.service.record_task_access(
            tenant_id="t-1", task_id="task-1", item_id="mem-1",
            access_kind=models.ACCESS_DECISION, actor_id="operator-01",
        )
        self.correct()
        old = self.service.store.get_item("t-1", "mem-1", 1)
        self.assertTrue(old.decision_influencing)  # 曾影响决策的旧事实仍在
        self.assertEqual(old.content, "旧事实：用户偏好咖啡")
        events = self.service.store.events_until("t-1", "9999-01-01T00:00:00+00:00")
        corrected = [e for e in events if e["event_type"] == models.EVT_MEMORY_CORRECTED]
        self.assertEqual(len(corrected), 1)
        self.assertIn('"old_decision_influencing": true', corrected[0]["payload_json"])

    def test_stale_derivatives_listed_after_correction(self) -> None:
        self.service.derive(
            tenant_id="t-1",
            item_id="sum-1",
            category=models.CATEGORY_SUMMARY,
            purpose="chat",
            content="偏好摘要",
            sources=["mem-1"],
            actor_id="operator-01",
        )
        self.correct()
        stale = self.service.stale_derivatives(tenant_id="t-1", item_id="mem-1")
        self.assertEqual(len(stale), 1)
        self.assertEqual(stale[0]["item_id"], "sum-1")
        self.assertEqual(stale[0]["pinned_source_version"], 1)
        self.assertEqual(stale[0]["current_source_version"], 2)

    def test_correct_held_item_rejected(self) -> None:
        self.service.apply_hold(
            tenant_id="t-1", hold_id="hold-1", item_ids=["mem-1"],
            reason="诉讼保全", actor_id="reviewer-02",
        )
        with self.assertRaises(StateConflictError):
            self.correct()


if __name__ == "__main__":
    unittest.main()
