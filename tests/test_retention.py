"""保留期限：用户选择、合同要求与法律保全共同决定。"""
from __future__ import annotations

import unittest
from datetime import timedelta

from helpers import T0, make_service
from memory_governance import models


class RetentionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.service, self.clock = make_service()
        self.service.ingest(
            tenant_id="t-1",
            item_id="mem-1",
            category=models.CATEGORY_USER_UPLOAD,
            purpose="chat",
            content="原文",
            source_kind="upload",
            origin="web",
            actor_id="operator-01",
            user_retention_days=30,
        )

    def test_user_choice_and_contract_combine_to_longest(self) -> None:
        self.service.publish_retention_rule(
            tenant_id="t-1", rule_id="r-contract", requirement=models.REQUIREMENT_CONTRACT,
            retention_days=90, scope={"purpose": "chat"}, actor_id="reviewer-02",
        )
        item = self.service.store.current_item("t-1", "mem-1")
        self.assertEqual(item.contract_retention_days, 90)
        expected = models.to_iso(T0 + timedelta(days=90))
        self.assertEqual(item.retain_until, expected)  # 合同 90 天覆盖用户 30 天

    def test_user_longer_choice_wins(self) -> None:
        self.service.publish_retention_rule(
            tenant_id="t-1", rule_id="r-contract", requirement=models.REQUIREMENT_CONTRACT,
            retention_days=10, scope={"purpose": "chat"}, actor_id="reviewer-02",
        )
        item = self.service.store.current_item("t-1", "mem-1")
        expected = models.to_iso(T0 + timedelta(days=30))
        self.assertEqual(item.retain_until, expected)  # 用户选择更长时从用户

    def test_rule_versions_append_only(self) -> None:
        self.service.publish_retention_rule(
            tenant_id="t-1", rule_id="r-1", requirement=models.REQUIREMENT_CONTRACT,
            retention_days=30, actor_id="reviewer-02",
        )
        self.service.publish_retention_rule(
            tenant_id="t-1", rule_id="r-1", requirement=models.REQUIREMENT_CONTRACT,
            retention_days=90, actor_id="reviewer-02",
        )
        versions = self.service.store.rule_versions("t-1", "r-1")
        self.assertEqual(len(versions), 2)  # 已发布规则不覆盖
        self.assertEqual(versions[0]["retention_days"], 30)
        item = self.service.store.current_item("t-1", "mem-1")
        self.assertEqual(item.contract_retention_days, 90)  # 生效取最新版本

    def test_indefinite_rule_means_no_expiry(self) -> None:
        self.service.publish_retention_rule(
            tenant_id="t-1", rule_id="r-forever", requirement=models.REQUIREMENT_REGULATION,
            retention_days=None, scope={"purpose": "chat"}, actor_id="reviewer-02",
        )
        item = self.service.store.current_item("t-1", "mem-1")
        self.assertIsNone(item.retain_until)

    def test_legal_hold_suspends_expiry(self) -> None:
        self.service.apply_hold(
            tenant_id="t-1", hold_id="hold-1", item_ids=["mem-1"],
            reason="监管调查", actor_id="reviewer-02",
        )
        decision = self.service.effective_retention(tenant_id="t-1", item_id="mem-1")
        self.assertTrue(decision["hold_active"])
        self.assertIsNone(decision["retain_until"])
        self.assertEqual(decision["hold_reasons"], ["监管调查"])
        self.assertTrue(any("用户选择" in line for line in decision["basis"]))

    def test_expired_items_respect_hold(self) -> None:
        self.clock.advance(days=31)
        expired = self.service.expired_items(tenant_id="t-1")
        self.assertEqual([i.item_id for i in expired], ["mem-1"])
        self.service.apply_hold(
            tenant_id="t-1", hold_id="hold-1", item_ids=["mem-1"],
            reason="诉讼保全", actor_id="reviewer-02",
        )
        self.assertEqual(self.service.expired_items(tenant_id="t-1"), [])


if __name__ == "__main__":
    unittest.main()
