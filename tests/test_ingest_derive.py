"""写入与派生：分级元数据、来源链与版本绑定（P-04-01）。"""
from __future__ import annotations

import unittest
from datetime import datetime

from helpers import make_service
from memory_governance import NotFoundError, ValidationError, models


class IngestTest(unittest.TestCase):
    def setUp(self) -> None:
        self.service, self.clock = make_service()

    def ingest(self, **overrides):
        params = dict(
            tenant_id="t-1",
            item_id="mem-1",
            category=models.CATEGORY_USER_UPLOAD,
            purpose="chat",
            content="用户上传的原文",
            source_kind="upload",
            origin="web",
            actor_id="operator-01",
            sensitive_categories=["personal"],
            user_retention_days=30,
        )
        params.update(overrides)
        return self.service.ingest(**params)

    def test_ingest_records_classification_metadata(self) -> None:
        item = self.ingest()
        self.assertEqual(item.version, 1)
        self.assertEqual(item.state, models.ACTIVE)
        self.assertEqual(item.sensitive_categories, ("personal",))
        self.assertEqual(item.user_retention_days, 30)
        self.assertEqual(item.content_hash, models.hash_content("用户上传的原文"))
        sources = self.service.store.sources_for("t-1", "mem-1")
        self.assertEqual(sources[0]["source_kind"], "upload")
        self.assertEqual(sources[0]["origin"], "web")
        events = self.service.store.events_until("t-1", "9999-01-01T00:00:00+00:00")
        self.assertEqual(events[0]["event_type"], models.EVT_MEMORY_INGESTED)

    def test_duplicate_business_id_rejected(self) -> None:
        self.ingest()
        with self.assertRaises(ValidationError):
            self.ingest()

    def test_naive_datetime_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            self.ingest(occurred_at=datetime(2026, 9, 25, 9, 0, 0))

    def test_derived_category_rejected_by_ingest(self) -> None:
        with self.assertRaises(ValidationError):
            self.ingest(category=models.CATEGORY_SUMMARY)


class DeriveTest(unittest.TestCase):
    def setUp(self) -> None:
        self.service, self.clock = make_service()
        self.service.ingest(
            tenant_id="t-1",
            item_id="raw-1",
            category=models.CATEGORY_TOOL_RESULT,
            purpose="chat",
            content="工具返回的原始结果",
            source_kind="tool",
            origin="search-api",
            actor_id="operator-01",
            sensitive_categories=["health"],
        )

    def derive(self, **overrides):
        params = dict(
            tenant_id="t-1",
            item_id="sum-1",
            category=models.CATEGORY_SUMMARY,
            purpose="chat",
            content="模型整理的摘要",
            sources=["raw-1"],
            actor_id="operator-01",
        )
        params.update(overrides)
        return self.service.derive(**params)

    def test_derive_requires_all_direct_sources(self) -> None:
        with self.assertRaises(ValidationError):
            self.derive(sources=[])
        with self.assertRaises(NotFoundError):
            self.derive(sources=["missing"])

    def test_derive_binds_source_version(self) -> None:
        self.derive()
        self.service.correct(
            tenant_id="t-1", item_id="raw-1", content="更正后的工具结果",
            actor_id="operator-01", reason="修正过期数据",
        )
        edges = self.service.store.edges_from("t-1", "raw-1")
        self.assertEqual(edges[0]["src_version"], 1)  # 仍绑定来源当时的版本
        self.assertEqual(edges[0]["dst_item_id"], "sum-1")

    def test_derive_inherits_sensitive_categories(self) -> None:
        item = self.derive()
        self.assertIn("health", item.sensitive_categories)  # 派生不得稀释敏感分级

    def test_derive_chain_to_index_fragment(self) -> None:
        self.derive()
        index = self.service.derive(
            tenant_id="t-1",
            item_id="idx-1",
            category=models.CATEGORY_INDEX_FRAGMENT,
            purpose="search",
            content="索引片段",
            sources=["sum-1"],
            actor_id="operator-01",
            relation="indexes",
        )
        self.assertEqual(index.category, models.CATEGORY_INDEX_FRAGMENT)
        self.assertIn("health", index.sensitive_categories)


if __name__ == "__main__":
    unittest.main()
