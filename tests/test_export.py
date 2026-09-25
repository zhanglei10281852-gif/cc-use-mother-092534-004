"""导出：沿依赖图打包，幂等，已清除条目明确标记。"""
from __future__ import annotations

import unittest

from helpers import make_service
from memory_governance import models


class ExportTest(unittest.TestCase):
    def setUp(self) -> None:
        self.service, self.clock = make_service()
        self.service.ingest(
            tenant_id="t-1", item_id="raw-1", category=models.CATEGORY_USER_UPLOAD,
            purpose="chat", content="原始内容", source_kind="upload", origin="web",
            actor_id="operator-01",
        )
        self.service.derive(
            tenant_id="t-1", item_id="sum-1", category=models.CATEGORY_SUMMARY,
            purpose="chat", content="摘要内容", sources=["raw-1"], actor_id="operator-01",
        )
        self.service.grant_task(
            tenant_id="t-1", task_id="task-1", purposes=["chat"], actor_id="operator-01"
        )
        self.service.record_task_access(
            tenant_id="t-1", task_id="task-1", item_id="raw-1", actor_id="operator-01"
        )

    def request_export(self, key="exp-key-1"):
        return self.service.request_export(
            tenant_id="t-1", requester_id="user-1", idempotency_key=key, item_ids=["raw-1"]
        )

    def test_export_contains_content_lineage_and_references(self) -> None:
        export = self.request_export()
        package = self.service.build_export(tenant_id="t-1", export_id=export["export_id"])
        self.assertEqual(package["manifest"]["items_total"], 2)  # 本体 + 下游摘要
        contents = {item["item_id"]: item["content"] for item in package["items"]}
        self.assertEqual(contents["raw-1"], "原始内容")
        self.assertEqual(contents["sum-1"], "摘要内容")
        self.assertEqual(len(package["derivation_edges"]), 1)
        self.assertEqual(package["source_records"][0]["origin"], "web")
        self.assertEqual(package["task_references"][0]["task_id"], "task-1")
        # 导出完成后条目恢复可读
        self.assertEqual(self.service.store.current_item("t-1", "raw-1").state, models.ACTIVE)

    def test_export_request_idempotent(self) -> None:
        first = self.request_export()
        second = self.request_export()
        self.assertTrue(second["idempotent_replay"])
        self.assertEqual(first["export_id"], second["export_id"])

    def test_erased_items_marked_unavailable(self) -> None:
        erasure = self.service.request_erasure(
            tenant_id="t-1", requester_id="user-1", idempotency_key="del-1", item_ids=["raw-1"]
        )
        self.service.run_erasure(tenant_id="t-1", request_id=erasure["request_id"])
        export = self.request_export(key="exp-key-2")
        package = self.service.build_export(tenant_id="t-1", export_id=export["export_id"])
        raw = next(i for i in package["items"] if i["item_id"] == "raw-1")
        self.assertIsNone(raw["content"])
        self.assertEqual(raw["unavailable_reason"], "erased")
        self.assertIn("raw-1", package["manifest"]["unavailable"])


if __name__ == "__main__":
    unittest.main()
