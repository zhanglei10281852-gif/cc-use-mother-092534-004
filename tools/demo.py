"""端到端演示：摄取 → 派生 → 更正 → 保全 → 导出 → 删除 → 证明 → 审计。"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from memory_governance import Auditor, MemoryGovernanceService, models  # noqa: E402


class Clock:
    def __init__(self) -> None:
        self.moment = datetime(2026, 9, 25, 9, 0, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        return self.moment

    def tick(self, minutes: int = 1) -> None:
        self.moment += timedelta(minutes=minutes)


def main() -> None:
    clock = Clock()
    svc = MemoryGovernanceService(clock=clock)

    raw = svc.ingest(
        tenant_id="tenant-a", item_id="upload-1", category=models.CATEGORY_USER_UPLOAD,
        purpose="chat", content="用户上传的合同扫描件", source_kind="upload", origin="web",
        actor_id="user-1", sensitive_categories=["personal"], user_retention_days=30,
    )
    svc.publish_retention_rule(
        tenant_id="tenant-a", rule_id="contract-min", requirement=models.REQUIREMENT_CONTRACT,
        retention_days=90, scope={"purpose": "chat"}, actor_id="legal-1",
    )
    clock.tick()
    summary = svc.derive(
        tenant_id="tenant-a", item_id="sum-1", category=models.CATEGORY_SUMMARY,
        purpose="chat", content="合同要点摘要", sources=["upload-1"], actor_id="agent-1",
    )
    svc.derive(
        tenant_id="tenant-a", item_id="idx-1", category=models.CATEGORY_INDEX_FRAGMENT,
        purpose="search", content="合同 扫描件 要点", sources=["sum-1"],
        actor_id="agent-1", relation="indexes",
    )
    clock.tick()
    svc.correct(
        tenant_id="tenant-a", item_id="upload-1", content="用户重新上传的清晰扫描件",
        actor_id="user-1", reason="原件模糊",
    )
    print(f"1. 写入与派生：raw v{raw.version}→v2，摘要 {summary.item_id}，索引 idx-1；"
          f"保留至 {svc.effective_retention(tenant_id='tenant-a', item_id='upload-1')['retain_until']}")

    clock.tick()
    svc.grant_task(tenant_id="tenant-a", task_id="task-9", purposes=["chat"],
                   sensitive_categories=["personal"], actor_id="ops")
    svc.record_task_access(tenant_id="tenant-a", task_id="task-9", item_id="upload-1",
                           access_kind=models.ACCESS_DECISION, actor_id="agent-1")
    clock.tick()
    before_erasure = clock.moment

    export = svc.request_export(tenant_id="tenant-a", requester_id="user-1",
                                idempotency_key="exp-2026-09", item_ids=["upload-1"])
    package = svc.build_export(tenant_id="tenant-a", export_id=export["export_id"])
    print(f"2. 导出：{package['manifest']['items_total']} 条记忆、"
          f"{len(package['derivation_edges'])} 条派生边、{len(package['task_references'])} 条任务引用")

    clock.tick()
    svc.apply_hold(tenant_id="tenant-a", hold_id="hold-7", item_ids=["idx-1"],
                   reason="监管检查 2026-09", actor_id="legal-1")
    request = svc.request_erasure(tenant_id="tenant-a", requester_id="user-1",
                                  idempotency_key="del-2026-09", item_ids=["upload-1"])
    again = svc.request_erasure(tenant_id="tenant-a", requester_id="user-1",
                                idempotency_key="del-2026-09", item_ids=["upload-1"])
    assert again["idempotent_replay"] and again["request_id"] == request["request_id"]
    result = svc.run_erasure(tenant_id="tenant-a", request_id=request["request_id"])
    proof = svc.erasure_proof(tenant_id="tenant-a", request_id=request["request_id"])
    frozen = next(e for e in proof["entries"] if e["hold_reason"])
    print(f"3. 删除：{result['state']}；清除 {proof['counts']['items_erased']} 条，"
          f"保全冻结 {proof['counts']['items_frozen']} 条（{frozen['hold_reason']}），"
          f"证明含原文={proof['content_included']}")

    auditor = Auditor("aud-1", {"tenant-a"})
    view = svc.audit_read_set(tenant_id="tenant-a", task_id="task-9",
                              as_of=before_erasure, auditor=auditor)
    readable = ", ".join(f"{e['item_id']}#v{e['version']}" for e in view["entries"])
    print(f"4. 审计：删除前时点 task-9 可读 {view['readable_count']} 条（{readable}），"
          f"全部脱敏={all(e['redacted'] for e in view['entries'])}")


if __name__ == "__main__":
    main()
