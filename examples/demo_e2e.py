"""端到端场景演示（不落盘，全部在内存 SQLite 中完成）。

覆盖：混来源摄取与派生链、更正追加、合同保留、法律保全、
幂等删除、中断续跑、分阶段可恢复清除、无原文证明与历史时点审计。

运行：python3 examples/demo_e2e.py
"""
from __future__ import annotations

import json
import sys
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from memorygovernance import MemoryGovernanceService, MemoryStore
from memorygovernance.timeutil import parse_iso, to_iso

TENANT, SUBJECT, TASK = "tenant-acme", "user-42", "loan-review-7"
T0 = "2026-09-01T00:00:00+00:00"


def at(base: str, days: int) -> str:
    return to_iso(parse_iso(base) + timedelta(days=days))


def main() -> None:
    svc = MemoryGovernanceService(MemoryStore(":memory:"))

    # 1) 三类来源分开摄取，写入时固化来源、目的、敏感类别。
    svc.ingest_memory(
        TENANT, item_id="upload", subject_id=SUBJECT, kind="conversation",
        content="用户上传：月收入 18000", purpose="授信审核",
        sensitivity_category="financial", source_type="user_upload",
        source_ref="form-9001", actor_id="operator", occurred_at=T0,
    )
    svc.ingest_memory(
        TENANT, item_id="tool", subject_id=SUBJECT, kind="tool_result",
        content="工具返回：征信无逾期", purpose="授信审核",
        sensitivity_category="financial", source_type="tool_return",
        source_ref="credit-api", actor_id="system", occurred_at=at(T0, 1),
    )
    svc.ingest_memory(
        TENANT, item_id="note", subject_id=SUBJECT, kind="summary",
        content="模型笔记：收入稳定，建议通过", purpose="授信审核",
        sensitivity_category="derived_financial", source_type="model_note",
        source_ref="model-note", actor_id="model", occurred_at=at(T0, 2),
        derived_from=[("upload", 1), ("tool", 1)],
    )
    svc.ingest_memory(
        TENANT, item_id="index", subject_id=SUBJECT, kind="index_fragment",
        content="索引片段：收入/征信/通过", purpose="全文检索",
        sensitivity_category="derived_financial", source_type="tool_return",
        source_ref="indexer", actor_id="indexer", occurred_at=at(T0, 3),
        derived_from=[("note", 1)],
    )
    svc.grant_task_access(TENANT, task_id=TASK, item_id="note", granted_at=at(T0, 3))
    svc.grant_task_access(TENANT, task_id=TASK, item_id="upload", granted_at=at(T0, 3))

    # 2) v1 事实影响了决策；随后的更正只能追加新版本。
    svc.record_decision_use(
        TENANT, decision_id="loan-dec-1", item_id="note", version_no=1,
        task_id=TASK, summary="依据 v1 笔记批准授信", actor_id="reviewer",
        decided_at=at(T0, 4),
    )
    svc.correct_memory(
        TENANT, "note", new_content="模型笔记 v2：补充负债后建议复核",
        reason="用户补充负债信息", actor_id="operator", occurred_at=at(T0, 5),
    )

    # 3) 合同最低保留 90 天（全局规则）；用户第 10 天请求删除，全部节点暂缓。
    svc.set_contract_retention(
        TENANT, rule_id="contract-fin", contract_minimum_days=90,
        actor_id="legal",
    )
    request = svc.request_erasure(
        TENANT, subject_id=SUBJECT, requested_by=SUBJECT,
        reason="注销账号", recovery_grace_days=30,
    )["request"]
    early = svc.run_erasure(TENANT, request["request_id"], now=at(T0, 10))
    print("第10天删除链状态：", early["request"]["status"], "（合同保留期未满，全部暂缓）")

    # 4) 第 95 天继续：合同期满进入可恢复隔离期；note 被新到的法律保全冻结。
    svc.apply_hold(
        TENANT, hold_id="hold-313", subject_id=SUBJECT, item_id="note",
        reason="金融监管协查（编号 REG-2026-313）", created_by="compliance",
    )
    mid = svc.run_erasure(TENANT, request["request_id"], now=at(T0, 95))
    print("第95天删除链状态：", mid["request"]["status"], "（可恢复期，note 冻结）")

    # 5) 宽限期满（第 130 天）：其余节点物理清除，note 保持冻结。
    done = svc.run_erasure(TENANT, request["request_id"], now=at(T0, 130))
    cert = done["certificate"]
    print("最终状态：", done["request"]["status"])
    print("证明计数：", json.dumps(cert["counts"], ensure_ascii=False))
    for entry in cert["entries"]:
        line = f"  - {entry['item_id']}：{entry['final_status']}"
        if entry["frozen_reason"]:
            line += f"（{entry['frozen_reason']}）"
        print(line)

    # 6) 审计员还原第 6 天时任务能读到什么：清单/散列可见，默认无原文；
    #    超出自身 scope 的记忆不可见。
    audit = svc.audit_task_readable(
        TENANT, task_id=TASK, at=at(T0, 6),
        scope_item_ids=["note"],
    )
    view = audit["readable"][0]
    print("历史时点当前版本：", view["current_version_at_time"],
          "；版本数：", len(view["versions"]),
          "；默认脱敏：", all(v["redacted"] for v in view["versions"]))

    # 幂等：重复请求返回同一条链。
    again = svc.request_erasure(TENANT, subject_id=SUBJECT, requested_by=SUBJECT)
    print("重复请求幂等：", again["idempotent"],
          "；同一请求：", again["request"]["request_id"] == request["request_id"])


if __name__ == "__main__":
    main()
