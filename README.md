# 智能体记忆分级治理

记忆摄取、来源链、保留策略、法律保全和可验证删除的领域服务。用户上传内容、
工具返回与模型笔记在写入时即分开定级并固化来源；派生摘要与搜索索引沿依赖图
接受统一的导出、删除与审计。

## 目录

- `domain/contract.json`：实体、状态、事件类型和关键业务规则。
- `domain/policies.json`：可被程序读取的策略样例（来源绑定、保全优先、
  只追加版本、幂等删除、保留裁决、分阶段删除、审计最小知情）。
- `examples/events.json`：按业务发生时间排列的事件样例。
- `examples/demo_e2e.py`：端到端场景演示（保留暂缓 → 隔离 → 保全冻结 → 清除证明）。
- `memorygovernance/`：治理服务实现（仅依赖 Python 标准库与 SQLite）。
  - `store.py`：SQLite 模式与存储，全部表带租户，事件只追加。
  - `service.py`：摄取/派生/更正、决策留痕、保留裁决、法律保全、
    任务授权、依赖图闭包、导出、分阶段删除、清除证明、历史时点审计。
- `tools/validate_contract.py`：使用 Python 标准库和 SQLite 内存表验证资料一致性。
- `tests/test_memory_governance.py`：19 个端到端测试，覆盖每条业务规则。

## 治理语义

| 需求 | 实现 |
| --- | --- |
| 写入保存来源、租户、目的、敏感类别 | `ingest_memory` 必填这些字段，`source_type` 限定四类来源 |
| 派生关系可追溯 | `derivation_edges` 绑定到全部直接来源的**具体版本**；删除/导出沿下游闭包 |
| 保留期限三方裁决 | `resolve_retention`：有效期限 = max(用户选择, 合同下限)；法律保全另行阻断 |
| 更正只追加不覆盖 | 新版本号追加，旧版本标记 superseded；`decision_used` 的版本永久只读 |
| 导出/删除覆盖派生与任务引用 | 下游闭包 + 隔离时撤销 `task_grants`，证明含引用撤销计数 |
| 保全节点冻结并说明原因 | 步骤置 `frozen`，保存 `hold_id` 与原因，其余节点继续清除；解除后重跑续跑 |
| 分阶段可恢复删除 | 阶段一隔离（原文移至 `quarantined_content`，可 `restore_erasure`）；宽限期满物理清除 |
| 幂等与中断续跑 | 同一租户+主体的重复请求返回同一清除链；`run_erasure` 只推进未完成步骤 |
| 无原文证明清单与计数 | `erasure_certificates` 只含标识、敏感类别、版本散列、状态、原因、计数；签发时做原文泄露自检 |
| 历史时点审计 | `audit_task_readable` 按授权/撤销/版本更替/隔离时点还原清单；默认仅散列，`content_access` 由授权层控制；`scope_item_ids` 限定审计人可见范围；已清除内容不可借审计复活 |

## 构建

```bash
python3 -m compileall -q .
```

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 资料校验

```bash
python3 tools/validate_contract.py
```

## 端到端演示

```bash
python3 examples/demo_e2e.py
```

所有命令都在项目根目录执行，不需要另行启动数据库、缓存或其他服务
（SQLite 文件模式时只需传入一个路径）。
