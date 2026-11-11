# 09 进度快照与下一步

快照时间：2026-11-11 20:00（Asia/Shanghai）

## 1. 分支与提交
| 分支 | 用途 | 状态 |
|---|---|---|
| `2026-competition-round2` | **官方基线 + 最终提交分支** | 与上游一致，未改动 |
| `round2-impl` | 日常开发分支 | 已有 2 个提交并推送到 fork |
| `agent/conversation.json` | 学习证据材料 | 未创建 |

最终提交动作（待做）：把 `round2-impl` 的成果并入 `2026-competition-round2`，
同一次 commit 包含 源码 + Dockerfile + agent/conversation.json，记录 40 位 SHA。

## 2. 功能进度
| 能力 | 状态 | 证据 |
|---|---|---|
| JSONL 协议（握手/执行/关闭） | ✅ | 官方握手用例 PASS |
| SQL 子集（建表/增删改查/WHERE/ORDER BY/LIMIT） | ✅ | 13 个公开用例 PASS |
| 向量语义 + 精确 TopK + 过滤 TopK | ✅ | vector_* 公开用例 PASS |
| 事务（提交/回滚/语句原子性/关闭回滚） | ✅（未含重启） | transaction_commit/rollback PASS |
| 持久化 + 崩溃恢复 | 🚧 | 7 个 --restart 用例失败 |
| CREATE VECTOR INDEX 维护 | 🚧（元信息已有，重启后失效） | vector_index_maintenance 失败于重启 |
| Docker 镜像 | 🚧 Dockerfile 已写 | 未端到端验证 |
| 性能（1000 次 TopK 查询） | ⏳ 快路径已实现，未接线/未测 | — |
| conversation.json | ⏳ 待你参与 | — |

## 3. 剩余工作（按优先级）
1. **P0 磁盘 WAL 存储**（storage-engine）：重开恢复、强杀恢复、checkpoint、流式扫描。
   完成后去掉 xfail，验收标准：官方公开用例 **20/20**，加固用例 durability 全绿。
2. **P0 快路径接线 + 性能实测**（Lead）：用 fastscan 替换引擎内置 TopK 循环，
   测 10k×128 / 1000 查询耗时、峰值内存，与朴素实现对比。
3. **P0 Docker 镜像端到端**（harness）：干净上下文构建、只读根 + /data、无网络、
   官方驱动 20/20。
4. **P1 隐藏考点加固**（verification 的结果 + 我修）：VARCHAR 边界、非法向量、
   恢复中再强杀、大数据超内存预算。
5. **P1 提交检查 + conversation.json**（harness + 你 + 我）：分支名/commit/文件体积/
   JSON 合法性/材料真实性。
6. **P2 性能调优**：批量写 fsync 策略、向量缓存预算、TopK 参数。

## 4. 风险
| 风险 | 影响 | 缓解 |
|---|---|---|
| 恢复实现有漏洞 | 功能准入 0 分 | 故障注入用例 + 恢复中再杀 + 多次重启 |
| 大数据超内存 | 资源测试失败 | 流式扫描 + 缓存预算 + 不用全量 Vec |
| 协议理解偏差（文档提到"不再用 JSON 封装"，但题包仍是 JSONL） | 全部用例失败 | 以题包为准；保留协议适配余量；关注上游分支新提交 |
| 截止前没时间做 conversation.json | 丢 40 分 | 现在就开始留证据，最后只做结构化 |
| 每天仅 2 次正式提交 | 试错成本高 | 本地全绿再提交；首次提交后按反馈修 |

## 5. 下次恢复指引（关机后从这里继续）
1. 看本文件 + `docs/COMPETITION_PLAN.md`；
2. 拉分支：`git fetch && git switch round2-impl`（或直接在现有工作区继续）；
3. 先跑一遍：`cargo test`（Windows）+ `bash minivectordb/tools/wsl_check.sh`（WSL）；
4. 看 `tools/verify/run_all.py` 的 XFAIL 列表，确认哪些是"已知未完成"；
5. 继续 P0 第 1 项（存储），完成后按第 3 节顺序推进。
