# 失败复现矩阵（提测失败 -> 复现 -> 修复 -> 复测）

> 规则：每一次"修完再提测"之前，必须能回答：**旧版本会失败、新版本通过，且失败点被最小用例覆盖**。
> 旧版本通过 git worktree 检出提测时的 commit 单独构建，与当前版本跑同一脚本对比。

| # | 平台失败类别 | 假设机制 | 复现命令 | 旧版本（提测版） | 新版本 | 判定 |
|---|---|---|---|---|---|---|
| R1 | 功能结果不符合要求 | 显式事务 COMMIT 后向量缓存未失效，ORDER BY 用过期距离 | `python tools/verify/commit_cache_repro.py <binary>`（worktree 检出 de03d1508 构建后对比） | 返回 `[[1,10.0],[2,1.0],[3,2.0]]`（顺序错误） | 返回 `[[2,1],[3,2],[1,10]]` | 已复现 -> 已修复 |
| R2 | 功能结果不符合要求 | BIGINT 有符号下界 `-9223372036854775808` 被词法器按 i64 解析溢出 | `official_like_cases.py --filter bigint_signed_min` | `ok:false syntax_error integer literal out of range` | 插入/查询正常 | 已复现 -> 已修复 |
| R3 | 功能结果不符合要求 | DDL/SQL 变体（反引号、表级主键、ENGINE/CHARSET、COLLATE、LIMIT offset、ORDER BY 序号、反斜杠转义）直接语法错误 | `official_like_cases.py --category high_risk` | 9 个 uncertain 失败中 7 个是语法/语义不符 | 全部通过（剩 2 个 affected_rows 语义分歧） | 已复现 -> 已修复 |
| R4 | JSON response timeout | shutdown 触发全量 checkpoint，随数据量线性增长 | `large_data_stress.py <binary> 200000`（旧版 flush 会 checkpoint） | 20 万行 shutdown 603ms；数据越大越慢 | shutdown 23-59ms（WAL 已 fsync，不重写） | 机制已复现 -> 已消除 |
| R5 | JSON response timeout | 表超缓存预算时每次查询先加载 256MiB 再放弃；大表查询全表扫描 + 全量物化结果集 | `cache_budget_demo.py` / `large_data_stress.py` | 缓存不可用时 6.08ms/查询；1M 行主键点查 2.25s | 缓存不可用时直接回退（有界 top-k）；主键点查 0.2ms | 机制已复现 -> 已消除 |
| R6 | 待观察 | 官方参考实现的 UPDATE affected_rows 可能用 changed-rows | `official_like_cases.py --category high_risk --filter update_affected_rows` | 当前返回匹配行数（同值=1，3 匹配 2 变化=3） | 同左（未改） | **未决**：语义分歧，需一次实验性提测验证 |

## 仍未排除的风险（无输入无法复现的部分）
1. 官方 29 个用例的输入不可见，以上是"机制级复现"，不等于"该用例级复现"。
2. 超时也可能来自超大结果集序列化（GB 级 SELECT 响应），本地未构造出同类负载。
3. VARCHAR 超长、NULL、DROP TABLE、SET vector_mode 等写法是否出现在正式用例中未知。

## 复现所需环境（本机可跑）
- `git worktree add <tmp> <submitted-sha>` -> `cargo build --release` -> 用同一脚本跑旧/新二进制。
- 所有脚本在 `tools/verify/`，报告 JSON 在 `.tmp/`，每条失败都打印期望/响应/stderr 尾部。
