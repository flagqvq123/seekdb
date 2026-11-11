# Round 2（MiniVectorDB）实施计划与状态

## 赛题要点
- 在 Docker 容器中实现单进程嵌入式数据库（SQL + 向量检索 + 事务 + 持久化），
  通过 stdin/stdout 的 JSON Lines 协议与判题器交互（见 STDIO_PROTOCOL.md）。
- 数据目录 /data；容器 2 CPU / 2 GiB / 128 pids / 只读根文件系统 / 无网络。
- 公开用例 20 个（mysqltest/cases，95 条 SQL），正式用例 29 个；全部通过后进入性能与 Agent 评分。
- 判题流程：握手 {"type":"ready"} → 逐条 execute → shutdown；每个响应必须单行、flush、id 原样回传、ok 布尔。

## 技术选型
- Rust（纯 std，零第三方 crate）：单二进制、启动快、无离线构建依赖风险。
- 代码位于 minivectordb/；分支 2026-competition-round2 作为基线，实现提交在 round2-impl 分支。

## 架构（minivectordb/src）
- json.rs       手写 JSON 解析/序列化（i64 精确、浮点非 NaN/Inf）
- sql/          lexer + 递归下降 parser（CREATE TABLE / CREATE VECTOR INDEX / INSERT / UPDATE / DELETE / SELECT / BEGIN / COMMIT / ROLLBACK，其余返回 unsupported_sql）
- exec/         表达式求值、类型强制、比较/排序、列解析、L2_DISTANCE
- storage/      Storage trait；MemStorage（参考实现）；DiskStorage（WAL + 崩溃恢复 + checkpoint，进行中）
- vector/       向量解析与校验、L2 距离、AnnIndex trait、FlatIndex
- ann.rs        高性能 ANN 索引（HNSW/IVF，进行中）
- engine.rs     语句调度、DDL/DML、事务控制、语句级原子性、SELECT 执行
- main.rs       JSONL 协议主循环（ready/id/ok/error、flush、shutdown）

## 当前状态（基线）
- WSL Linux 构建 + 官方 protocol_check 驱动可跑通：13/20 公开用例 PASS。
- 失败 7 个均为 --restart 类（storage_restart、storage_mutations、storage_multiple_tables、
  vector_index_maintenance、transaction_failed_insert、transaction_autocommit_atomic、
  transaction_shutdown_rollback）：根因是 DiskStorage 仍是内存 stub，重启丢数据，由存储任务修复。

## 分工
- Lead：json/sql/exec/engine/main、集成、最终验收与提交
- storage-engine：DiskStorage（WAL、fsync、恢复、checkpoint、大表流式 scan）
- ann-index：ANN 索引（召回率/延迟指标）
- harness：Dockerfile/.dockerignore、跨平台验证脚本、Runbook
- verification：Windows 原生 JSONL 客户端 + 隐藏考点加固用例

## 里程碑
1. 存储落盘 → 20/20 公开用例 PASS（预计今天）
2. Docker 镜像 + 只读根文件系统验证
3. 隐藏考点加固（非法向量、崩溃恢复、BIGINT 边界、原子性、ANN 召回）
4. 性能优化（TopK 堆、索引接入、批量写、冷启动）
5. 提交：仓库 URL + 镜像说明 + 验证报告

## 本地验证命令
- Windows 技能：cargo test（minivectordb 目录）
- Linux 全量公开用例（WSL）：
  bash minivectordb/tools/wsl_check.sh
- 官方驱动单点：
  python3 protocol_check/run.py --command '["<binary>","--data-dir","{data_dir}"]' --output <新目录>
