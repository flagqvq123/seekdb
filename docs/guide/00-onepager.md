# 00 一页速览

## 这是什么
OceanBase seekdb 2026 数据库大赛 **Round 2**（赛题：实现 MiniVectorDB）。
不是改 seekdb 源码，而是从零实现一个"能跑 SQL + 向量检索 + 事务 + 崩溃恢复"的
单进程轻量数据库，打包成 Docker 镜像交评测。

## 评测怎么进行（一句话版）
平台用我们仓库里的 Dockerfile 构建镜像 → 容器里跑我们的程序 → 通过 **stdin/stdout**
一条条发请求、收响应 → 先跑功能用例（29 个正式用例，**全过才有分**）→ 再跑性能
benchmark（10k 条 × 128 维、1000 次 TopK 查询）→ 再评 `conversation.json` 里的学习证据。

## 分数
- 功能准入：0/1 门槛，任一用例失败 → **总分 0**。
- 性能：40 分，按排名（耗时 60% / 磁盘 I/O 20% / 峰值内存 20%）。
- Agent Prompt：40 分，按 conversation.json 里"你自己的学习证据"排名。
- 提交：仓库 `2026-competition-round2` 分支 + 40 位 commit SHA；**每天最多 2 次**正式提交。

## 我们现在的实现（7 个模块）
~~~
json.rs      手写 JSON 解析/输出（i64 精确、不输出 NaN/Inf）
sql/         SQL 词法 + 递归下降语法分析 → AST
exec/        表达式求值、类型转换、比较/排序、列解析
storage/     Storage trait + 内存实现 + 磁盘 WAL 实现（进行中）
vector/      向量解析校验、L2 距离、float32 语义
fastscan.rs  扁平向量数组上的快速精确 TopK
engine.rs    语句调度：DDL/DML/事务/SELECT（含向量快路径）
main.rs      JSONL 协议主循环（ready/id/ok/error/shutdown）
~~~

## 当前状态（截至 2026-11-11 20:00）
- WSL Linux 构建 + 官方驱动：**公开用例 13/20**，失败 7 个全是"重启后数据没了"。
- 根因：磁盘存储还是内存占位实现（DiskStorage stub），WAL 落盘正在由子智能体实现。
- 独立加固用例：45 个，30 PASS；失败集中在持久化（预期）+ 已被我们修掉的两个真 bug。
- Docker 镜像：Dockerfile 已写，待存储落地后做端到端验证。
