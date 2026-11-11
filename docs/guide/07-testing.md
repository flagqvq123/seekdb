# 07 测试与验证

## 1. 三层验证（缺一不可）
| 层 | 工具 | 跑在哪 | 回答什么问题 |
|---|---|---|---|
| 官方驱动 | `protocol_check/run.py` | WSL/Linux（依赖 Unix API） | 协议 + 20 个公开用例是否真的过 |
| 自研回归 | `tools/verify/run_all.py` | Windows 直接跑 | 快速迭代 + 隐藏考点加固（45 个用例） |
| 镜像端到端 | `docker build/run` | Linux 容器 | 提交形态是否真的能跑（只读根、/data、无网络） |

## 2. 常用命令

**官方公开用例（WSL，Linux 原生构建 + 官方驱动）**
~~~bash
wsl -d Ubuntu -e bash /mnt/d/4dsh/seekdb-competition/seekdb/minivectordb/tools/wsl_check.sh
~~~

**Windows 快速回归（不用 WSL）**
~~~powershell
cd D:\4dsh\seekdb-competition\seekdb
cargo build --manifest-path minivectordb/Cargo.toml
python tools/verify/run_all.py --binary minivectordb/target/debug/minivectordb.exe --xfail-group durability --xfail-public-persistence
~~~
（`--xfail-*` 是"已知会失败"的豁免；存储落地后应去掉这些参数，全部转绿才算真过。）

**单独跑某个用例组**
~~~powershell
python tools/verify/edge_cases.py --binary <exe> --group atomicity
~~~

**单元测试**
~~~powershell
cd minivectordb; cargo test
~~~

**Docker 镜像（待存储落地后）**
~~~bash
docker build -t minivectordb:dev .
docker run --rm -i --network=none --read-only --mount type=bind,src=/tmp/mvdb-data,dst=/data minivectordb:dev
~~~

## 3. 怎么判断"真的过了"（重要）
- `run_public.py` / `wsl_check.sh` 的 `result.json` 里 `passed=true` 且 **0 个 FAIL** 才叫过；
  有 XFAIL 的"PASS"只代表"除了已知问题都过了"。
- 每个用例输出 PASS 不代表语义对；要看它断言了什么（例如是否比较了行顺序/并列规则）。
- 最可靠的证据是：**同一个用例，改代码前后各跑一次，行为差异符合预期**。这就是
  conversation.json 里"独立验证"维度要的证据。

## 4. 用例清单（自研 45 个，按组）
| 组 | 覆盖 |
|---|---|
| protocol | 握手、id 类型回显、空结果 columns、错误后继续、shutdown 干净退出 |
| invalid_vector | NaN/Infinity/维度/溢出/空向量/非数字 |
| atomicity | 多行 INSERT 中途失败（自动提交/显式事务）、事务回滚范围 |
| sql | 比较/AND/ORDER BY 并列/LIMIT 0/多表/unicode/BIGINT 极值/不支持语句 |
| vector | float32 语义、过滤后 TopK、并列、索引交错维护、ann 模式 |
| spec | VARCHAR 字符数边界、主键更新报错、嵌套事务报错 |
| durability | 强杀保留已提交、未提交不残留、COMMIT 后立即强杀、恢复中再强杀、多次重启 |
| large_data | 2 万行 × 128 维批量写入与查询（后续加大到超内存预算） |

## 5. 当前结果快照（2026-11-11 20:00）
- 官方公开用例：13/20（7 个 `--restart` 失败 = 磁盘存储未落地，预期）。
- 自研 45 用例：30 PASS；durability 全组失败（同上）；其余为已修复问题的回归。
- 已抓出的真实缺陷：①语句回滚范围过大（已修）②VARCHAR 长度未校验（已修）。
