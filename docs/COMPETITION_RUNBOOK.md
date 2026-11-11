# MiniVectorDB 2026 Round2 运行手册

本文件是 minivectordb crate 的构建、协议自测、Docker 封装、提交检查与性能
benchmark 的单一入口说明。协议权威定义见 `STDIO_PROTOCOL.md`，官方公开用例说明见
`mysqltest/README.md`，赛题原文见 `docs/COMPETITION_SPEC.md`。

## 1. 产物与目录

| 路径 | 用途 | 负责人 |
| --- | --- | --- |
| `minivectordb/` | Rust crate（纯 std，无第三方依赖） | Lead + 各模块负责人 |
| `Dockerfile` | 多阶段构建，默认 stdio JSONL 前台进程 | harness |
| `.dockerignore` | 把构建上下文限制在 minivectordb crate | harness |
| `tools/verify/run_public.py` | 跨平台公开用例 runner（binary / image） | harness |
| `tools/verify/wsl_run.sh` | WSL 内构建 Linux release 并跑公开用例 | harness |
| `tools/verify/submission_lint.py` | 提交前检查（分支/SHA/Dockerfile/conversation.json/20-20 公开用例门槛） | harness |
| `tools/verify/clean_clone_check.py` | 干净导出 commit -> docker build -> 判题参数跑协议样例 | harness |
| `tools/verify/benchmark.py` | 3.3 节形状的性能 benchmark | harness |
| `docs/SUBMISSION_CHECKLIST.md` | 从合并分支到记录 40 位 SHA 的提测步骤 | harness |
| `agent/conversation.json` | Agent prompt 材料（结构骨架，学生填真实证据） | 用户/Lead |
| `protocol-results/` | 所有测试产物（已被 .gitignore 忽略） | - |

正式提交要求：`2026-competition-round2` 分支上的同一次 commit 必须同时包含源码、
`Dockerfile` 和 `agent/conversation.json`（<= 128 KiB、UTF-8、合法 JSON 对象）。

## 2. 环境前提

- Windows 本地：Rust stable（cargo/rustc）+ Python 3.9+（本机 3.14 已验证）。
- Docker Desktop（Linux 容器模式），用于构建和评测镜像。
- WSL Ubuntu（已装 Linux Rust 工具链和 python3），用于 Linux 二进制验证。
- 不需要网络也能跑通公开用例；仅 `docker build` 首次拉取基础镜像需要网络。

## 3. 本地构建（Windows / Linux）

```bash
cd minivectordb
cargo build --release
cargo test
```

Windows 可执行文件：`minivectordb/target/release/minivectordb.exe`；
Linux 为同目录下的 `minivectordb`。crate 无第三方依赖，`cargo build --release --locked` 可离线复现。

## 4. 公开用例验证（跨平台 runner）

`tools/verify/run_public.py` 包装 `protocol_check/run.py`，统一为一套命令行：

- Linux/WSL 上自动委托官方 `protocol_check/run.py`（与判题器相同的 transport/超时语义）。
- Windows 上使用内置便携 transport（reader 线程代替 selectors），并复用
  `protocol_check` 的 config/manifest/cases/`_assertions.py`，断言完全一致。
- 每次运行自动生成带时间戳目录：`protocol-results/public-<binary|image>-<YYYYmmdd-HHMMSS>/`，
  内含每个用例的 `<case>.stderr.log`、`cases.json`、`summary.json`、`summary.md`。
- 控制台输出 PASS/FAIL 表格 + 失败用例 stderr 摘要；退出码 0=全部通过，1=有失败。

```bash
# 全部 20 个公开用例（Windows 构建产物）
python tools/verify/run_public.py --binary minivectordb/target/release/minivectordb.exe

# 只检查握手
python tools/verify/run_public.py --binary minivectordb/target/release/minivectordb.exe --handshake-only

# 指定用例 / 指定输出目录
python tools/verify/run_public.py --binary ... --case storage_restart --case sql_crud
python tools/verify/run_public.py --binary ... --output protocol-results/my-run

# 直接测 Docker 镜像（判题器同款容器参数：2 CPU / 2 GiB / 128 pid / --read-only / volume /data）
python tools/verify/run_public.py --image minivectordb:round2
```

注意：`--binary` 会以 `[binary, --data-dir, {data_dir}]` 启动进程（占位符由 runner 替换为临时目录；
`--binary-arg` 可追加参数）。image 模式使用命名 volume，与判题器一致。

## 5. WSL：Linux release 构建 + 全部公开用例

```powershell
wsl -d Ubuntu -e bash /mnt/d/4dsh/seekdb-competition/seekdb/tools/verify/wsl_run.sh
```

脚本行为：检查 cargo/python3 -> `cargo build --release --locked`（日志写
`protocol-results/wsl-build-<stamp>.log`）-> 调用 run_public.py（Linux 上自动委托
官方 protocol_check/run.py）。额外参数原样转发，例如 `--handshake-only`。
在 WSL 内也可直接 `bash tools/verify/wsl_run.sh --case sql_crud`。

## 6. Docker 构建与运行

构建（在仓库根目录；.dockerignore 使上下文只有 15 个文件，实测约 1 KiB、镜像约 115 MiB）：

```bash
docker build -t minivectordb:round2 .
```

镜像结构：`rust:1.90-slim-bookworm` 多阶段编译（`--locked`）-> `debian:bookworm-slim`
运行阶段只含二进制；非 root uid 10001；`/data` 在镜像内已 chown，命名 volume 首次挂载会继承属主；
`ENTRYPOINT` 直接运行二进制（无 shell 包装、无 SQL 提示符、无网络监听），默认数据目录 `/data`。
基础镜像按 tag 锁定（2026-11-11 实测 digest：rust `sha256:64232e65...`、debian
`sha256:734a72b3...`）；构建阶段不装任何依赖、不联网，`Cargo.lock` + `--locked` 锁定 crate 依赖。

判题器同款运行方式（等价于 STDIO_PROTOCOL.md 第 7 节；Windows 可用 WSL/Git Bash 或直接
`python tools/verify/run_public.py --image`）：

```bash
test_root=$(mktemp -d)
mkdir "$test_root/data"; chmod 777 "$test_root/data"
cat > "$test_root/requests.jsonl" <<'JSONL'
{"id":1,"op":"execute","sql":"CREATE TABLE docs (id BIGINT PRIMARY KEY, content VARCHAR(16));","search_mode":"exact"}
{"id":2,"op":"execute","sql":"INSERT INTO docs VALUES (1, '你好');","search_mode":"exact"}
{"id":3,"op":"execute","sql":"SELECT id, content FROM docs ORDER BY id;","search_mode":"exact"}
{"id":4,"op":"shutdown"}
JSONL
timeout 180s docker run --rm -i --network=none --cpus=2 --memory=2g --memory-swap=2g \
  --pids-limit=128 --read-only --cap-drop=ALL --security-opt=no-new-privileges \
  --mount "type=bind,src=$test_root/data,dst=/data" minivectordb:round2 \
  < "$test_root/requests.jsonl"
```

期望 stdout 恰好 5 行：`{"type":"ready"}`、三条成功响应（第 2 条 affected_rows=1，第 3 条
columns=[id, content]、rows=[[1, "你好"]]）、`{"id":4,"ok":true}`，退出码 0。

只测镜像握手：`python tools/verify/run_public.py --image minivectordb:round2 --handshake-only`。

### 6.1 干净 commit 复核（模拟平台构建）

```bash
python tools/verify/clean_clone_check.py                 # 导出 HEAD 已提交文件 -> build -> 跑 §7 样例
python tools/verify/clean_clone_check.py --archive-full  # 导出整棵树（更慢、更严格）
python tools/verify/clean_clone_check.py --from-worktree  # 仅调试：用当前工作区，不证明可提交性
```

该脚本用 `git archive` 只导出该 ref 的已提交内容（不含本地 `target/`、二进制和未提交修改），
因此能发现"Dockerfile/源码没进 commit""依赖本地预编译产物""构建上下文过大"等问题。
预期输出 `RESULT: PASS` 和 `COMMIT_SHA=...`。首次构建需基础镜像缓存在本机（或可联网）。

注意：`submission_lint.py` 的 docker build 使用当前工作区，因此队友还有未提交改动时可能因瞬时
编译错误失败；正式提测前应在干净、可编译的工作区（或已提交的最终 commit）上跑。

## 7. 提交前检查（submission lint）

```bash
python tools/verify/submission_lint.py                 # 完整检查（会 docker build --no-cache）
python tools/verify/submission_lint.py --skip-build     # 不构建镜像，只查分支/SHA/文件/JSON/git
python tools/verify/submission_lint.py --use-cache      # 允许构建缓存
```

检查项：当前分支（或 detached HEAD 可达分支）必须是 `2026-competition-round2`；
`Dockerfile` 存在、多阶段、无本地预编译 COPY、使用 `--locked`；`.dockerignore` 存在；
构建上下文 < 5 MiB；镜像 entrypoint 不是 shell；只读根文件系统 + bind /data 下握手/退出正常；
`agent/conversation.json` 存在、<=128 KiB、UTF-8、合法 JSON 对象；git 工作区干净；
HEAD 同时包含 Dockerfile、crate 源码和 conversation.json；最后打印 40 位 `COMMIT_SHA=...`。

lint 还会用刚构建的镜像跑完整 20 个公开用例作为功能门槛（`--skip-public-cases` 仅用于
本地临时跳过），并输出分组 checklist 与 `SUBMISSION_READY=YES/NO`。

当前仓库提醒：工作区在 `round2-impl` 分支且存在未提交文件，lint 会因
`branch`/`git_clean`/`head_files` 失败——正式提交前 Lead 需切到
`2026-competition-round2` 并创建包含全部产物的单个 commit。详见
`docs/SUBMISSION_CHECKLIST.md`。

## 8. 性能 benchmark（3.3 节形状）

```bash
python tools/verify/benchmark.py --image minivectordb:round2
python tools/verify/benchmark.py --binary minivectordb/target/release/minivectordb.exe
python tools/verify/benchmark.py --image minivectordb:round2 \
  --records 2000 --queries 200 --batch 100 --dim 128 --k 10 --seed 20261111
```

负载：10,000 条 128 维记录，每批 100 条 + 每批显式 BEGIN/COMMIT；1,000 条 exact TopK
（K=10，一半带 `WHERE tag = n` 过滤）。输出 `benchmark.json` 关键字段：
`T_write_ms`、`T_query_ms`、`peak_mem_mb`、`io_bytes_approx`（附
`io_bytes_source`/`peak_mem_source`/`data_dir_bytes`/`query_qps`/`write_rows_per_second`）。

口径说明：镜像模式用 bind mount 让 host 能看到 /data 字节数，峰值内存用
`docker stats` 采样（cgroup 口径，含页缓存）；Linux 原生模式用 `/proc/<pid>/io`
与 `VmHWM`；Windows 原生模式用进程 peak working set，`io_bytes_approx` 退化为
/data 目录文件字节数（下界）。这些是本地近似值，正式平台用 cgroup v2 memory.peak/io.stat。

## 9. 常见失败定位

| 现象 | 检查方向 |
| --- | --- |
| `expected ready, got ...` | 首行必须恰好是 `{"type":"ready"}`，且已在等待输入前 flush |
| `invalid JSON response` | stdout 混入日志/提示符/空行；日志必须写 stderr |
| `JSON response timeout` | 没有主动 ready、未逐条 flush、进程卡死或死锁 |
| `database exited before response` | 容器入口/依赖/权限问题，看 `<case>.stderr.log` |
| `response id differs` / `missing boolean ok` | id 类型被改（1 vs "1"）、ok 不是布尔 |
| `extra stdout or nonzero exit after shutdown` | shutdown 响应后继续输出或非 0 退出 |
| `table_not_found` after `--restart` | 持久化/恢复失败；看 `--restart` 用例的 stderr 与 `/data` 内 CURRENT/ckpt/wal 文件 |
| Windows 原生启动报 `os error 5` 打不开数据目录 | 某些主机的 `%TEMP%` 不允许新 WAL 建文件；runner 已改用仓库内 `protocol-results/tmp`，或改用 WSL/Docker |
| `docker build` 拉取失败 | Docker Hub 连接被重置时重试；镜像已缓存后无需网络 |
| 只读根文件系统下崩溃 | 程序只能写 /data；临时文件不要写 /tmp 之外的根目录 |
| /data 权限拒绝 | 命名 volume 会继承镜像 /data 属主；bind mount 需 777（判题器示例亦如此） |

## 10. 当前状态与已知限制（2026-11-11 21:20 更新）

- Rust crate 可编译；`cargo build --release --locked` 通过，`cargo test` 32 passed / 0 failed。
- 20 个公开用例：**20/20 PASS**（Windows 原生约 1.1s；Docker 镜像带容器启动开销约 20s）。
  WAL 持久化落地后，此前 7 个 `--restart` 失败用例（storage_restart 等）已全部通过。
- 已用 `clean_clone_check.py --ref HEAD` 对 commit `b0c0f78` 完成干净复核：`git archive`
  只导出已提交内容 -> docker build PASS（上下文 394 B）-> 判题参数跑 §7 样例 PASS；
  再用该镜像跑公开用例 **20/20 PASS**（18.2 s）。
- Dockerfile 在本轮复核中发现并修复一处真实缺陷：`ARG RUNTIME_IMAGE` 原本写在第一个
  `FROM` 之后，导致第二个 `FROM` 的镜像名为空、构建失败。现在两个全局 ARG 都在第一个
  `FROM` 之前；`clean_clone_check.py` 从已提交树重建镜像并通过 §7 协议样例。
- 构建上下文 384 B（仅 minivectordb 的 Cargo.toml/Cargo.lock/src 共 15 个文件）；镜像约 115 MiB；
  非 root uid 10001；只读根文件系统 + 命名 volume / 777 bind mount 下均可写 /data。
- Windows 便携 transport 与官方 selectors transport 的唯一已知差异：请求发送瞬间未读取的
  "多余 stdout" 可能延迟到 shutdown 才被判定（官方实现会在每次发送前 poll）；对正常实现无影响。
- 本机部分 Windows 主机的 `%TEMP%` 不允许新 WAL 建文件（os error 5）；runner 已默认改用
  仓库内 `protocol-results/tmp`，因此原生验证不受影响。
- 协议仍以题包 JSONL 为准。赛题文档有一句暗示后续协议可能调整，若上游更换驱动需重跑
  `run_public.py --runner portable` 适配。
- 提交分支尚未切到 `2026-competition-round2`；`agent/conversation.json` 已创建结构骨架
  （TODO 占位，待学生填真实证据）。lint 当前仅剩 `branch`/`git_clean`/`head_files`
  失败，属提交动作本身，见 `docs/SUBMISSION_CHECKLIST.md`。

## 11. 下次恢复从哪继续

1. 把当前工作区（含本轮 Dockerfile 修复与验证脚本）提交到 `round2-impl`，再合并/切到
   `2026-competition-round2`（见 `docs/SUBMISSION_CHECKLIST.md` 第 1-2 步）。
2. `python tools/verify/run_public.py --binary ...` 与 `--image ...` 复核 20/20。
3. `python tools/verify/clean_clone_check.py --ref HEAD`（提交后跑）确认干净树可构建。
4. 由学生填写 `agent/conversation.json` 真实证据后跑完整 `python tools/verify/submission_lint.py`，
   取得 `SUBMISSION_READY=YES`、`COMMIT_SHA=...` 后按检查清单提测。
