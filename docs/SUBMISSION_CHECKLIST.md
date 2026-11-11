# MiniVectorDB 正式提测检查清单

目标：把当前可编译版本合入 `2026-competition-round2` 分支，锁定一次 40 位 commit SHA，
保证平台从该 commit 的干净 clone 能构建镜像、启动容器并通过协议与 20 个公开用例。
平台每天最多 2 次正式提交；环境预检不占额度、但预检通过后正式提测会锁定 commit 与镜像。

## 0. 提测前的完成标准（全部满足才动平台）

| 项目 | 命令 | 期望 |
| --- | --- | --- |
| 编译 | `cd minivectordb && cargo build --release --locked && cargo test` | 0 失败 |
| 公开用例（原生） | `python tools/verify/run_public.py --binary minivectordb/target/release/minivectordb.exe` | 20/20 PASS |
| 公开用例（镜像） | `python tools/verify/run_public.py --image minivectordb:round2` | 20/20 PASS |
| 干净 clone 构建+协议 | `python tools/verify/clean_clone_check.py` | RESULT: PASS |
| 提交前检查 | `python tools/verify/submission_lint.py` | SUBMISSION_READY=YES |
| Agent 材料 | `agent/conversation.json` | <=128 KiB、UTF-8、合法 JSON 对象、真实材料已填 |

注意：`run_public.py` 在 Linux/WSL 自动使用官方 `protocol_check/run.py`；Windows 用等价便携
transport。`submission_lint.py` 内置 20/20 公开用例门槛（`--skip-public-cases` 可临时跳过，但正式提测
前必须跑完整版）。

## 1. 收尾开发分支（round2-impl）

```bash
cd <repo>
cargo test --manifest-path minivectordb/Cargo.toml
python tools/verify/run_public.py --binary minivectordb/target/release/minivectordb.exe
```

确认没有把生成物提交进仓库：`protocol-results/` 已在 .gitignore 中，`minivectordb/target/` 不应被
`.dockerignore` 之外的提交包含（Dockerfile 只 COPY 源码，不依赖 target）。

## 2. 合并到 2026-competition-round2

```bash
git fetch origin
git switch 2026-competition-round2
git pull --ff-only origin 2026-competition-round2
git merge --no-ff round2-impl          # 或 rebase，按团队约定
git status                             # 必须干净
git log --oneline -5
```

合并后必须确认以下文件都在同一分支/同一最终 commit 中：

- `minivectordb/`（Cargo.toml、Cargo.lock、src/；Cargo.lock 必须与 Cargo.toml 一致）
- `Dockerfile`、`.dockerignore`
- `agent/conversation.json`
- 说明文档：`docs/COMPETITION_RUNBOOK.md`、`docs/SUBMISSION_CHECKLIST.md`、`STDIO_PROTOCOL.md`

```bash
git ls-files Dockerfile .dockerignore agent/conversation.json minivectordb/src | head
```

## 3. 填写 agent/conversation.json

当前仓库里是 `agent/conversation.json` 结构骨架（所有 TODO 占位），必须由学生填入真实
prompt、交互、实验输出与复盘，且：

- UTF-8、顶层为 JSON 对象、<= 131072 字节、无重复键、无 NaN/Infinity、无密钥/token；
- 每个 > 0.5 分档的实例同时给出 student_evidence 与 result_evidence，引用用 RFC6901
  JSON pointer + <=240 字符精确原文；
- 未执行的内容标为 planned，不得写成已验证；
- 合成材料必须标 `synthetic: true`。

```bash
python - <<'PY'
import json, pathlib
p = pathlib.Path("agent/conversation.json")
raw = p.read_bytes()
assert len(raw) <= 131072, (len(raw), "too large")
obj = json.loads(raw.decode("utf-8"))
assert isinstance(obj, dict)
assert "TODO" not in raw.decode("utf-8"), "replace skeleton placeholders"
print("conversation.json OK:", len(raw), "bytes,", len(obj), "top-level keys")
PY
git check-ignore -v agent/conversation.json   # 必须无输出（不能被忽略）
```

## 4. 提交前 lint（含 20/20 公开用例门槛）

```bash
python tools/verify/run_public.py --image minivectordb:round2   # 可先单独确认 20/20
python tools/verify/submission_lint.py
```

lint 会依次做：分支检查 -> 40 位 SHA -> Dockerfile/`.dockerignore` 静态检查 -> conversation.json
结构与大小 -> HEAD 是否已包含全部产物 -> git 工作区干净 -> `docker build --no-cache` 并从
构建日志核对上下文 <5 MiB -> 镜像 entrypoint 非 shell/非 root -> 只读根文件系统 + bind /data
冒烟 -> 用该镜像跑 20 个公开用例（门槛）-> 输出 checklist、`COMMIT_SHA=...`、
`SUBMISSION_READY=YES/NO`。

全绿前不要提交平台。若 `git_clean` 报未提交文件，先 `git add`/`git commit` 再重跑。

## 5. 干净 clone 复核（模拟平台构建）

```bash
python tools/verify/clean_clone_check.py --prepull
# 或指定最终提交：
python tools/verify/clean_clone_check.py --ref 2026-competition-round2
```

该脚本从 git 导出该 ref 的已提交文件到临时目录（不含本地 target/、不上传未提交文件），
docker build 后按判题参数（`--network=none --read-only --cpus=2 --memory=2g --pids-limit=128`）
跑 STDIO_PROTOCOL 第 7 节样例并逐行断言。预期输出 `RESULT: PASS` 与 `COMMIT_SHA=...`。

## 6. 创建提交并推送

```bash
git add minivectordb Dockerfile .dockerignore agent/conversation.json tools/verify docs
git status --short                # 只应看到预期文件
git commit -m "feat(minivectordb): round2 submission (storage, vector, verification tools)"
git push origin 2026-competition-round2
git rev-parse HEAD               # 40 位 SHA，记下来
git ls-remote origin refs/heads/2026-competition-round2   # 与上一步 SHA 一致
```

注意：不要提交 `minivectordb/target/`、`protocol-results/`、二进制或本地绝对路径；不要 force-push 覆盖
他人提交；提交后同一天内不要再用同一版本反复消耗额度。

## 7. 平台填报字段

| 字段 | 值 |
| --- | --- |
| 源码仓库 HTTPS URL | `https://github.com/<你的组织>/seekdb.git`（必须平台可访问） |
| 分支名称 | `2026-competition-round2` |
| 完整 commit SHA | 第 6 步 `git rev-parse HEAD` 的 40 位十六进制 |
| Dockerfile | 位于 commit 根目录，由平台构建（不要提交本地镜像/二进制） |
| Agent prompt 材料 | `agent/conversation.json`（平台默认读取路径，需与源码同一次 commit） |
| 运行入口 | 无需填 host/port；平台用 `docker run -i` 通过 stdin/stdout 通信 |

提测流程：环境预检（不占额度）-> 确认版本锁定（commit SHA / 镜像摘要 / conversation.json
摘要）-> 正式评测（消耗当日 1 次，每天最多 2 次）-> 反馈用例准入与性能/Agent 结果。

## 8. 提测后

- 记录本次 SHA、镜像摘要、公开用例与 lint 报告路径到 `protocol-results/`（本地保留，不提交）。
- 若只失败个别用例，按反馈类别定位；修复后重新 lint + clean-clone，再消耗下一次额度。
- 最终参评版本必须在截止前指定；未指定时默认最近一次公开用例全通过的正式提交。
- 任何一次提测失败（崩溃/超时/资源超限/用例失败）总分均为 0，所以宁可不提也不提未验证版本。

## 9. 常见问题

| 问题 | 处理 |
| --- | --- |
| docker daemon 未运行 | 启动 Docker Desktop，等 `docker version` 返回 Server 版本 |
| docker build 拉基础镜像失败 | 本机需代理时先在 Docker Desktop 配置代理；或先 `docker pull rust:1.90-slim-bookworm debian:bookworm-slim`（clean_clone_check.py --prepull）；仍失败可用 `docker save/load` 迁移镜像并记录错误 |
| build 报 base name (RUNTIME_IMAGE) should not be blank | Dockerfile 的全局 `ARG` 必须写在第一个 `FROM` 之前（本仓库已修复） |
| 构建上下文过大 | 检查 `.dockerignore` 是否仍只放行 `minivectordb`；lint 会要求 <5 MiB |
| lint 报 branch/head_files | 先切到 2026-competition-round2 并把 Dockerfile/conversation.json 一起 commit |
| 公开用例 gate 失败 | 看 `protocol-results/lint-*/gate-public/summary.json` 的失败用例与 stderr |
| 只读根文件系统下写失败 | 程序只能写 /data；不要把日志/临时文件写到其他目录 |
| Windows 上跑不了官方 protocol_check | 用 `run_public.py`（便携 transport）或 `wsl_run.sh` 在 WSL 里跑 |
