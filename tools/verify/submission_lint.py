#!/usr/bin/env python3
"""Pre-submission lint for the 2026-competition-round2 submission.

Checks the rules that cost a submission attempt outright:
  * the commit is on the expected branch (default: 2026-competition-round2)
  * Dockerfile exists, is multi-stage, builds from this clean context, and the
    image starts as a foreground stdin/stdout process (no shell wrapper)
  * .dockerignore keeps the build context small
  * agent/conversation.json exists, is UTF-8, <= 128 KiB and a valid JSON object
  * the git worktree is clean and HEAD already contains Dockerfile / sources /
    agent/conversation.json (all in the same commit)
  * prints the full 40-char commit SHA to hand to the platform

Usage:
  python tools/verify/submission_lint.py
  python tools/verify/submission_lint.py --skip-build
  python tools/verify/submission_lint.py --use-cache --skip-status

Exit codes: 0 = no FAIL, 1 = at least one FAIL, 2 = usage/setup error.
"""
import argparse
import datetime
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RESULTS_ROOT = ROOT / "protocol-results"
EXPECTED_BRANCH = "2026-competition-round2"
CONVERSATION = Path("agent/conversation.json")
MAX_CONVERSATION_BYTES = 128 * 1024
MAX_CONTEXT_BYTES = 5 * 1024 * 1024

STATUSES = ("PASS", "FAIL", "WARN", "SKIP")


def strict_json(text):
    def invalid(value):
        raise ValueError("non-finite JSON number: %s" % value)

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON key: %s" % key)
            result[key] = value
        return result

    return json.loads(text, parse_constant=invalid, object_pairs_hook=pairs)


def add(checks, name, status, detail=""):
    checks.append({"name": name, "status": status, "detail": detail})
    return checks[-1]


def run(argv, cwd=None, timeout=1800, text=True):
    try:
        return subprocess.run(argv, cwd=cwd, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, timeout=timeout,
                              text=text, check=False)
    except FileNotFoundError:
        return None
    except subprocess.SubprocessError as exc:
        return exc


def git(*args, timeout=300):
    return run(["git", "-C", str(ROOT), *args], timeout=timeout)


def git_text(*args):
    done = git(*args)
    if done is None or isinstance(done, Exception) or done.returncode != 0:
        return None
    return done.stdout.strip()


def check_branch(checks, expected):
    current = git("symbolic-ref", "--short", "-q", "HEAD")
    refs = ["refs/heads/" + expected, "refs/remotes/origin/" + expected]
    ref = next((item for item in refs
                if git("rev-parse", "--verify", "--quiet", item).returncode == 0), None)
    if current is not None and not isinstance(current, Exception) and current.returncode == 0:
        branch = current.stdout.strip()
        if branch == expected:
            return add(checks, "branch", "PASS", "on " + branch)
        return add(checks, "branch", "FAIL",
                   "on branch '%s', expected '%s' (git switch %s)" % (branch, expected, expected))
    if ref is None:
        return add(checks, "branch", "FAIL",
                   "detached HEAD and branch '%s' not found locally/remotely" % expected)
    ancestor = git("merge-base", "--is-ancestor", "HEAD", ref)
    if ancestor.returncode == 0:
        return add(checks, "branch", "PASS", "detached HEAD reachable from " + ref)
    return add(checks, "branch", "FAIL", "detached HEAD is not on " + ref)


def check_sha(checks):
    sha = git_text("rev-parse", "HEAD")
    if not sha or not re.fullmatch(r"[0-9a-f]{40}", sha):
        add(checks, "commit_sha", "FAIL", "cannot resolve a 40-char HEAD SHA: %r" % sha)
        return None
    add(checks, "commit_sha", "PASS", sha)
    return sha


def check_head_contents(checks):
    required = ["Dockerfile", "minivectordb/Cargo.toml", "minivectordb/src/main.rs"]
    optional = [".dockerignore"]
    missing = [path for path in required
               if git("cat-file", "-e", "HEAD:" + path).returncode != 0]
    if missing:
        add(checks, "head_files", "FAIL",
            "not committed in HEAD: " + ", ".join(missing) + " (commit before submitting)")
        return
    if git("cat-file", "-e", "HEAD:" + CONVERSATION.as_posix()).returncode != 0:
        add(checks, "head_files", "FAIL",
            "%s is not committed in HEAD (same commit as the source is required)"
            % CONVERSATION.as_posix())
        return
    absent = [path for path in optional
              if git("cat-file", "-e", "HEAD:" + path).returncode != 0]
    if absent:
        add(checks, "head_files", "WARN",
            "committed, but missing optional " + ", ".join(absent))
    else:
        add(checks, "head_files", "PASS", "Dockerfile, sources and conversation.json are in HEAD")


def check_dockerfile(checks):
    path = ROOT / "Dockerfile"
    if not path.is_file() or path.stat().st_size == 0:
        add(checks, "dockerfile", "FAIL", "Dockerfile missing or empty")
        return
    text = path.read_text(encoding="utf-8", errors="replace")
    froms = len(re.findall(r"(?im)^\s*FROM\s+", text))
    if froms < 2:
        add(checks, "dockerfile", "FAIL", "single-stage Dockerfile (need multi-stage source build)")
        return
    if not re.search(r"(?im)^\s*(ENTRYPOINT|CMD)\s+", text):
        add(checks, "dockerfile", "FAIL", "no ENTRYPOINT/CMD: no default startup")
        return
    bad_copy = []
    for line in text.splitlines():
        stripped = line.strip()
        if not re.match(r"(?i)^COPY\b", stripped) or "--from" in stripped:
            continue
        lowered = stripped.lower()
        if ".exe" in lowered or re.search(r"(^|\s)target/", lowered):
            bad_copy.append(stripped)
    if bad_copy:
        add(checks, "dockerfile", "FAIL",
            "COPY of local prebuilt artifact(s): " + "; ".join(bad_copy[:3]))
        return
    if "minivectordb" not in text:
        add(checks, "dockerfile", "FAIL", "Dockerfile does not build the minivectordb crate")
        return
    if "--locked" not in text:
        add(checks, "dockerfile", "WARN", "cargo build does not use --locked")
        return
    add(checks, "dockerfile", "PASS", "multi-stage, locked, source build")


def check_dockerignore(checks):
    path = ROOT / ".dockerignore"
    if not path.is_file():
        add(checks, "dockerignore", "FAIL",
            ".dockerignore missing: the whole seekdb tree would enter the build context")
        return
    text = path.read_text(encoding="utf-8", errors="replace")
    if "minivectordb" not in text:
        add(checks, "dockerignore", "FAIL", ".dockerignore does not mention minivectordb")
    elif "**" not in text and "*" not in text:
        add(checks, "dockerignore", "WARN", ".dockerignore has no exclusion patterns")
    else:
        add(checks, "dockerignore", "PASS", "crate-scoped build context")


def check_conversation(checks):
    path = ROOT / CONVERSATION
    if not path.is_file():
        add(checks, "conversation", "FAIL", CONVERSATION.as_posix() + " missing")
        return None
    size = path.stat().st_size
    if size > MAX_CONVERSATION_BYTES:
        add(checks, "conversation", "FAIL",
            "%s is %d bytes > 128 KiB (%d)" % (CONVERSATION.as_posix(), size, MAX_CONVERSATION_BYTES))
        return None
    raw = path.read_bytes()
    try:
        text = raw.decode("utf-8")
    except UnicodeError as exc:
        add(checks, "conversation", "FAIL", "not valid UTF-8: %s" % exc)
        return None
    try:
        value = strict_json(text)
    except ValueError as exc:
        add(checks, "conversation", "FAIL", "invalid JSON: %s" % exc)
        return None
    if not isinstance(value, dict):
        add(checks, "conversation", "FAIL", "top-level JSON value is not an object")
        return None
    add(checks, "conversation", "PASS", "%d bytes, valid JSON object" % size)
    markers = [name for name in ("TODO", "FIXME", "PLACEHOLDER", "XXX")
               if name in text]
    evidence_keys = [key for key in ("user_prompts", "conversations", "experiments",
                                     "retrospective")
                     if key in value]
    if markers or not evidence_keys:
        add(checks, "conversation_filled", "WARN",
            "skeleton not ready for scoring: markers=%s evidence_keys=%s; fill with real "
            "student evidence before the scored submission"
            % (markers or "none", evidence_keys or "none"))
    else:
        add(checks, "conversation_filled", "PASS", "contains evidence sections")
    legacy = ROOT / "conversation.json"
    if legacy.is_file():
        add(checks, "conversation_legacy", "WARN",
            "legacy ./conversation.json exists; the platform reads "
            + CONVERSATION.as_posix())
    return size


def check_git_status(checks, skip):
    if skip:
        add(checks, "git_clean", "SKIP", "--skip-status")
        return
    done = git("status", "--porcelain=v1", "--untracked-files=normal")
    if done is None or isinstance(done, Exception):
        add(checks, "git_clean", "FAIL", "git status failed")
        return
    lines = [line for line in done.stdout.splitlines() if line.strip()]
    if lines:
        shown = "; ".join(line[:80] for line in lines[:5])
        add(checks, "git_clean", "FAIL",
            "%d uncommitted/untracked path(s): %s%s"
            % (len(lines), shown, " ..." if len(lines) > 5 else ""))
    else:
        add(checks, "git_clean", "PASS", "worktree clean")


def parse_context_bytes(text):
    match = re.search(r"transferring context:\s*([0-9.]+)\s*(B|kB|MB|GB)", text)
    if not match:
        return None
    value = float(match.group(1))
    unit = match.group(2)
    factor = {"B": 1, "kB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3}[unit]
    return int(value * factor)


def check_build(checks, tag, use_cache):
    version = run(["docker", "version", "--format", "{{.Server.Version}}"], timeout=120)
    if version is None or isinstance(version, Exception) or version.returncode != 0:
        add(checks, "docker_build", "FAIL", "docker daemon unavailable")
        return None
    argv = ["docker", "build", "--progress=plain", "-t", tag]
    if not use_cache:
        argv.append("--no-cache")
    argv.append(str(ROOT))
    done = run(argv, cwd=str(ROOT), timeout=2400)
    if done is None or isinstance(done, Exception):
        add(checks, "docker_build", "FAIL", "docker build could not run: %s" % done)
        return None
    output = (done.stdout or "") + (done.stderr or "")
    if done.returncode != 0:
        tail = "\n".join(output.strip().splitlines()[-8:])
        add(checks, "docker_build", "FAIL", "docker build failed (%d): %s"
            % (done.returncode, tail[-1200:]))
        return None
    context = parse_context_bytes(output)
    if context is None:
        add(checks, "docker_build", "WARN", "built, but context size was not reported")
        return tag
    if context > MAX_CONTEXT_BYTES:
        add(checks, "docker_build", "FAIL",
            "build context is %.1f MiB (> 5 MiB); check .dockerignore" % (context / 1048576.0))
        return None
    add(checks, "docker_build", "PASS", "built from clean context (%d bytes sent)" % context)
    return tag


def check_image(checks, tag):
    fmt = "{{json .Config.Entrypoint}}|{{json .Config.Cmd}}|{{json .Config.User}}"
    done = run(["docker", "image", "inspect", tag, "--format", fmt], timeout=120)
    if done is None or isinstance(done, Exception) or done.returncode != 0:
        add(checks, "image_entry", "FAIL", "cannot inspect image %s" % tag)
        return
    parts = done.stdout.strip().split("|")
    entrypoint = parts[0] if parts else ""
    user = parts[2] if len(parts) > 2 else ""
    if re.search(r"\"(/(bin/)?(sh|bash|dash))\"", entrypoint):
        add(checks, "image_entry", "FAIL", "entrypoint is a shell: %s" % entrypoint)
        return
    if entrypoint.strip() in ("", "null"):
        add(checks, "image_entry", "FAIL", "no ENTRYPOINT in image")
        return
    detail = "entrypoint=%s user=%s" % (entrypoint, user)
    if user.strip() in ("", "null", "\"\"") or user.strip() == "\"0\"" or user.strip() == "0":
        add(checks, "image_entry", "WARN", detail + " (runs as root)")
    else:
        add(checks, "image_entry", "PASS", detail)


def check_smoke(checks, tag):
    data_dir = tempfile.mkdtemp(prefix="mvdb-lint-data-")
    try:
        if os.name != "nt":
            os.chmod(data_dir, 0o777)
        argv = ["docker", "run", "--rm", "-i", "--network=none", "--cpus=2",
                "--memory=2g", "--memory-swap=2g", "--pids-limit=128",
                "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges",
                "--mount", "type=bind,src=%s,dst=/data" % data_dir, tag]
        payload = '{"id":1,"op":"shutdown"}\n'
        try:
            done = subprocess.run(argv, input=payload, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, timeout=180, text=True,
                                  check=False)
        except subprocess.TimeoutExpired:
            add(checks, "image_smoke", "FAIL", "container smoke test timed out")
            return
        except OSError as exc:
            add(checks, "image_smoke", "FAIL", "docker run failed: %s" % exc)
            return
        lines = [line for line in done.stdout.splitlines() if line.strip()]
        if done.returncode != 0:
            tail = (done.stderr or "").strip().splitlines()[-5:]
            add(checks, "image_smoke", "FAIL",
                "container exit %d: %s" % (done.returncode, " | ".join(tail)))
            return
        try:
            messages = [json.loads(line) for line in lines]
        except ValueError as exc:
            add(checks, "image_smoke", "FAIL", "non-JSON stdout: %s" % exc)
            return
        if messages[:1] != [{"type": "ready"}]:
            add(checks, "image_smoke", "FAIL", "first stdout line is not ready: %r"
                % (messages[:1],))
            return
        if len(messages) != 2 or messages[1].get("ok") is not True:
            add(checks, "image_smoke", "FAIL", "unexpected stdout: %r" % (messages,))
            return
        add(checks, "image_smoke", "PASS", "ready + shutdown on read-only rootfs")
    finally:
        try:
            import shutil
            shutil.rmtree(data_dir, ignore_errors=True)
        except OSError:
            pass


CHECKLIST_GROUPS = [
    ("Git / submission", ["branch", "commit_sha", "git_clean", "head_files"]),
    ("Docker image", ["dockerfile", "dockerignore", "docker_build", "image_entry",
                      "image_smoke"]),
    ("Agent material", ["conversation", "conversation_filled"]),
    ("Functional gate", ["public_cases"]),
]


def check_public_cases(checks, tag, report_dir):
    """Run the 20 public cases against the lint-built image as a hard gate."""
    out_dir = report_dir / "gate-public"
    argv = [sys.executable, str(ROOT / "tools" / "verify" / "run_public.py"),
            "--image", tag, "--output", str(out_dir)]
    done = run(argv, cwd=str(ROOT), timeout=3600)
    summary_path = out_dir / "summary.json"
    if done is None or isinstance(done, Exception):
        add(checks, "public_cases", "FAIL", "could not run run_public.py: %s" % done)
        return
    if not summary_path.is_file():
        output = (done.stdout or "") + (done.stderr or "")
        add(checks, "public_cases", "FAIL",
            "run_public.py produced no summary.json (exit %s): %s"
            % (done.returncode, " ".join(output.strip().splitlines()[-3:])[:600]))
        return
    summary = strict_json(summary_path.read_text(encoding="utf-8"))
    totals = summary.get("totals", {})
    failing = [case["id"] for case in summary.get("cases", [])
               if case.get("status") != "PASS"]
    if summary.get("passed") and totals.get("total") == 20 and totals.get("PASS") == 20:
        add(checks, "public_cases", "PASS", "20/20 public cases passed")
        return
    add(checks, "public_cases", "FAIL",
        "%d/%d passed (%d failed); failing: %s%s"
        % (totals.get("PASS", 0), totals.get("total", 0), len(failing),
           ", ".join(failing[:8]), " ..." if len(failing) > 8 else ""))


def build_checklist(checks):
    by_name = {row["name"]: row for row in checks}
    checklist = []
    for group, names in CHECKLIST_GROUPS:
        items = []
        for name in names:
            row = by_name.get(name)
            if row is None:
                continue
            items.append({"item": row["name"], "status": row["status"],
                          "detail": row["detail"]})
        checklist.append({"group": group, "items": items})
    return checklist


def print_report(checks, sha, extra):
    print("")
    print("SUBMISSION CHECKLIST")
    for group in (extra.get("checklist") if isinstance(extra, dict) else None) or []:
        print("  [%s]" % group["group"])
        for item in group["items"]:
            detail = " ".join(str(item["detail"]).split())
            if len(detail) > 100:
                detail = detail[:97] + "..."
            print("    %-5s %-16s %s" % (item["status"], item["item"], detail))
    print("")
    print("%-18s  %-5s  %s" % ("CHECK", "STATE", "DETAIL"))
    print("-" * 78)
    for row in checks:
        detail = " ".join(str(row["detail"]).split())
        if len(detail) > 130:
            detail = detail[:127] + "..."
        print("%-18s  %-5s  %s" % (row["name"], row["status"], detail))
    failed = [row for row in checks if row["status"] == "FAIL"]
    warned = [row for row in checks if row["status"] == "WARN"]
    print("")
    if sha:
        print("COMMIT_SHA=%s" % sha)
    print("RESULT: %s  (%d checks, %d fail, %d warn)"
          % ("FAIL" if failed else "PASS", len(checks), len(failed), len(warned)))
    print("SUBMISSION_READY=%s" % ("NO" if failed else "YES"))
    if failed:
        print("Fix the FAIL rows above before the formal submission "
              "(each failed submission costs an attempt).")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Pre-submission lint.")
    parser.add_argument("--branch", default=EXPECTED_BRANCH,
                        help="expected submission branch (default: %s)" % EXPECTED_BRANCH)
    parser.add_argument("--skip-build", action="store_true", help="do not run docker build")
    parser.add_argument("--use-cache", action="store_true", help="allow docker build cache")
    parser.add_argument("--skip-status", action="store_true", help="skip git status check")
    parser.add_argument("--skip-smoke", action="store_true", help="skip container smoke test")
    parser.add_argument("--skip-public-cases", action="store_true",
                        help="skip the 20/20 public-case gate (run_public.py --image)")
    parser.add_argument("--image-tag", help="image tag used by the lint build")
    parser.add_argument("--output", type=Path, help="JSON report path")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)

    if not (ROOT / ".git").exists():
        print("error: %s is not a git checkout" % ROOT, file=sys.stderr)
        return 2
    if git("rev-parse", "--show-toplevel") is None:
        print("error: git is required", file=sys.stderr)
        return 2

    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    out_path = args.output or (RESULTS_ROOT / ("lint-" + stamp + ".json"))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    report_dir = out_path.parent

    checks = []
    check_branch(checks, args.branch)
    sha = check_sha(checks)
    check_dockerfile(checks)
    check_dockerignore(checks)
    conversation_bytes = check_conversation(checks)
    check_head_contents(checks)
    check_git_status(checks, args.skip_status)

    tag = args.image_tag or ("minivectordb:lint-" + (sha[:12] if sha else "unknown"))
    if args.skip_build:
        add(checks, "docker_build", "SKIP", "--skip-build")
        add(checks, "image_entry", "SKIP", "--skip-build")
        add(checks, "image_smoke", "SKIP", "--skip-build")
    else:
        built = check_build(checks, tag, args.use_cache)
        if built:
            check_image(checks, tag)
            if args.skip_smoke:
                add(checks, "image_smoke", "SKIP", "--skip-smoke")
            else:
                check_smoke(checks, tag)
            if args.skip_public_cases:
                add(checks, "public_cases", "SKIP", "--skip-public-cases")
            else:
                check_public_cases(checks, tag, report_dir)
        else:
            add(checks, "public_cases", "SKIP", "image build failed")

    checklist = build_checklist(checks)
    failed = [row for row in checks if row["status"] == "FAIL"]
    report = {
        "generated_at": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
        "scope": "pre-submission lint",
        "repo": str(ROOT),
        "expected_branch": args.branch,
        "commit_sha": sha,
        "image": None if args.skip_build else tag,
        "conversation_bytes": conversation_bytes,
        "passed": not failed,
        "submission_ready": not failed,
        "checklist": checklist,
        "checks": checks,
    }
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n",
                        encoding="utf-8")
    print_report(checks, sha, report)
    print("report: %s" % out_path)
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
