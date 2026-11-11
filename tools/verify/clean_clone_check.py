#!/usr/bin/env python3
"""Clean-checkout build + judge-style run check for the submission image.

Proves that what is actually committed can be rebuilt by the platform from a
fresh checkout, independent of the local worktree (no prebuilt artifacts, no
untracked files, no stale target directory):

  1. export the committed tree of <ref> from git (default: HEAD) into a fresh
     temporary directory - only submission-relevant paths by default, the
     whole tree with --archive-full;
  2. docker build the image from that directory and record the build-context
     size (must stay small thanks to .dockerignore);
  3. docker run the image with the judge resource/security flags and the
     STDIO_PROTOCOL section 7 sample, then validate every response line;
  4. write a JSON report under protocol-results/ and clean up.

Network note: no build step needs network once the base images are cached.
If Docker Hub is unreachable, pre-pull the pinned bases first (option
--prepull) or configure Docker Desktop proxy, e.g.:
  docker pull rust:1.90-slim-bookworm
  docker pull debian:bookworm-slim
Build-time proxy (only needed if a RUN step downloads something):
  set HTTP_PROXY / HTTPS_PROXY, or pass --build-arg HTTP_PROXY=...

Usage:
  python tools/verify/clean_clone_check.py
  python tools/verify/clean_clone_check.py --ref 2026-competition-round2
  python tools/verify/clean_clone_check.py --archive-full --keep-image
  python tools/verify/clean_clone_check.py --skip-build   # export + inspect only

Exit codes: 0 = PASS, 1 = FAIL, 2 = setup problem (no git/docker).
"""
import argparse
import datetime
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RESULTS_ROOT = ROOT / "protocol-results"
DEFAULT_PATHS = ["Dockerfile", ".dockerignore", "minivectordb"]
BASE_IMAGES = ["rust:1.90-slim-bookworm", "debian:bookworm-slim"]
REQUESTS = [
    {"id": 1, "op": "execute",
     "sql": "CREATE TABLE docs (id BIGINT PRIMARY KEY, content VARCHAR(16));",
     "search_mode": "exact"},
    {"id": 2, "op": "execute",
     "sql": "INSERT INTO docs VALUES (1, 'hello world');",
     "search_mode": "exact"},
    {"id": 3, "op": "execute",
     "sql": "SELECT id, content FROM docs ORDER BY id;",
     "search_mode": "exact"},
    {"id": 4, "op": "shutdown"},
]


def run(argv, cwd=None, timeout=1800, text=True):
    return subprocess.run(argv, cwd=cwd, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, timeout=timeout,
                          text=text, check=False)


def git_text(*args):
    done = run(["git", "-C", str(ROOT), *args], timeout=300)
    if done.returncode != 0:
        return None
    return done.stdout.strip()


def parse_context_bytes(text):
    match = re.search(r"transferring context:\s*([0-9.]+)\s*(B|kB|MB|GB)", text)
    if not match:
        return None
    return int(float(match.group(1)) * {"B": 1, "kB": 1024, "MB": 1024 ** 2,
                                        "GB": 1024 ** 3}[match.group(2)])


def tail(text, lines=15):
    return "\n".join(text.strip().splitlines()[-lines:])


def check_daemon():
    try:
        done = run(["docker", "version", "--format", "{{.Server.Version}}"], timeout=120)
    except (OSError, subprocess.SubprocessError) as exc:
        return "docker not available: %s" % exc
    if done.returncode != 0:
        return ("docker daemon is not running (start Docker Desktop); stderr: %s"
                % (done.stderr or "").strip()[:300])
    return None


def export_tree(ref, destination):
    """Extract committed paths (or the full tree) of ref into destination."""
    full = destination["full"]
    paths = [] if full else DEFAULT_PATHS
    archive = destination["archive"]
    argv = ["git", "-C", str(ROOT), "archive", "--format=tar",
            "--output=" + str(archive), ref]
    if paths:
        argv += paths
    done = run(argv, timeout=600)
    if done.returncode != 0:
        return "git archive failed: %s" % (done.stderr or done.stdout).strip()[:800]
    with tarfile.open(archive) as tar:
        tar.extractall(destination["src"])
    archive.unlink()
    return None


def build_image(src_dir, tag, extra_build_args, use_cache):
    argv = ["docker", "build", "--progress=plain", "-t", tag]
    if not use_cache:
        argv.append("--no-cache")
    for item in extra_build_args:
        argv += ["--build-arg", item]
    argv.append(str(src_dir))
    done = run(argv, cwd=str(src_dir), timeout=2400)
    output = (done.stdout or "") + (done.stderr or "")
    return done.returncode, output


def inspect_image(tag):
    fmt = "{{json .Config.Entrypoint}}|{{json .Config.Cmd}}|{{json .Config.User}}"
    done = run(["docker", "image", "inspect", tag, "--format", fmt], timeout=120)
    if done.returncode != 0:
        return None
    parts = done.stdout.strip().split("|")
    return {"entrypoint": parts[0] if parts else "",
            "cmd": parts[1] if len(parts) > 1 else "",
            "user": parts[2] if len(parts) > 2 else ""}


def run_protocol_sample(tag, data_dir):
    if os.name != "nt":
        os.chmod(data_dir, 0o777)
    argv = ["docker", "run", "--rm", "-i", "--network=none", "--cpus=2",
            "--memory=2g", "--memory-swap=2g", "--pids-limit=128",
            "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges",
            "--mount", "type=bind,src=%s,dst=/data" % data_dir, tag]
    payload = "\n".join(json.dumps(item) for item in REQUESTS) + "\n"
    try:
        done = subprocess.run(argv, input=payload, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, timeout=300, text=True, check=False)
    except subprocess.TimeoutExpired:
        return ["container run timed out (300s)"], ""
    errors = []
    stderr = done.stderr or ""
    if done.returncode != 0:
        errors.append("container exit code %s" % done.returncode)
    lines = [line for line in (done.stdout or "").splitlines() if line.strip()]
    if len(lines) != 5:
        errors.append("expected 5 stdout lines, got %d: %r" % (len(lines), lines[:6]))
        return errors, stderr
    try:
        messages = [json.loads(line) for line in lines]
    except ValueError as exc:
        return ["non-JSON stdout line: %s" % exc], stderr
    if messages[0] != {"type": "ready"}:
        errors.append("first line is not {type: ready}: %r" % (messages[0],))
    for expected_id, response in enumerate(messages[1:], 1):
        if type(response.get("id")) is not int or response["id"] != expected_id:
            errors.append("response %d has wrong id: %r" % (expected_id, response))
        if response.get("ok") is not True:
            errors.append("response %d is not ok: %r" % (expected_id, response))
    if messages[2].get("affected_rows") != 1:
        errors.append("INSERT affected_rows != 1: %r" % (messages[2],))
    if messages[3].get("columns") != ["id", "content"]:
        errors.append("SELECT columns mismatch: %r" % (messages[3],))
    if messages[3].get("rows") != [[1, "hello world"]]:
        errors.append("SELECT rows mismatch: %r" % (messages[3],))
    return errors, stderr


def main(argv=None):
    parser = argparse.ArgumentParser(description="Clean-checkout build + protocol check.")
    parser.add_argument("--ref", default="HEAD",
                        help="git commit-ish to export (default: HEAD)")
    parser.add_argument("--archive-full", action="store_true",
                        help="export the whole tree instead of submission paths only")
    parser.add_argument("--image", help="image tag (default: minivectordb:clean-<sha12>)")
    parser.add_argument("--use-cache", action="store_true", help="allow docker build cache")
    parser.add_argument("--build-arg", action="append", default=[], metavar="K=V",
                        help="extra docker build arg (repeatable)")
    parser.add_argument("--prepull", action="store_true",
                        help="docker pull the pinned base images before building")
    parser.add_argument("--skip-build", action="store_true",
                        help="only export/check the ref, do not build or run")
    parser.add_argument("--dockerfile", type=Path,
                        help="dev only: use this Dockerfile instead of the committed one")
    parser.add_argument("--dockerignore", type=Path,
                        help="dev only: use this .dockerignore instead of the committed one")
    parser.add_argument("--from-worktree", action="store_true",
                        help="dev only: build the current worktree instead of the commit "
                             "(does NOT prove submission reproducibility)")
    parser.add_argument("--keep-image", action="store_true", help="keep the built image")
    parser.add_argument("--keep-workdir", action="store_true", help="keep the temp checkout")
    parser.add_argument("--output", type=Path, help="JSON report path")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)

    if shutil.which("git") is None:
        print("error: git not found", file=sys.stderr)
        return 2
    if not (ROOT / ".git").exists():
        print("error: %s is not a git checkout" % ROOT, file=sys.stderr)
        return 2
    sha = git_text("rev-parse", args.ref)
    if not sha:
        print("error: cannot resolve ref %r" % args.ref, file=sys.stderr)
        return 2
    branch = git_text("symbolic-ref", "--short", "-q", "HEAD") or "(detached)"
    dirty = bool(git_text("status", "--porcelain=v1", "--untracked-files=no"))
    for required, label in (("Dockerfile", "Dockerfile"),
                            (".dockerignore", ".dockerignore"),
                            ("minivectordb/Cargo.toml", "crate manifest"),
                            ("minivectordb/src/main.rs", "crate main")):
        if run(["git", "-C", str(ROOT), "cat-file", "-e", "%s:%s" % (args.ref, required)],
               timeout=120).returncode != 0:
            print("error: %s is not committed in %s; the platform clone cannot build"
                  % (label, args.ref), file=sys.stderr)
            return 1
    if args.ref == "HEAD" and dirty:
        print("warning: worktree has uncommitted changes; exporting committed HEAD only")
    if check_daemon() and not args.skip_build:
        print("error: " + (check_daemon() or ""), file=sys.stderr)
        return 2

    tag = args.image or ("minivectordb:clean-" + sha[:12])
    work = Path(tempfile.mkdtemp(prefix="mvdb-clean-"))
    src = work / "src"
    src.mkdir()
    report = {
        "generated_at": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
        "scope": "clean-checkout build + judge-style protocol sample",
        "repo": str(ROOT),
        "branch": branch,
        "ref": args.ref,
        "commit_sha": sha,
        "worktree_dirty": dirty,
        "archive": "full" if args.archive_full else "submission-paths",
        "image": None if args.skip_build else tag,
        "checks": [],
        "passed": False,
    }

    def add(name, status, detail=""):
        report["checks"].append({"name": name, "status": status, "detail": detail})
        print("%-22s %-5s %s" % (name, status, detail))

    try:
        add("commit", "PASS", "%s (%s)" % (sha, args.ref))
        add("clean_tree", "PASS", "exported committed tree, no local target/ or binaries")
        if args.prepull and not args.skip_build:
            pulled = 0
            for image in BASE_IMAGES:
                done = run(["docker", "pull", "--quiet", image], timeout=900)
                if done.returncode == 0:
                    pulled += 1
                else:
                    add("prepull", "WARN", "%s pull failed: %s"
                        % (image, (done.stderr or "").strip()[:200]))
            if pulled == len(BASE_IMAGES):
                add("prepull", "PASS", "base images present")
        if args.from_worktree:
            src = ROOT
            add("export", "WARN", "--from-worktree: building current worktree; "
                "not a submission reproducibility proof")
        else:
            err = export_tree(args.ref, {"full": args.archive_full,
                                         "archive": work / "src.tar", "src": src})
            if err:
                add("export", "FAIL", err)
                return 1
            add("export", "PASS", "git archive %s -> %s" % (sha[:12], src))
        for option, target in ((args.dockerfile, "Dockerfile"),
                               (args.dockerignore, ".dockerignore")):
            if option is None:
                continue
            override = option.expanduser().resolve()
            if not override.is_file():
                add("override", "FAIL", "%s not found: %s" % (target, override))
                return 1
            shutil.copyfile(override, Path(src) / target)
            add("override", "WARN", "dev: using worktree %s instead of committed copy"
                % target)
        if args.skip_build:
            add("docker_build", "SKIP", "--skip-build")
            add("protocol_sample", "SKIP", "--skip-build")
            report["passed"] = not any(item["status"] == "FAIL" for item in report["checks"])
            return finish(report, work, args, tag, None)
        status, output = build_image(src, tag, args.build_arg, args.use_cache)
        context = parse_context_bytes(output)
        if status != 0:
            add("docker_build", "FAIL", "exit %s: %s" % (status, tail(output, 10)))
            return 1
        if context is None:
            add("docker_build", "PASS", "built (context size not reported)")
        elif context > 5 * 1024 * 1024:
            add("docker_build", "FAIL", "context %.1f MiB > 5 MiB" % (context / 1048576.0))
            return 1
        else:
            add("docker_build", "PASS", "built, context=%d bytes" % context)
        image = inspect_image(tag) or {}
        entry = image.get("entrypoint", "")
        user = image.get("user", "")
        if re.search(r"\"(/(bin/)?(sh|bash|dash))\"", entry) or entry.strip() in ("", "null"):
            add("image_entry", "FAIL", "entrypoint=%r" % entry)
            return 1
        add("image_entry", "PASS", "entrypoint=%s user=%s" % (entry, user))
        data_dir = work / "data"
        data_dir.mkdir()
        errors, stderr = run_protocol_sample(tag, str(data_dir))
        if errors:
            add("protocol_sample", "FAIL", "; ".join(errors[:4]) +
                (" | stderr: " + tail(stderr, 3) if stderr.strip() else ""))
            return 1
        add("protocol_sample", "PASS", "ready + 3 SQL + shutdown on read-only rootfs")
        report["passed"] = True
        return finish(report, work, args, tag, sha)
    finally:
        pass


def finish(report, work, args, tag, sha):
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    out = args.output or (RESULTS_ROOT / ("clean-clone-" + stamp + ".json"))
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n",
                   encoding="utf-8")
    if not args.keep_image and not args.skip_build:
        run(["docker", "image", "rm", "-f", tag], timeout=120)
    if args.keep_workdir:
        print("workdir kept: %s" % work)
    else:
        shutil.rmtree(work, ignore_errors=True)
    print("")
    print("RESULT: %s" % ("PASS" if report["passed"] else "FAIL"))
    if sha:
        print("COMMIT_SHA=%s" % sha)
    print("report: %s" % out)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
