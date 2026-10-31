#!/usr/bin/env python3
"""Exercise the real resolver with bare Git remotes and a local Buzz boundary."""

import argparse
import fcntl
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DRIVER = ROOT / ".pagoda/resolver.py"
BUZZ = ROOT / "scripts/fixtures/pagoda-buzz.py"
STAMP_FAILURE = ROOT / "scripts/fixtures/pagoda-stamp-failure.py"


def run(*args, cwd=None, env=None, ok=True):
    result = subprocess.run(
        args, cwd=cwd, env=env, text=True, capture_output=True, check=False, timeout=45
    )
    if ok and result.returncode:
        raise AssertionError(f"{args}: {result.stderr}\n{result.stdout}")
    return result


def git(path, *args):
    return run("git", "-C", str(path), *args).stdout.strip()


def write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(data)


def commit(path, message):
    git(path, "add", "-A")
    git(path, "commit", "-q", "-m", message)
    return git(path, "rev-parse", "HEAD")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact", required=True, type=Path)
    args = parser.parse_args()
    artifact = args.artifact.resolve()
    artifact.parent.mkdir(parents=True, exist_ok=True)
    fixture = Path(tempfile.mkdtemp(prefix="resolver-e2e-", dir=artifact.parent))
    env = os.environ.copy()
    for key in list(env):
        if key.startswith("GIT_"):
            del env[key]
            os.environ.pop(key, None)
    env.update(
        {
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_AUTHOR_NAME": "Resolver fixture",
            "GIT_AUTHOR_EMAIL": "fixture@example.test",
            "GIT_COMMITTER_NAME": "Resolver fixture",
            "GIT_COMMITTER_EMAIL": "fixture@example.test",
        }
    )
    os.environ.update(env)
    # Local bare remotes are the same Git boundary the production driver uses.
    upstream, fork, seed = (fixture / x for x in ("upstream.git", "fork.git", "seed"))
    run("git", "init", "--bare", str(upstream), env=env)
    run("git", "init", "--bare", str(fork), env=env)
    run("git", "init", "-b", "main", str(seed), env=env)
    write(seed / "contract.txt", "base\n")
    write(seed / ".pagoda/patches.txt", "# fixture has no extra patches\n")
    shutil.copyfile(ROOT / ".pagoda/sync.sh", seed / ".pagoda/sync.sh")
    base = commit(seed, "base")
    git(seed, "remote", "add", "origin", str(fork))
    git(seed, "remote", "add", "upstream", str(upstream))
    git(seed, "push", "upstream", "main")
    git(seed, "push", "origin", "main")
    git(seed, "switch", "-c", "pagoda")
    write(seed / "contract.txt", "fork behavior\n")
    original_fork = commit(seed, "fork behavior")
    git(seed, "push", "origin", "pagoda")
    git(seed, "switch", "main")
    write(seed / "contract.txt", "upstream behavior\n")
    upstream_sha = commit(seed, "upstream behavior")
    git(seed, "push", "upstream", "main")
    checks = []

    def scenario(name):
        state = fixture / name
        state.mkdir()
        boundary = state / "buzz"
        boundary.mkdir()
        config = {
            "fork_url": str(fork),
            "upstream_url": str(upstream),
            "channel": "fixture-channel",
            "reply_to": "e" * 64,
            "agent_name": "Astra",
            "agent_pubkey": "a" * 64,
            "owner_name": "Kevin C",
            "owner_pubkey": "b" * 64,
            "sender_pubkey": "f" * 64,
            "buzz_command": [sys.executable, str(BUZZ)],
            "notification_interval_seconds": 600,
            "workflow_url": "https://github.com/example/fork/actions/workflows/pagoda-sync.yml",
        }
        write(state / "config.json", json.dumps(config))
        this_env = env | {"PAGODA_BUZZ_FIXTURE": str(boundary)}

        def invoke(*extra, ok=True, stamp_failure=None):
            command = [sys.executable, str(DRIVER)]
            if stamp_failure:
                command = [
                    sys.executable,
                    str(STAMP_FAILURE),
                    str(DRIVER),
                    stamp_failure,
                ]
            return run(
                *command,
                "--state",
                str(state),
                *extra,
                env=this_env,
                ok=ok,
            )

        return state, boundary, invoke

    def read(state):
        return json.loads((state / "repairs" / upstream_sha / "state.json").read_text())

    def events(boundary):
        path = boundary / "events.json"
        return json.loads(path.read_text()) if path.exists() else []

    def advance_stamp(state):
        stamp = state / "repairs" / upstream_sha / "notify.stamp"
        os.utime(stamp, (time.time() - 601, time.time() - 601))

    state, boundary, invoke = scenario("journey")
    invoke(
        "tick",
        "--run-url",
        "https://github.com/example/fork/actions/runs/1",
        "--run-upstream",
        upstream_sha,
    )
    first = read(state)
    work = Path(first["worktree"])
    assert git(work, "diff", "--name-only", "--diff-filter=U") == "contract.txt"
    assert len(events(boundary)) == 1
    assert first["delivery"]["status"] == "verified"
    assert first["phase"] == "requested"
    for _ in range(3):
        invoke(
            "tick",
            "--run-url",
            "https://github.com/example/fork/actions/runs/2",
            "--run-upstream",
            upstream_sha,
        )
    assert len(events(boundary)) == 1
    assert read(state)["attempt"] == 1
    assert git(fork, "rev-parse", "pagoda") == original_fork
    checks.append("one repair request per SHA; repeated run URLs do not reset or push")

    # A claimed success cannot bypass unresolved files or exact-commit evidence.
    bad = invoke(
        "result",
        "--upstream",
        upstream_sha,
        "--attempt",
        "1",
        "--outcome",
        "resolved",
        "--commit",
        original_fork,
        "--evidence",
        str(state / "missing.json"),
        ok=False,
    )
    assert bad.returncode != 0
    assert read(state)["phase"] == "requested"
    invoke(
        "result",
        "--upstream",
        upstream_sha,
        "--attempt",
        "1",
        "--outcome",
        "failed",
        "--reason",
        "compile failure beyond merge hunks",
    )
    invoke("tick")
    assert len(events(boundary)) == 1, "retry must honor stamp mtime"
    advance_stamp(state)
    invoke("tick")
    assert len(events(boundary)) == 2 and read(state)["attempt"] == 2
    invoke(
        "result",
        "--upstream",
        upstream_sha,
        "--attempt",
        "2",
        "--outcome",
        "failed",
        "--reason",
        "second agent attempt failed",
    )
    advance_stamp(state)
    invoke(
        "tick",
        "--run-url",
        "https://github.com/example/fork/actions/runs/3",
        "--run-upstream",
        upstream_sha,
    )
    assert len(events(boundary)) == 3
    assert ["p", "b" * 64] in events(boundary)[-1]["tags"]
    invoke("tick")
    assert len(events(boundary)) == 3 and read(state)["failures"] == 2
    checks.append("two failed agent attempts persist across runs; one owner escalation")

    for mode in ("empty", "wrong", "wrong-author", "wrong-mention", "ambiguous"):
        delivery_state, delivery_boundary, delivery = scenario("delivery-" + mode)
        write(delivery_boundary / "mode", mode)
        delivery("tick", ok=False)
        assert read(delivery_state)["delivery"]["status"] != "verified"
        assert len(events(delivery_boundary)) == 1
        write(delivery_boundary / "mode", "ok")
        delivery("tick")
        assert len(events(delivery_boundary)) == 1
        assert read(delivery_state)["delivery"]["status"] == "verified"
    checks.append(
        "missing, wrong, spoofed and ambiguous delivery reconcile without duplicate send"
    )

    blocked_state, blocked_boundary, blocked = scenario("state-blocked")
    (blocked_state / "repairs").write_text("not a directory")
    assert blocked("tick", ok=False).returncode != 0
    assert not events(blocked_boundary)
    stamp_state, stamp_boundary, stamp = scenario("stamp-blocked")
    # A retained request with a filesystem obstacle at the rate-limit seam.
    stamp("tick")
    invoke_path = stamp_state / "repairs" / upstream_sha
    stamp(
        "result",
        "--upstream",
        upstream_sha,
        "--attempt",
        "1",
        "--outcome",
        "failed",
        "--reason",
        "fixture failure",
    )
    (invoke_path / "notify.stamp").unlink()
    (invoke_path / "notify.stamp").mkdir()
    assert stamp("tick", ok=False).returncode != 0
    assert len(events(stamp_boundary)) == 1
    checks.append("unwritable state and stamp fail closed before send")

    # Fail after the stamp type check, then restart with real writable storage.
    # Both first delivery and retries must retain a sendable request. Reserving
    # an uncertain outbox first poisons it with an event that never existed.
    for operation in ("open", "fsync", "utime"):
        for retry in (False, True):
            recovery_state, recovery_boundary, recover = scenario(
                f"stamp-recovery-{operation}-{'retry' if retry else 'initial'}"
            )
            if retry:
                recover("tick")
                recover(
                    "result",
                    "--upstream",
                    upstream_sha,
                    "--attempt",
                    "1",
                    "--outcome",
                    "failed",
                    "--reason",
                    "fixture retry",
                )
                advance_stamp(recovery_state)
            before = len(events(recovery_boundary))
            failure = recover("tick", ok=False, stamp_failure=operation)
            assert failure.returncode != 0
            assert f"fixture stamp {operation} failure" in failure.stderr
            assert len(events(recovery_boundary)) == before
            # fsync/utime failures can leave a fresh stamp. Respect its rate
            # limit, then model the next eligible cron tick without sleeping.
            stamp_path = recovery_state / "repairs" / upstream_sha / "notify.stamp"
            if stamp_path.exists():
                advance_stamp(recovery_state)
            recover("tick")
            delivered = read(recovery_state)
            assert delivered["phase"] == "requested"
            assert delivered["delivery"]["status"] == "verified"
            assert delivered["attempt"] == (2 if retry else 1)
            assert len(events(recovery_boundary)) == before + 1
            assert (
                delivered["delivery"]["event_id"] == events(recovery_boundary)[-1]["id"]
            )
            recover("tick")
            assert len(events(recovery_boundary)) == before + 1
    checks.append(
        "stamp open/fsync/utime failures recover on initial and retry delivery without duplicates"
    )

    lock_state, lock_boundary, locked = scenario("lock")
    with (lock_state / ".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        locked("tick")
        assert not events(lock_boundary)
    locked("tick")
    assert len(events(lock_boundary)) == 1
    write(lock_state / "repairs" / upstream_sha / "state.json", "")
    assert locked("tick", ok=False).returncode != 0
    assert len(events(lock_boundary)) == 1
    checks.append(
        "overlapping ticks and truncated state cannot send duplicate requests"
    )

    decision_state, decision_boundary, decision = scenario("semantics")
    decision("tick")
    decision(
        "result",
        "--upstream",
        upstream_sha,
        "--attempt",
        "1",
        "--outcome",
        "decision",
        "--reason",
        "Which existing permission policy should remain?",
    )
    advance_stamp(decision_state)
    decision("tick")
    assert read(decision_state)["failures"] == 0
    assert read(decision_state)["phase"] == "escalated"
    assert ["p", "b" * 64] in events(decision_boundary)[-1]["tags"]
    checks.append(
        "product semantics escalate without inventing behavior or consuming failure attempts"
    )

    done_state, done_boundary, done = scenario("recovery")
    done("tick")
    repair = read(done_state)
    repair_work = Path(repair["worktree"])
    write(repair_work / "contract.txt", "fork behavior\nupstream behavior\n")
    repaired = commit(repair_work, "preserve both behaviors")
    evidence = done_state / "verification.json"
    write(
        evidence,
        json.dumps(
            {"commit": original_fork, "checks": [{"name": "stale", "exit_code": 0}]}
        ),
    )
    assert (
        done(
            "result",
            "--upstream",
            upstream_sha,
            "--attempt",
            "1",
            "--outcome",
            "resolved",
            "--commit",
            repaired,
            "--evidence",
            str(evidence),
            ok=False,
        ).returncode
        != 0
    )
    write(
        evidence,
        json.dumps(
            {"commit": repaired, "checks": [{"name": "failed", "exit_code": 1}]}
        ),
    )
    assert (
        done(
            "result",
            "--upstream",
            upstream_sha,
            "--attempt",
            "1",
            "--outcome",
            "resolved",
            "--commit",
            repaired,
            "--evidence",
            str(evidence),
            ok=False,
        ).returncode
        != 0
    )
    write(
        evidence,
        json.dumps(
            {
                "commit": repaired,
                "checks": [{"name": "fixture behavior", "exit_code": 0}],
            }
        ),
    )
    # Exercise a real behavior check at the same HEAD recorded in the receipt.
    assert git(repair_work, "rev-parse", "HEAD") == repaired
    assert (repair_work / "contract.txt").read_text().splitlines() == [
        "fork behavior",
        "upstream behavior",
    ]
    done(
        "result",
        "--upstream",
        upstream_sha,
        "--attempt",
        "1",
        "--outcome",
        "resolved",
        "--commit",
        repaired,
        "--evidence",
        str(evidence),
    )
    done("tick")
    assert read(done_state)["phase"] == "verified", "local commit is not publication"
    assert git(fork, "rev-parse", "pagoda") == original_fork
    # A delayed failure must not reopen a verified attempt or change its receipt.
    # Reject other duplicate outcomes too, before any state or delivery mutation.
    verified_path = done_state / "repairs" / upstream_sha / "state.json"
    verified_bytes = verified_path.read_bytes()
    verified_events = events(done_boundary)
    for outcome in ("failed", "decision", "resolved"):
        stale = done(
            "result",
            "--upstream",
            upstream_sha,
            "--attempt",
            "1",
            "--outcome",
            outcome,
            "--reason",
            "late result after verification",
            "--commit",
            repaired,
            "--evidence",
            str(evidence),
            ok=False,
        )
        assert stale.returncode != 0, f"verified attempt accepted late {outcome}"
        assert verified_path.read_bytes() == verified_bytes
        done("tick")
        assert read(done_state)["phase"] == "verified"
        assert events(done_boundary) == verified_events
    checks.append("verified attempts reject late failed/decision/resolved results without mutation")
    git(repair_work, "push", str(fork), "HEAD:refs/heads/pagoda")
    done("tick")
    assert read(done_state)["phase"] == "complete"
    assert len(events(done_boundary)) == 1
    sync = fixture / "scheduled-sync"
    run("git", "clone", "-b", "pagoda", str(fork), str(sync), env=env)
    result = run(
        "bash",
        ".pagoda/sync.sh",
        cwd=sync,
        env=env
        | {"PAGODA_UPSTREAM_URL": str(upstream), "PAGODA_FORK_REMOTE": "origin"},
    )
    assert "changed=false" in result.stdout
    checks.append(
        "agent resolution verified at exact commit; remote publication observed; sync resumes"
    )

    # The active request is pinned even while the upstream ref advances.
    pinned_state, pinned_boundary, pinned = scenario("upstream-advances")
    # A new patch can conflict at the same upstream SHA after the first repair.
    git(seed, "switch", "-c", "new-patch", base)
    write(seed / "contract.txt", "new patch behavior\n")
    patch_sha = commit(seed, "new patch")
    git(seed, "push", "origin", "HEAD:refs/heads/new-patch")
    write(sync / ".pagoda/patches.txt", "branch:new-patch\n")
    commit(sync, "register patch")
    git(sync, "push", "origin", "HEAD:refs/heads/pagoda")
    advance_stamp(done_state)
    done("tick")
    assert read(done_state)["phase"] == "requested"
    assert read(done_state)["generation"] == 2
    assert len(events(done_boundary)) == 2
    pinned("tick")
    assert any(t["sha"] == patch_sha for t in read(pinned_state)["targets"])
    git(seed, "switch", "main")
    write(seed / "next.txt", "another upstream change\n")
    newer_sha = commit(seed, "next upstream")
    git(seed, "push", "upstream", "main")
    pinned("tick")
    assert len(events(pinned_boundary)) == 1
    assert read(pinned_state)["attempt"] == 1
    assert (
        json.loads((pinned_state / "active.json").read_text())["upstream"]
        == upstream_sha
    )
    assert not (pinned_state / "repairs" / newer_sha).exists()
    checks.append(
        "changed patches reopen same-SHA state; newer upstream preserves the active repair"
    )

    report = {
        "status": "passed",
        "checks": checks,
        "fixture": str(fixture),
        "base": base,
        "upstream": upstream_sha,
        "fork_before": original_fork,
        "repair_commit": repaired,
        "driver_git_head": git(ROOT, "rev-parse", "HEAD"),
        "source_sha256": {
            str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (DRIVER, Path(__file__).resolve(), BUZZ, STAMP_FAILURE)
        },
        "driver_worktree_status": git(
            ROOT,
            "status",
            "--short",
            "--",
            ".pagoda/resolver.py",
            "scripts/test-pagoda-resolver.py",
        ),
    }
    write(artifact, json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
