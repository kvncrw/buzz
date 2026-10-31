#!/usr/bin/env python3
"""Queue fork merge repairs for an agent, retaining state by upstream commit.

Only Git reads, local worktree changes, and Buzz notifications are automated.
The agent owns validation and a normal fast-forward push after validation.
"""

import argparse
import fcntl
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

SHA = re.compile(r"^[0-9a-f]{40}$")
EVENT = re.compile(r"^[0-9a-f]{64}$")


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def atomic(path, value):
    """Persist before external effects; an empty/truncated file is never state."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as out:
        temporary = Path(out.name)
        try:
            out.write(json.dumps(value, indent=2) + "\n")
            out.flush()
            os.fsync(out.fileno())
            os.replace(temporary, path)
            directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            temporary.unlink(missing_ok=True)


def read(path):
    return json.loads(path.read_text())


class Resolver:
    def __init__(self, root):
        self.root = root
        self.config = read(root / "config.json")
        for key in (
            "fork_url",
            "upstream_url",
            "channel",
            "reply_to",
            "agent_name",
            "agent_pubkey",
            "owner_name",
            "owner_pubkey",
            "sender_pubkey",
            "workflow_url",
        ):
            require(
                isinstance(self.config.get(key), str) and self.config[key],
                f"missing config: {key}",
            )
        require(
            self.config["sender_pubkey"] != self.config["agent_pubkey"],
            "the sender must differ from the repair agent; self-mentions do not wake it",
        )
        self.cache = root / "cache.git"
        self.repairs = root / "repairs"
        self.repairs.mkdir(exist_ok=True)
        self.logs = root / "log"
        self.logs.mkdir(exist_ok=True)
        self.buzz = self.config.get("buzz_command", ["buzz"])
        require(
            isinstance(self.buzz, list)
            and self.buzz
            and all(isinstance(x, str) for x in self.buzz),
            "buzz_command must be an argument array",
        )

    def command(self, args, *, cwd=None, content=None, check=True):
        # Files bound memory use even when Git emits many conflict paths.
        with tempfile.TemporaryFile(dir=self.logs) as out:
            process = subprocess.Popen(
                args,
                cwd=cwd,
                text=True,
                start_new_session=True,
                stdin=subprocess.PIPE if content is not None else subprocess.DEVNULL,
                stdout=out,
                stderr=out,
            )
            try:
                process.communicate(content, timeout=45)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.communicate()
                raise
            size = out.tell()
            out.seek(max(0, size - 131072))
            output = out.read().decode(errors="replace")
        if check and process.returncode:
            raise RuntimeError(
                f"{args[0]} failed (exit {process.returncode}): {output[-4000:]}"
            )
        return process.returncode, output.strip()

    def git(self, *args, work=None, check=True):
        return self.command(["git", "-C", str(work or self.cache), *args], check=check)

    def ancestor(self, older, newer):
        status, output = self.git(
            "merge-base", "--is-ancestor", older, newer, check=False
        )
        require(status in (0, 1), f"cannot determine ancestry: {output}")
        return status == 0

    def save(self, state):
        atomic(self.repairs / state["upstream"] / "state.json", state)

    def load(self, upstream):
        require(bool(SHA.fullmatch(upstream)), "expected a full upstream SHA")
        path = self.repairs / upstream / "state.json"
        return read(path) if path.exists() else None

    def fetch(self):
        if not self.cache.exists():
            self.command(["git", "init", "--bare", str(self.cache)])
        for remote, ref in (("fork", "pagoda"), ("upstream", "main")):
            self.git(
                "fetch",
                "--no-tags",
                "--quiet",
                self.config[remote + "_url"],
                f"refs/heads/{ref}:refs/remotes/{remote}/{ref}",
            )
        fork = self.git("rev-parse", "refs/remotes/fork/pagoda")[1]
        upstream = self.git("rev-parse", "refs/remotes/upstream/main")[1]
        return fork, upstream

    def targets(self, fork, upstream):
        targets = [{"label": "upstream main", "sha": upstream}]
        patches = self.git("show", f"{fork}:.pagoda/patches.txt")[1]
        for line in patches.splitlines():
            line = "".join(line.split("#", 1)[0].split())
            if not line:
                continue
            kind, _, value = line.partition(":")
            if kind == "branch":
                self.git("check-ref-format", "refs/heads/" + value)
                url, ref = self.config["fork_url"], "refs/heads/" + value
            elif kind == "upstream-pr" and value.isdigit():
                url, ref = self.config["upstream_url"], f"refs/pull/{value}/head"
            else:
                raise RuntimeError(f"unsupported patch entry: {line}")
            self.git("fetch", "--quiet", "--no-tags", url, ref)
            targets.append(
                {"label": line, "sha": self.git("rev-parse", "FETCH_HEAD")[1]}
            )
        return targets

    def prepare(self, state):
        work = Path(state["worktree"])
        if not work.exists():
            self.git("worktree", "add", "--detach", str(work), state["fork_before"])
        require(
            self.git("rev-parse", "--show-toplevel", work=work)[1] == str(work),
            "repair worktree path does not match its Git root",
        )
        conflicts = self.git("diff", "--name-only", "--diff-filter=U", work=work)[
            1
        ].splitlines()
        if not conflicts:
            require(
                not self.git("status", "--porcelain", work=work)[1],
                "preparing worktree contains edits; preserve them and inspect it",
            )
            for target in state["targets"]:
                if self.ancestor(
                    target["sha"], self.git("rev-parse", "HEAD", work=work)[1]
                ):
                    continue
                status, output = self.git(
                    "-c",
                    "user.name=Pagoda resolver",
                    "-c",
                    "user.email=resolver@localhost",
                    "-c",
                    "commit.gpgsign=false",
                    "-c",
                    "core.hooksPath=/dev/null",
                    "merge",
                    "--no-edit",
                    "--no-ff",
                    "--signoff",
                    target["sha"],
                    work=work,
                    check=False,
                )
                conflicts = self.git(
                    "diff", "--name-only", "--diff-filter=U", work=work
                )[1].splitlines()
                if conflicts:
                    break
                require(
                    status == 0, f"merge failed without conflicts: {output[-2000:]}"
                )
        state["conflicts"] = conflicts
        state["phase"] = "request_pending" if conflicts else "waiting_sync"
        self.save(state)

    def verified_event(self, event, delivery):
        tags = event.get("tags", [])
        return (
            event.get("id") == delivery.get("event_id")
            and event.get("kind") == 9
            and event.get("pubkey") == self.config["sender_pubkey"]
            and bool(event.get("content"))
            and event.get("content") == delivery["content"]
            and ["h", self.config["channel"]] in tags
            and ["p", delivery["mention"]] in tags
            and any(
                len(t) >= 4
                and t[:2] == ["e", self.config["reply_to"]]
                and t[3] == "reply"
                for t in tags
            )
        )

    def readback(self, state):
        delivery = state["delivery"]
        if not delivery.get("event_id"):
            # An accepted send can lose its response. Reconcile the exact durable
            # body and author instead of generating another notification.
            _, raw = self.command(
                self.buzz
                + [
                    "messages",
                    "get",
                    "--channel",
                    self.config["channel"],
                    "--since",
                    str(delivery["created_at"] - 1),
                    "--limit",
                    "100",
                ]
            )
            candidates = json.loads(raw)
            require(
                isinstance(candidates, list), "invalid Buzz reconciliation response"
            )
            matches = []
            for event in candidates:
                candidate = delivery | {"event_id": event.get("id")}
                if self.verified_event(event, candidate):
                    matches.append(event)
            require(
                len(matches) == 1,
                "ambiguous delivery needs event reconciliation; automatic resend suppressed",
            )
            delivery["event_id"] = matches[0]["id"]
            self.save(state)
        _, raw = self.command(
            self.buzz + ["social", "event", "--event", delivery["event_id"]]
        )
        events = json.loads(raw)
        if isinstance(events, dict):
            events = [events]
        require(
            isinstance(events, list)
            and len(events) == 1
            and self.verified_event(events[0], delivery),
            "Buzz readback does not match the pending notification",
        )
        delivery["status"] = "verified"
        state["phase"] = "escalated" if delivery["kind"] == "decision" else "requested"
        self.save(state)

    def notify(self, state):
        existing = state.get("delivery")
        if existing and existing["status"] != "verified":
            self.readback(state)
            return
        directory = self.repairs / state["upstream"]
        stamp = directory / "notify.stamp"
        if stamp.exists():
            require(stamp.is_file(), "notification stamp is not a file")
            interval = max(60, self.config.get("notification_interval_seconds", 600))
            if time.time() - stamp.stat().st_mtime < interval:
                return
        decision = state["phase"] == "decision_pending"
        name = self.config["owner_name" if decision else "agent_name"]
        mention = self.config["owner_pubkey" if decision else "agent_pubkey"]
        request_id = (
            f"pagoda-repair:{state['upstream']}:g{state['generation']}:"
            f"{'decision' if decision else state['attempt']}"
        )
        if decision:
            task = (
                f"A decision is needed after {state['failures']} failed agent attempts. "
                f"Reason: {state['reports'][-1]['reason']}"
            )
        else:
            task = (
                f"Repair attempt {state['attempt']} of 2. Resolve the merge and compiler/test failures; "
                "preserve patch intent. Escalate product-semantics conflicts instead of choosing behavior."
            )
        content = (
            f"@{name} {task}\n\nUpstream: `{state['upstream']}`. "
            f"Conflict paths: {', '.join(state['conflicts'])}.\n"
            f"Worktree: `{state['worktree']}`.\n"
            f"Request: `{directory / 'state.json'}`. Runbook: `{self.root / 'RESOLVER.md'}`.\n"
            f"Sync evidence: {state['run_url']} ({state['run_url_kind']}).\n"
            f"Validate the full affected package at the exact commit, record the result, "
            f"then use a normal fast-forward push. Resolver never pushes.\n[{request_id}]"
        )
        delivery = {
            "status": "uncertain",
            "content": content,
            "mention": mention,
            "kind": "decision" if decision else "repair",
            "created_at": int(time.time()),
        }
        # A failed stamp reservation must leave the request eligible for retry.
        # Record uncertain delivery only after every stamp operation succeeds.
        with stamp.open("w") as output:
            output.flush()
            os.fsync(output.fileno())
        os.utime(stamp, None)
        state["delivery"] = delivery
        self.save(state)
        code, raw = self.command(
            self.buzz
            + [
                "messages",
                "send",
                "--channel",
                self.config["channel"],
                "--reply-to",
                self.config["reply_to"],
                "--mention",
                mention,
                "--content",
                "-",
            ],
            content=content,
            check=False,
        )
        try:
            sent = json.loads(raw)
        except json.JSONDecodeError:
            sent = {}
        if EVENT.fullmatch(str(sent.get("event_id", ""))):
            delivery["event_id"] = sent["event_id"]
            self.save(state)
        require(
            code == 0 and sent.get("accepted") is True and delivery.get("event_id"),
            "Buzz send outcome uncertain; reconcile before any resend",
        )
        self.readback(state)

    def tick(self, args):
        fork, upstream = self.fetch()
        active_path = self.root / "active.json"
        active = read(active_path).get("upstream") if active_path.exists() else None
        state = self.load(active) if active else None
        if state and state["phase"] not in ("complete", "synchronized"):
            if state.get("result_commit"):
                synchronized = self.ancestor(state["result_commit"], fork)
            else:
                synchronized = all(
                    self.ancestor(t["sha"], fork) for t in state["targets"]
                )
            if synchronized:
                state["phase"] = (
                    "complete" if state.get("result_commit") else "synchronized"
                )
                state["observed_fork"] = fork
                self.save(state)
        if not state or state["phase"] in ("complete", "synchronized"):
            targets = self.targets(fork, upstream)
            if all(self.ancestor(t["sha"], fork) for t in targets):
                return
            state = self.load(upstream)
            if not state or state["phase"] in ("complete", "synchronized"):
                previous = state
                generation = previous["generation"] + 1 if previous else 1
                failures = previous["failures"] if previous else 0
                state = {
                    "upstream": upstream,
                    "fork_before": fork,
                    "targets": targets,
                    "worktree": str(
                        self.root / "worktrees" / f"{upstream}-g{generation}"
                    ),
                    "conflicts": [],
                    "phase": "preparing",
                    "attempt": failures + 1,
                    "failures": failures,
                    "generation": generation,
                    "reports": previous["reports"] if previous else [],
                    "run_url": self.config["workflow_url"],
                    "run_url_kind": "workflow index; exact run not supplied",
                }
                if previous:
                    history = previous.get("history", [])
                    history.append(
                        {
                            k: previous.get(k)
                            for k in (
                                "generation",
                                "worktree",
                                "phase",
                                "result_commit",
                                "verification",
                            )
                        }
                    )
                    state["history"] = history
                self.save(state)
            atomic(active_path, {"upstream": upstream})
        if args.run_url:
            require(
                args.run_upstream == state["upstream"],
                "run URL must identify the active upstream SHA",
            )
            state.update(run_url=args.run_url, run_url_kind="supplied exact run")
            self.save(state)
        if state["phase"] == "preparing":
            self.prepare(state)
            if state["phase"] == "request_pending" and state["failures"] >= 2:
                state["phase"] = "decision_pending"
                self.save(state)
        if state["phase"] in ("request_pending", "decision_pending"):
            self.notify(state)

    def result(self, args):
        state = self.load(args.upstream)
        require(state is not None, "unknown repair")
        require(
            state["phase"] == "requested" and state["attempt"] == args.attempt,
            "result must match the active, delivered attempt",
        )
        if args.outcome == "resolved":
            require(
                args.commit and SHA.fullmatch(args.commit),
                "report the full verified commit",
            )
            work = Path(state["worktree"])
            require(
                self.git("rev-parse", "HEAD", work=work)[1] == args.commit,
                "reported commit is not worktree HEAD",
            )
            require(
                not self.git("status", "--porcelain", work=work)[1],
                "repair worktree is dirty or unresolved",
            )
            require(
                self.git("rev-parse", "--verify", "MERGE_HEAD", work=work, check=False)[
                    0
                ]
                != 0,
                "repair merge is unfinished",
            )
            require(
                all(self.ancestor(t["sha"], args.commit) for t in state["targets"])
                and self.ancestor(state["fork_before"], args.commit),
                "repair drops required merge ancestry",
            )
            require(args.evidence, "exact-commit verification evidence is required")
            evidence = read(Path(args.evidence))
            checks = evidence.get("checks")
            require(
                evidence.get("commit") == args.commit
                and isinstance(checks, list)
                and checks
                and all(
                    isinstance(c, dict)
                    and c.get("name")
                    and type(c.get("exit_code")) is int
                    and c["exit_code"] == 0
                    for c in checks
                ),
                "evidence must record passing checks for the reported commit",
            )
            state.update(
                phase="verified", result_commit=args.commit, verification=evidence
            )
        else:
            require(
                args.reason and args.reason.strip(),
                "failure or decision needs a concrete reason",
            )
            state["reports"].append(
                {
                    "attempt": args.attempt,
                    "outcome": args.outcome,
                    "reason": args.reason,
                }
            )
            if args.outcome == "failed":
                state["failures"] += 1
            if args.outcome == "decision" or state["failures"] >= 2:
                state["phase"] = "decision_pending"
            else:
                state["attempt"] += 1
                state["phase"] = "request_pending"
        self.save(state)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    tick = commands.add_parser("tick")
    tick.add_argument("--run-url")
    tick.add_argument("--run-upstream")
    result = commands.add_parser("result")
    result.add_argument("--upstream", required=True)
    result.add_argument("--attempt", type=int, required=True)
    result.add_argument(
        "--outcome", choices=("failed", "decision", "resolved"), required=True
    )
    result.add_argument("--commit")
    result.add_argument("--evidence")
    result.add_argument("--reason")
    args = parser.parse_args()
    root = args.state.resolve()
    require(root.is_dir(), "state directory must already exist")
    # This is the existing resolver's OS flock, also held for agent reports.
    with (root / ".lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            require(args.command == "tick", "resolver is busy; retry the result report")
            return
        driver = Resolver(root)
        getattr(driver, args.command)(args)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, ValueError, subprocess.TimeoutExpired) as error:
        print(f"resolver: {error}", file=sys.stderr)
        sys.exit(1)
