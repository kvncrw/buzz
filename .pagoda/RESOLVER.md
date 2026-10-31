# Repair a fork sync conflict

Writing standard: googledev-style.

## Contract and failure cases

The existing headshot resolver queues work for an agent. It never pushes code.
Keep its existing cron entry and lock; do not install a second resolver.
The owner configures the existing sender seat, repair agent, decision maker,
channel, and thread in a local `config.json`. Credentials stay in the seat
environment file, outside the repository.

The end-to-end fixture must exercise these failures before installation:

1. An upstream change conflicts with the fork. Repeated ticks create one
   durable request for the upstream SHA and retain the conflicted worktree.
2. A newer workflow run reports the same SHA. Its URL can update provenance,
   but cannot reset the two-agent-attempt budget.
3. The state directory or rate-limit stamp cannot be written. No notification
   leaves the process. The lock also prevents overlapping ticks and reports.
4. Buzz accepts a send but readback is missing or different. Keep the returned
   event ID, retry that read, and never mark the request delivered early.
5. A send has an ambiguous outcome. Preserve an uncertain outbox record and
   reconcile the existing event before allowing any resend.
6. An agent reports an unresolved, dirty, untested, or wrong commit. Reject
   the report; do not publish or close the repair.
7. One agent attempt fails. Queue a second attempt in the same worktree.
   A second failure or a product-semantics decision notifies the decision maker.
8. The agent resolves the conflict, tests the exact commit, and pushes it.
   Observe the commit on the fork before completing the repair. The scheduled
   sync can then run again without conflict.
9. Upstream advances during a repair. Keep the active repair and its attempt
   budget; process the newer SHA after the active repair finishes.

## Verification

Run `python3 scripts/test-pagoda-resolver.py --artifact <path-to-result.json>`.
The fixture uses local bare repositories and substitutes only the Buzz process
boundary. It exercises the real driver and `.pagoda/sync.sh` and retains its
repositories, requests, and JSON evidence beside the artifact.

## Installation and operation

1. Keep the existing `buzz-fork-resolver` directory and its cron entry. Archive
   its current `resolve.sh` before replacing that entry point with
   `resolver-install-wrapper.sh`. Install `resolver.py` and this runbook beside
   it. Do not replace or reset its old `work/`, `attempts/`, or `resolve_file.py`.
2. Copy `resolver-config.example.json` to the existing resolver directory as
   `config.json`. Set the actual fork, channel, reply destination, public keys,
   and workflow URL. The wrapper sources the existing neighboring
   `fable-seat/buzz-env`. That sender must differ from the repair agent.
3. Run the fixture before installation. Then verify the single existing cron
   entry invokes `resolve.sh`. The driver holds the existing `.lock` with OS
   `flock`, including when an agent reports a result. Do not wrap it in another
   lock on the same file. The wrapper has no network-polling loop.
4. Run one installed tick and inspect `active.json` and the active
   `repairs/<upstream-sha>/state.json`. The tick fetches Git refs, without listing
   GitHub workflow runs. It creates a separate `cache.git` and isolated repair
   worktrees. Clean merges wait for the existing sync workflow; only a reproduced
   merge conflict queues an agent repair.

`config.json` contains routing metadata only. Keep it outside the source repo.
The driver never calls an LLM directly and never starts another resolver.
The normal interval between notification attempts is 600 seconds, measured by
the stamp's mtime. A later workflow run cannot reset an upstream SHA's failure
count. A changed patch at the same SHA creates a new worktree generation and
preserves that failure count. A newer upstream SHA waits for the active repair.

An optional exact sync run URL is supplied by a caller that already knows it:

```bash
python3 "$resolver_dir/resolver.py" --state "$resolver_dir" tick \
  --run-upstream "$upstream_sha" --run-url "$run_url"
```

Without that input, the request links the configured workflow index and says
that no exact run was supplied. The driver does not invent a run URL or poll
the GitHub API to discover one.

## Complete a repair

1. Read the request's `state.json` and the repository's instructions. Use its
   existing worktree. Preserve both sides' product intent; never use blanket
   `ours` or `theirs` resolution. Resolve compiler and test failures outside
   the conflicted hunks as well.
2. Finish the merge and include every SHA listed in `targets`. Preserve the
   original `fork_before` ancestry. Run the full affected package checks and
   the behavior-specific E2E checks. Obtain the required independent review.
3. Commit the repair and rerun the checks at that exact commit. Write a JSON
   receipt outside the worktree. Include the full commit SHA and a nonempty
   `checks` array with each check's `name` and numeric `exit_code`. Preserve
   detailed command output beside the receipt. The driver validates receipt
   structure and Git state; the agent remains responsible for running those
   checks and for the receipt's truth.
4. Report the result with the active attempt number:

```bash
python3 "$resolver_dir/resolver.py" --state "$resolver_dir" result \
  --upstream "$upstream_sha" --attempt "$attempt" --outcome resolved \
  --commit "$verified_commit" --evidence "$receipt_path"
```

5. Push the verified commit using a normal fast-forward push. Fetch the current
   remote immediately before pushing. If it has advanced incompatibly, merge
   its changes, repeat validation, and submit the same `result` command with
   the new verified commit before publication. Never force-push. The driver observes
   the verified commit on remote `pagoda` before marking it `complete`.
6. Post the commit, checks, and workflow evidence in the originating thread.
   Completion observed without an agent receipt is labeled `synchronized`,
   which means another actor restored sync, not that this driver tested it.

Report a failed attempt with `--outcome failed --reason '<concrete failure>'`.
Report a product decision with `--outcome decision --reason '<specific choice>'`.
Both use the same `result`, upstream, and attempt arguments. Only a reported
agent failure consumes the budget. Duplicate or stale results are rejected.
The first failure queues a second attempt in the same worktree. The second
failure or an explicit decision notifies the configured decision maker. Later
workflow runs cannot reopen that exhausted budget.

## Recover delivery failures

An accepted notification counts as delivered only when a read of the returned
event ID matches its nonempty body, author, channel, mention, and reply target.
The outbox is durable before sending. Missing or wrong readback retries the
existing event, without another send. If a send loses its response, the next
tick searches at most 100 channel events since the attempt for the exact body
and author. It records the event ID only when there is one match.

If there is no unique match, the driver exits with an error and preserves the
uncertain outbox. Inspect relay history and local state before repairing that
record. Do not delete it to trigger another send: the first send may have
succeeded. A disk or stamp failure also suppresses notification. The separate
job heartbeat owns operational status; this driver does not add another timer.

## Rollback

Restore the archived `resolve.sh` while the existing `.lock` is free. Keep the
new state and worktrees for inspection. Do not run old and new entry points
concurrently. No release, relay deployment, or remote Git ref is changed by
rolling this driver back.
