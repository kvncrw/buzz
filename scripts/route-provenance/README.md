# Route provenance discovery fixture

Writing standard: googledev-style.

This fixture runs the installed Claude ACP adapter against a loopback HTTP
test double and a synthetic MCP tool. It does not call a model, publish a Buzz
message, or change gateway configuration. Supply the installed adapter entry
point and an artifact directory outside `/tmp`.

```bash
node scripts/route-provenance/probe.mjs /absolute/path/to/dist/index.js /absolute/artifact/directory
```

## Failure checks defined before implementation

1. Concurrent ACP sessions must carry different header and MCP environment tokens.
2. The session token observed by the HTTP fixture must match the tool process.
3. Reusing a session for another turn must not be mistaken for fresh turn context.
   The unmodified adapter is expected to retain creation-time headers and tool
   environment. Record that limitation explicitly; this discovery run does not
   pass the production turn-correlation gate.
4. A failed ACP request, missing tool call, unexpected tool, or timeout fails
   the fixture. Cleanup terminates the fixture process group and HTTP listener.
5. Save only synthetic identifiers and assertions, never inherited credentials
   or real prompts. Use a separate Claude config directory and disable hooks.
6. Reload the retained session through `session/load` with a new header and
   MCP token. Require the next HTTP request and tool execution to carry the new
   token, and require earlier synthetic prompts to remain in history. Record
   whether the prior tool process has exited. This observation alone does not
   prove cancellation or background-tool fencing.
7. Resume the same persisted session through supported `session/resume`, passing
   the full SDK and MCP configuration with changed tokens. Require prior prompts
   in the next model request and no ACP history replay during resume.
8. Start a deliberately delayed MCP call with an old token, then send the ACP
   `session/cancel` notification. Require the prompt to settle as cancelled.
   Observe whether the MCP call continues after cancellation and whether its
   token remains unchanged. This synthetic tool records cancellation but does
   not cooperate with it; it represents work that cannot be recalled.
9. Resume with a new token while the delayed call is pending. Require fresh HTTP
   and tool calls to match the new token, and require the independent session to
   remain usable. Record old-process survival and any delayed completion; do
   not assume closing the SDK query synchronously kills all old work.
10. Attribute results to the current Git commit, all fixture source hashes,
    installed adapter source hashes, and installed SDK version. Timeouts fail
    the fixture, and cleanup has a bounded process-group kill path.
11. Remove session B's MCP registration before its second prompt. That run must
    fail, even if the model returns `end_turn`. Require exactly one completed
    tool call correlated to each successful fixture turn. Recognize ancillary
    SDK requests by their own request shape, never by a missing tool alone.
12. Close an adapter subprocess's stdin while it remains alive, then send an RPC.
    An `EPIPE` must reject pending requests, run cleanup, and write a failed
    result artifact. Use a small synthetic subprocess for this transport fault;
    the normal and missing-tool runs use the installed adapter.
13. Hold the stream for 2,000 ms after `content_block_stop`, after
    `message_delta`, and after `message_stop` in separate fresh sessions.
    Inspect the MCP lifecycle file during each hold, then complete normally.
    Require exactly one matching tool call per normal request.
14. In fresh sessions, replace completion after the first two boundaries with
    an Anthropic error or a socket failure. Record every retry with a new nonce;
    permit at most three tool-producing requests per case and bound ACP calls
    to 25 seconds. Later attempts complete normally to make recovery observable.
15. Cancel fresh prompts during each preterminal hold. Record tool lifecycle,
    stream frames, response closure, cancellation, and prompt settlement. Do
    not treat a tool starting early as a fixture failure.
16. Run an independent control prompt during every held case. Require its
    token and nonce to match its own completed tool. Never accept a timeout as
    evidence that a tool waits for a later boundary.
17. Exercise the installed CLI's send and edit paths against a loopback relay
    double, using an ephemeral synthetic key. Capture the signed event and
    require readback to match both that event and the intended labeled content.
    An accepted write response alone must never verify delivery.
18. Remove or change the label on readback while retaining the event ID. Require
    `UNVERIFIED`. Also reject an absent event, changed body, author, signature,
    timestamp, kind, or tags, and compact responses missing signed fields.
19. Submit genuinely signed unlabeled content while the intent requires a
    label. Even an exact readback of that event must remain `UNVERIFIED`.
    Cover degraded and unknown labels on send and edit, plus primary recovery.
20. Run the ID-only verifier as an explicit negative control. It must fail the
    stripped-label assertion. The strict verifier must pass the same scenario.
    These checks do not implement automatic stamping or token retirement.
21. With the opt-in publication lifecycle fixture, retire session A's context
    before forwarding ACP cancellation. Its already-running MCP tool must
    attempt a real CLI send afterward, fail, and submit no message. Disabling
    retirement must make this assertion fail, not pass through a timeout.
22. Resume A with a new context and keep B independent. Both must publish through
    the guarded CLI; B's context must remain unchanged. Loading or resuming a
    changed context must retire A's previous context under the stable lock.
23. Use only an ephemeral fixture key and loopback relay. Record CLI/adapter
    hashes, context transitions, attempted sends, and submitted signed events.
    Cleanup must terminate all known tools. This is isolated harness wiring,
    not activation of the production ACP harness or proof of route receipts.
24. Exercise the installed LiteLLM response-header and streaming-iterator
    dispatchers with synthetic nested responses. A selected deployment alone
    must remain unknown. Before exposing the first content chunk, correlate
    outer, inner, and leaf records by turn, root request, parent, and attempt.
    The outer alias must not replace the leaf deployment.
25. Hold terminal completion while observing the receipt at the consumer
    boundary. Completion-only recording must fail this assertion. A later
    stream error or cancellation must not create a completed receipt; bytes
    already contributed remain recorded. Publication retirement is separate.
26. Missing inner evidence, mismatched tokens, duplicate IDs, absent deployment
    IDs, a different root request, and a new store after restart must remain
    unknown or fail closed. Receipt-store failure must prevent chunk release.
27. Interleave independent relay, author, channel, session, and turn scopes.
    Primary recovery starts clean in a new turn. A primary call or health probe
    cannot erase a fallback contribution in the old turn. Failed attempts must
    not replace the contributing attempt.
28. Record installed LiteLLM source hashes and exact Git state. This callback
    fixture supplies synthetic authenticated scope and hop links. It does not
    prove live forwarding, authentication, durable storage, or CLI stamping.
29. Exercise the installed Anthropic SSE encoder and proxy SSE generator. The
    iterator receives bytes on that path. Preserve the encoded bytes exactly;
    record the fallback before releasing a complete tool-use event. Repeat at
    every byte split, including UTF-8 and CRLF boundaries. Comments and pings
    must not count as model contribution.
30. Reject malformed JSON, mismatched event types, oversized unfinished frames,
    invalid UTF-8, and content after terminal completion. An error event must
    never create a completed receipt, even if followed by `message_stop`.
    Incomplete framing remains unknown and cannot claim completion.
31. While terminal completion is held, pass the receipt to an isolated real CLI
    send and require exact signed readback of the fixture-derived label. This
    proves receipt availability at publication time, not automatic production
    stamping or an authenticated cross-process store.
32. Enable CLI stamping only with `BUZZ_ROUTE_LABELS=1` and a valid publication
    context. Read `route.json` under the existing signing lock. Disabled sends
    and edits retain their content. Enabled primary, degraded, and unknown
    snapshots must produce exact signed content and relay readback.
33. Missing, malformed, oversized, nonregular, expired, future-dated, wrong-token,
    wrong-relay, wrong-author, wrong-channel, or invalid-version snapshots must
    produce unknown. Unsafe fallback names and inconsistent primary/degraded
    states must also produce unknown. Never print a snapshot in diagnostics.
34. Replace a snapshot during network preflight and require the signing-time
    snapshot. Retired contexts still reject publication. Missing fencing or
    invalid label configuration must not silently publish uninstrumented text.
35. Preserve human-authored leading marker lines byte for byte, including
    identical, stale, repeated, indented, LF, and CRLF forms. Raw send/edit
    inputs have no proof that the CLI stamped them. Reject empty input. Check the final
    UTF-8 byte length after stamping, including the 65,536-byte boundary.
36. Concurrent turns consume only their own snapshots; a new primary turn is
    clean. Multiple verified fallback contributors use one sorted, deduplicated
    label. The trusted producer must supply the cumulative turn snapshot;
    this CLI consumer does not authenticate receipt ingress or reconstruct it.
37. Rewrite the signed body and recompute its ID while retaining the old
    signature. Reject both a rewritten labeled body and a stripped label with
    a primary-shaped intent. Exercise trusted and forged snapshot paths through
    real CLI readback. Collect every failure in the result artifact.
38. Require the independently captured sign-time event ID, even for a different
    event with a valid signature. A missing sign-time ID fails closed.
39. Retire the token while the custom emoji palette query is held. Retirement
    must acquire the exclusive lock before the query returns, and the CLI must
    then reject signing. A lock timeout fails this scenario; it cannot stand
    in for successful retirement. No network request may hold the signing lock.

Run the two negative checks by adding `missing-b2-tool` or `stdin-epipe` as the
third argument. Both must exit with status 1 and write `passed: false`, with
no surviving subprocesses. A passing negative run is a fixture failure.

Before production labels can be enabled, extend coverage to cancellation,
late callbacks, gateway hops, restart fencing, nested fallbacks, and signed
send/edit readback. A passing discovery run is not proof of those properties.

## Opt-in CLI publication fence

The CLI has a disabled-by-default fence on ordinary `messages send` and
`messages edit`. Automatic labels are a separate opt-in described below.
The production harness lifecycle is not connected.
Set both `BUZZ_PUBLICATION_CONTEXT` (an absolute directory) and
`BUZZ_PUBLICATION_TOKEN` (the current opaque token) to require fencing.
With neither set, existing behavior is unchanged. Partial configuration rejects
signing. The directory must contain regular files `lock` and `context.json`:

```json
{
  "version": 1,
  "token": "synthetic-turn-token",
  "state": "active",
  "expires_at": 1900000000,
  "relay": "http://localhost:3000",
  "agent_pubkey": "the signing agent's hex public key",
  "channel_id": "the destination channel UUID"
}
```

The trusted harness owns these files. Retirement takes an exclusive OS lock on
`lock` before changing the JSON state to `retired`. The signer takes a shared
lock, checks identity, destination, token and expiry, and holds it through
signing. Do not replace the lock inode. The JSON record can be replaced under
the lock. A busy lock, nonregular file, missing context, or invalid record fails
closed. A signature completed before retirement cannot be recalled.

Run the send/edit race fixture against the newly built CLI:

```bash
cargo build --locked -p buzz-cli
node scripts/route-provenance/retirement-probe.mjs /absolute/worktree/target/debug/buzz /absolute/artifact/directory
cargo test --locked -p buzz-cli
```

The fixture pauses a real CLI command during its relay preflight, retires or
supersedes the token with an exclusive lock, then lets the command continue.
It requires zero message submissions from the invalidated command. It also
checks active contexts, identity/destination mismatches, expiry, missing and
malformed files, FIFOs, and an exclusive lock held by another process. All
traffic stays on a loopback relay double with an ephemeral synthetic key.

## Opt-in automatic labels

Set `BUZZ_ROUTE_LABELS=1` only for an instrumented model-authored publication.
The flag requires a valid publication context. Unset or `0` preserves existing
content; other values reject publication. The signer reads `route.json` from
the context directory under the same lock that fences retirement. It holds
that lock through formatting, final size validation, and signing.

The trusted producer writes a cumulative snapshot while holding the exclusive
lock. It must never reuse a token across turns or clear a known fallback
contribution within a turn. This CLI consumer does not authenticate ingress or
rebuild the contribution history. The producer and production wiring remain
separate implementation gates.

```json
{
  "version": 1,
  "token": "the matching context token",
  "relay": "http://localhost:3000",
  "agent_pubkey": "the signing agent's hex public key",
  "channel_id": "the destination channel UUID",
  "observed_at": 1899999900,
  "expires_at": 1900000000,
  "status": "degraded",
  "fallbacks": ["verified-leaf-name"]
}
```

`primary` requires an empty fallback list. `degraded` requires at least one
verified leaf. Multiple leaves are sorted and deduplicated inside one prefix.
The snapshot permits at most 16 names, each 1–128 ASCII letters, digits, or
`._:/-` characters. Its expiry cannot exceed the context expiry. The snapshot
must be a regular file, at most 16 KiB; future observations, expired evidence,
schema errors, and scope mismatches produce `[routing unknown]`. A missing
snapshot also produces unknown. An invalid or retired context rejects signing.

The formatter preserves raw send/edit input byte for byte, including leading
lines that resemble labels. Input text carries no proof that the CLI stamped
it, so matching a label's text and position is insufficient to remove it.
Pass the raw body when editing; copying a previously rendered prefix into the
input retains it as body text. Empty input is rejected before upload.
Final content, including media links and the new label, must fit
65,536 bytes. The fence cannot recall an already signed event; it assumes
cooperating tools and is not a security boundary against code with the key.

Run the acceptance fixture against the newly built binary:

```bash
node scripts/route-provenance/stamping-probe.mjs /absolute/worktree/target/debug/buzz /absolute/artifact/directory
```

The nested receipt fixture now passes raw message bodies to this consumer.
Its three signed readbacks prove automatic stamping before terminal completion
in the isolated pipeline. The fixture still supplies trusted scope and writes
the snapshot; authenticated transport and live forwarding are not proved.

The separate delivery fixture compares intended labels with signed readback.
Install locked workspace dependencies first (`pnpm install --frozen-lockfile`).
The verifier uses the workspace-pinned `@noble/curves` BIP-340 implementation
to check the signature over the recomputed event ID. Its API also requires
`signedEventId` captured at signing, independently of readback. A valid
signature alone cannot establish which event the caller intended to publish.
It checks the exact expected prefix at offset zero, leaving marker-shaped
human text in the body intact.

```bash
node scripts/route-provenance/delivery-probe.mjs /absolute/path/to/buzz /absolute/artifact/directory
node scripts/route-provenance/delivery-probe.mjs /absolute/path/to/buzz /absolute/artifact/directory legacy-id-only
```

The second command must fail on stripped-label readback. Explicit fixture
labels do not prove automatic stamping. Neither fixture enables live labels.
The first command also rejects recomputed IDs with retained signatures, both
against a trusted snapshot and when a caller supplies a forged snapshot. It
rejects a different validly signed event and an absent sign-time ID.

## Isolated ACP lifecycle wiring

Set `BUZZ_FIXTURE_PUBLICATION_CLI` to the absolute path of the newly built CLI
when running `probe.mjs`. The synthetic MCP tool then makes real CLI sends to
a separate loopback relay. The fixture issues contexts on session creation,
retires old contexts on configuration change, and retires before forwarding
ACP cancellation. It requires the delayed cancelled tool's attempted send to
fail, then verifies successful sends by the resumed and independent sessions.
The original correlation and terminal-ordering cases still run.

Add `BUZZ_FIXTURE_SKIP_RETIREMENT=1` for the negative control. It must fail
with `Cancelled tool was allowed to publish`. This demonstrates the effect of
retirement, rather than relying on process teardown to prevent publication.
The production ACP harness is unchanged. No model, real relay, or gateway is
called; the generated signing key exists only in the fixture's child environment.
