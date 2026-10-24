# Harness issuer acceptance

Writing standard: googledev-style.

This PR issues publication authority in `buzz-acp` for an explicitly opted-in
Claude ACP seat. It does not change the Codex adapter or enable any live seat.
No verified route snapshot is invented: without gateway evidence the existing
CLI displays `[routing unknown]`.

## Failure cases, defined before implementation

1. Default-off sessions must keep their existing protocol requests unchanged.
   The effective operator launch environment must win over persona defaults,
   including an explicit `BUZZ_ROUTE_LABELS=0` override.
2. An unsupported adapter, invalid lifetime, or failed write must fail before
   the model receives an authorized channel prompt. Preserve the configured
   seven-day ceiling (604,800 seconds). Heartbeats have no channel: skip
   issuance and explicitly disable labels and clear credentials in their
   native and MCP environments, including session reuse. The CLI treats empty
   credentials as absent, while partial configuration still fails closed.
3. Each turn gets a new directory and token. Native Claude tools and supplied
   MCP tools must receive the same context. Other session parameters survive.
4. Reusing a session refreshes its environment through the installed adapter's
   `session/resume` operation and preserves conversation history. An unsuccessful
   resume must prevent the prompt; it must never reuse the prior token.
5. Cancellation revokes authority before waiting for the adapter. Completion,
   setup failure, panic, process exit, and timeout must also revoke it. A lease
   lock held by the issuing process makes abrupt process death fail closed.
   Order normal retirement against signing with the stable publication lock.
   Wait at most one second for an admitted signer. If it remains stuck, revoke
   the issuer lease and report failure; do not claim orderly retirement. Process
   death cannot recall an event already admitted to the signing critical section.
   A signer drain uses `PublicationDrainTimeout`, never a protocol error.
   Preserve completed prompt and cancellation outcomes and report retirement
   failure separately. An acknowledged Stop drops its batch without `turn_error`;
   Steer and Interrupt preserve cancelled work without retry penalties.
   Send Stop after revocation, concurrently with signer draining, so the adapter
   retains its five-second control grace and timeout classification.
6. Two concurrent seats and two channels must not share contexts. A new turn
   cannot recover an old turn's primary or degraded snapshot.
7. The CLI must reject an orphaned version-two context, even if its JSON still
   says active. Version-one fixture compatibility does not prove crash fencing.
8. Run the actual harness/CLI workflow and the full ACP and CLI package suites.
   Save source state, binary hashes, assertions, and a negative-control result.
   Run full repository CI in parallel with Grok's review. Fable's merge requires
   both a green CI verdict and Grok's approval on the same commit.
9. Session creation and resume must omit parameters from `acp_write` observer
   frames while delivering the complete environment to the adapter.

## Remaining transport boundary

Gateway contribution recording and authenticated nested-hop evidence remain
separate from issuance. Until those records exist, the only honest label is
`[routing unknown]`. Live fallback/recovery evidence remains outstanding.
