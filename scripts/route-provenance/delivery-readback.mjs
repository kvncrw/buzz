import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";

const fields = ["id", "pubkey", "created_at", "kind", "tags", "content", "sig"];
const unverified = (reason) => ({ status: "UNVERIFIED", reason });

/**
 * Check delivery against an intent and its trusted signing-time event snapshot.
 * The caller must obtain that snapshot from its signer, never from readback.
 * This fixture verifier does not authenticate route evidence or verify Schnorr.
 */
export function verifyDelivery(intent, events) {
  const { event, expectedContent, requiredPrefix } = intent ?? {};
  if (
    !event ||
    typeof expectedContent !== "string" ||
    !expectedContent.trim() ||
    typeof requiredPrefix !== "string" ||
    !Array.isArray(events)
  ) {
    return unverified("invalid-intent-or-readback");
  }
  if (
    requiredPrefix !== "" &&
    requiredPrefix !== "[routing unknown]" &&
    !/^\[degraded \([A-Za-z0-9._:/-]{1,128}(?:, [A-Za-z0-9._:/-]{1,128}){0,15}\)\]$/.test(
      requiredPrefix,
    )
  ) {
    return unverified("invalid-required-prefix");
  }
  const marker = /^(?:\[routing unknown\]|\[degraded \([^\r\n]*\)\])/;
  if (requiredPrefix) {
    if (!expectedContent.startsWith(`${requiredPrefix}\n\n`))
      return unverified("intended-label-missing");
    const body = expectedContent.slice(requiredPrefix.length + 2);
    if (!body.trim() || marker.test(body.trimStart()))
      return unverified("empty-body-or-duplicate-label");
  } else if (marker.test(expectedContent.trimStart())) {
    return unverified("unexpected-primary-label");
  }
  if (event.content !== expectedContent)
    return unverified("signed-content-differs-from-intent");
  if (
    !/^[0-9a-f]{64}$/.test(event.id) ||
    !/^[0-9a-f]{64}$/.test(event.pubkey) ||
    !/^[0-9a-f]{128}$/.test(event.sig) ||
    !Number.isSafeInteger(event.created_at) ||
    event.created_at < 0 ||
    !Number.isSafeInteger(event.kind) ||
    event.kind < 0 ||
    !Array.isArray(event.tags) ||
    !event.tags.every(
      (tag) =>
        Array.isArray(tag) && tag.every((value) => typeof value === "string"),
    )
  ) {
    return unverified("incomplete-signed-snapshot");
  }
  const canonical = JSON.stringify([
    0,
    event.pubkey,
    event.created_at,
    event.kind,
    event.tags,
    event.content,
  ]);
  if (createHash("sha256").update(canonical).digest("hex") !== event.id)
    return unverified("invalid-snapshot-id");
  const matching = events.filter((item) => item?.id === event.id);
  if (matching.length !== 1) return unverified("event-absent-or-ambiguous");
  for (const field of fields) {
    if (!isDeepStrictEqual(matching[0][field], event[field]))
      return unverified(`readback-mismatch:${field}`);
  }
  return { status: "VERIFIED", eventId: event.id };
}
