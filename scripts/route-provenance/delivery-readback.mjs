import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { schnorr } from "@noble/curves/secp256k1.js";

const fields = ["id", "pubkey", "created_at", "kind", "tags", "content", "sig"];
const unverified = (reason) => ({ status: "UNVERIFIED", reason });

/**
 * Check delivery against an intent and its trusted signing-time event snapshot.
 * The caller must obtain that snapshot and signedEventId from its signer,
 * never from readback. Signature validity does not authenticate route evidence.
 */
export function verifyDelivery(intent, events) {
  const { event, signedEventId, expectedContent, requiredPrefix } =
    intent ?? {};
  if (
    !event ||
    typeof signedEventId !== "string" ||
    !/^[0-9a-f]{64}$/.test(signedEventId) ||
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
  if (event.id !== signedEventId)
    return unverified("snapshot-differs-from-sign-time-id");
  if (requiredPrefix) {
    if (!expectedContent.startsWith(`${requiredPrefix}\n\n`))
      return unverified("intended-label-missing");
    const body = expectedContent.slice(requiredPrefix.length + 2);
    // Remove only the exact CLI prefix at offset zero for the body check.
    // Identical marker-shaped text inside the intended body is human content.
    if (!body.trim()) return unverified("empty-body");
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
  try {
    if (
      !schnorr.verify(
        Buffer.from(event.sig, "hex"),
        Buffer.from(event.id, "hex"),
        Buffer.from(event.pubkey, "hex"),
      )
    )
      return unverified("invalid-snapshot-signature");
  } catch {
    return unverified("invalid-snapshot-signature");
  }
  const matching = events.filter((item) => item?.id === signedEventId);
  if (matching.length !== 1) return unverified("event-absent-or-ambiguous");
  for (const field of fields) {
    if (!isDeepStrictEqual(matching[0][field], event[field]))
      return unverified(`readback-mismatch:${field}`);
  }
  return { status: "VERIFIED", eventId: event.id };
}
