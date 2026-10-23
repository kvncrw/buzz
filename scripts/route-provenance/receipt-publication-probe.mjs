import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { readFileSync } from "node:fs";
import path from "node:path";
import { promisify } from "node:util";
import { verifyDelivery } from "./delivery-readback.mjs";
import { publicationLifecycle } from "./publication-lifecycle.mjs";

// The fixture supplies a trusted snapshot while the Python stream is suspended.
// The CLI derives the label. Authenticated production transport remains separate.
const [binary, run] = process.argv.slice(2);
const snapshots = JSON.parse(readFileSync(0, "utf8"));
assert.equal(snapshots.length, 3);
const lifecycle = await publicationLifecycle(binary, run, false);
const report = { passed: false, events: [], assertions: [] };
try {
  for (const [index, snapshot] of snapshots.entries()) {
    const expected = ["degraded", "primary", "unknown"][index];
    assert.equal(snapshot.status, expected);
    let prefix = "";
    if (expected === "degraded") {
      assert.deepEqual(snapshot.fallbacks, ["fixture-fallback"]);
      prefix = `[degraded (${snapshot.fallbacks[0]})]`;
    } else if (expected === "unknown") prefix = "[routing unknown]";
    const body = `Synthetic pre-terminal publication ${index}.`;
    const content = prefix ? `${prefix}\n\n${body}` : body;
    const env = Object.fromEntries(
      lifecycle
        .environment(`receipt-${index}`, `turn-${index}`)
        .map(({ name, value }) => [name, value]),
    );
    env.BUZZ_ROUTE_LABELS = "1";
    const context = JSON.parse(
      readFileSync(
        path.join(env.BUZZ_PUBLICATION_CONTEXT, "context.json"),
        "utf8",
      ),
    );
    const route = {
      version: 1,
      token: context.token,
      relay: context.relay,
      agent_pubkey: context.agent_pubkey,
      channel_id: context.channel_id,
      observed_at: Math.floor(Date.now() / 1000),
      expires_at: context.expires_at,
      status: snapshot.status,
      fallbacks: snapshot.fallbacks,
    };
    await promisify(execFile)(
      "python3",
      [
        "-c",
        "import fcntl,sys\nfrom pathlib import Path\np=Path(sys.argv[1])\nwith (p/'lock').open('r+') as lock:\n fcntl.flock(lock,fcntl.LOCK_EX)\n (p/'route.json').write_text(sys.argv[2])\n",
        env.BUZZ_PUBLICATION_CONTEXT,
        JSON.stringify(route),
      ],
      { env: { PATH: process.env.PATH }, timeout: 5000 },
    );
    const cli = async (args) =>
      JSON.parse(
        (
          await promisify(execFile)(binary, args, {
            env,
            timeout: 5000,
            maxBuffer: 1024 * 1024,
          })
        ).stdout,
      );
    const response = await cli([
      "messages",
      "send",
      "--channel",
      env.BUZZ_FIXTURE_PUBLICATION_CHANNEL,
      "--content",
      body,
    ]);
    assert.equal(response.accepted, true);
    const event = lifecycle.events.find(
      (item) => item.id === response.event_id,
    );
    assert(event);
    const readback = await cli([
      "messages",
      "get",
      "--channel",
      env.BUZZ_FIXTURE_PUBLICATION_CHANNEL,
      "--kinds",
      "9",
      "--limit",
      "20",
    ]);
    const result = verifyDelivery(
      {
        event,
        expectedContent: content,
        requiredPrefix: prefix,
      },
      readback,
    );
    assert.equal(result.status, "VERIFIED");
    report.assertions.push(`${expected}:automatic-stamp-and-exact-readback`);
    report.events.push(event);
  }
  report.passed = true;
  process.stdout.write(JSON.stringify(report));
} finally {
  lifecycle.close();
}
