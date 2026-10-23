// Failure fixtures defined before the automatic CLI stamping implementation.
import assert from "node:assert/strict";
import { execFile, execFileSync } from "node:child_process";
import { createHash, randomBytes, randomUUID } from "node:crypto";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import http from "node:http";
import path from "node:path";
import { promisify } from "node:util";
import { verifyDelivery } from "./delivery-readback.mjs";

const [binary, output] = process.argv.slice(2);
assert(path.isAbsolute(binary) && path.isAbsolute(output));
assert(!/^\/(?:var\/)?tmp(?:\/|$)/.test(output));
const run = path.join(output, `stamping-${randomUUID()}`);
mkdirSync(run, { recursive: true, mode: 0o700 });
const execute = promisify(execFile);
const hash = (file) =>
  createHash("sha256").update(readFileSync(file)).digest("hex");
const git = (...args) => execFileSync("git", args, { encoding: "utf8" }).trim();
const sourceFiles = [
  "package.json",
  "pnpm-lock.yaml",
  "Cargo.lock",
  "crates/buzz-cli/Cargo.toml",
  "crates/buzz-cli/src/lib.rs",
  "crates/buzz-cli/src/client.rs",
  "crates/buzz-cli/src/commands/messages.rs",
  "crates/buzz-cli/src/publication_fence.rs",
  "crates/buzz-cli/src/route_labels.rs",
  "scripts/route-provenance/stamping-probe.mjs",
  "scripts/route-provenance/delivery-readback.mjs",
];
const sources = () =>
  Object.fromEntries(
    sourceFiles.map((file) => [file, existsSync(file) ? hash(file) : null]),
  );
const report = {
  fixture: "automatic-cli-route-stamping",
  passed: false,
  productionGate1Passed: false,
  sourceCommit: git("rev-parse", "HEAD"),
  gitStatus: git("status", "--short"),
  sourceHashes: sources(),
  binarySha256: hash(binary),
  assertions: [],
  events: [],
};
let barrier;
let serverFailure;
const server = http.createServer(async (request, response) => {
  try {
    let raw = "";
    for await (const chunk of request) {
      raw += chunk;
      assert(raw.length < 1024 * 1024);
    }
    if (request.url === "/upload") {
      response.setHeader("content-type", "application/json");
      response.end(
        JSON.stringify({
          url: `${common.BUZZ_RELAY_URL}/fixture.png`,
          sha256: request.headers["x-sha-256"],
          size: 68,
          type: "image/png",
          uploaded: Math.floor(Date.now() / 1000),
          dim: "1x1",
        }),
      );
      return;
    }
    const body = JSON.parse(raw);
    response.setHeader("content-type", "application/json");
    if (request.url === "/events") {
      report.events.push(body);
      response.end(JSON.stringify({ accepted: true, event_id: body.id }));
    } else if (request.url === "/query") {
      if (barrier) {
        const action = barrier;
        barrier = undefined;
        await action();
      }
      const member = report.events[0]?.pubkey;
      const readable = [
        ...report.events,
        {
          kind: 39002,
          tags: [
            ["d", channel],
            ["p", member],
          ],
        },
        {
          kind: 0,
          pubkey: member,
          content: JSON.stringify({ display_name: "Fixture Member" }),
        },
      ];
      response.end(
        JSON.stringify(
          readable.filter((event) =>
            body.some(
              (f) =>
                (!f.ids || f.ids.includes(event.id)) &&
                (!f.kinds || f.kinds.includes(event.kind)),
            ),
          ),
        ),
      );
    } else throw new Error("Unexpected fixture endpoint");
  } catch (error) {
    serverFailure = error;
    response.statusCode = 500;
    response.end("{}");
  }
});
let common;
let channel;
let parent;
const cli = async (args, extra = {}) => {
  try {
    const result = await execute(binary, args, {
      env: { ...common, ...extra },
      timeout: 5000,
      maxBuffer: 2 * 1024 * 1024,
    });
    if (serverFailure) throw serverFailure;
    return { code: 0, output: JSON.parse(result.stdout) };
  } catch (error) {
    if (serverFailure) throw serverFailure;
    if (!Number.isInteger(error.code)) throw error;
    return { code: error.code };
  }
};
const writeJson = (file, value) =>
  writeFileSync(file, JSON.stringify(value), { mode: 0o600 });
const lockedWrite = async (directory, filename, value) =>
  execute(
    "python3",
    [
      "-c",
      "import fcntl,sys\nfrom pathlib import Path\np=Path(sys.argv[1])\nwith (p/'lock').open('r+') as lock:\n fcntl.flock(lock,fcntl.LOCK_EX)\n (p/sys.argv[2]).write_text(sys.argv[3])\n",
      directory,
      filename,
      JSON.stringify(value),
    ],
    { env: { PATH: process.env.PATH }, timeout: 5000 },
  );
const degraded = "[degraded (fixture-leaf)]";
const unknown = "[routing unknown]";
const sampleBody =
  "Synthetic body.\n\n```text\n[degraded (literal-code)]\n```\n";

async function exercise(operation, scenario) {
  const body = `${operation}:${scenario}\n\n${sampleBody}`;
  const directory = path.join(run, `${operation}-${scenario}`);
  mkdirSync(directory, { mode: 0o700 });
  writeFileSync(path.join(directory, "lock"), "", { mode: 0o600 });
  const now = Math.floor(Date.now() / 1000);
  const context = {
    version: 1,
    token: randomUUID(),
    state: "active",
    expires_at: now + 600,
    relay: common.BUZZ_RELAY_URL,
    agent_pubkey: report.events[0].pubkey,
    channel_id: channel,
  };
  const snapshot = {
    version: 1,
    token: context.token,
    relay: context.relay,
    agent_pubkey: context.agent_pubkey,
    channel_id: channel,
    observed_at: now,
    expires_at: now + 300,
    status: "degraded",
    fallbacks: ["fixture-leaf"],
  };
  const env = {
    BUZZ_PUBLICATION_CONTEXT: directory,
    BUZZ_PUBLICATION_TOKEN: context.token,
    BUZZ_ROUTE_LABELS: "1",
  };
  let input = body;
  let prefix = unknown;
  let rejected = false;
  if (
    [
      "degraded",
      "double-prefix",
      "stale-prefix",
      "at-limit",
      "overflow",
      "unicode-overflow",
      "latest-before-sign",
      "concurrent-degraded",
    ].includes(scenario)
  )
    prefix = degraded;
  if (["mention", "attachment"].includes(scenario)) prefix = degraded;
  if (scenario === "mention") input = `@Fixture Member ${body}`;
  if (scenario === "human-label-mention") {
    input = `[degraded (@Fixture Member)]\n\n${body}`;
    prefix = degraded;
  }
  if (
    ["primary", "primary-keeps-input-prefix", "concurrent-primary"].includes(
      scenario,
    )
  ) {
    snapshot.status = "primary";
    snapshot.fallbacks = [];
    prefix = "";
  }
  if (scenario === "unknown") {
    snapshot.status = "unknown";
    snapshot.fallbacks = [];
  }
  if (scenario === "unknown-with-fallback") snapshot.status = "unknown";
  if (scenario === "multiple") {
    snapshot.fallbacks = ["leaf-z", "leaf-a", "leaf-z"];
    prefix = "[degraded (leaf-a, leaf-z)]";
  }
  if (scenario === "disabled") {
    env.BUZZ_ROUTE_LABELS = "0";
    prefix = "";
  }
  if (scenario === "unset") {
    delete env.BUZZ_ROUTE_LABELS;
    prefix = "";
  }
  if (scenario === "disabled-keeps-prefix") {
    delete env.BUZZ_ROUTE_LABELS;
    input = `${degraded}\n\n${body}`;
    prefix = "";
  }
  if (["double-prefix", "primary-keeps-input-prefix"].includes(scenario))
    input = `${degraded}\n\n${unknown}\n\n${body}`;
  if (scenario === "stale-prefix") input = `  [degraded (old-leaf)]\n\n${body}`;
  if (scenario === "single-newline-prefix") {
    input = `${unknown}\n${body}`;
    prefix = degraded;
  }
  if (scenario === "crlf-prefix") {
    input = `${unknown}\r\n\r\n${body}`;
    prefix = degraded;
  }
  if (scenario === "empty") {
    input = " \n";
    rejected = true;
  }
  if (scenario === "label-only") {
    input = degraded;
    prefix = degraded;
  }
  if (scenario === "at-limit")
    input = "x".repeat(65536 - Buffer.byteLength(`${prefix}\n\n`));
  if (scenario === "overflow") {
    input = "x".repeat(65536);
    rejected = true;
  }
  if (scenario === "unicode-overflow") {
    input = "é".repeat(32768);
    rejected = true;
  }
  if (scenario === "invalid-config") {
    env.BUZZ_ROUTE_LABELS = "yes";
    rejected = true;
  }
  if (scenario === "missing-fence") {
    delete env.BUZZ_PUBLICATION_CONTEXT;
    delete env.BUZZ_PUBLICATION_TOKEN;
    rejected = true;
  }
  if (scenario === "retired") {
    context.state = "retired";
    rejected = true;
  }
  if (scenario === "wrong-token") snapshot.token = randomUUID();
  if (scenario === "wrong-author") snapshot.agent_pubkey = "0".repeat(64);
  if (scenario === "wrong-channel") snapshot.channel_id = randomUUID();
  if (scenario === "wrong-relay") snapshot.relay = "https://other.invalid";
  if (scenario === "expired") snapshot.expires_at = now - 1;
  if (scenario === "future") snapshot.observed_at = now + 60;
  if (scenario === "outlives-context")
    snapshot.expires_at = context.expires_at + 1;
  if (scenario === "bad-version") snapshot.version = 2;
  if (scenario === "bad-status") snapshot.status = "success";
  if (scenario === "invalid-name") snapshot.fallbacks = ["bad\n@Someone"];
  if (scenario === "long-name") snapshot.fallbacks = ["x".repeat(129)];
  if (scenario === "empty-fallbacks") snapshot.fallbacks = [];
  if (scenario === "primary-conflict") snapshot.status = "primary";
  if (scenario === "too-many")
    snapshot.fallbacks = Array.from({ length: 17 }, (_, i) => `leaf-${i}`);
  writeJson(path.join(directory, "context.json"), context);
  const route = path.join(directory, "route.json");
  if (scenario === "fifo") await execute("mkfifo", [route]);
  else if (scenario === "directory") mkdirSync(route);
  else if (scenario === "symlink")
    symlinkSync(path.join(directory, "context.json"), route);
  else if (scenario !== "missing") {
    if (scenario === "malformed") writeFileSync(route, "{");
    else if (scenario === "oversize") writeFileSync(route, " ".repeat(16385));
    else writeJson(route, snapshot);
  }
  let barrierRan = false;
  if (scenario === "latest-before-sign" || scenario === "retired-before-sign") {
    if (scenario === "latest-before-sign")
      writeJson(route, { ...snapshot, status: "primary", fallbacks: [] });
    else rejected = true;
    barrier = async () => {
      await lockedWrite(
        directory,
        scenario === "latest-before-sign" ? "route.json" : "context.json",
        scenario === "latest-before-sign"
          ? snapshot
          : { ...context, state: "retired" },
      );
      barrierRan = true;
    };
  }
  const args =
    operation === "edit"
      ? ["messages", "edit", "--event", parent, "--content", input]
      : [
          "messages",
          "send",
          "--channel",
          channel,
          "--reply-to",
          parent,
          "--content",
          input,
          ...(operation === "forum" ? ["--kind", "45003"] : []),
        ];
  if (scenario === "mention") args.push("--mention", context.agent_pubkey);
  if (scenario === "attachment") {
    const png = path.join(directory, "fixture.png");
    writeFileSync(
      png,
      Buffer.from(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=",
        "base64",
      ),
    );
    args.push("--file", png);
  }
  const before = report.events.length;
  const result = await cli(args, env);
  assert.equal(
    result.code === 0,
    !rejected,
    `${operation}:${scenario}: wrong exit status ${result.code}`,
  );
  if (scenario.endsWith("before-sign"))
    assert(barrierRan, "Pre-sign barrier was not reached");
  if (rejected)
    assert.equal(
      report.events.length,
      before,
      "Rejected content was published",
    );
  else {
    const event = report.events.find(
      (item) => item.id === result.output.event_id,
    );
    assert(event);
    let expectedBody = input;
    if (scenario === "attachment")
      expectedBody += `\n![image](${common.BUZZ_RELAY_URL}/fixture.png)`;
    const expected = prefix ? `${prefix}\n\n${expectedBody}` : expectedBody;
    assert.equal(
      event.content,
      expected,
      `${operation}:${scenario}: stamped content mismatch`,
    );
    assert.equal(
      event.kind,
      operation === "edit" ? 40003 : operation === "forum" ? 45003 : 9,
    );
    assert(event.tags.some((tag) => tag[0] === "h" && tag[1] === channel));
    assert(event.tags.some((tag) => tag[0] === "e" && tag[1] === parent));
    if (scenario === "mention") {
      assert(
        event.tags.some(
          (tag) => tag[0] === "p" && tag[1] === context.agent_pubkey,
        ),
      );
      assert.deepEqual(result.output.mention_pubkeys, [context.agent_pubkey]);
    }
    if (scenario === "human-label-mention") {
      assert(
        !event.tags.some((tag) => tag[0] === "p"),
        "Bracketed human text changed existing mention behavior",
      );
      assert.deepEqual(result.output.mention_pubkeys, []);
    }
    if (scenario === "attachment")
      assert(
        event.tags.some((tag) => tag[0] === "imeta" && tag.includes("dim 1x1")),
      );
    const readback = await cli([
      "messages",
      "get",
      "--channel",
      channel,
      "--kinds",
      "9,40003,45003",
      "--limit",
      "200",
    ]);
    assert.equal(
      verifyDelivery(
        {
          event,
          signedEventId: event.id,
          expectedContent: expected,
          requiredPrefix: prefix,
        },
        readback.output,
      ).status,
      "VERIFIED",
    );
  }
  report.assertions.push({
    operation,
    scenario,
    rejected,
    code: result.code,
    barrierRan,
  });
}
try {
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  common = {
    PATH: process.env.PATH,
    HOME: run,
    TMPDIR: run,
    TMP: run,
    TEMP: run,
    BUZZ_RELAY_URL: `http://127.0.0.1:${server.address().port}`,
    BUZZ_PRIVATE_KEY: randomBytes(32).toString("hex"),
  };
  channel = randomUUID();
  const seed = await cli([
    "messages",
    "send",
    "--channel",
    channel,
    "--content",
    "Synthetic parent",
  ]);
  assert.equal(seed.code, 0);
  parent = seed.output.event_id;
  for (const operation of ["send", "edit"]) {
    for (const scenario of [
      "degraded",
      "double-prefix",
      "primary",
      "unknown",
      "multiple",
      "disabled",
      "unset",
      "disabled-keeps-prefix",
      "missing",
      "malformed",
      "oversize",
      "fifo",
      "directory",
      "symlink",
      "wrong-token",
      "wrong-author",
      "wrong-channel",
      "wrong-relay",
      "expired",
      "future",
      "outlives-context",
      "bad-version",
      "bad-status",
      "invalid-name",
      "long-name",
      "empty-fallbacks",
      "primary-conflict",
      "too-many",
      "unknown-with-fallback",
      "single-newline-prefix",
      "crlf-prefix",
      "stale-prefix",
      "primary-keeps-input-prefix",
      "empty",
      "label-only",
      "at-limit",
      "overflow",
      "unicode-overflow",
      "invalid-config",
      "missing-fence",
      "retired",
      "latest-before-sign",
      "retired-before-sign",
    ])
      await exercise(operation, scenario);
  }
  await exercise("forum", "degraded");
  await exercise("send", "mention");
  await exercise("send", "human-label-mention");
  await exercise("send", "attachment");
  await Promise.all([
    exercise("send", "concurrent-degraded"),
    exercise("send", "concurrent-primary"),
  ]);
  assert.equal(git("rev-parse", "HEAD"), report.sourceCommit);
  assert.deepEqual(sources(), report.sourceHashes);
  assert.equal(hash(binary), report.binarySha256);
  report.passed = true;
} catch (error) {
  report.error = error.message;
  process.exitCode = 1;
} finally {
  server.closeAllConnections();
  await new Promise((resolve) => server.close(resolve));
  report.limitation =
    "Fixture-owned trusted snapshots. No authenticated receipt transport, live gateway, production lifecycle, or production enablement.";
  const artifact = path.join(run, "result.json");
  writeJson(artifact, report);
  console.log(
    JSON.stringify({
      passed: report.passed,
      artifact,
      scenarios: report.assertions.length,
      error: report.error,
    }),
  );
}
