import assert from "node:assert/strict";
import { execFile, execFileSync } from "node:child_process";
import { createHash, randomBytes, randomUUID } from "node:crypto";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const [binary, output, mode = "strict"] = process.argv.slice(2);
assert(binary && output && path.isAbsolute(binary) && path.isAbsolute(output));
assert(["strict", "legacy-id-only"].includes(mode));
assert(!/^\/(?:var\/)?tmp(?:\/|$)/.test(output), "Use disk-backed artifacts");
const run = path.join(output, `delivery-${randomUUID()}`);
mkdirSync(run, { recursive: true, mode: 0o700 });
const hash = (file) =>
  createHash("sha256").update(readFileSync(file)).digest("hex");
const directory = path.dirname(fileURLToPath(import.meta.url));
const report = {
  fixture: "signed-label-delivery-readback",
  mode,
  passed: false,
  productionGate1Passed: false,
  sourceCommit: execFileSync("git", ["rev-parse", "HEAD"], {
    encoding: "utf8",
  }).trim(),
  gitStatus: execFileSync("git", ["status", "--short"], {
    encoding: "utf8",
  }).trim(),
  sourceHashes: Object.fromEntries(
    [
      binary,
      fileURLToPath(import.meta.url),
      path.join(directory, "README.md"),
      "crates/buzz-cli/src/client.rs",
    ].map((file) => [file, hash(file)]),
  ),
  assertions: [],
  events: [],
};
let transform = (events) => events;
let serverError;
const server = http.createServer(async (request, response) => {
  try {
    let raw = "";
    for await (const chunk of request) {
      raw += chunk;
      assert(raw.length <= 1024 * 1024, "Fixture request exceeds cap");
    }
    const body = JSON.parse(raw);
    response.setHeader("content-type", "application/json");
    if (request.method === "POST" && request.url === "/events") {
      assert.match(body.sig, /^[0-9a-f]{128}$/);
      assert.match(body.id, /^[0-9a-f]{64}$/);
      if (!report.events.some((event) => event.id === body.id))
        report.events.push(body);
      response.end(
        JSON.stringify({ event_id: body.id, accepted: true, message: "" }),
      );
    } else if (request.method === "POST" && request.url === "/query") {
      assert(Array.isArray(body));
      const events = report.events.filter((event) =>
        body.some(
          (filter) =>
            (!filter.ids || filter.ids.includes(event.id)) &&
            (!filter.kinds || filter.kinds.includes(event.kind)) &&
            (!filter["#h"] ||
              event.tags.some(
                (tag) => tag[0] === "h" && filter["#h"].includes(tag[1]),
              )),
        ),
      );
      response.end(JSON.stringify(transform(structuredClone(events))));
    } else {
      throw new Error(
        `Unexpected fixture endpoint: ${request.method} ${request.url}`,
      );
    }
  } catch (error) {
    serverError = error;
    response.statusCode = 500;
    response.end(JSON.stringify({ error: "fixture failed" }));
  }
});
const execute = promisify(execFile);
let env;
const cli = async (args) => {
  const result = await execute(binary, args, {
    env,
    timeout: 10000,
    maxBuffer: 1024 * 1024,
  });
  if (serverError) throw serverError;
  return JSON.parse(result.stdout);
};

try {
  const verify =
    mode === "legacy-id-only"
      ? (intent, events) => ({
          status: events.some((event) => event.id === intent.event.id)
            ? "VERIFIED"
            : "UNVERIFIED",
        })
      : (await import("./delivery-readback.mjs")).verifyDelivery;
  if (mode === "strict")
    report.sourceHashes[path.join(directory, "delivery-readback.mjs")] = hash(
      path.join(directory, "delivery-readback.mjs"),
    );
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  env = {
    PATH: process.env.PATH,
    HOME: run,
    TMPDIR: run,
    TMP: run,
    TEMP: run,
    BUZZ_RELAY_URL: `http://127.0.0.1:${server.address().port}`,
    BUZZ_PRIVATE_KEY: randomBytes(32).toString("hex"),
  };
  const channel = randomUUID();
  const body = "Synthetic delivery fixture.\n\n```text\nunchanged body\n```";
  const readback = () =>
    cli([
      "messages",
      "get",
      "--channel",
      channel,
      "--kinds",
      "9,40003",
      "--limit",
      "200",
    ]);
  const check = async (
    name,
    intent,
    expected,
    mutation = (events) => events,
  ) => {
    transform = mutation;
    let events;
    try {
      events = await readback();
    } finally {
      transform = (items) => items;
    }
    const result = verify(intent, events);
    report.assertions.push({
      name,
      expected,
      actual: result.status,
      reason: result.reason,
    });
    assert.equal(result.status, expected, name);
  };
  let parent;
  for (const [operation, prefix] of [
    ["send", "[degraded (synthetic-leaf)]"],
    ["send", "[routing unknown]"],
    ["send", ""],
    ["edit", "[degraded (synthetic-leaf)]"],
    ["edit", "[routing unknown]"],
  ]) {
    const content = prefix ? `${prefix}\n\n${body}` : body;
    const args =
      operation === "send"
        ? [
            "messages",
            "send",
            "--channel",
            channel,
            "--content",
            content,
            ...(parent ? ["--reply-to", parent] : []),
          ]
        : ["messages", "edit", "--event", parent, "--content", content];
    const response = await cli(args);
    assert.equal(response.accepted, true);
    const event = report.events.find((item) => item.id === response.event_id);
    assert(event, "CLI did not submit its returned event ID");
    assert.equal(event.content, content);
    assert.equal(event.kind, operation === "send" ? 9 : 40003);
    if (parent)
      assert(event.tags.some((tag) => tag[0] === "e" && tag[1] === parent));
    parent ??= event.id;
    const intent = { event, expectedContent: content, requiredPrefix: prefix };
    const name = `${operation}:${prefix || "primary"}`;
    await check(`${name}:exact`, intent, "VERIFIED");
    if (prefix) {
      await check(`${name}:stripped-label`, intent, "UNVERIFIED", (events) =>
        events.map((item) =>
          item.id === event.id ? { ...item, content: body } : item,
        ),
      );
      await check(`${name}:wrong-label`, intent, "UNVERIFIED", (events) =>
        events.map((item) =>
          item.id === event.id
            ? { ...item, content: `[degraded (wrong-leaf)]\n\n${body}` }
            : item,
        ),
      );
    }
    for (const [field, value] of Object.entries({
      content: `${content}\nchanged`,
      pubkey: "0".repeat(64),
      kind: 1,
      created_at: event.created_at - 1,
      tags: [],
      sig: "0".repeat(128),
    })) {
      await check(`${name}:changed-${field}`, intent, "UNVERIFIED", (events) =>
        events.map((item) =>
          item.id === event.id ? { ...item, [field]: value } : item,
        ),
      );
    }
    await check(`${name}:absent`, intent, "UNVERIFIED", (events) =>
      events.filter((item) => item.id !== event.id),
    );
    await check(`${name}:compact`, intent, "UNVERIFIED", (events) =>
      events.map(({ id, content, created_at }) => ({
        id,
        content,
        created_at,
      })),
    );
    if (prefix) {
      const unlabeled = await cli(
        operation === "send"
          ? ["messages", "send", "--channel", channel, "--content", body]
          : ["messages", "edit", "--event", parent, "--content", body],
      );
      const unlabeledEvent = report.events.find(
        (item) => item.id === unlabeled.event_id,
      );
      await check(
        `${name}:signed-without-intended-label`,
        { ...intent, event: unlabeledEvent },
        "UNVERIFIED",
      );
    }
  }
  report.sourceCommitAtFinish = execFileSync("git", ["rev-parse", "HEAD"], {
    encoding: "utf8",
  }).trim();
  assert.equal(report.sourceCommitAtFinish, report.sourceCommit);
  for (const [file, digest] of Object.entries(report.sourceHashes))
    assert.equal(hash(file), digest, `Source changed: ${file}`);
  report.passed = true;
} catch (error) {
  report.error = error.message;
  process.exitCode = 1;
} finally {
  server.closeAllConnections();
  await new Promise((resolve) => server.close(resolve));
  report.limitation =
    "Installed CLI signs explicitly supplied labels against a loopback relay double. This validates readback only, not automatic stamping, receipt authenticity, token retirement, or a live relay.";
  const artifact = path.join(run, "result.json");
  writeFileSync(artifact, `${JSON.stringify(report, null, 2)}\n`, {
    mode: 0o600,
  });
  console.log(
    JSON.stringify({
      passed: report.passed,
      assertions: report.assertions.length,
      artifact,
      error: report.error,
    }),
  );
}
