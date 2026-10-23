// End-to-end acceptance defined before implementing the CLI publication fence.
import assert from "node:assert/strict";
import { execFile, execFileSync, spawn } from "node:child_process";
import { createHash, randomBytes, randomUUID } from "node:crypto";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import http from "node:http";
import path from "node:path";
import { promisify } from "node:util";

const [binary, output] = process.argv.slice(2);
assert(binary && output && path.isAbsolute(binary) && path.isAbsolute(output));
assert(!/^\/(?:var\/)?tmp(?:\/|$)/.test(output));
const run = path.join(output, `retirement-${randomUUID()}`);
mkdirSync(run, { recursive: true, mode: 0o700 });
const execute = promisify(execFile);
const hash = (file) =>
  createHash("sha256").update(readFileSync(file)).digest("hex");
const report = {
  fixture: "cli-presign-retirement",
  passed: false,
  productionGate1Passed: false,
  sourceCommit: execFileSync("git", ["rev-parse", "HEAD"], {
    encoding: "utf8",
  }).trim(),
  binarySha256: hash(binary),
  assertions: [],
  events: [],
};
report.gitStatus = execFileSync("git", ["status", "--short"], {
  encoding: "utf8",
}).trim();
report.sourceHashes = Object.fromEntries(
  [
    "Cargo.lock",
    "crates/buzz-cli/Cargo.toml",
    "crates/buzz-cli/src/lib.rs",
    "crates/buzz-cli/src/client.rs",
    "crates/buzz-cli/src/commands/messages.rs",
    "crates/buzz-cli/src/publication_fence.rs",
    "scripts/route-provenance/retirement-probe.mjs",
  ].map((file) => [file, hash(file)]),
);
let queryBarrier;
let emojiBarrier;
let serverError;
const server = http.createServer(async (request, response) => {
  try {
    let raw = "";
    for await (const chunk of request) {
      raw += chunk;
      assert(raw.length < 1024 * 1024);
    }
    const body = JSON.parse(raw);
    response.setHeader("content-type", "application/json");
    if (request.url === "/events") {
      report.events.push(body);
      response.end(
        JSON.stringify({ event_id: body.id, accepted: true, message: "" }),
      );
    } else if (request.url === "/query") {
      if (emojiBarrier && body.some((filter) => filter.kinds?.includes(30030))) {
        const barrier = emojiBarrier;
        emojiBarrier = undefined;
        await barrier();
      }
      if (queryBarrier) {
        const barrier = queryBarrier;
        queryBarrier = undefined;
        await barrier();
      }
      response.end(
        JSON.stringify(
          report.events.filter((event) =>
            body.some((filter) => !filter.ids || filter.ids.includes(event.id)),
          ),
        ),
      );
    } else throw new Error("Unexpected fixture endpoint");
  } catch (error) {
    serverError = error;
    response.statusCode = 500;
    response.end("{}");
  }
});
let env;
const cli = async (args, contextEnv = {}) => {
  try {
    const result = await execute(binary, args, {
      env: { ...env, ...contextEnv },
      timeout: 3000,
    });
    if (serverError) throw serverError;
    return { code: 0, output: JSON.parse(result.stdout) };
  } catch (error) {
    if (serverError) throw serverError;
    if (!Number.isInteger(error.code)) throw error;
    return { code: error.code, stderr: error.stderr };
  }
};
const retire = async (directory, record) => {
  // The same stable lock inode must serialize retirement with signing.
  await execute(
    "python3",
    [
      "-c",
      "import fcntl,json,sys\nfrom pathlib import Path\np=Path(sys.argv[1])\nwith (p/'lock').open('r+') as lock:\n fcntl.flock(lock,fcntl.LOCK_EX)\n (p/'context.json').write_text(sys.argv[2])\n",
      directory,
      JSON.stringify(record),
    ],
    { env: { PATH: process.env.PATH }, timeout: 1000 },
  );
};
try {
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const relay = `http://127.0.0.1:${server.address().port}`;
  env = {
    PATH: process.env.PATH,
    HOME: run,
    TMPDIR: run,
    TMP: run,
    TEMP: run,
    BUZZ_RELAY_URL: relay,
    BUZZ_PRIVATE_KEY: randomBytes(32).toString("hex"),
  };
  const channel = randomUUID();
  const parent = await cli([
    "messages",
    "send",
    "--channel",
    channel,
    "--content",
    "synthetic parent",
  ]);
  assert.equal(parent.code, 0);
  const author = report.events[0].pubkey;
  for (const operation of ["send", "edit"]) {
    for (const scenario of [
      "active",
      "retired-before-entry",
      "retired-during-preflight",
      "superseded-during-preflight",
      "wrong-channel",
      "wrong-author",
      "wrong-relay",
      "expired",
      "missing-context",
      "malformed-context",
      "missing-token",
      "context-fifo",
      "lock-fifo",
      "busy-lock",
      ...(operation === "send" ? ["retired-during-emoji-preflight"] : []),
    ]) {
      const directory = path.join(run, `${operation}-${scenario}`);
      mkdirSync(directory, { mode: 0o700 });
      if (scenario === "lock-fifo")
        await execute("mkfifo", [path.join(directory, "lock")]);
      else writeFileSync(path.join(directory, "lock"), "", { mode: 0o600 });
      const token = randomUUID();
      const record = {
        version: 1,
        token,
        state: "active",
        expires_at: Math.floor(Date.now() / 1000) + 600,
        relay,
        agent_pubkey: author,
        channel_id: channel,
      };
      if (scenario === "retired-before-entry") record.state = "retired";
      if (scenario === "wrong-channel") record.channel_id = randomUUID();
      if (scenario === "wrong-author") record.agent_pubkey = "0".repeat(64);
      if (scenario === "wrong-relay") record.relay = "http://different.invalid";
      if (scenario === "expired") record.expires_at = 1;
      if (scenario === "context-fifo")
        await execute("mkfifo", [path.join(directory, "context.json")]);
      else if (scenario !== "missing-context")
        writeFileSync(
          path.join(directory, "context.json"),
          scenario === "malformed-context" ? "{" : JSON.stringify(record),
          { mode: 0o600 },
        );
      let retiredDuringPreflight = false;
      if (scenario.endsWith("during-preflight")) {
        const barrier = async () => {
          await retire(directory, {
            ...record,
            ...(scenario.startsWith("superseded")
              ? { token: randomUUID() }
              : { state: "retired" }),
          });
          retiredDuringPreflight = true;
        };
        if (scenario === "retired-during-emoji-preflight") emojiBarrier = barrier;
        else queryBarrier = barrier;
      }
      const before = report.events.length;
      const args =
        operation === "send"
          ? [
              "messages",
              "send",
              "--channel",
              channel,
              "--reply-to",
              parent.output.event_id,
              "--content",
              `synthetic ${scenario}${scenario === "retired-during-emoji-preflight" ? " :wave:" : ""}`,
            ]
          : [
              "messages",
              "edit",
              "--event",
              parent.output.event_id,
              "--content",
              `synthetic ${scenario}`,
            ];
      let holder;
      let holderExit;
      let result;
      try {
        if (scenario === "busy-lock") {
          holder = spawn(
            "python3",
            [
              "-c",
              "import fcntl,sys\nf=open(sys.argv[1],'r+')\nfcntl.flock(f,fcntl.LOCK_EX)\nprint('locked',flush=True)\nsys.stdin.read()\n",
              path.join(directory, "lock"),
            ],
            {
              env: { PATH: process.env.PATH },
              stdio: ["pipe", "pipe", "inherit"],
            },
          );
          holderExit = new Promise((resolve) => holder.once("exit", resolve));
          await new Promise((resolve, reject) => {
            const timer = setTimeout(
              () => reject(new Error("Lock holder did not become ready")),
              2000,
            );
            holder.once("error", (error) => {
              clearTimeout(timer);
              reject(error);
            });
            holder.stdout.once("data", () => {
              clearTimeout(timer);
              resolve();
            });
          });
        }
        result = await cli(args, {
          BUZZ_PUBLICATION_CONTEXT: directory,
          ...(scenario === "missing-token"
            ? {}
            : { BUZZ_PUBLICATION_TOKEN: token }),
        });
      } finally {
        if (holder) {
          holder.stdin.end();
          await holderExit;
        }
      }
      const submissions = report.events.length - before;
      if (scenario.endsWith("during-preflight"))
        assert(retiredDuringPreflight, "Race barrier was not exercised");
      const accepted = scenario === "active";
      report.assertions.push({
        operation,
        scenario,
        code: result.code,
        submissions,
        retiredDuringPreflight,
      });
      assert.equal(
        submissions,
        accepted ? 1 : 0,
        `${operation}:${scenario}: submitted after invalid context`,
      );
      assert.equal(
        result.code === 0,
        accepted,
        `${operation}:${scenario}: wrong exit status`,
      );
      queryBarrier = undefined;
      emojiBarrier = undefined;
    }
  }
  report.sourceCommitAtFinish = execFileSync("git", ["rev-parse", "HEAD"], {
    encoding: "utf8",
  }).trim();
  assert.equal(report.sourceCommitAtFinish, report.sourceCommit);
  assert.equal(hash(binary), report.binarySha256);
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
    "Local CLI fence only. No harness token lifecycle, route receipts, automatic labels, or live relay is exercised.";
  const artifact = path.join(run, "result.json");
  writeFileSync(artifact, `${JSON.stringify(report, null, 2)}\n`, {
    mode: 0o600,
  });
  console.log(
    JSON.stringify({
      passed: report.passed,
      artifact,
      scenarios: report.assertions.length,
      error: report.error,
    }),
  );
}
