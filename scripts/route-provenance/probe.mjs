import assert from "node:assert/strict";
import { spawn, execFileSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import {
  mkdirSync,
  readFileSync,
  writeFileSync,
  appendFileSync,
  existsSync,
} from "node:fs";
import http from "node:http";
import { createRequire } from "node:module";
import path from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { createStreamOrdering } from "./stream-order.mjs";
import { publicationLifecycle } from "./publication-lifecycle.mjs";

const [adapter, output, faultMode = "none"] = process.argv.slice(2);
assert(
  ["none", "missing-b2-tool", "stdin-epipe"].includes(faultMode),
  "Unknown fixture fault mode",
);
assert(
  adapter && output && path.isAbsolute(adapter) && path.isAbsolute(output),
  "Supply absolute adapter and artifact paths",
);
assert(
  !/^\/(?:var\/)?tmp(?:\/|$)/.test(output),
  "Use disk-backed artifact storage",
);
mkdirSync(output, { recursive: true, mode: 0o700 });
const run = path.join(output, randomUUID());
mkdirSync(run, { mode: 0o700 });
const config = path.join(run, "claude-config");
const workspace = path.join(run, "workspace");
mkdirSync(config);
mkdirSync(workspace);
const toolEvidence = path.join(run, "tools.jsonl");
const toolLifecycle = path.join(run, "tool-lifecycle.jsonl");
const requests = [];
const completions = [];
const updates = [];
const pending = new Map();
let sequence = 0;
let child;
let fatal;
let cleaningUp = false;
let publication;
const report = {
  schema: 1,
  fixture: "claude-acp-session-correlation",
  passed: false,
  productionGate1Passed: false,
  runDirectory: run,
  adapterSha256: createHash("sha256")
    .update(readFileSync(adapter))
    .digest("hex"),
  adapterVersion: JSON.parse(
    readFileSync(
      path.join(path.dirname(adapter), "..", "package.json"),
      "utf8",
    ),
  ).version,
  fixtureSha256: createHash("sha256")
    .update(readFileSync(fileURLToPath(import.meta.url)))
    .digest("hex"),
  sourceCommit: execFileSync("git", ["rev-parse", "HEAD"], {
    encoding: "utf8",
  }).trim(),
  assertions: {},
};
report.faultMode = faultMode;
const fixtureDirectory = path.dirname(fileURLToPath(import.meta.url));
const sdkEntry = createRequire(adapter).resolve(
  "@anthropic-ai/claude-agent-sdk",
);
const hash = (file) =>
  createHash("sha256").update(readFileSync(file)).digest("hex");
report.sdkVersion = JSON.parse(
  readFileSync(path.join(path.dirname(sdkEntry), "package.json"), "utf8"),
).version;
report.sourceHashes = Object.fromEntries(
  [
    adapter,
    path.join(path.dirname(adapter), "acp-agent.js"),
    sdkEntry,
    ...["probe.mjs", "mcp-probe.mjs", "stream-order.mjs", "README.md"].map(
      (file) => path.join(fixtureDirectory, file),
    ),
  ].map((file) => [file, hash(file)]),
);
report.gitStatus = execFileSync("git", ["status", "--short"], {
  encoding: "utf8",
}).trim();
const publicationBinary = process.env.BUZZ_FIXTURE_PUBLICATION_CLI;
if (publicationBinary) {
  assert(path.isAbsolute(publicationBinary));
  report.sourceHashes[publicationBinary] = hash(publicationBinary);
  report.sourceHashes[
    path.join(fixtureDirectory, "publication-lifecycle.mjs")
  ] = hash(path.join(fixtureDirectory, "publication-lifecycle.mjs"));
}
const readLines = (file) =>
  existsSync(file)
    ? readFileSync(file, "utf8")
        .trim()
        .split("\n")
        .filter(Boolean)
        .map(JSON.parse)
    : [];
const sleep = (milliseconds) =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));
const alive = (pid) => {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if (error.code !== "ESRCH") throw error;
    return false;
  }
};
async function until(predicate, message, timeout = 10000) {
  const deadline = performance.now() + timeout;
  while (performance.now() < deadline) {
    if (fatal) throw fatal;
    const result = predicate();
    if (result) return result;
    await sleep(25);
  }
  throw new Error(`Timeout: ${message}`);
}

function fail(error) {
  if (!cleaningUp) fatal ??= error;
  for (const p of pending.values()) {
    clearTimeout(p.timer);
    p.reject(error);
  }
  pending.clear();
}

function writeMessage(message) {
  if (fatal) return false;
  try {
    if (message.method === "session/cancel")
      publication?.cancel(message.params.sessionId);
    if (child.stdin.destroyed || child.stdin.writableEnded)
      throw new Error("Adapter stdin is closed");
    child.stdin.write(`${JSON.stringify(message)}\n`, (error) => {
      if (error) fail(error);
    });
    return true;
  } catch (error) {
    fail(error);
    return false;
  }
}

function ancillaryKind(body, tool, isToolResult) {
  if (
    tool ||
    isToolResult ||
    body.tools?.length ||
    body.messages.length !== 1 ||
    body.messages[0].role !== "user"
  )
    return null;
  const content = body.messages[0].content;
  if (
    !Array.isArray(content) ||
    content.length !== 1 ||
    content[0].type !== "text"
  )
    return null;
  const text = content[0].text;
  const systems = Array.isArray(body.system)
    ? body.system
        .filter((part) => part.type === "text")
        .map((part) => part.text)
    : [];
  // Observed SDK 0.3.286 requests: title generation wraps its synthetic input;
  // model validation sends one token of "Hi" without streaming.
  if (
    body.stream === true &&
    body.max_tokens === 32000 &&
    text.startsWith("<session>\n") &&
    text.includes("\n</session>\n") &&
    systems.some((system) =>
      system.startsWith(
        "You are naming a coding session so the user can pick it out of a long list of sessions.",
      ),
    )
  )
    return "session-title";
  if (
    body.stream !== true &&
    body.max_tokens === 1 &&
    text === "Hi" &&
    systems.includes(
      "You are Claude Code, Anthropic's official CLI for Claude.",
    )
  )
    return "model-validation";
  return null;
}

function stream(res, content, stopReason, streaming) {
  if (!streaming) {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(
      JSON.stringify({
        id: `msg_${randomUUID()}`,
        type: "message",
        role: "assistant",
        model: "claude-sonnet-4-5",
        content: [content],
        stop_reason: stopReason,
        stop_sequence: null,
        usage: { input_tokens: 1, output_tokens: 1 },
      }),
    );
    return;
  }
  res.writeHead(200, {
    "content-type": "text/event-stream",
    "cache-control": "no-cache",
  });
  const emit = (event, data) =>
    res.write(
      `event: ${event}\ndata: ${JSON.stringify({ type: event, ...data })}\n\n`,
    );
  emit("message_start", {
    message: {
      id: `msg_${randomUUID()}`,
      type: "message",
      role: "assistant",
      model: "claude-sonnet-4-5",
      content: [],
      stop_reason: null,
      stop_sequence: null,
      usage: { input_tokens: 1, output_tokens: 0 },
    },
  });
  const tool = content.type === "tool_use";
  emit("content_block_start", {
    index: 0,
    content_block: tool
      ? { ...content, input: {} }
      : { type: "text", text: "" },
  });
  emit("content_block_delta", {
    index: 0,
    delta: tool
      ? {
          type: "input_json_delta",
          partial_json: JSON.stringify(content.input),
        }
      : { type: "text_delta", text: content.text },
  });
  emit("content_block_stop", { index: 0 });
  emit("message_delta", {
    delta: { stop_reason: stopReason, stop_sequence: null },
    usage: { output_tokens: 1 },
  });
  emit("message_stop", {});
  res.end();
}

const ordering = createStreamOrdering({
  readLifecycle: () => readLines(toolLifecycle),
  sleep,
  until,
  writeMessage,
});
const server = http.createServer(async (req, res) => {
  try {
    let raw = "";
    for await (const chunk of req) {
      raw += chunk;
      if (raw.length > 4 * 1024 * 1024)
        throw new Error("Fixture request exceeds cap");
    }
    if (req.url?.includes("count_tokens")) {
      res.writeHead(200, { "content-type": "application/json" });
      res.end('{"input_tokens":10}');
      return;
    }
    if (!req.url?.startsWith("/v1/messages")) {
      res.writeHead(404);
      res.end("{}");
      return;
    }
    const body = JSON.parse(raw);
    const session = req.headers["x-buzz-fixture-session"];
    const creationTurn = req.headers["x-buzz-fixture-turn"];
    assert.equal(typeof session, "string", "Model request lost session header");
    const last = body.messages.at(-1);
    const content = Array.isArray(last.content)
      ? last.content
      : [{ type: "text", text: last.content }];
    const isToolResult = content.some((part) => part.type === "tool_result");
    const nonce = randomUUID();
    const tool = body.tools?.find((item) => item.name.endsWith("route_probe"));
    const ancillary = ancillaryKind(body, tool, isToolResult);
    const fixturePromptCount = body.messages.filter(
      (message) =>
        message.role === "user" &&
        JSON.stringify(message.content).includes(
          "Call the synthetic route_probe tool once",
        ),
    ).length;
    const fixtureTurns = body.messages
      .filter((message) => message.role === "user")
      .map(
        (message) =>
          JSON.stringify(message.content).match(
            /Fixture turn: ([a-z0-9-]+)/,
          )?.[1],
      )
      .filter(Boolean);
    const delayMs = Number(
      JSON.stringify(content).match(
        /Delay the synthetic tool by (\d+) ms/,
      )?.[1] ?? 0,
    );
    const request = {
      at: Date.now(),
      session,
      creationTurn,
      nonce,
      isToolResult,
      ancillary,
      fixturePromptCount,
      fixtureTurns,
      delayMs,
      streaming: body.stream === true,
    };
    requests.push(request);
    if (isToolResult || ancillary) {
      stream(
        res,
        { type: "text", text: "FIXTURE_OK" },
        "end_turn",
        body.stream === true,
      );
    } else {
      assert(tool, "Synthetic MCP tool not offered");
      const toolContent = {
        type: "tool_use",
        id: `toolu_${nonce}`,
        name: tool.name,
        input: { nonce, delayMs },
      };
      if (!(await ordering.handle(res, toolContent, request)))
        stream(res, toolContent, "tool_use", body.stream === true);
    }
  } catch (error) {
    fail(error);
    if (!res.headersSent) res.writeHead(500);
    res.end(
      '{"type":"error","error":{"type":"api_error","message":"fixture failed"}}',
    );
  }
});
await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
const endpoint = `http://127.0.0.1:${server.address().port}`;
const env = {
  PATH: process.env.PATH,
  HOME: process.env.HOME,
  USER: process.env.USER,
  LANG: "C.UTF-8",
  TMPDIR: run,
  TMP: run,
  TEMP: run,
  CLAUDE_CONFIG_DIR: config,
  ANTHROPIC_API_KEY: "synthetic-fixture-token",
  ANTHROPIC_BASE_URL: endpoint,
  ANTHROPIC_MODEL: "claude-sonnet-4-5",
  CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: "1",
  DISABLE_AUTOUPDATER: "1",
  CLAUDE_CODE_DISABLE_AUTO_MEMORY: "1",
};

function rpc(method, params) {
  const id = ++sequence;
  return new Promise((resolve, reject) => {
    if (fatal) {
      reject(fatal);
      return;
    }
    const timer = setTimeout(() => {
      pending.delete(id);
      reject(new Error(`Timeout: ${method}`));
    }, 25000);
    pending.set(id, { resolve, reject, timer });
    writeMessage({ jsonrpc: "2.0", id, method, params });
  });
}

try {
  if (publicationBinary)
    publication = await publicationLifecycle(
      publicationBinary,
      run,
      process.env.BUZZ_FIXTURE_SKIP_RETIREMENT === "1",
    );
  const childArguments =
    faultMode === "stdin-epipe"
      ? [
          "-e",
          "require('node:fs').closeSync(0); process.stdout.write(JSON.stringify({method:'fixture/input_closed'})+'\\n'); setTimeout(()=>{},200);",
        ]
      : [adapter];
  child = spawn(process.execPath, childArguments, {
    env,
    cwd: workspace,
    detached: true,
    stdio: ["pipe", "pipe", "pipe"],
  });
  let inputClosed = false;
  child.stdin.on("error", fail);
  child.stdout.on("error", fail);
  child.stderr.on("error", fail);
  child.stderr.on("data", (chunk) =>
    appendFileSync(path.join(run, "adapter.stderr"), chunk, { mode: 0o600 }),
  );
  child.on("error", fail);
  child.on("exit", () => {
    if (!cleaningUp) fail(new Error("Adapter exited"));
  });
  createInterface({ input: child.stdout }).on("line", (line) => {
    try {
      const message = JSON.parse(line);
      if (
        message.method === "fixture/input_closed" &&
        faultMode === "stdin-epipe"
      )
        inputClosed = true;
      if (message.method === "session/update")
        updates.push({
          sessionId: message.params.sessionId,
          kind: message.params.update.sessionUpdate,
        });
      if (message.method && message.id !== undefined) {
        const allow =
          message.method === "session/request_permission" &&
          message.params.options?.find((item) => item.kind === "allow_once");
        const response = allow
          ? {
              result: {
                outcome: { outcome: "selected", optionId: allow.optionId },
              },
            }
          : {
              error: {
                code: -32601,
                message: "Fixture client does not execute tools",
              },
            };
        writeMessage({ jsonrpc: "2.0", id: message.id, ...response });
      } else if (pending.has(message.id)) {
        const p = pending.get(message.id);
        pending.delete(message.id);
        clearTimeout(p.timer);
        if (message.error)
          p.reject(
            new Error(`ACP ${message.error.code}: ${message.error.message}`),
          );
        else p.resolve(message.result);
      }
    } catch (error) {
      fail(error);
    }
  });
  if (faultMode === "stdin-epipe")
    await until(() => inputClosed, "synthetic child closes stdin");
  await rpc("initialize", { protocolVersion: 1, clientCapabilities: {} });
  function sessionParams(token, turn) {
    return {
      cwd: workspace,
      mcpServers: [
        {
          name: "route-fixture",
          command: process.execPath,
          args: [path.join(fixtureDirectory, "mcp-probe.mjs")],
          env: [
            { name: "BUZZ_FIXTURE_SESSION", value: token },
            { name: "BUZZ_FIXTURE_TURN", value: turn },
            { name: "BUZZ_FIXTURE_EVIDENCE", value: toolEvidence },
            { name: "BUZZ_FIXTURE_LIFECYCLE", value: toolLifecycle },
            ...(publication?.environment(token, turn) ?? []),
          ],
        },
      ],
      _meta: {
        claudeCode: {
          options: {
            tools: [],
            persistSession: true,
            settingSources: [],
            settings: { disableAllHooks: true },
            env: {
              ANTHROPIC_CUSTOM_HEADERS: `X-Buzz-Fixture-Session: ${token}\nX-Buzz-Fixture-Turn: ${turn}`,
            },
          },
        },
      },
    };
  }
  async function session(token) {
    const newSession = await rpc(
      "session/new",
      sessionParams(token, `${token}-turn1`),
    );
    publication?.register(newSession.sessionId, token);
    return { token, id: newSession.sessionId };
  }
  const a = await session("session-a");
  const b = await session("session-b");
  const prompt = async (s, turn, delayMs = 0, expectedStop = "end_turn") => {
    const result = await rpc("session/prompt", {
      sessionId: s.id,
      prompt: [
        {
          type: "text",
          text: `Call the synthetic route_probe tool once, then reply FIXTURE_OK. Fixture turn: ${turn}. Delay the synthetic tool by ${delayMs} ms.`,
        },
      ],
      _meta: { buzzFixture: { turnId: turn } },
    });
    assert.equal(result.stopReason, expectedStop);
    completions.push({
      session: s.token,
      requestedTurn: turn,
      stopReason: result.stopReason,
    });
    return result;
  };
  await Promise.all([
    prompt(a, "session-a-turn1"),
    prompt(b, "session-b-turn1"),
  ]);
  await prompt(a, "session-a-turn2");
  if (fatal) throw fatal;
  const tools = readFileSync(toolEvidence, "utf8")
    .trim()
    .split("\n")
    .map(JSON.parse);
  const modelCalls = requests.filter(
    (item) => !item.isToolResult && !item.ancillary,
  );
  assert.equal(tools.length, 3);
  assert.equal(modelCalls.length, 3);
  for (const call of modelCalls) {
    const tool = tools.find((item) => item.nonce === call.nonce);
    assert(tool, "Model request did not reach tool");
    assert.equal(tool.session, call.session);
    assert.equal(tool.creationTurn, call.creationTurn);
  }
  assert.equal(tools.filter((item) => item.session === a.token).length, 2);
  assert.equal(tools.filter((item) => item.session === b.token).length, 1);
  assert(
    tools
      .filter((item) => item.session === a.token)
      .every((item) => item.creationTurn === "session-a-turn1"),
  );
  assert(
    requests.filter((item) => !item.ancillary).every((item) => item.streaming),
  );
  report.assertions = {
    threeCompletedTurns: true,
    concurrentSessionIsolation: true,
    httpToMcpCorrelation: true,
    streaming: true,
    creationContextPersistsAcrossTurns: true,
  };
  const oldToolPid = tools.find((item) => item.session === a.token).pid;
  const loadStarted = performance.now();
  const loaded = await rpc("session/load", {
    sessionId: a.id,
    ...sessionParams(a.token, "session-a-turn3"),
  });
  assert.equal(loaded.sessionId, a.id);
  report.reloadMilliseconds = Math.round(performance.now() - loadStarted);
  try {
    process.kill(oldToolPid, 0);
    report.oldToolAliveAfterLoad = true;
  } catch (error) {
    if (error.code !== "ESRCH") throw error;
    report.oldToolAliveAfterLoad = false;
  }
  await prompt(a, "session-a-turn3");
  if (fatal) throw fatal;
  const allTools = readFileSync(toolEvidence, "utf8")
    .trim()
    .split("\n")
    .map(JSON.parse);
  assert.equal(allTools.length, 4);
  const rotated = allTools.at(-1);
  assert.equal(rotated.creationTurn, "session-a-turn3");
  assert.equal(rotated.session, a.token);
  const rotatedRequest = requests.find((item) => item.nonce === rotated.nonce);
  assert.equal(rotatedRequest.creationTurn, "session-a-turn3");
  assert.equal(rotatedRequest.fixturePromptCount, 3);
  report.assertions.supportedLoadRotatesBothContexts = true;
  report.assertions.historyPreservedAcrossLoad = true;

  // Installed acp-agent.js: resumeSession/getOrCreateSession preserve config
  // through the standard session/resume method, without load's history replay.
  const resumeUpdates = updates.length;
  const resumeStarted = performance.now();
  const resumed = await rpc("session/resume", {
    sessionId: a.id,
    ...sessionParams(a.token, "session-a-turn4"),
  });
  assert.equal(resumed.sessionId, a.id);
  report.resumeMilliseconds = Math.round(performance.now() - resumeStarted);
  const replayKinds = [
    "user_message_chunk",
    "agent_message_chunk",
    "agent_thought_chunk",
  ];
  assert(
    !updates
      .slice(resumeUpdates)
      .some(
        (update) =>
          update.sessionId === a.id && replayKinds.includes(update.kind),
      ),
    "Resume unexpectedly replayed transcript chunks",
  );
  await prompt(a, "session-a-turn4");
  const resumedTool = readLines(toolEvidence).at(-1);
  const resumedRequest = requests.find(
    (item) => item.nonce === resumedTool.nonce,
  );
  assert.equal(resumedTool.creationTurn, "session-a-turn4");
  assert.equal(resumedRequest.creationTurn, "session-a-turn4");
  assert.equal(resumedRequest.fixturePromptCount, 4);
  assert.deepEqual(resumedRequest.fixtureTurns, [
    "session-a-turn1",
    "session-a-turn2",
    "session-a-turn3",
    "session-a-turn4",
  ]);
  report.assertions.supportedResumeRotatesBothContexts = true;
  report.assertions.resumePreservesHistoryWithoutAcpReplay = true;

  const delayedPrompt = prompt(a, "session-a-cancelled", 8000, "cancelled");
  // Install a rejection handler before awaiting the independent start signal.
  delayedPrompt.catch(() => {});
  const delayed = await until(
    () =>
      readLines(toolLifecycle).find(
        (item) => item.event === "call_started" && item.delayMs === 8000,
      ),
    "delayed MCP call starts",
  );
  assert.equal(delayed.creationTurn, "session-a-turn4");
  const cancelledAt = Date.now();
  writeMessage({
    jsonrpc: "2.0",
    method: "session/cancel",
    params: { sessionId: a.id },
  });
  await delayedPrompt;
  report.cancellation = {
    cancelledAt,
    settledAt: Date.now(),
    oldToolPid: delayed.pid,
    oldToken: delayed.creationTurn,
    oldToolAliveAfterCancel: alive(delayed.pid),
  };
  assert(
    report.cancellation.settledAt < delayed.at + delayed.delayMs,
    "Cancellation only settled after the delayed tool finished",
  );
  // Leave a short observation interval before teardown, without assuming the
  // adapter awaits query.close or that this process cooperates with cancellation.
  await sleep(250);
  if (publication) {
    const attempt = await until(
      () =>
        readLines(toolLifecycle).find(
          (item) =>
            item.event === "publication_attempt" &&
            item.nonce === delayed.nonce,
        ),
      "cancelled tool attempts guarded publication",
    );
    assert(
      attempt.at >= cancelledAt,
      "Publication attempt preceded cancellation",
    );
    publication.assertCancelled(delayed.nonce, attempt);
    report.assertions.cancelledPublicationRejected = true;
  }
  report.cancellation.lifecycleBeforeResume = readLines(toolLifecycle).filter(
    (item) => item.nonce === delayed.nonce,
  );
  const cancelResume = await rpc("session/resume", {
    sessionId: a.id,
    ...sessionParams(a.token, "session-a-turn5"),
  });
  assert.equal(cancelResume.sessionId, a.id);
  report.cancellation.resumedAt = Date.now();
  report.cancellation.oldToolAliveAfterResume = alive(delayed.pid);
  if (faultMode === "missing-b2-tool")
    await rpc("session/resume", {
      sessionId: b.id,
      ...sessionParams(b.token, "session-b-turn1"),
      mcpServers: [],
    });
  await Promise.all([
    prompt(a, "session-a-turn5"),
    prompt(b, "session-b-turn2"),
  ]);
  const recoveredTool = readLines(toolEvidence).find(
    (item) => item.creationTurn === "session-a-turn5",
  );
  assert(recoveredTool, "No new tool call after cancelled-turn resume");
  const recoveredRequest = requests.find(
    (item) => item.nonce === recoveredTool.nonce,
  );
  assert.equal(recoveredRequest.creationTurn, "session-a-turn5");
  for (const turn of resumedRequest.fixtureTurns)
    assert(
      recoveredRequest.fixtureTurns.includes(turn),
      `Resume after cancellation lost ${turn}`,
    );
  await sleep(Math.max(0, delayed.at + delayed.delayMs + 300 - Date.now()));
  const lifecycle = readLines(toolLifecycle);
  const delayedEvents = lifecycle.filter(
    (item) => item.nonce === delayed.nonce,
  );
  assert(
    delayedEvents.every(
      (item) =>
        item.session === a.token && item.creationTurn === "session-a-turn4",
    ),
    "Old tool observed a replacement token",
  );
  const delayedCompletion = delayedEvents.find(
    (item) => item.event === "call_completed",
  );
  Object.assign(report.cancellation, {
    oldWorkContinuedAfterCancel: delayedEvents.some(
      (item) => item.event === "call_progress" && item.at > cancelledAt,
    ),
    oldToolAliveAfterObservation: alive(delayed.pid),
    oldCompletedAfterCancel: Boolean(
      delayedCompletion && delayedCompletion.at > cancelledAt,
    ),
    oldCompletedAfterResume: Boolean(
      delayedCompletion && delayedCompletion.at > report.cancellation.resumedAt,
    ),
    lifecycle: delayedEvents,
    observedUntil: Date.now(),
  });
  const finalTools = readLines(toolEvidence);
  for (const completion of completions.filter(
    (item) => item.stopReason === "end_turn",
  )) {
    const turnRequests = requests.filter(
      (item) =>
        !item.ancillary &&
        !item.isToolResult &&
        item.session === completion.session &&
        item.fixtureTurns.at(-1) === completion.requestedTurn,
    );
    assert.equal(
      turnRequests.length,
      1,
      `Expected one model tool request for ${completion.requestedTurn}`,
    );
    assert.equal(
      finalTools.filter((item) => item.nonce === turnRequests[0].nonce).length,
      1,
      `Expected one completed tool for ${completion.requestedTurn}`,
    );
  }
  for (const tool of finalTools) {
    const request = requests.find((item) => item.nonce === tool.nonce);
    assert(request, "Tool has no corresponding synthetic model request");
    assert.equal(tool.session, request.session);
    assert.equal(tool.creationTurn, request.creationTurn);
  }
  assert(
    !lifecycle.some((item) => item.event === "error"),
    "Synthetic MCP tool failed",
  );
  if (fatal) throw fatal;
  report.assertions.delayedPromptSettlesCancelled = true;
  report.assertions.resumeAfterCancelRotatesBothContexts = true;
  report.assertions.oldToolObservationsRetainOldToken = true;
  report.assertions.parallelSessionRemainsUsable = true;
  report.assertions.allCompletedToolCallsMatchTheirRequest = true;
  report.assertions.everyCompletedTurnHasOneCorrelatedTool = true;
  publication?.assertActive(finalTools, readLines(toolLifecycle));
  if (publication)
    report.assertions.recoveredAndIndependentSessionsPublish = true;
  report.streamOrdering = await ordering.run({
    session,
    rpc,
    prompt,
    requests,
    readTools: () => readLines(toolEvidence),
  });
  report.assertions.terminalStreamOrderingRecorded = true;
  report.sourceCommitAtFinish = execFileSync("git", ["rev-parse", "HEAD"], {
    encoding: "utf8",
  }).trim();
  assert.equal(
    report.sourceCommitAtFinish,
    report.sourceCommit,
    "Git HEAD changed during fixture",
  );
  for (const [file, digest] of Object.entries(report.sourceHashes))
    assert.equal(hash(file), digest, `Source changed during fixture: ${file}`);
  report.tokenHashes = Object.fromEntries(
    [...new Set(requests.map((item) => item.creationTurn))].map((token) => [
      token,
      createHash("sha256").update(token).digest("hex"),
    ]),
  );
  assert(
    !/Failed to re-assert model/.test(
      readFileSync(path.join(run, "adapter.stderr"), "utf8"),
    ),
    "Synthetic backend failed resumed model validation",
  );
  report.passed = true;
  report.limitation =
    "Synthetic session/load and session/resume rotate HTTP/MCP context while preserving history. Cancellation observations are bounded to this installed adapter and deliberately uncooperative MCP tool. No production receipt enforcement, restart fencing, real gateway correlation, or publication labels were tested.";
  report.requests = requests;
  report.tools = readLines(toolEvidence);
  report.completions = completions;
} catch (error) {
  report.error = error.message;
  report.errorCode = error.code;
  report.requests = requests;
  report.tools = readLines(toolEvidence);
  report.lifecycle = readLines(toolLifecycle);
  report.streamOrdering = ordering.evidence;
  process.exitCode = 1;
} finally {
  cleaningUp = true;
  fail(new Error("Fixture cleanup"));
  report.completions = completions;
  if (child?.pid) {
    try {
      process.kill(-child.pid, "SIGTERM");
    } catch {}
    await Promise.race([
      new Promise((resolve) => child.once("exit", resolve)),
      new Promise((resolve) => setTimeout(resolve, 1000)),
    ]);
    try {
      process.kill(-child.pid, "SIGKILL");
    } catch {}
  }
  await sleep(100);
  report.cleanup = {
    adapterAlive: child?.pid ? alive(child.pid) : false,
    toolPidsStillAlive: [
      ...new Set(readLines(toolLifecycle).map((item) => item.pid)),
    ].filter(alive),
  };
  if (report.cleanup.adapterAlive || report.cleanup.toolPidsStillAlive.length) {
    report.passed = false;
    report.error = "Fixture process survived bounded cleanup";
    process.exitCode = 1;
  }
  for (const p of pending.values()) clearTimeout(p.timer);
  server.closeAllConnections();
  server.close();
  if (publication) {
    try {
      publication.close();
    } catch (error) {
      report.passed = false;
      report.error = error.message;
      process.exitCode = 1;
    }
    report.publication = {
      events: publication.events,
      transitions: publication.transitions,
    };
  }
  const artifact = path.join(run, "result.json");
  writeFileSync(artifact, `${JSON.stringify(report, null, 2)}\n`, {
    mode: 0o600,
  });
  console.log(
    JSON.stringify({
      passed: report.passed,
      productionGate1Passed: false,
      artifact,
      error: report.error,
    }),
  );
}
