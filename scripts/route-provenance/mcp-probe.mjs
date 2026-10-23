import { appendFileSync } from "node:fs";
import { createInterface } from "node:readline";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const reply = (id, result) =>
  process.stdout.write(`${JSON.stringify({ jsonrpc: "2.0", id, result })}\n`);
const event = (details) =>
  appendFileSync(
    process.env.BUZZ_FIXTURE_LIFECYCLE,
    `${JSON.stringify({ at: Date.now(), pid: process.pid, session: process.env.BUZZ_FIXTURE_SESSION, creationTurn: process.env.BUZZ_FIXTURE_TURN, ...details })}\n`,
    { mode: 0o600 },
  );
const calls = new Map();
async function publish(nonce) {
  const binary = process.env.BUZZ_FIXTURE_PUBLICATION_CLI;
  if (!binary) return;
  let code = 0;
  try {
    await promisify(execFile)(
      binary,
      [
        "messages",
        "send",
        "--channel",
        process.env.BUZZ_FIXTURE_PUBLICATION_CHANNEL,
        "--content",
        `synthetic lifecycle publication ${nonce}`,
      ],
      { timeout: 3000 },
    );
  } catch (error) {
    if (!Number.isInteger(error.code)) throw error;
    code = error.code;
  }
  event({ event: "publication_attempt", nonce, code });
}
process.stdout.on("error", (error) => {
  if (error.code !== "EPIPE") throw error;
  event({ event: "stdout_closed" });
});
process.on("exit", (code) => event({ event: "exit", code }));
event({ event: "started" });

async function toolCall(request) {
  const { nonce, delayMs = 0 } = request.params.arguments;
  if (
    typeof nonce !== "string" ||
    !Number.isInteger(delayMs) ||
    delayMs < 0 ||
    delayMs > 10000
  )
    throw new Error("Invalid synthetic tool arguments");
  calls.set(request.id, nonce);
  event({ event: "call_started", nonce, delayMs });
  // This fixture deliberately represents a tool that cannot recall started work.
  // MCP cancellation is observed below, but does not cancel this timer.
  if (delayMs) {
    const progress = setInterval(
      () => event({ event: "call_progress", nonce }),
      100,
    );
    try {
      const publicationDelay = Math.min(200, delayMs);
      await new Promise((resolve) => setTimeout(resolve, publicationDelay));
      await publish(nonce);
      await new Promise((resolve) =>
        setTimeout(resolve, delayMs - publicationDelay),
      );
    } finally {
      clearInterval(progress);
    }
  } else await publish(nonce);
  const record = {
    session: process.env.BUZZ_FIXTURE_SESSION,
    creationTurn: process.env.BUZZ_FIXTURE_TURN,
    nonce,
    pid: process.pid,
  };
  appendFileSync(
    process.env.BUZZ_FIXTURE_EVIDENCE,
    `${JSON.stringify(record)}\n`,
    { mode: 0o600 },
  );
  event({ event: "call_completed", nonce });
  reply(request.id, {
    content: [{ type: "text", text: JSON.stringify(record) }],
  });
  calls.delete(request.id);
}

for await (const line of createInterface({ input: process.stdin })) {
  const request = JSON.parse(line);
  if (request.method === "notifications/cancelled")
    event({
      event: "cancel_received",
      nonce: calls.get(request.params?.requestId),
    });
  if (request.id === undefined) continue;
  if (request.method === "initialize") {
    reply(request.id, {
      protocolVersion: request.params.protocolVersion,
      capabilities: { tools: {} },
      serverInfo: { name: "route-fixture", version: "1" },
    });
  } else if (request.method === "tools/list") {
    reply(request.id, {
      tools: [
        {
          name: "route_probe",
          description: "Return the synthetic fixture session token.",
          inputSchema: {
            type: "object",
            properties: {
              nonce: { type: "string" },
              delayMs: { type: "integer", minimum: 0, maximum: 10000 },
            },
            required: ["nonce"],
            additionalProperties: false,
          },
        },
      ],
    });
  } else if (
    request.method === "tools/call" &&
    request.params.name === "route_probe"
  ) {
    void toolCall(request).catch((error) => {
      event({ event: "error", message: error.message });
      process.exitCode = 1;
    });
  } else if (request.method === "ping") {
    reply(request.id, {});
  } else {
    process.stdout.write(
      `${JSON.stringify({ jsonrpc: "2.0", id: request.id, error: { code: -32601, message: "Unsupported fixture method" } })}\n`,
    );
  }
}
