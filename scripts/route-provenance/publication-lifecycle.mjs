import assert from "node:assert/strict";
import { execFile, execFileSync } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import http from "node:http";
import path from "node:path";
import { promisify } from "node:util";

// This is fixture-owned lifecycle wiring, not a production context issuer.
export async function publicationLifecycle(binary, run, skipRetirement) {
  const events = [];
  const transitions = [];
  const current = new Map();
  const sessions = new Map();
  let failure;
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
        events.push(body);
        response.end(JSON.stringify({ accepted: true, event_id: body.id }));
      } else if (request.url === "/query") {
        response.end(JSON.stringify(events));
      } else throw new Error("Unexpected publication fixture endpoint");
    } catch (error) {
      failure = error;
      response.statusCode = 500;
      response.end("{}");
    }
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const channel = randomUUID();
  const common = {
    PATH: process.env.PATH,
    HOME: run,
    TMPDIR: run,
    BUZZ_RELAY_URL: `http://127.0.0.1:${server.address().port}`,
    BUZZ_PRIVATE_KEY: randomBytes(32).toString("hex"),
  };
  try {
    await promisify(execFile)(
      binary,
      [
        "messages",
        "send",
        "--channel",
        channel,
        "--content",
        "synthetic lifecycle parent",
      ],
      { env: common, timeout: 5000 },
    );
    assert.equal(events.length, 1);
  } catch (error) {
    server.closeAllConnections();
    server.close();
    throw error;
  }
  function retire(token) {
    const previous = current.get(token);
    if (previous?.record.state !== "active") return;
    if (skipRetirement) {
      transitions.push({ at: Date.now(), event: "retirement-skipped", token });
      return;
    }
    const record = { ...previous.record, state: "retired" };
    execFileSync(
      "python3",
      [
        "-c",
        "import fcntl,json,sys\nfrom pathlib import Path\np=Path(sys.argv[1])\nwith (p/'lock').open('r+') as lock:\n fcntl.flock(lock,fcntl.LOCK_EX)\n (p/'context.json').write_text(sys.argv[2])\n",
        previous.directory,
        JSON.stringify(record),
      ],
      { env: { PATH: process.env.PATH }, timeout: 5000 },
    );
    previous.record = record;
    transitions.push({
      at: Date.now(),
      event: "retired",
      token,
      turn: previous.turn,
    });
  }
  return {
    events,
    transitions,
    register(id, token) {
      sessions.set(id, token);
    },
    cancel(id) {
      assert(sessions.has(id), "Cancellation lacks registered fixture session");
      retire(sessions.get(id));
    },
    environment(token, turn) {
      let context = current.get(token);
      if (!context || context.turn !== turn) {
        retire(token);
        const directory = path.join(run, `publication-${randomUUID()}`);
        mkdirSync(directory, { mode: 0o700 });
        const record = {
          version: 1,
          token: randomUUID(),
          state: "active",
          expires_at: Math.floor(Date.now() / 1000) + 600,
          relay: common.BUZZ_RELAY_URL,
          agent_pubkey: events[0].pubkey,
          channel_id: channel,
        };
        writeFileSync(path.join(directory, "lock"), "", { mode: 0o600 });
        writeFileSync(
          path.join(directory, "context.json"),
          JSON.stringify(record),
          { mode: 0o600 },
        );
        context = { directory, record, turn };
        current.set(token, context);
        transitions.push({ at: Date.now(), event: "issued", token, turn });
      }
      return Object.entries({
        ...common,
        BUZZ_FIXTURE_PUBLICATION_CLI: binary,
        BUZZ_FIXTURE_PUBLICATION_CHANNEL: channel,
        BUZZ_PUBLICATION_CONTEXT: context.directory,
        BUZZ_PUBLICATION_TOKEN: context.record.token,
      }).map(([name, value]) => ({ name, value }));
    },
    assertCancelled(nonce, attempt) {
      if (failure) throw failure;
      assert.notEqual(attempt.code, 0, "Cancelled tool was allowed to publish");
      assert.equal(
        events.filter((event) => event.content.includes(nonce)).length,
        0,
        "Cancelled tool submitted a signed event",
      );
    },
    assertActive(tools, lifecycle) {
      if (failure) throw failure;
      for (const turn of ["session-a-turn5", "session-b-turn1"]) {
        const matching = tools.filter((tool) => tool.creationTurn === turn);
        assert(matching.length > 0);
        for (const tool of matching) {
          assert(
            lifecycle.some(
              (item) =>
                item.nonce === tool.nonce &&
                item.event === "publication_attempt" &&
                item.code === 0,
            ),
          );
          assert.equal(
            events.filter((event) => event.content.includes(tool.nonce)).length,
            1,
          );
        }
      }
      assert.equal(
        transitions.filter(
          (item) => item.token === "session-b" && item.event === "issued",
        ).length,
        1,
      );
      assert(
        !transitions.some(
          (item) => item.token === "session-b" && item.event === "retired",
        ),
      );
    },
    close() {
      for (const token of current.keys()) retire(token);
      server.closeAllConnections();
      server.close();
    },
  };
}
