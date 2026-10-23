import assert from "node:assert/strict";

// Discovery only: this observes the installed adapter, not a production guard.
export function createStreamOrdering({ readLifecycle, sleep, until, writeMessage }) {
  const cases = [];
  const events = [];
  const bySession = new Map();
  const record = (test, nonce, event, details = {}) => {
    const entry = { at: Date.now(), case: test.name, nonce, event, ...details };
    events.push(entry);
    return entry;
  };

  async function handle(res, content, request) {
    const test = bySession.get(request.session);
    if (!test) return false;
    const attempt = ++test.attempts;
    assert(attempt <= 3, `Retry cap exceeded for ${test.name}`);
    const nonce = request.nonce;
    test.requests.push({ nonce, attempt, streaming: request.streaming });
    record(test, nonce, "request", { attempt });
    res.on("close", () => record(test, nonce, "response_closed", { finished: res.writableFinished }));
    if (!request.streaming) {
      assert(attempt > 1, "First ordering request must stream");
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ id: `msg_${nonce}`, type: "message", role: "assistant", model: "claude-sonnet-4-5", content: [content], stop_reason: "tool_use", stop_sequence: null, usage: { input_tokens: 1, output_tokens: 1 } }));
      record(test, nonce, "nonstream_transport_end");
      return true;
    }
    res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
    const emit = (event, data) => {
      assert(!res.destroyed, `Response closed before ${event} in ${test.name}`);
      record(test, nonce, event);
      res.write(`event: ${event}\ndata: ${JSON.stringify({ type: event, ...data })}\n\n`);
    };
    const hold = async (boundary) => {
      if (attempt !== 1 || test.boundary !== boundary) return false;
      const started = record(test, nonce, "hold_started", { boundary });
      test.holding = true;
      test.holdNonce = nonce;
      const deadline = performance.now() + 2000;
      const samples = [];
      do {
        const matching = readLifecycle().filter((item) => item.nonce === nonce);
        samples.push({ at: Date.now(), started: matching.some((item) => item.event === "call_started"), completed: matching.some((item) => item.event === "call_completed"), responseClosed: res.destroyed });
        await sleep(25);
      } while (performance.now() < deadline);
      test.holding = false;
      test.hold = { startedAt: started.at, endedAt: Date.now(), boundary, nonce, samples, toolStartedDuringHold: samples.some((item) => item.started), toolCompletedDuringHold: samples.some((item) => item.completed) };
      record(test, nonce, "hold_ended");
      if (test.ending === "cancel") {
        if (!res.destroyed) res.destroy();
        record(test, nonce, "cancel_transport_end");
        test.streamDone = true;
        return true;
      }
      if (test.ending === "error") {
        emit("error", { error: { type: "api_error", message: "synthetic terminal failure" } });
        res.end();
        record(test, nonce, "transport_end");
        test.streamDone = true;
        return true;
      }
      if (test.ending === "disconnect") {
        record(test, nonce, "socket_failure");
        res.destroy();
        test.streamDone = true;
        return true;
      }
      return false;
    };
    emit("message_start", { message: { id: `msg_${nonce}`, type: "message", role: "assistant", model: "claude-sonnet-4-5", content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 1, output_tokens: 0 } } });
    emit("content_block_start", { index: 0, content_block: { ...content, input: {} } });
    emit("content_block_delta", { index: 0, delta: { type: "input_json_delta", partial_json: JSON.stringify(content.input) } });
    emit("content_block_stop", { index: 0 });
    if (await hold("content_block_stop")) return true;
    emit("message_delta", { delta: { stop_reason: "tool_use", stop_sequence: null }, usage: { output_tokens: 1 } });
    if (await hold("message_delta")) return true;
    emit("message_stop", {});
    if (await hold("message_stop")) return true;
    // Some clients close immediately on message_stop; that is an observation.
    if (!res.destroyed) res.end();
    record(test, nonce, "transport_end");
    test.streamDone = true;
    return true;
  }

  async function run({ session, rpc, prompt, requests, readTools }) {
    const specifications = [
      ["content_block_stop", "normal"], ["message_delta", "normal"], ["message_stop", "normal"],
      ["content_block_stop", "error"], ["message_delta", "error"],
      ["content_block_stop", "disconnect"], ["message_delta", "disconnect"],
      ["content_block_stop", "cancel"], ["message_delta", "cancel"],
    ];
    for (const [boundary, ending] of specifications) {
      const name = `${boundary.replaceAll("_", "-")}-${ending}`;
      const test = { name, boundary, ending, attempts: 0, requests: [] };
      cases.push(test);
      const subject = await session(`order-${name}`);
      const control = await session(`control-${name}`);
      bySession.set(subject.token, test);
      const subjectPrompt = rpc("session/prompt", { sessionId: subject.id, prompt: [{ type: "text", text: `Call the synthetic route_probe tool once, then reply FIXTURE_OK. Fixture turn: order-${name}.` }] }).then(
        (result) => ({ kind: "result", stopReason: result.stopReason }),
        (error) => ({ kind: "error", message: error.message }),
      );
      await until(() => test.holding, `terminal hold starts for ${name}`);
      test.controlStartedAt = Date.now();
      assert(test.holding);
      const controlPrompt = prompt(control, `control-${name}`);
      controlPrompt.catch(() => {});
      if (ending === "cancel") {
        await sleep(500);
        assert(test.holding, "Cancel must be issued while stream is held");
        record(test, test.holdNonce, "acp_cancel_sent");
        writeMessage({ jsonrpc: "2.0", method: "session/cancel", params: { sessionId: subject.id } });
      }
      test.outcome = await subjectPrompt;
      record(test, test.holdNonce, "acp_prompt_settled", test.outcome);
      assert(!test.outcome.message?.includes("Timeout:"), `Ordering case timed out: ${name}`);
      await controlPrompt;
      test.controlCompletedAt = Date.now();
      await until(() => test.streamDone, `stream observation finishes for ${name}`);
      if (ending === "normal") assert.deepEqual(test.outcome, { kind: "result", stopReason: "end_turn" });
      if (ending === "cancel") assert.deepEqual(test.outcome, { kind: "result", stopReason: "cancelled" });
      const tools = readTools();
      const lifecycle = readLifecycle();
      for (const request of test.requests) {
        const observed = lifecycle.filter((item) => item.nonce === request.nonce);
        assert(observed.filter((item) => item.event === "call_started").length <= 1, "Request started a tool twice");
        assert(observed.filter((item) => item.event === "call_completed").length <= 1, "Request completed a tool twice");
        for (const item of observed) {
          assert.equal(item.session, subject.token);
          assert.equal(item.creationTurn, `${subject.token}-turn1`);
        }
        request.lifecycle = observed;
      }
      if (ending === "normal") {
        assert.equal(test.requests.length, 1, "Normal stream unexpectedly retried");
        assert.equal(tools.filter((item) => item.nonce === test.requests[0].nonce).length, 1, "Normal stream must complete exactly one tool");
      }
      const controlRequests = requests.filter((item) => item.session === control.token && !item.ancillary && !item.isToolResult);
      assert.equal(controlRequests.length, 1);
      const controlTools = tools.filter((item) => item.nonce === controlRequests[0].nonce);
      assert.equal(controlTools.length, 1);
      assert.equal(controlTools[0].session, control.token);
      assert.equal(controlTools[0].creationTurn, `${control.token}-turn1`);
      test.controlPassed = true;
    }
    return { cases, events, productionGate1Passed: false };
  }
  return { handle, run, evidence: { cases, events } };
}
