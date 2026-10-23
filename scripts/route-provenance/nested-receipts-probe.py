"""Execute the installed LiteLLM callback pipeline using synthetic streams."""

import asyncio
import hashlib
import importlib.metadata
import json
import os
import subprocess
import sys
import uuid
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

os.environ["LITELLM_LOCAL_MODEL_COST_MAP"] = "True"

import litellm
from litellm.llms.anthropic.experimental_pass_through.messages.streaming_iterator import (
    BaseAnthropicMessagesStreamingIterator,
)
from litellm.proxy._types import UserAPIKeyAuth
from litellm.proxy.common_request_processing import ProxyBaseLLMRequestProcessing
from litellm.proxy.common_utils.user_api_key_cache import UserApiKeyCache
from litellm.proxy.utils import ProxyLogging
from litellm.utils import async_pre_call_deployment_hook
from route_receipts import ReceiptBook, RouteReceiptBridge


async def exercise(report, mode):
    book = ReceiptBook()
    deployments = {
        "gateway": {"leaf": False, "fallback": False, "name": "outer-alias"},
        "local-strong": {"leaf": False, "fallback": True, "name": "local-strong"},
        "primary": {"leaf": True, "fallback": False, "name": "fixture-primary"},
        "fallback-leaf": {"leaf": True, "fallback": True, "name": "fixture-fallback"},
    }
    bridge = RouteReceiptBridge(
        book,
        deployments,
        internal_destinations={"http://gateway.synthetic.invalid/v1"},
        completion_only=mode == "completion-only",
    )
    litellm.callbacks = [bridge]
    proxy = ProxyLogging(UserApiKeyCache())
    auth = UserAPIKeyAuth()
    assertions = report["assertions"]

    def check(name, condition):
        assertions[name] = bool(condition)
        if not condition:
            raise AssertionError(name)

    def issue(**changes):
        scope = {
            "relay": "relay-a",
            "author": "agent-a",
            "channel": "channel-a",
            "session": "session-a",
            "turn": str(uuid.uuid4()),
        }
        scope.update(changes)
        return book.issue(scope)

    def request(token, root, hop, parent=None, purpose="reply"):
        book.begin(token, root, hop, parent=parent, purpose=purpose)
        # This fixture supplies trusted registrations. Headers are correlation,
        # not authentication; live ingress validation remains a separate gate.
        return {
            "metadata": {},
            "proxy_server_request": {
                "headers": {
                    "x-buzz-turn-token": token,
                    "x-buzz-root-request": root,
                    "x-buzz-hop": hop,
                }
            },
        }

    async def selected(data, deployment):
        response = SimpleNamespace(
            _hidden_params={
                "model_id": deployment,
                "custom_llm_provider": "fixture",
                "api_base": "http://synthetic.invalid",
            }
        )
        await proxy.post_call_response_headers_hook(
            data=data, user_api_key_dict=auth, response=response
        )

    def wrap(data, source):
        return proxy.async_post_call_streaming_iterator_hook(
            response=source, request_data=data, user_api_key_dict=auth
        )

    forwarding = issue()
    forward_data = request(forwarding, "forward-root", "forward-parent")
    forward_data["api_base"] = "http://gateway.synthetic.invalid/v1"
    forward_data["proxy_server_request"]["headers"]["x-api-key"] = (
        "synthetic-do-not-forward"
    )
    forward_data["extra_headers"] = {
        "X-Buzz-Untrusted": "discard",
        "provider-option": "keep",
    }
    before = deepcopy(forward_data)
    outgoing = await async_pre_call_deployment_hook(forward_data, "acompletion")
    sent = outgoing["extra_headers"]
    check(
        "forward_only_correlation_allowlist",
        set(sent)
        == {
            "provider-option",
            "x-buzz-turn-token",
            "x-buzz-root-request",
            "x-buzz-hop",
        },
    )
    check(
        "forward_preserves_root_and_turn",
        sent["x-buzz-turn-token"] == forwarding
        and sent["x-buzz-root-request"] == "forward-root",
    )
    check(
        "forward_registers_child_link",
        book.nodes[forwarding, sent["x-buzz-hop"]]["parent"] == "forward-parent",
    )
    check("forward_does_not_mutate_input", forward_data == before)
    second = await async_pre_call_deployment_hook(forward_data, "acompletion")
    check(
        "attempts_have_distinct_hop_ids",
        second["extra_headers"]["x-buzz-hop"] != sent["x-buzz-hop"],
    )
    for destination in [
        "https://provider.synthetic.invalid",
        "http://gateway.synthetic.invalid.evil/v1",
    ]:
        external = {**forward_data, "api_base": destination, "extra_headers": sent}
        cleaned = await async_pre_call_deployment_hook(external, "acompletion")
        check(
            "no_correlation_to_" + destination,
            cleaned["extra_headers"] == {"provider-option": "keep"},
        )

    async def source(ending="complete"):
        yield {
            "type": "content_block_delta",
            "delta": {"type": "text_delta", "text": "fixture"},
        }
        if ending == "error":
            raise RuntimeError("synthetic stream failure")
        if ending == "cancel":
            raise asyncio.CancelledError()
        yield {"type": "message_stop"}

    async def chain(token, root, ending="complete", omit_leaf=False):
        outer = request(token, root, root + "-outer")
        inner = request(token, root, root + "-inner", parent=root + "-outer")
        leaf = request(token, root, root + "-leaf", parent=root + "-inner")
        await selected(outer, "gateway")
        await selected(inner, "local-strong")
        if not omit_leaf:
            await selected(leaf, "fallback-leaf")
        stream = wrap(leaf, source(ending)) if not omit_leaf else source(ending)
        return wrap(outer, wrap(inner, stream))

    token = issue()
    stream = await chain(token, "nested")
    check(
        "selection_without_bytes_is_unknown",
        book.snapshot(token)["status"] == "unknown",
    )
    chunk = await anext(stream)
    snap = book.snapshot(token)
    report["before_terminal"] = snap
    check(
        "leaf_available_before_terminal",
        snap == {"status": "degraded", "fallbacks": ["fixture-fallback"]},
    )
    check("stream_chunk_unchanged", chunk["delta"]["text"] == "fixture")
    check(
        "no_premature_completion", not any(n["completed"] for n in book.nodes.values())
    )
    await selected(request(token, "probe", "health", purpose="health"), "primary")
    check("health_probe_does_not_clear_fallback", book.snapshot(token) == snap)
    remaining = [chunk async for chunk in stream]
    check("terminal_preserved", remaining == [{"type": "message_stop"}])
    check(
        "all_nested_hops_complete",
        all(n["completed"] for n in book.nodes.values() if n["root"] == "nested"),
    )
    check("outer_completion_preserves_leaf", book.snapshot(token) == snap)

    primary = request(token, "later", "later-primary")
    await selected(primary, "primary")
    _ = [c async for c in wrap(primary, source())]
    check("later_primary_cannot_clear_turn", book.snapshot(token) == snap)

    fresh = issue()
    fresh_data = request(fresh, "fresh", "fresh-primary")
    await selected(fresh_data, "primary")
    _ = [c async for c in wrap(fresh_data, source())]
    check(
        "new_primary_turn_recovers",
        book.snapshot(fresh) == {"status": "primary", "fallbacks": []},
    )

    conflicting = issue()
    conflict_data = request(conflicting, "conflict", "conflict")
    await selected(conflict_data, "fallback-leaf")
    _ = [c async for c in wrap(conflict_data, source())]
    await selected(conflict_data, "primary")
    check(
        "conflicting_late_selection_is_unknown",
        book.snapshot(conflicting)
        == {"status": "unknown", "fallbacks": ["fixture-fallback"]},
    )

    missing = issue()
    missing_stream = await chain(missing, "missing", omit_leaf=True)
    await anext(missing_stream)
    check(
        "missing_cluster_leaf_is_unknown", book.snapshot(missing)["status"] == "unknown"
    )
    _ = [c async for c in missing_stream]
    check(
        "outer_completion_cannot_fill_missing_leaf",
        book.snapshot(missing)["status"] == "unknown",
    )

    for ending in ["error", "cancel"]:
        failed = issue()
        failed_stream = await chain(failed, ending, ending)
        await anext(failed_stream)
        try:
            await anext(failed_stream)
        except (RuntimeError, asyncio.CancelledError):
            pass
        else:
            raise AssertionError("Expected stream failure")
        nodes = [v for (t, _), v in book.nodes.items() if t == failed]
        check(
            ending + "_does_not_claim_completion",
            all(n["failed"] and not n["completed"] for n in nodes),
        )
        check(
            ending + "_preserves_contribution",
            book.snapshot(failed)["status"] == "degraded",
        )
        book.retire(failed)
        try:
            book.snapshot(failed)
        except ValueError:
            check(ending + "_retirement_rejects_snapshot", True)
        else:
            raise AssertionError("Retired turn remained active")

    isolated = []
    for field in ["relay", "author", "channel", "session", "turn"]:
        other = issue(**{field: "independent-" + field})
        data = request(other, "same-root", "same-hop")
        await selected(data, "primary")
        isolated.append((field, other, wrap(data, source())))
    await asyncio.gather(*(anext(s) for _, _, s in isolated))
    for field, other, s in isolated:
        check(field + "_scope_isolated", book.snapshot(other)["status"] == "primary")
        _ = [c async for c in s]
    check("parallel_primary_cannot_clear_original", book.snapshot(token) == snap)

    invalid = issue()
    request(invalid, "root-a", "parent")
    for name, operation in [
        (
            "wrong_root_rejected",
            lambda: request(invalid, "root-b", "child", parent="parent"),
        ),
        ("duplicate_hop_rejected", lambda: request(invalid, "root-a", "parent")),
        (
            "missing_parent_rejected",
            lambda: request(invalid, "root-a", "orphan", parent="absent"),
        ),
        (
            "unregistered_token_rejected",
            lambda: request("invalid-token", "root", "hop"),
        ),
    ]:
        try:
            operation()
        except ValueError:
            check(name, True)
        else:
            raise AssertionError(name)

    absent = issue()
    absent_data = request(absent, "absent", "absent")
    await selected(absent_data, None)
    _ = [c async for c in wrap(absent_data, source())]
    check("absent_deployment_is_unknown", book.snapshot(absent)["status"] == "unknown")

    unfinished = issue()
    unfinished_data = request(unfinished, "unfinished", "unfinished")
    await selected(unfinished_data, "primary")

    async def silent_eof():
        yield {"type": "content_block_delta", "delta": {"text": "fixture"}}

    _ = [c async for c in wrap(unfinished_data, silent_eof())]
    check(
        "eof_without_terminal_is_not_completed",
        book.nodes[unfinished, "unfinished"]["failed"],
    )

    encoder = BaseAnthropicMessagesStreamingIterator(None, {})
    tool_frame = {
        "type": "content_block_start",
        "index": 0,
        "content_block": {
            "type": "tool_use",
            "id": "synthetic-tool",
            "name": "publish",
            "input": {},
        },
    }
    wire_tool = encoder._convert_chunk_to_sse_format(tool_frame)
    wire_stop = encoder._convert_chunk_to_sse_format({"type": "message_stop"})

    async def wire_source(chunks):
        for chunk in chunks:
            yield chunk

    wire_turn = issue()
    wire_outer = request(wire_turn, "wire", "wire-outer")
    wire_inner = request(wire_turn, "wire", "wire-inner", parent="wire-outer")
    wire_leaf = request(wire_turn, "wire", "wire-leaf", parent="wire-inner")
    await selected(wire_outer, "gateway")
    await selected(wire_inner, "local-strong")
    await selected(wire_leaf, "fallback-leaf")
    serialized = ProxyBaseLLMRequestProcessing.async_sse_data_generator(
        response=wrap(wire_inner, wrap(wire_leaf, wire_source([wire_tool, wire_stop]))),
        user_api_key_dict=auth,
        request_data=wire_outer,
        proxy_logging_obj=proxy,
    )
    check(
        "installed_sse_generator_preserves_tool_bytes",
        await anext(serialized) == wire_tool,
    )
    wire_snapshot = book.snapshot(wire_turn)
    check("serialized_tool_has_leaf_before_release", wire_snapshot == snap)
    check(
        "serialized_tool_terminal_still_held",
        not any(n["completed"] for (t, _), n in book.nodes.items() if t == wire_turn),
    )
    report["serialized_before_terminal"] = wire_snapshot
    binary = Path("target/debug/buzz").resolve()
    publication = await asyncio.create_subprocess_exec(
        "node",
        str(Path(__file__).with_name("receipt-publication-probe.mjs")),
        str(binary),
        report["artifactDirectory"],
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            publication.communicate(
                json.dumps(
                    [wire_snapshot, book.snapshot(fresh), book.snapshot(missing)]
                ).encode()
            ),
            timeout=20,
        )
    finally:
        if publication.returncode is None:
            publication.kill()
            await publication.wait()
    if publication.returncode:
        raise AssertionError("Publication fixture failed: " + stderr.decode()[-2000:])
    report["publication"] = json.loads(stdout)
    check("receipt_label_signed_before_terminal", report["publication"]["passed"])
    check(
        "publication_cannot_consume_terminal",
        not any(n["completed"] for (t, _), n in book.nodes.items() if t == wire_turn),
    )
    _ = [c async for c in serialized]
    check(
        "serialized_nested_hops_complete",
        all(n["completed"] for (t, _), n in book.nodes.items() if t == wire_turn),
    )

    fragmented = (
        'event: content_block_delta\r\ndata: {"type":"content_block_delta",'
        '"delta":{"type":"text_delta","text":"café"}}\r\n\r\n'
    ).encode()
    for split in range(1, len(fragmented)):
        split_turn = issue()
        split_data = request(split_turn, "split", "split")
        await selected(split_data, "fallback-leaf")
        parts = [fragmented[:split], fragmented[split:], wire_stop]
        fragmented_stream = wrap(split_data, wire_source(parts))
        check(
            f"split_{split}_first_bytes_preserved",
            await anext(fragmented_stream) == parts[0],
        )
        check(
            f"split_{split}_incomplete_unknown",
            book.snapshot(split_turn)["status"] == "unknown",
        )
        check(
            f"split_{split}_second_bytes_preserved",
            await anext(fragmented_stream) == parts[1],
        )
        check(
            f"split_{split}_receipt_before_release", book.snapshot(split_turn) == snap
        )
        _ = [c async for c in fragmented_stream]
    report["byte_splits_checked"] = len(fragmented) - 1

    ping_turn = issue()
    ping_data = request(ping_turn, "ping", "ping")
    await selected(ping_data, "fallback-leaf")
    ping = b': keepalive\n\nevent: ping\ndata: {"type":"ping"}\n\n'
    _ = [c async for c in wrap(ping_data, wire_source([ping, wire_stop]))]
    check(
        "heartbeat_does_not_contribute", book.snapshot(ping_turn)["status"] == "unknown"
    )

    for name, chunks in [
        ("malformed_json", [b"event: content_block_start\ndata: {bad}\n\n"]),
        (
            "event_type_mismatch",
            [b'event: ping\ndata: {"type":"content_block_start"}\n\n'],
        ),
        ("invalid_utf8", [b"\xff\n\n"]),
        ("oversized_unfinished_frame", [b"data: " + b"x" * 65536]),
        ("content_after_terminal", [wire_stop, wire_tool]),
    ]:
        bad_turn = issue()
        bad_data = request(bad_turn, name, name)
        await selected(bad_data, "fallback-leaf")
        try:
            _ = [c async for c in wrap(bad_data, wire_source(chunks))]
        except (ValueError, UnicodeError):
            check(name + "_rejected", True)
        else:
            raise AssertionError(name + " was accepted")
        check(name + "_no_completion", not book.nodes[bad_turn, name]["completed"])

    error_turn = issue()
    error_data = request(error_turn, "error-event", "error-event")
    await selected(error_data, "fallback-leaf")
    error_frame = encoder._convert_chunk_to_sse_format(
        {"type": "error", "error": {"type": "overloaded_error"}}
    )
    _ = [
        c
        async for c in wrap(
            error_data, wire_source([wire_tool, error_frame, wire_stop])
        )
    ]
    check(
        "error_event_never_completed", book.nodes[error_turn, "error-event"]["failed"]
    )
    check("error_event_keeps_contribution", book.snapshot(error_turn) == snap)

    for name, chunks in [
        ("partial_frame", [wire_tool[:20]]),
        ("trailing_partial_after_terminal", [wire_stop, wire_tool[:20]]),
    ]:
        partial_turn = issue()
        partial_data = request(partial_turn, name, name)
        await selected(partial_data, "fallback-leaf")
        _ = [c async for c in wrap(partial_data, wire_source(chunks))]
        check(name + "_unknown", book.snapshot(partial_turn)["status"] == "unknown")
        check(name + "_not_completed", not book.nodes[partial_turn, name]["completed"])

    malformed = issue()
    malformed_data = request(malformed, "malformed", "malformed")
    await selected(malformed_data, "primary")

    async def unsupported_frame():
        yield object()

    try:
        await anext(wrap(malformed_data, unsupported_frame()))
    except TypeError:
        check("unsupported_frame_blocks_release", True)
    else:
        raise AssertionError("Unsupported protocol frame was accepted")

    bad_token = {
        "proxy_server_request": {
            "headers": {
                "x-buzz-turn-token": fresh,
                "x-buzz-root-request": "nested",
                "x-buzz-hop": "nested-outer",
            }
        }
    }
    try:
        await anext(wrap(bad_token, source()))
    except ValueError:
        check("other_turn_header_cannot_select_receipt", True)
    else:
        raise AssertionError("Mismatched turn header selected a receipt")

    retry = issue()
    retry_outer = request(retry, "retry", "retry-outer")
    await selected(retry_outer, "gateway")
    abandoned = request(retry, "retry", "attempt-one", parent="retry-outer")
    await selected(abandoned, "primary")
    book.finish(retry, "attempt-one", failed=True)
    winner = request(retry, "retry", "attempt-two", parent="retry-outer")
    await selected(winner, "fallback-leaf")
    retry_stream = wrap(retry_outer, wrap(winner, source()))
    await anext(retry_stream)
    check(
        "failed_attempt_does_not_mask_winner",
        book.snapshot(retry)["fallbacks"] == ["fixture-fallback"],
    )
    _ = [c async for c in retry_stream]
    check(
        "retry_does_not_change_other_turn", book.snapshot(fresh)["status"] == "primary"
    )

    broken = issue()
    data = request(broken, "broken", "broken")
    await selected(data, "primary")
    book.write_fault = True
    try:
        await anext(wrap(data, source()))
    except OSError:
        check("store_failure_blocks_chunk", True)
    else:
        raise AssertionError("Store failure allowed chunk release")
    finally:
        book.write_fault = False

    restarted = ReceiptBook()
    try:
        restarted.snapshot(token)
    except ValueError:
        check("restart_cannot_adopt_old_receipt", True)
    else:
        raise AssertionError("Restart adopted stale state")
    report["nodes"] = [{"token": t, "hop": h, **n} for (t, h), n in book.nodes.items()]


def main():
    output = Path(sys.argv[1]).resolve()
    if str(output).startswith(("/tmp/", "/var/tmp/")):
        raise ValueError("Use disk-backed artifacts")
    mode = sys.argv[2] if len(sys.argv) > 2 else "normal"
    if mode not in {"normal", "completion-only"}:
        raise ValueError("Unknown negative control")
    run = output / ("nested-" + str(uuid.uuid4()))
    run.mkdir(parents=True, mode=0o700)
    git = lambda *args: subprocess.check_output(["git", *args], text=True).strip()
    sources = [
        Path(__file__),
        Path(__file__).with_name("route_receipts.py"),
        Path(__file__).with_name("receipt-publication-probe.mjs"),
        Path(__file__).with_name("publication-lifecycle.mjs"),
        Path(__file__).with_name("delivery-readback.mjs"),
        Path("target/debug/buzz"),
        Path("crates/buzz-cli/src/route_labels.rs"),
        Path("crates/buzz-cli/src/publication_fence.rs"),
        Path("crates/buzz-cli/src/commands/messages.rs"),
        Path(litellm.__file__).parent / "proxy/utils.py",
        Path(litellm.__file__).parent / "integrations/custom_logger.py",
        Path(litellm.__file__).parent / "proxy/common_request_processing.py",
        Path(litellm.__file__).parent
        / "llms/anthropic/experimental_pass_through/messages/streaming_iterator.py",
        Path(litellm.__file__).parent.parent / "httpx_sse/_decoders.py",
    ]
    hashes = lambda: {
        str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources
    }
    report = {
        "fixture": "installed-litellm-nested-callbacks",
        "mode": mode,
        "artifactDirectory": str(run),
        "passed": False,
        "productionGate1Passed": False,
        "sourceCommit": git("rev-parse", "HEAD"),
        "gitStatus": git("status", "--short"),
        "litellmVersion": importlib.metadata.version("litellm"),
        "sourceHashes": hashes(),
        "assertions": {},
    }
    try:
        asyncio.run(exercise(report, mode))
        assert report["sourceHashes"] == hashes(), "Source changed during execution"
        assert report["sourceCommit"] == git("rev-parse", "HEAD"), "HEAD changed"
        report["passed"] = True
    except BaseException as error:  # noqa: BLE001 -- Persist evidence, then exit unsuccessfully.
        report["error"] = str(error)
    finally:
        (run / "result.json").write_text(json.dumps(report, indent=2) + "\n")
        print(
            json.dumps(
                {
                    "passed": report["passed"],
                    "assertions": len(report["assertions"]),
                    "error": report.get("error"),
                    "artifact": str(run / "result.json"),
                }
            )
        )
    sys.exit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
