"""Isolated receipt prototype; no live configuration or persistent storage."""

import codecs
import json
import uuid

from httpx_sse._decoders import SSEDecoder, SSELineDecoder
from litellm.integrations.custom_logger import CustomLogger


class AnthropicFrames:
    """Inspect bounded SSE frames without altering the bytes sent downstream."""

    def __init__(self):
        self.utf8 = codecs.getincrementaldecoder("utf-8")()
        self.lines = SSELineDecoder()
        self.events = SSEDecoder()
        self.event_bytes = 0

    def feed(self, chunk):
        if isinstance(chunk, dict):
            return [chunk]
        if isinstance(chunk, bytes):
            text = self.utf8.decode(chunk)
        elif isinstance(chunk, str):
            text = chunk
        else:
            raise TypeError("Unsupported receipt fixture stream shape")
        frames = []
        for line in self.lines.decode(text):
            self.event_bytes += len(line.encode("utf-8")) + 1
            if self.event_bytes > 65536:
                raise ValueError("Receipt frame exceeds limit")
            event = self.events.decode(line)
            if not line:
                self.event_bytes = 0
            if event is None or not event.data:
                continue
            frame = json.loads(event.data)
            if not isinstance(frame, dict) or not isinstance(frame.get("type"), str):
                raise TypeError("Invalid Anthropic frame")
            if event.event not in {"message", frame["type"]}:
                raise ValueError("Conflicting SSE event type")
            frames.append(frame)
        buffered = sum(len(part.encode("utf-8")) for part in self.lines.buffer)
        if self.event_bytes + buffered > 65536:
            raise ValueError("Receipt frame exceeds limit")
        return frames

    def complete(self):
        self.utf8.decode(b"", final=True)
        return not (self.event_bytes or self.lines.buffer or self.lines.trailing_cr)


class ReceiptBook:
    """Join trusted synthetic hop registrations without sharing turn state."""

    def __init__(self):
        self.turns = {}
        self.nodes = {}
        self.write_fault = False

    def issue(self, scope):
        required = {"relay", "author", "channel", "session", "turn"}
        if set(scope) != required or not all(
            isinstance(v, str) and v for v in scope.values()
        ):
            raise ValueError("Incomplete trusted scope")
        token = str(uuid.uuid4())
        self.turns[token] = {"scope": dict(scope), "active": True}
        return token

    def active(self, token):
        if token not in self.turns or not self.turns[token]["active"]:
            raise ValueError("Unknown or retired turn")

    def retire(self, token):
        self.active(token)
        self.turns[token]["active"] = False

    def begin(self, token, root, hop, parent=None, purpose="reply"):
        self.active(token)
        if not root or not hop or (token, hop) in self.nodes:
            raise ValueError("Invalid or reused hop")
        if parent is not None:
            previous = self.nodes.get((token, parent))
            if previous is None or previous["root"] != root:
                raise ValueError("Parent is missing or belongs to another request")
        self.nodes[token, hop] = {
            "root": root,
            "parent": parent,
            "purpose": purpose,
            "selection": None,
            "contributed": False,
            "completed": False,
            "failed": False,
            "conflict": False,
        }

    def lookup(self, data):
        headers = data.get("proxy_server_request", {}).get("headers", {})
        token = headers.get("x-buzz-turn-token")
        hop = headers.get("x-buzz-hop")
        self.active(token)
        node = self.nodes.get((token, hop))
        if node is None or node["root"] != headers.get("x-buzz-root-request"):
            raise ValueError("Receipt does not match registered request")
        return token, hop, node

    def contribute(self, token, hop):
        self.active(token)
        if self.write_fault:
            raise OSError("Synthetic receipt store unavailable")
        self.nodes[token, hop]["contributed"] = True

    def finish(self, token, hop, failed=False):
        node = self.nodes[token, hop]
        node["failed"] = failed
        node["completed"] = not failed

    def snapshot(self, token):
        self.active(token)
        fallbacks = set()

        def walk(hop, inherited_fallback=False):
            node = self.nodes[token, hop]
            selection = node["selection"]
            if not node["contributed"] or selection is None:
                return False
            degraded = inherited_fallback or selection["fallback"]
            if selection["leaf"]:
                if degraded:
                    fallbacks.add(selection["name"])
                return not node["conflict"]
            children = [
                (h, n)
                for (t, h), n in self.nodes.items()
                if t == token and n["parent"] == hop and n["contributed"]
            ]
            if not children:
                return False
            # Visit every branch; short-circuiting would drop known contributions.
            branches = [walk(h, degraded) for h, _ in children]
            known = all(branches)
            return known and not node["conflict"]

        roots = [
            h
            for (t, h), n in self.nodes.items()
            if t == token and n["parent"] is None and n["purpose"] == "reply"
        ]
        root_results = [walk(h) for h in roots]
        known = bool(roots) and all(root_results)
        return {
            "status": ("degraded" if fallbacks else "primary") if known else "unknown",
            "fallbacks": sorted(fallbacks),
        }


class RouteReceiptBridge(CustomLogger):
    """Record contribution before release through LiteLLM's awaited hook."""

    def __init__(
        self, book, deployments, internal_destinations=(), completion_only=False
    ):
        self.book = book
        self.deployments = deployments
        self.internal_destinations = frozenset(internal_destinations)
        self.completion_only = completion_only

    async def async_pre_call_deployment_hook(self, kwargs, call_type):
        outgoing = dict(kwargs)
        extra = {
            name: value
            for name, value in kwargs.get("extra_headers", {}).items()
            if not name.lower().startswith("x-buzz-")
        }
        outgoing["extra_headers"] = extra
        headers = kwargs.get("proxy_server_request", {}).get("headers", {})
        if "x-buzz-turn-token" not in headers:
            return outgoing
        token, parent, node = self.book.lookup(kwargs)
        if kwargs.get("api_base") in self.internal_destinations:
            child = str(uuid.uuid4())
            self.book.begin(token, node["root"], child, parent=parent)
            extra.update(
                {
                    "x-buzz-turn-token": token,
                    "x-buzz-root-request": node["root"],
                    "x-buzz-hop": child,
                }
            )
        return outgoing

    async def async_post_call_response_headers_hook(
        self,
        data,
        user_api_key_dict,
        response,
        request_headers=None,
        litellm_call_info=None,
    ):
        _, _, node = self.book.lookup(data)
        deployment = (litellm_call_info or {}).get("model_id")
        selected = self.deployments.get(deployment)
        # An unknown deployment is evidence absence, never a guessed alias.
        if node["selection"] is not None and node["selection"] != selected:
            node["conflict"] = True
        else:
            node["selection"] = dict(selected) if selected else None

    async def async_post_call_streaming_iterator_hook(
        self, user_api_key_dict, response, request_data
    ):
        token, hop, _ = self.book.lookup(request_data)
        frames = AnthropicFrames()
        terminal = False
        error_event = False
        contributed = False
        try:
            async for chunk in response:
                self.book.active(token)
                for frame in frames.feed(chunk):
                    kind = frame.get("type")
                    if terminal:
                        raise ValueError("Frame after terminal completion")
                    if kind in {"content_block_start", "content_block_delta"}:
                        contributed = True
                        if not self.completion_only:
                            self.book.contribute(token, hop)
                    terminal = kind == "message_stop"
                    error_event = error_event or kind == "error"
                yield chunk
            if self.completion_only and contributed:
                self.book.contribute(token, hop)
            self.book.finish(
                token, hop, failed=not terminal or error_event or not frames.complete()
            )
        except BaseException:
            self.book.finish(token, hop, failed=True)
            raise
