#!/usr/bin/env python3
"""Local Buzz boundary for the pagoda resolver E2E fixture."""

import hashlib
import json
import os
import sys
import time
from pathlib import Path

root = Path(os.environ["PAGODA_BUZZ_FIXTURE"])
mode = (root / "mode").read_text() if (root / "mode").exists() else "ok"
args = sys.argv[1:]


def value(flag):
    return args[args.index(flag) + 1]


if args[:2] == ["messages", "send"]:
    content = sys.stdin.read()
    path = root / "events.json"
    events = json.loads(path.read_text()) if path.exists() else []
    event_id = hashlib.sha256(f"{len(events)}:{content}".encode()).hexdigest()
    events.append(
        {
            "id": event_id,
            "kind": 9,
            "pubkey": "f" * 64,
            "content": content,
            "created_at": int(time.time()),
            "tags": [
                ["h", value("--channel")],
                ["e", value("--reply-to"), "", "reply"],
                ["p", value("--mention")],
            ],
        }
    )
    path.write_text(json.dumps(events))
    if mode == "ambiguous":
        sys.exit(2)
    print(json.dumps({"accepted": True, "event_id": event_id}))
elif args[:2] == ["social", "event"]:
    events = json.loads((root / "events.json").read_text())
    found = [e.copy() for e in events if e["id"] == value("--event")]
    if mode == "empty":
        found = []
    elif mode == "wrong" and found:
        found[0]["content"] = "wrong content"
    elif mode == "wrong-author" and found:
        found[0]["pubkey"] = "0" * 64
    elif mode == "wrong-mention" and found:
        found[0]["tags"] = [tag for tag in found[0]["tags"] if tag[0] != "p"]
    print(json.dumps(found))
elif args[:2] == ["messages", "get"]:
    path = root / "events.json"
    print(path.read_text() if path.exists() else "[]")
else:
    raise SystemExit(f"unexpected Buzz fixture command: {args}")
