#!/usr/bin/env python3
"""Persistent GitHub boundary for the desktop publication journey."""

import hashlib
import json
import os
from pathlib import Path
import sys

path = Path(os.environ["FAKE_GH_STATE"])
state = json.loads(path.read_text())
args = sys.argv[1:]
state.setdefault("calls", []).append(args)


def finish(value=None, error=None):
    path.write_text(json.dumps(state))
    if error:
        print(error, file=sys.stderr)
        raise SystemExit(1)
    if value is not None:
        print(json.dumps(value))
    raise SystemExit(0)


def flag(name):
    return args[args.index(name) + 1]


if args[0] == "api":
    if "/releases/tags/" in args[1]:
        finish(state.get("release"), None if state.get("release") else "HTTP 404")
    if "/commits/" in args[1]:
        finish({"sha": state["tag_sha"]} if state.get("tag_sha") else None,
               None if state.get("tag_sha") else "HTTP 404")
elif args[:2] == ["release", "create"]:
    assert "--draft" in args and "--latest=false" in args
    assert not state.get("release")
    state["release"] = {"id": 1, "tag_name": args[2], "draft": True,
                        "target_commitish": flag("--target"), "assets": []}
    finish()
elif args[:2] == ["release", "upload"]:
    assert state["release"]["draft"], "published release was mutated"
    files = args[3:args.index("--repo")]
    failure = os.environ.get("FAKE_GH_FAILURE")
    if failure == "partial-upload":
        files = files[:1]
    elif failure == "missing-upload":
        files = files[:-1]
    for filename in files:
        asset = Path(filename)
        entries = state["release"]["assets"]
        entries[:] = [entry for entry in entries if entry["name"] != asset.name]
        entries.append({"name": asset.name, "size": asset.stat().st_size,
                        "state": "uploaded",
                        "digest": "sha256:" + hashlib.sha256(asset.read_bytes()).hexdigest()})
    if failure == "corrupt-upload":
        state["release"]["assets"][0]["digest"] = "sha256:" + "0" * 64
    finish(error="upload interrupted" if failure == "partial-upload" else None)
elif args[:2] == ["release", "edit"]:
    assert "--draft=false" in args and "--latest=false" in args
    state["published_assets"] = list(state["release"]["assets"])
    state["release"]["draft"] = False
    state["tag_sha"] = state["release"]["target_commitish"]
    finish()
finish(error=f"unexpected gh invocation: {args}")
