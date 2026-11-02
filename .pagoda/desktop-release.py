#!/usr/bin/env python3
"""Stage both desktop builds and publish one complete, immutable release."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
PLATFORMS = ("linux-x86_64", "windows-x86_64")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def run(*args):
    return subprocess.check_output(args, text=True).strip()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def identity(args):
    require(re.fullmatch(r"[0-9a-f]{40}", args.sha), "source must be a full commit SHA")
    require(re.fullmatch(r"\d+\.\d+\.\d+-pagoda\.[1-9]\d*", args.version),
            "version must be X.Y.Z-pagoda.RUN_ID")
    require(run("git", "rev-parse", "HEAD") == args.sha, "checkout differs from source SHA")


def metadata(path):
    require(path.is_file() and not path.is_symlink() and path.stat().st_size > 0,
            f"missing, empty, or symlinked artifact: {path}")
    with path.open("rb") as handle:
        digest = hashlib.file_digest(handle, "sha256").hexdigest()
    return {"size": path.stat().st_size, "sha256": digest}


def only(directory, pattern):
    files = list(directory.glob(pattern))
    require(len(files) == 1, f"expected exactly one {pattern} in {directory}")
    return files[0]


def record(args):
    identity(args)
    if args.platform == "linux-x86_64":
        appimage = only(args.bundle / "appimage", "*.AppImage")
        archives = list((args.bundle / "appimage").glob("*.AppImage.tar.gz"))
        require(len(archives) <= 1, "multiple Linux updater archives")
        updater = archives[0] if archives else appimage
        files = [only(args.bundle / "deb", "*.deb"), appimage, updater]
    else:
        updater = only(args.bundle / "nsis", "*.exe")
        files = [updater]
    signature = updater.with_name(updater.name + ".sig")
    require(signature.read_text(encoding="utf-8").strip(), "empty updater signature")
    files.append(signature)
    require(all(args.version in file.name for file in files), "artifact version mismatch")
    require(not args.output.exists(), "artifact staging directory already exists")
    args.output.mkdir(parents=True)
    entries = {}
    names = {}
    for file in dict.fromkeys(files):
        name = file.name
        # Windows has an updater signature but no Authenticode certificate.
        if args.platform == "windows-x86_64":
            name = name.replace(".exe", "_alpha-unsigned.exe")
        require(re.fullmatch(r"[A-Za-z0-9._+-]+", name), f"unsafe asset name: {name}")
        entries[name] = metadata(file)
        names[file] = name
        shutil.copyfile(file, args.output / name)
    write_json(args.output / "receipt.json", {
        "source_sha": args.sha, "version": args.version, "platform": args.platform,
        "updater": names[updater], "signature": names[signature], "files": entries,
    })
    print(f"Recorded {args.platform} at {args.sha} ({args.version})")


def assemble(args):
    receipts, files, triples = {}, {}, []
    tag = "desktop-v" + args.version
    base = f"https://github.com/{args.repo}/releases/download/{tag}"
    for platform in PLATFORMS:
        directory = args.artifacts / platform
        receipt = json.loads((directory / "receipt.json").read_text(encoding="utf-8"))
        require(receipt["source_sha"] == args.sha and receipt["version"] == args.version
                and receipt["platform"] == platform, f"{platform}: build identity mismatch")
        require(set(p.name for p in directory.iterdir()) == set(receipt["files"]) | {"receipt.json"},
                f"{platform}: unrecorded or missing files")
        for name, recorded in receipt["files"].items():
            require(re.fullmatch(r"[A-Za-z0-9._+-]+", name) and name not in files,
                    f"unsafe or duplicate release asset: {name}")
            file = directory / name
            require(metadata(file) == recorded, f"{platform}: artifact changed: {name}")
            files[name] = file
        updater, sig = receipt["updater"], receipt["signature"]
        require(updater in receipt["files"] and sig == updater + ".sig" and sig in receipt["files"],
                f"{platform}: missing updater or detached signature")
        require((directory / sig).read_text(encoding="utf-8").strip(), f"{platform}: empty signature")
        triples.append(f"{platform}:{directory / sig}:{base}/{updater}")
        receipts[platform] = receipt
    args.output.mkdir(parents=True, exist_ok=True)
    expected = set(files) | {"latest.json", "release-provenance.json"}
    require(set(p.name for p in args.output.iterdir()) <= expected, "unexpected files in release staging")
    for name, source in files.items():
        shutil.copyfile(source, args.output / name)
    manifest = json.loads(run("bash", str(ROOT / "desktop/scripts/generate-oss-latest-json.sh"),
                              args.version, *triples))
    # Stable across retries so a published release can be verified read-only.
    manifest["pub_date"] = run("git", "show", "-s", "--format=%cI", args.sha)
    write_json(args.output / "latest.json", manifest)
    write_json(args.output / "release-provenance.json", receipts)
    return {name: metadata(args.output / name) for name in sorted(expected)}


def api(endpoint, optional=False):
    result = subprocess.run(["gh", "api", endpoint], text=True, capture_output=True)
    if result.returncode:
        # commits/<missing tag> answers 422 "No commit found", not 404.
        if optional and ("HTTP 404" in result.stderr or "No commit found for SHA" in result.stderr):
            return None
        raise ValueError(f"GitHub read failed: {result.stderr.strip()}")
    return json.loads(result.stdout)


def find_release(repo, tag, required=True):
    # releases/tags/<tag> 404s while the release is a draft, so look it up in the list.
    result = subprocess.run(["gh", "api", "--paginate", "--slurp", f"repos/{repo}/releases?per_page=100"],
                            text=True, capture_output=True)
    if result.returncode:
        raise ValueError(f"GitHub read failed: {result.stderr.strip()}")
    matches = [r for page in json.loads(result.stdout) for r in page if r["tag_name"] == tag]
    require(len(matches) <= 1, f"more than one release carries tag {tag}")
    require(matches or not required, f"release {tag} not found")
    return matches[0] if matches else None


def verify_assets(release, expected):
    actual = release["assets"]
    require(len(actual) == len(expected) and {a["name"] for a in actual} == set(expected),
            "remote release has an incomplete or unexpected asset set")
    for asset in actual:
        wanted = expected[asset["name"]]
        require(asset.get("state") == "uploaded" and asset["size"] == wanted["size"]
                and asset.get("digest") == "sha256:" + wanted["sha256"],
                f"remote asset integrity mismatch: {asset['name']}")


def publish(args):
    identity(args)
    require(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", args.repo), "invalid repository")
    expected = assemble(args)  # Both builds validated before any remote mutation.
    tag = "desktop-v" + args.version
    commit_endpoint = f"repos/{args.repo}/commits/{tag}"

    def verify_tag(required=False):
        commit = api(commit_endpoint, optional=not required)
        require(commit is None or commit["sha"] == args.sha, "existing tag targets another source")

    def verify_identity(release):
        require(release["target_commitish"] == args.sha and release["tag_name"] == tag,
                "existing release targets another source")

    verify_tag()
    release = find_release(args.repo, tag, required=False)
    if release:
        verify_identity(release)
        if not release["draft"]:
            verify_tag(required=True)
            verify_assets(release, expected)
            print(f"Already published and verified: https://github.com/{args.repo}/releases/tag/{tag}")
            return
        require({a["name"] for a in release["assets"]} <= set(expected),
                "draft contains unexpected assets; refusing to modify it")
    else:
        notes = (f"Linux x86_64 and Windows x64 built from {args.sha}.\n\n"
                 "Windows is not Authenticode-signed; both updater payloads are signed.\n\n"
                 "This release keeps the current manual updater pin. Promote only after acceptance.")
        run("gh", "release", "create", tag, "--repo", args.repo, "--draft", "--latest=false",
            "--target", args.sha, "--title", f"Buzz Desktop {args.version} (pagoda)", "--notes", notes)
    run("gh", "release", "upload", tag, *(str(args.output / name) for name in expected),
        "--repo", args.repo, "--clobber")
    release = find_release(args.repo, tag)
    verify_identity(release)
    require(release["draft"], "release became public during upload")
    verify_assets(release, expected)
    verify_tag()
    run("gh", "release", "edit", tag, "--repo", args.repo, "--draft=false", "--latest=false")
    release = find_release(args.repo, tag)
    verify_identity(release)
    require(not release["draft"], "release is still a draft")
    verify_assets(release, expected)
    verify_tag(required=True)
    print(f"Published and verified: https://github.com/{args.repo}/releases/tag/{tag}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("record", "publish"):
        sub = commands.add_parser(command)
        sub.add_argument("--sha", required=True)
        sub.add_argument("--version", required=True)
        sub.add_argument("--output", type=Path, required=True)
        if command == "record":
            sub.add_argument("--platform", choices=PLATFORMS, required=True)
            sub.add_argument("--bundle", type=Path, required=True)
        else:
            sub.add_argument("--repo", required=True)
            sub.add_argument("--artifacts", type=Path, required=True)
    args = parser.parse_args()
    try:
        {"record": record, "publish": publish}[args.command](args)
    except (ValueError, OSError, KeyError, subprocess.CalledProcessError) as error:
        print(f"::error::{error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
