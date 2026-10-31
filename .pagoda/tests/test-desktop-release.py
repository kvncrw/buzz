#!/usr/bin/env python3
"""Exercise the real recorder/publisher with only GitHub replaced.

Failure design (written before implementation):
* Missing platform, wrong source/version, empty signature, or tampered asset
  must fail before a GitHub write.
* Interrupted, silently missing, or corrupt uploads must leave a draft. Retry
  must repair that same draft, verify the complete set, and only then publish.
* Existing wrong-source releases/tags must never be changed. Identical published
  retries are read-only; changed bytes at the same version must fail read-only.
"""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
HELPER = ROOT / ".pagoda/desktop-release.py"
VERSION = "0.5.28-pagoda.40750000123"
SHA = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
RESULTS = []


class ReleaseJourney(unittest.TestCase):
    def setUp(self):
        # Explicit repo-local scratch, never a RAM-backed system temp directory.
        scratch = ROOT / "target/pagoda-release-fixtures"
        scratch.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=scratch)
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)
        self.state = self.path / "github.json"
        self.state.write_text("{}")
        bindir = self.path / "bin"
        bindir.mkdir()
        (bindir / "gh").symlink_to(Path(__file__).with_name("fake-gh.py"))
        self.env = {**os.environ, "PATH": str(bindir) + os.pathsep + os.environ["PATH"],
                    "FAKE_GH_STATE": str(self.state)}
        self.artifacts = self.path / "artifacts"
        self.artifacts.mkdir()
        self.bundle = self.path / "bundle"
        for name, content in {
            f"deb/Buzz_{VERSION}_amd64.deb": "debian package",
            f"appimage/Buzz_{VERSION}_amd64.AppImage": "linux executable",
            f"appimage/Buzz_{VERSION}_amd64.AppImage.sig": "linux signature",
            f"nsis/Buzz_{VERSION}_x64-setup.exe": "windows executable",
            f"nsis/Buzz_{VERSION}_x64-setup.exe.sig": "windows signature",
        }.items():
            file = self.bundle / name
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text(content)

    def invoke(self, *args, success=True):
        result = subprocess.run([sys.executable, str(HELPER), *args], cwd=ROOT,
                                env=self.env, text=True, capture_output=True)
        self.assertEqual(result.returncode == 0, success, result.stdout + result.stderr)
        RESULTS.append({"case": self.id(), "command": list(args[:1]),
                        "exit": result.returncode, "output": result.stdout + result.stderr})
        return result

    def record(self, platform, success=True):
        return self.invoke("record", "--platform", platform, "--sha", SHA,
                           "--version", VERSION, "--bundle", str(self.bundle),
                           "--output", str(self.artifacts / platform), success=success)

    def both(self):
        self.record("linux-x86_64")
        self.record("windows-x86_64")

    def publish(self, success=True):
        return self.invoke("publish", "--sha", SHA, "--version", VERSION,
                           "--repo", "fixture/buzz", "--artifacts", str(self.artifacts),
                           "--output", str(self.path / "staged"), success=success)

    def assert_no_writes(self):
        self.assertFalse(any(call[0] == "release" for call in json.loads(
            self.state.read_text()).get("calls", [])))

    def test_validation_before_any_publication(self):
        self.record("linux-x86_64")
        self.publish(success=False)
        self.assert_no_writes()
        self.record("windows-x86_64")
        receipt = self.artifacts / "windows-x86_64/receipt.json"
        original = receipt.read_text()
        for field, bad in [("source_sha", "0" * 40), ("version", "0.0.0")]:
            with self.subTest(field=field):
                data = json.loads(original)
                data[field] = bad
                receipt.write_text(json.dumps(data))
                self.publish(success=False)
                self.assert_no_writes()
        receipt.write_text(original)
        asset = next((self.artifacts / "windows-x86_64").glob("*.exe"))
        asset.write_text("tampered")
        self.publish(success=False)
        self.assert_no_writes()

    def test_empty_signature_never_records_success(self):
        next((self.bundle / "nsis").glob("*.sig")).write_text(" \n")
        self.record("windows-x86_64", success=False)
        self.assertFalse((self.artifacts / "windows-x86_64/receipt.json").exists())

    def test_partial_upload_retry_and_immutable_publication(self):
        self.both()
        for failure in ["partial-upload", "missing-upload", "corrupt-upload"]:
            with self.subTest(failure=failure):
                self.state.write_text("{}")
                self.env["FAKE_GH_FAILURE"] = failure
                self.publish(success=False)
                state = json.loads(self.state.read_text())
                self.assertTrue(state["release"]["draft"])
                self.assertNotIn("published_assets", state)
                del self.env["FAKE_GH_FAILURE"]
                self.publish()
                state = json.loads(self.state.read_text())
                self.assertFalse(state["release"]["draft"])
                manifest = json.loads((self.path / "staged/latest.json").read_text())
                self.assertEqual(set(manifest["platforms"]),
                                 {"linux-x86_64", "windows-x86_64"})
                names = {asset["name"] for asset in state["published_assets"]}
                self.assertEqual(names, {file.name for file in (self.path / "staged").iterdir()})
                self.assertIn("latest.json", names)
                state["calls"] = []
                self.state.write_text(json.dumps(state))
                self.publish()
                self.assert_no_writes()
        # A fresh rerun can rebuild different bytes at the same SHA/version.
        # It may not clobber the already published installers or updater.
        next((self.bundle / "nsis").glob("*.exe")).write_text("different rebuild")
        shutil.rmtree(self.artifacts / "windows-x86_64")
        self.record("windows-x86_64")
        self.publish(success=False)
        self.assert_no_writes()

    def test_source_identity_collision_is_read_only(self):
        self.both()
        for state in [
            {"tag_sha": "f" * 40},
            {"release": {"id": 1, "tag_name": "desktop-v" + VERSION, "draft": True,
                         "target_commitish": "f" * 40, "assets": []}},
        ]:
            with self.subTest(state=state):
                self.state.write_text(json.dumps(state))
                self.publish(success=False)
                self.assert_no_writes()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact", type=Path)
    options, remaining = parser.parse_known_args()
    run = unittest.main(argv=[sys.argv[0], *remaining], exit=False)
    if options.artifact:
        options.artifact.parent.mkdir(parents=True, exist_ok=True)
        options.artifact.write_text(json.dumps({"head": SHA, "passed": run.result.wasSuccessful(),
                                               "checks": RESULTS}, indent=2) + "\n")
    raise SystemExit(0 if run.result.wasSuccessful() else 1)
