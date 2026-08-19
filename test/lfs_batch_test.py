# SPDX-FileCopyrightText: 2023-present Amazon.com, Inc. or its affiliates
#
# SPDX-License-Identifier: Apache-2.0

import json
import os
import shutil
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from git_remote_s3 import lfs

pytestmark = pytest.mark.skipif(
    shutil.which("git") is None or shutil.which("git-lfs") is None,
    reason="requires the git and git-lfs binaries",
)

# Minimal stand-in for the real transfer agent: records that git-lfs selected it
# and completes every upload without touching S3.
_AGENT_STUB = """#!{python}
import json
import os
import sys

with open(os.environ["LFS_AGENT_MARKER"], "a") as marker:
    marker.write("invoked\\n")

for line in sys.stdin:
    if not line.strip():
        continue
    event = json.loads(line)
    if event["event"] == "terminate":
        break
    if event["event"] == "init":
        sys.stdout.write("{{}}\\n")
    else:
        sys.stdout.write(json.dumps({{"event": "complete", "oid": event["oid"]}}) + "\\n")
    sys.stdout.flush()
"""

# Stand-in for the git remote helper, so that the `git ls-remote` git-lfs runs
# against the s3 remote resolves offline.
_REMOTE_HELPER_STUB = """#!{python}
import sys

for line in sys.stdin:
    command = line.strip()
    if command == "capabilities":
        sys.stdout.write("fetch\\npush\\n\\n")
    elif command.startswith("list"):
        sys.stdout.write("\\n")
    else:
        break
    sys.stdout.flush()
"""


class _BatchHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length)
        if not self.path.endswith("/objects/batch"):
            self._respond(404, b"")
            return
        body = json.loads(raw)
        self.server.batch_requests.append(body)
        objects = [
            {
                "oid": obj["oid"],
                "size": obj["size"],
                "authenticated": True,
                "actions": {
                    "upload": {
                        "href": f"{_base_url(self.server)}/upload/{obj['oid']}",
                        "header": {},
                    }
                },
            }
            for obj in body.get("objects", [])
        ]
        payload = json.dumps({"transfer": "basic", "objects": objects}).encode()
        self._respond(200, payload, content_type="application/vnd.git-lfs+json")

    def do_PUT(self):
        length = int(self.headers.get("Content-Length") or 0)
        self.rfile.read(length)
        self.server.uploads.append(self.path)
        self._respond(200, b"")

    def do_GET(self):
        self._respond(404, b"")

    def _respond(self, status, payload, content_type="application/json"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if payload:
            self.wfile.write(payload)

    def log_message(self, *args):
        pass


def _base_url(server):
    host, port = server.server_address[0], server.server_address[1]
    return f"http://{host}:{port}"


def _lfs_url(server):
    return f"{_base_url(server)}/repo.git/info/lfs"


def _git(args, cwd, env=None):
    res = subprocess.run(
        args,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=60,
    )
    assert res.returncode == 0, res.stderr.decode()
    return res.stdout.decode().strip()


def _write_stub(path, source):
    path.write_text(source.format(python=sys.executable))
    path.chmod(0o755)


@pytest.fixture
def batch_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _BatchHandler)
    server.batch_requests = []
    server.uploads = []
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()
    server.server_close()
    thread.join(timeout=10)


@pytest.fixture
def push_env(tmp_path):
    """Environment for the push subprocesses: stub agent and remote helper on PATH."""
    stub_bin = tmp_path / "bin"
    stub_bin.mkdir()
    _write_stub(stub_bin / "git-lfs-s3", _AGENT_STUB)
    _write_stub(stub_bin / "git-remote-s3", _REMOTE_HELPER_STUB)

    env = os.environ.copy()
    env["PATH"] = os.pathsep.join([str(stub_bin), env["PATH"]])
    env["HOME"] = str(tmp_path)
    env["GIT_TERMINAL_PROMPT"] = "0"
    env["LFS_AGENT_MARKER"] = str(tmp_path / "agent-invoked")
    return env


@pytest.fixture
def lfs_repo(tmp_path, monkeypatch, batch_server):
    """A repo holding one LFS object, with an s3 remote configured by
    'install --remote' and a plain HTTP remote pointing at batch_server."""
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(["git", "init", "-q", "-b", "main", str(repo)], cwd=repo)
    _git(["git", "config", "user.email", "test@example.com"], cwd=repo)
    _git(["git", "config", "user.name", "Test"], cwd=repo)
    _git(["git", "lfs", "install", "--local"], cwd=repo)
    _git(["git", "lfs", "track", "*.bin"], cwd=repo)
    (repo / "big.bin").write_bytes(b"0" * 4096)
    _git(["git", "add", ".gitattributes", "big.bin"], cwd=repo)
    _git(["git", "commit", "-q", "-m", "add lfs object"], cwd=repo)

    _git(["git", "remote", "add", "s3", "s3://bucket/repo"], cwd=repo)
    _git(
        ["git", "remote", "add", "web", f"{_base_url(batch_server)}/repo.git"], cwd=repo
    )
    _git(["git", "config", "remote.web.lfsurl", _lfs_url(batch_server)], cwd=repo)
    _git(
        ["git", "config", f"lfs.{_lfs_url(batch_server)}.locksverify", "false"],
        cwd=repo,
    )

    monkeypatch.chdir(repo)
    lfs.install(remote_name="s3")
    return repo


def test_batch_request_to_non_s3_remote_advertises_s3_agent(
    lfs_repo, batch_server, push_env
):
    """Pins the current advertisement: lfs.customtransfer.git-lfs-s3.path is
    repo-wide, so git-lfs names git-lfs-s3 in the batch API of a non-S3 remote."""
    _git(["git", "lfs", "push", "web", "main"], cwd=lfs_repo, env=push_env)

    assert len(batch_server.batch_requests) == 1
    assert "git-lfs-s3" in batch_server.batch_requests[0]["transfers"]
    assert len(batch_server.uploads) == 1


def test_basictransfersonly_omits_transfers_from_non_s3_batch_request(
    lfs_repo, batch_server, push_env
):
    """Pins the documented workaround: with lfs.basictransfersonly set, git-lfs
    omits the transfers field entirely and the push still succeeds."""
    _git(["git", "config", "lfs.basictransfersonly", "true"], cwd=lfs_repo)

    _git(["git", "lfs", "push", "web", "main"], cwd=lfs_repo, env=push_env)

    assert len(batch_server.batch_requests) == 1
    assert "transfers" not in batch_server.batch_requests[0]
    assert len(batch_server.uploads) == 1


def test_reserved_chars_in_prefix_still_select_the_standalone_agent(
    lfs_repo, batch_server, push_env, tmp_path
):
    """git-lfs matches the scoped standalonetransferagent key against the endpoint
    resolved from lfsurl, so both must carry the identical encoded URL."""
    _git(
        ["git", "remote", "add", "nasty", "s3://bucket/deep dir/repo%zz"], cwd=lfs_repo
    )
    lfs.install(remote_name="nasty")

    _git(["git", "lfs", "push", "nasty", "main"], cwd=lfs_repo, env=push_env)

    assert (tmp_path / "agent-invoked").exists()
    assert batch_server.batch_requests == []


def test_basictransfersonly_keeps_s3_remote_on_standalone_agent(
    lfs_repo, batch_server, push_env, tmp_path
):
    """The workaround does not disable the scoped agent: the s3 remote still
    transfers through git-lfs-s3 and never reaches a batch endpoint."""
    _git(["git", "config", "lfs.basictransfersonly", "true"], cwd=lfs_repo)

    _git(["git", "lfs", "push", "s3", "main"], cwd=lfs_repo, env=push_env)

    assert (tmp_path / "agent-invoked").exists()
    assert batch_server.batch_requests == []
