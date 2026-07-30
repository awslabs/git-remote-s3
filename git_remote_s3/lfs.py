# SPDX-FileCopyrightText: 2023-present Amazon.com, Inc. or its affiliates
#
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright 2026 FullDuplex Media
# Changed: Fixed LFS tmp-path for submodules; added per-remote scoping, bucket-alias, and Access Grants.

import sys
import logging
import json
import subprocess
import boto3
import threading
import os
from typing import Any
from .common import (
    parse_git_url,
    resolve_bucket_alias,
    register_s3_access_grants,
    s3_region_kwargs,
    synthetic_lfs_url,
    BucketAliasError,
)
from .git import validate_ref_name

logger = logging.getLogger(__name__)


def _resolve_git_dir() -> str:
    """Returns the absolute path of the current git directory.

    Returns:
        str: absolute path to the gitdir; resolves submodule gitlink files.
    """
    return subprocess.check_output(["git", "rev-parse", "--absolute-git-dir"], text=True).strip()


def _configure_logging() -> None:
    """Configures the LFS agent's file logger under the resolved gitdir."""
    log_format = "%(asctime)s - %(levelname)s - %(process)d - %(message)s"
    try:
        log_dir = os.path.join(_resolve_git_dir(), "lfs", "tmp")
        os.makedirs(log_dir, exist_ok=True)
        logging.basicConfig(
            level=logging.ERROR,
            format=log_format,
            filename=os.path.join(log_dir, "git-lfs-s3.log"),
        )
    except (subprocess.CalledProcessError, OSError):
        logging.basicConfig(level=logging.ERROR, format=log_format)


class ProgressPercentage:
    def __init__(self, oid: str):
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self.oid = oid

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            progress_event = {
                "event": "progress",
                "oid": self.oid,
                "bytesSoFar": self._seen_so_far,
                "bytesSinceLast": bytes_amount,
            }
            sys.stdout.write(f"{json.dumps(progress_event)}\n")
            sys.stdout.flush()


def write_error_event(*, oid: str, error: str, flush=False):
    err_event = {
        "event": "complete",
        "oid": oid,
        "error": {"code": 2, "message": error},
    }
    sys.stdout.write(f"{json.dumps(err_event)}\n")
    if flush:
        sys.stdout.flush()


class LFSProcess:
    def __init__(self, s3uri: str, remote_name: str | None = None):
        uri_scheme, profile, bucket, prefix = parse_git_url(s3uri)
        if bucket is None or prefix is None:
            logger.error(f"s3 uri {s3uri} is invalid")
            error_event = {"error": {"code": 32, "message": f"s3 uri {s3uri} is invalid"}}
            sys.stdout.write(f"{json.dumps(error_event)}\n")
            sys.stdout.flush()
            return
        try:
            bucket = resolve_bucket_alias(bucket, remote_name)
        except BucketAliasError as e:
            logger.error(str(e))
            error_event = {"error": {"code": 32, "message": str(e)}}
            sys.stdout.write(f"{json.dumps(error_event)}\n")
            sys.stdout.flush()
            return
        self.prefix = prefix
        self.bucket = bucket
        self.profile = profile
        # boto3 resource objects are dynamically typed; there are no first-party stubs.
        self.s3_bucket: Any = None
        sys.stdout.write("{}\n")
        sys.stdout.flush()

    def init_s3_bucket(self):
        if self.s3_bucket is not None:
            return
        session = boto3.Session() if self.profile is None else boto3.Session(profile_name=self.profile)
        s3 = session.resource("s3", **s3_region_kwargs(session, self.bucket))
        # Bucket operations flow through the resource's underlying client, so the
        # plugin is registered there.
        register_s3_access_grants(s3.meta.client, session)
        self.s3_bucket = s3.Bucket(self.bucket)

    def upload(self, event: dict):
        logger.debug("upload")
        try:
            self.init_s3_bucket()
            if list(self.s3_bucket.objects.filter(Prefix=f"{self.prefix}/lfs/{event['oid']}")):
                logger.debug("object already exists")
                sys.stdout.write(f"{json.dumps({'event': 'complete', 'oid': event['oid']})}\n")
                sys.stdout.flush()
                return
            self.s3_bucket.upload_file(
                event["path"],
                f"{self.prefix}/lfs/{event['oid']}",
                Callback=ProgressPercentage(event["oid"]),
            )
            sys.stdout.write(f"{json.dumps({'event': 'complete', 'oid': event['oid']})}\n")
        except Exception as e:
            logger.error(e)
            write_error_event(oid=event["oid"], error=str(e))
        sys.stdout.flush()

    def download(self, event: dict):
        logger.debug("download")
        try:
            self.init_s3_bucket()
            temp_dir = os.path.join(_resolve_git_dir(), "lfs", "tmp")
            os.makedirs(temp_dir, exist_ok=True)
            self.s3_bucket.download_file(
                Key=f"{self.prefix}/lfs/{event['oid']}",
                Filename=f"{temp_dir}/{event['oid']}",
                Callback=ProgressPercentage(event["oid"]),
            )
            done_event = {
                "event": "complete",
                "oid": event["oid"],
                "path": f"{temp_dir}/{event['oid']}",
            }
            sys.stdout.write(f"{json.dumps(done_event)}\n")
        except Exception as e:
            logger.error(e)
            write_error_event(oid=event["oid"], error=str(e))

        sys.stdout.flush()


def _git_config_get(key: str) -> str | None:
    """Returns the current value of a git config key, or None if unset."""
    res = subprocess.run(
        ["git", "config", "--get", key],
        capture_output=True,
    )
    if res.returncode != 0:
        return None
    return res.stdout.decode("utf-8").strip()


def _git_config_set(key: str, value: str) -> None:
    """Sets a git config key to value, replacing any existing values."""
    res = subprocess.run(
        ["git", "config", "--replace-all", key, value],
        stderr=subprocess.PIPE,
    )
    if res.returncode != 0:
        sys.stderr.write(res.stderr.decode("utf-8").strip() + "\n")
        sys.stderr.flush()
        sys.exit(1)


def _git_config_unset_all(key: str) -> None:
    """Unsets all values of a git config key; a missing key is not an error."""
    subprocess.run(
        ["git", "config", "--unset-all", key],
        capture_output=True,
    )


def _list_git_remotes() -> list:
    """Returns the list of configured git remote names (empty on error)."""
    res = subprocess.run(
        ["git", "remote"],
        capture_output=True,
    )
    if res.returncode != 0:
        return []
    return [r for r in res.stdout.decode("utf-8").splitlines() if r.strip()]


def _resolve_s3_remote(remote_name: str) -> tuple:
    """Validates that remote_name exists and points at an S3 URL.

    Returns (bucket, resolved_bucket, prefix). ``bucket`` is the URL's bucket
    component verbatim — a DNS alias stays an alias, so config rendered from it
    survives the underlying bucket being re-pointed. ``resolved_bucket`` is the
    alias-resolved bucket name (equal to ``bucket`` when not aliased); resolving
    here makes a broken alias fail at install time rather than at first
    transfer. Exits 1 with a clear error message otherwise.
    """
    res = subprocess.run(
        ["git", "remote", "get-url", remote_name],
        capture_output=True,
    )
    if res.returncode != 0:
        sys.stderr.write(
            f"error: remote '{remote_name}' is not configured. "
            f"Add it first with: "
            f"git remote add {remote_name} s3://<bucket>/<prefix>\n"
        )
        sys.stderr.flush()
        sys.exit(1)
    url = res.stdout.decode("utf-8").strip()
    _, _, bucket, prefix = parse_git_url(url)
    if bucket is None or prefix is None:
        sys.stderr.write(
            f"error: remote '{remote_name}' has URL '{url}', which is not "
            f"an s3:// or s3+zip:// URL. --remote can only scope LFS "
            f"configuration for S3 remotes.\n"
        )
        sys.stderr.flush()
        sys.exit(1)
    try:
        resolved_bucket = resolve_bucket_alias(bucket, remote_name)
    except BucketAliasError as e:
        sys.stderr.write(f"error: {e}\n")
        sys.stderr.flush()
        sys.exit(1)
    return bucket, resolved_bucket, prefix


def install(*, remote_name: str | None = None) -> None:
    """Installs git-lfs-s3 as a custom transfer agent.

    With remote_name=None, writes unscoped configuration that applies to
    every remote in the repo (back-compat). With remote_name set, writes
    per-remote scoped configuration so the agent only fires for that one
    remote — required for coexistence with non-S3 LFS remotes.
    """
    if remote_name is None:
        _install_unscoped()
    else:
        _install_scoped(remote_name)


def _install_unscoped() -> None:
    remotes = _list_git_remotes()
    if len(remotes) > 1:
        sys.stderr.write(
            f"warning: multiple remotes configured ({', '.join(remotes)}); "
            "'git-lfs-s3 install' writes unscoped configuration that "
            "applies to ALL remotes. If any non-S3 remote uses LFS, "
            "push/pull may fail. Use 'git-lfs-s3 install --remote <name>' "
            "to scope to a single S3 remote.\n"
        )
        sys.stderr.flush()
    _git_config_set("lfs.customtransfer.git-lfs-s3.path", "git-lfs-s3")
    _git_config_set("lfs.standalonetransferagent", "git-lfs-s3")
    sys.stdout.write("git-lfs-s3 installed\n")
    sys.stdout.flush()


def _install_scoped(remote_name: str) -> None:
    bucket, resolved_bucket, prefix = _resolve_s3_remote(remote_name)
    lfs_url = synthetic_lfs_url(bucket, prefix)
    # Older installs rendered the resolved bucket name into the synthetic URL;
    # that form is provably ours, so migrate it to the alias form instead of
    # refusing to touch it.
    legacy_lfs_url = synthetic_lfs_url(resolved_bucket, prefix)

    existing_lfsurl = _git_config_get(f"remote.{remote_name}.lfsurl")
    if existing_lfsurl is not None and existing_lfsurl not in (lfs_url, legacy_lfs_url):
        sys.stderr.write(
            f"error: remote.{remote_name}.lfsurl is already set to "
            f"'{existing_lfsurl}'. git-lfs-s3 will not overwrite an "
            f"existing LFS URL. If this was set in error, unset it with:\n"
            f"  git config --unset remote.{remote_name}.lfsurl\n"
        )
        sys.stderr.flush()
        sys.exit(1)

    if _git_config_get("lfs.standalonetransferagent") is not None:
        sys.stderr.write(
            "warning: lfs.standalonetransferagent is set unscoped; this "
            "applies git-lfs-s3 to ALL remotes and will defeat per-remote "
            "scoping. Unset it with:\n"
            "  git config --unset lfs.standalonetransferagent\n"
        )
        sys.stderr.flush()

    _git_config_set("lfs.customtransfer.git-lfs-s3.path", "git-lfs-s3")
    _git_config_set(f"remote.{remote_name}.lfsurl", lfs_url)
    _git_config_set(f"lfs.{lfs_url}.standalonetransferagent", "git-lfs-s3")
    if existing_lfsurl == legacy_lfs_url and legacy_lfs_url != lfs_url:
        _git_config_unset_all(f"lfs.{legacy_lfs_url}.standalonetransferagent")
        sys.stdout.write(f"migrated remote '{remote_name}' LFS config from resolved bucket '{resolved_bucket}'\n")
    sys.stdout.write(f"git-lfs-s3 installed for remote '{remote_name}' (LFS alias: {lfs_url})\n")
    sys.stdout.flush()


def main():  # noqa: C901
    _configure_logging()
    if len(sys.argv) > 1:
        if sys.argv[1] == "install":
            remote_name: str | None = None
            args = sys.argv[2:]
            i = 0
            while i < len(args):
                if args[i] == "--remote":
                    if i + 1 >= len(args):
                        sys.stderr.write("error: --remote requires a value\n")
                        sys.stderr.flush()
                        sys.exit(2)
                    remote_name = args[i + 1]
                    i += 2
                else:
                    sys.stderr.write(f"error: unknown argument to install: {args[i]}\n")
                    sys.stderr.flush()
                    sys.exit(2)
            install(remote_name=remote_name)
            sys.exit(0)
        elif sys.argv[1] == "debug":
            logger.setLevel(logging.DEBUG)
        elif sys.argv[1] == "enable-debug":
            subprocess.run(
                [
                    "git",
                    "config",
                    "--add",
                    "lfs.customtransfer.git-lfs-s3.args",
                    "debug",
                ]
            )
            print("debug enabled")
            sys.exit(0)
        elif sys.argv[1] == "disable-debug":
            subprocess.run(["git", "config", "--unset", "lfs.customtransfer.git-lfs-s3.args"])
            print("debug disabled")
            sys.exit(0)
        else:
            print(f"unknown command {sys.argv[1]}")
            sys.exit(1)

    lfs_process = None
    while True:
        logger.debug("git-lfs-s3 starting")
        line = sys.stdin.readline()
        logger.debug(line)
        event = json.loads(line)
        if event["event"] == "init":
            # This is just another precaution but not strictly necessary since git would
            # already have validated the origin name
            if not validate_ref_name(event["remote"]):
                logger.error(f"invalid ref {event['remote']}")
                sys.stdout.write("{}\n")
                sys.stdout.flush()
                sys.exit(1)
            result = subprocess.run(
                ["git", "remote", "get-url", event["remote"]],
                capture_output=True,
            )
            if result.returncode != 0:
                logger.error(result.stderr.decode("utf-8").strip())
                error_event = {
                    "error": {
                        "code": 2,
                        "message": f'cannot resolve remote "{event["remote"]}"',
                    }
                }
                sys.stdout.write(f"{json.dumps(error_event)}")
                sys.stdout.flush()
                sys.exit(1)
            s3uri = result.stdout.decode("utf-8").strip()
            lfs_process = LFSProcess(s3uri=s3uri, remote_name=event["remote"])

        elif event["event"] == "upload":
            assert lfs_process is not None  # git always sends "init" before any transfer event
            lfs_process.upload(event)
        elif event["event"] == "download":
            assert lfs_process is not None  # git always sends "init" before any transfer event
            lfs_process.download(event)
