# SPDX-FileCopyrightText: 2023-present Amazon.com, Inc. or its affiliates
#
# SPDX-License-Identifier: Apache-2.0

import sys
import logging
import json
import subprocess
import boto3
import threading
import os
from .common import parse_git_url
from .git import validate_ref_name

if "lfs" in __name__:
    logging.basicConfig(
        level=logging.ERROR,
        format="%(asctime)s - %(levelname)s - %(process)d - %(message)s",
        filename=".git/lfs/tmp/git-lfs-s3.log",
    )

logger = logging.getLogger(__name__)


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
    def __init__(self, s3uri: str):
        uri_scheme, profile, bucket, prefix = parse_git_url(s3uri)
        if bucket is None or prefix is None:
            logger.error(f"s3 uri {s3uri} is invalid")
            error_event = {
                "error": {"code": 32, "message": f"s3 uri {s3uri} is invalid"}
            }
            sys.stdout.write(f"{json.dumps(error_event)}\n")
            sys.stdout.flush()
            return
        self.prefix = prefix
        self.bucket = bucket
        self.profile = profile
        self.s3_bucket = None
        sys.stdout.write("{}\n")
        sys.stdout.flush()

    def init_s3_bucket(self):
        if self.s3_bucket is not None:
            return
        if self.profile is None:
            session = boto3.Session()
        else:
            session = boto3.Session(profile_name=self.profile)
        s3 = session.resource("s3")
        self.s3_bucket = s3.Bucket(self.bucket)

    def upload(self, event: dict):
        logger.debug("upload")
        try:
            self.init_s3_bucket()
            if list(
                self.s3_bucket.objects.filter(
                    Prefix=f"{self.prefix}/lfs/{event['oid']}"
                )
            ):
                logger.debug("object already exists")
                sys.stdout.write(
                    f"{json.dumps({'event': 'complete', 'oid': event['oid']})}\n"
                )
                sys.stdout.flush()
                return
            self.s3_bucket.upload_file(
                event["path"],
                f"{self.prefix}/lfs/{event['oid']}",
                Callback=ProgressPercentage(event["oid"]),
            )
            sys.stdout.write(
                f"{json.dumps({'event': 'complete', 'oid': event['oid']})}\n"
            )
        except Exception as e:
            logger.error(e)
            write_error_event(oid=event["oid"], error=str(e))
        sys.stdout.flush()

    def download(self, event: dict):
        logger.debug("download")
        try:
            self.init_s3_bucket()
            temp_dir = os.path.abspath(".git/lfs/tmp")
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


def install():
    result = subprocess.run(
        ["git", "config", "--add", "lfs.customtransfer.git-lfs-s3.path", "git-lfs-s3"],
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        sys.stderr.write(result.stderr.decode("utf-8").strip())
        sys.stderr.flush()
        sys.exit(1)
    result = subprocess.run(
        ["git", "config", "--add", "lfs.standalonetransferagent", "git-lfs-s3"],
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        sys.stderr.write(result.stderr.decode("utf-8").strip())
        sys.stderr.flush()
        sys.exit(1)

    sys.stdout.write("git-lfs-s3 installed\n")
    sys.stdout.flush()

def _lfs_only_url(remote: str) -> str | None:
    """
    Try to get LFS-specific URL (for hybrid setups like GitHub + S3).
    Priority: .lfsconfig (tracked) > .git/config (local)
    
    Args:
        remote: The remote name to look up
        
    Returns:
        The LFS URL if found, None otherwise
    """
    # 1. Try .lfsconfig first (tracked file, shared with team)
    result = subprocess.run(
        ["git", "config", "--file", ".lfsconfig", "--get", f"remote.{remote}.lfsurl"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode == 0 and result.stdout.strip():
        s3uri = result.stdout.decode("utf-8").strip()
        logger.debug(f"Using lfsurl from .lfsconfig: {s3uri}")
        return s3uri
    
    # 2. Try local .git/config (allows per-developer override)
    result = subprocess.run(
        ["git", "config", "--get", f"remote.{remote}.lfsurl"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode == 0 and result.stdout.strip():
        s3uri = result.stdout.decode("utf-8").strip()
        logger.debug(f"Using lfsurl from .git/config: {s3uri}")
        return s3uri
    
    return None


def main():  # noqa: C901
    if len(sys.argv) > 1:
        if "install" == sys.argv[1]:
            install()
            sys.exit(0)
        elif "debug" == sys.argv[1]:
            logger.setLevel(logging.DEBUG)
        elif "enable-debug" == sys.argv[1]:
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
        elif "disable-debug" == sys.argv[1]:
            subprocess.run(
                ["git", "config", "--unset", "lfs.customtransfer.git-lfs-s3.args"]
            )
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
            # Try to get LFS-specific URL (for hybrid setups like GitHub + S3)
            # Priority: .lfsconfig (tracked) > .git/config (local) > git remote url (fallback)
            s3uri = _lfs_only_url(event["remote"])
            if s3uri is None:
                # 3. Fall back to regular remote URL
                result = subprocess.run(
                    ["git", "remote", "get-url", event["remote"]],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                if result.returncode != 0:
                    logger.error(result.stderr.decode("utf-8").strip())
                    error_event = {
                        "error": {
                            "code": 2,
                            "message": f"cannot resolve remote \"{event['remote']}\"",
                        }
                    }
                    sys.stdout.write(f"{json.dumps(error_event)}")
                    sys.stdout.flush()
                    sys.exit(1)
                s3uri = result.stdout.decode("utf-8").strip()
                logger.debug(f"Using remote URL: {s3uri}")
            lfs_process = LFSProcess(s3uri=s3uri)

        elif event["event"] == "upload":
            lfs_process.upload(event)
        elif event["event"] == "download":
            lfs_process.download(event)
