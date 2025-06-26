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
import math
from .common import parse_git_url
from .git import validate_ref_name

# Constants for multipart upload configuration
DEFAULT_PART_SIZE = 100 * 1024 * 1024  # 100MB
MULTIPART_UPLOAD_THRESHOLD = 2 * 1024 * 1024 * 1024  # 2GB

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


class MultipartUploadProgressPercentage:
    """Track and report multipart upload progress for LFS."""
    
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


def get_file_size(file_path):
    """Get the size of a file in bytes."""
    try:
        return os.path.getsize(file_path)
    except (OSError, IOError) as e:
        logger.error(f"Error getting file size: {e}")
        return 0


def should_use_multipart_upload(file_path):
    """Determine if a file should use multipart upload based on its size."""
    file_size = get_file_size(file_path)
    logger.info(f"File size: {file_size} bytes, threshold: {MULTIPART_UPLOAD_THRESHOLD} bytes")
    return file_size > MULTIPART_UPLOAD_THRESHOLD


def multipart_upload_file(s3_client, file_path, bucket, key, callback=None):
    """
    Upload a file to S3 using multipart upload for large files.
    
    Args:
        s3_client: boto3 S3 client
        file_path: Path to the file to upload
        bucket: S3 bucket name
        key: S3 object key
        callback: Optional progress callback function
    
    Returns:
        True if successful, False otherwise
    """
    file_size = get_file_size(file_path)
    
    if file_size == 0:
        logger.error(f"File {file_path} is empty or cannot be read")
        return False
    
    try:
        # Step 1: Initialize multipart upload
        logger.info(f"Starting multipart upload for {file_path} to {bucket}/{key}")
        
        # Create the multipart upload
        mpu = s3_client.create_multipart_upload(
            Bucket=bucket,
            Key=key
        )
        upload_id = mpu['UploadId']
        
        # Step 2: Upload parts
        parts = []
        part_number = 1
        part_size = DEFAULT_PART_SIZE
        
        # Calculate number of parts needed
        total_parts = math.ceil(file_size / part_size)
        logger.info(f"File will be uploaded in {total_parts} parts of {part_size} bytes each")
        
        try:
            with open(file_path, 'rb') as file_data:
                while True:
                    # Read the next chunk from the file
                    data = file_data.read(part_size)
                    if not data:
                        break
                    
                    # Upload the part
                    logger.info(f"Uploading part {part_number}/{total_parts}")
                    part = s3_client.upload_part(
                        Body=data,
                        Bucket=bucket,
                        Key=key,
                        UploadId=upload_id,
                        PartNumber=part_number
                    )
                    
                    # Track progress if callback provided
                    if callback:
                        callback(len(data))
                    
                    # Add the part to our list of parts
                    parts.append({
                        'PartNumber': part_number,
                        'ETag': part['ETag']
                    })
                    
                    part_number += 1
            
            # Step 3: Complete the multipart upload
            logger.info(f"Completing multipart upload for {key}")
            s3_client.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            
            logger.info(f"Multipart upload completed successfully for {key}")
            return True
            
        except Exception as e:
            logger.error(f"Error during multipart upload: {e}")
            # Abort the multipart upload if there was an error
            s3_client.abort_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id
            )
            logger.info(f"Multipart upload aborted for {key}")
            raise
            
    except Exception as e:
        logger.error(f"Failed to upload {file_path} to {bucket}/{key}: {e}")
        return False


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
        self.s3_client = None
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
        self.s3_client = session.client("s3")

    def upload(self, event: dict):
        logger.debug("upload")
        try:
            self.init_s3_bucket()
            
            # Check if object already exists
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
            
            file_path = event["path"]
            s3_key = f"{self.prefix}/lfs/{event['oid']}"
            
            # Check if we should use multipart upload
            if should_use_multipart_upload(file_path):
                logger.info(f"Using multipart upload for large LFS object: {file_path}")
                
                # Create a progress tracker
                progress_callback = MultipartUploadProgressPercentage(
                    oid=event["oid"]
                )
                
                # Perform multipart upload
                success = multipart_upload_file(
                    self.s3_client,
                    file_path,
                    self.bucket,
                    s3_key,
                    callback=progress_callback
                )
                
                if not success:
                    raise Exception("Failed to upload LFS object using multipart upload")
            else:
                # Use standard upload for smaller files
                self.s3_bucket.upload_file(
                    event["path"],
                    s3_key,
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
            lfs_process = LFSProcess(s3uri=s3uri)

        elif event["event"] == "upload":
            lfs_process.upload(event)
        elif event["event"] == "download":
            lfs_process.download(event)
