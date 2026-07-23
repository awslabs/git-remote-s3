# SPDX-FileCopyrightText: 2023-present Amazon.com, Inc. or its affiliates
#
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright 2026 FullDuplex Media
# Changed: Added doctor Access Grants diagnostics, bucket-alias resolution, and scoped list prefixes.

import boto3
from .remote import parse_git_url, DEFAULT_LOCK_TTL_SECONDS
from .common import (
    resolve_bucket_alias,
    register_s3_access_grants,
    register_s3_access_grants_strict,
    s3_region_kwargs,
    scoped_list_prefix,
    BucketAliasError,
)
import argparse
import sys
import uuid
from typing import Any
from botocore.exceptions import (
    ClientError,
    ProfileNotFound,
    CredentialRetrievalError,
    NoCredentialsError,
    UnknownCredentialError,
)
from .git import get_remote_url, GitError
import datetime


_ACCESS_GRANTS_HINTS = {
    "GetAccessGrantsInstanceForPrefix": ("caller role is missing s3:GetAccessGrantsInstanceForPrefix"),
    "GetDataAccess": ("caller role is missing s3:GetDataAccess or has no matching grant"),
}


def _access_grants_hint(operation, code):
    """Maps an (operation, error code) to an actionable IAM hint, or None."""
    if code != "AccessDenied":
        return None
    return _ACCESS_GRANTS_HINTS.get(operation)


class Doctor:
    def __init__(
        self,
        profile,
        bucket,
        prefix,
        delete_bundle,
        lock_ttl_seconds=60,
        delete_stale_locks=False,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix
        self.delete_bundle = delete_bundle
        self.profile = profile
        self.session = boto3.Session(profile_name=profile)
        self.s3 = register_s3_access_grants(
            self.session.client("s3", **s3_region_kwargs(self.session, bucket)),
            self.session,
        )
        self.lock_ttl_seconds = lock_ttl_seconds
        self.delete_stale_locks = delete_stale_locks

    def run(self):
        self.check_access_grants()
        repos = self.analyze_repo()
        for r in repos:
            print(f"{r}:")
            head_ref = "Invalid"
            for ref in repos[r]["refs"]:
                if repos[r]["HEAD"] == ref:
                    head_ref = ref
                ref_value = repos[r]["refs"][ref]
                part_1 = "*" if ref_value["protected"] else ""
                part_2 = "Ok" if len(ref_value["bundles"]) == 1 else "Multiple refs"
                print(f" {part_1} {ref}: {part_2}")
            if head_ref == "Invalid":
                repos[r]["HEAD"] = head_ref
            print(f"  HEAD: {head_ref}")

        self.fix_issues(repos)

    def check_access_grants(self):
        """Diagnoses the S3 Access Grants path and names any missing permission.

        The production S3 client registers the plugin with fallback enabled, so a
        broken Access Grants setup silently drops to direct credentials. This probe
        registers a SEPARATE, fallback-disabled plugin and drives the full vend
        path (including the ``GetAccessGrantsInstanceForPrefix`` preflight) against
        the repo prefix, surfacing the real error rather than masking it.

        This is informational: an IAM-access-key caller with no grant will
        legitimately report "not available" and keep using direct credentials.
        """
        print("\nChecking S3 Access Grants entitlement...")
        probe = register_s3_access_grants_strict(
            self.session.client("s3", **s3_region_kwargs(self.session, self.bucket)),
            self.session,
        )
        # Scoped to exactly this repo, matching the S3Remote listing path (see
        # scoped_list_prefix): a bare prefix would also match a sibling repo.
        scoped_prefix = scoped_list_prefix(self.prefix)
        try:
            probe.list_objects_v2(Bucket=self.bucket, Prefix=scoped_prefix)
        except probe.exceptions.ClientError as x:
            operation = getattr(x, "operation_name", None) or "S3 request"
            error = x.response.get("Error", {})
            code = error.get("Code", "Unknown")
            message = error.get("Message", "")
            hint = _access_grants_hint(operation, code)
            if code == "AccessDenied":
                print(" Access Grants: not available (using direct S3 credentials)")
            else:
                print(" Access Grants: ERROR")
            print(f"  {operation} failed: {code}")
            if hint:
                print(f"  hint: {hint}")
            elif message:
                print(f"  {message}")
        except Exception as x:  # a failed diagnostic must not abort the doctor run
            print(f" Access Grants: could not be checked ({x})")
        else:
            print(f" Access Grants: OK (vended credentials for s3://{self.bucket}/{scoped_prefix})")

    def fix_issues(self, repos):
        for r in repos:
            for ref in repos[r]["refs"]:
                if len(repos[r]["refs"][ref]["bundles"]) > 1:
                    self.fix_multiple_bundles(repos, r, ref)

            if repos[r]["HEAD"] == "Invalid":
                self.fix_head(repos, r)

        # After fixing references, scan and handle stale locks
        self.list_and_handle_stale_locks()

    def list_and_handle_stale_locks(self):
        print("\nScanning for stale locks...")
        objs = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=scoped_list_prefix(self.prefix)).get("Contents", [])

        now = datetime.datetime.now(tz=datetime.timezone.utc)
        stale = []
        for o in objs:
            key = o["Key"]
            if key.endswith(".lock"):
                last_modified = o.get("LastModified")
                if last_modified is not None:
                    age = (now - last_modified).total_seconds()
                    if age > self.lock_ttl_seconds:
                        stale.append((key, int(age)))

        if not stale:
            print("No stale locks found.")
            return

        print("Found stale locks:")
        for key, age in stale:
            print(f" - {key} (age: {age}s)")

        if self.delete_stale_locks:
            print("\nDeleting stale locks...")
            for key, _ in stale:
                try:
                    self.s3.delete_object(Bucket=self.bucket, Key=key)
                    print(f"Deleted {key}")
                except ClientError as e:
                    print(f"Failed to delete {key}: {e}")
        else:
            print("\nRun with --delete-stale-locks to remove them automatically.")

    def analyze_repo(self):
        objs = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=scoped_list_prefix(self.prefix)).get("Contents", [])

        repos: dict[str, Any] = {}
        for o in objs:
            key = o["Key"]
            key_parts = key.split("/")
            repo_name = key_parts[0]
            if repo_name not in repos:
                repos[repo_name] = {"refs": {}, "HEAD": "Missing"}
            refs = "/".join(key_parts[1:-1])
            if key_parts[1] == "HEAD":
                head_ref = self.s3.get_object(Bucket=self.bucket, Key=key).get("Body").read().decode("utf-8").strip()
                repos[repo_name]["HEAD"] = head_ref
                continue
            if not repos[repo_name]["refs"].get(refs, None):
                repos[repo_name]["refs"][refs] = {"protected": False, "bundles": []}
            if key_parts[-1] == "PROTECTED#":
                repos[repo_name]["refs"][refs]["protected"] = True
            else:
                sha = key_parts[-1].split(".")[0]
                repos[repo_name]["refs"][refs]["bundles"].append({"sha": sha, "lastModified": o["LastModified"]})
        return repos

    def fix_multiple_bundles(self, repos: dict, r: str, ref: str) -> None:
        print(f"\nFix multiple bundles for repo {r} and ref {ref}")
        bundles = repos[r]["refs"][ref]["bundles"]
        for i, sha in enumerate(bundles):
            print(f"{i + 1}. {sha['sha']} {sha['lastModified']}")
        while True:
            try:
                i = int(input("Enter the number of the bundle to keep: "))
                if i > 0 and i <= len(bundles):
                    sha = bundles[i - 1]["sha"]
                    print(f"Keeping {sha}")
                    input("Press enter to confirm or Ctrl+C to cancel")
                    for s in [sha["sha"] for sha in bundles]:
                        if s != sha:
                            if self.delete_bundle:
                                print(f"Removing {s}")
                                self.s3.delete_object(
                                    Bucket=self.bucket,
                                    Key=f"{self.prefix}/{ref}/{s}.bundle",
                                )
                            else:
                                tmp_branch = f"{ref}_{str(uuid.uuid4())[:8]}"
                                print(f"Moving {s} to new branch {tmp_branch}")
                                self.s3.copy_object(
                                    CopySource={
                                        "Bucket": self.bucket,
                                        "Key": f"{self.prefix}/{ref}/{s}.bundle",
                                    },
                                    Bucket=self.bucket,
                                    Key=f"{self.prefix}/{tmp_branch}/{s}.bundle",
                                )
                                self.s3.delete_object(
                                    Bucket=self.bucket,
                                    Key=f"{self.prefix}/{ref}/{s}.bundle",
                                )
                    break
            except ValueError:
                print("Invalid input")

    def fix_head(self, repos: dict, r: str) -> None:
        print(f"\nFix invalid HEAD for repo {r}")
        heads = [k for k in repos[r]["refs"] if "heads" in k]
        for i, head in enumerate(heads):
            print(f"{i + 1}. {head.split('/')[-1]}")
        while True:
            try:
                i = int(input("Enter the number of the branch to use as head: "))
                if i > 0 and i <= len(heads):
                    head = heads[i - 1]
                    print(f"Setting {head} as HEAD")
                    self.s3.put_object(
                        Bucket=self.bucket,
                        Key=f"{self.prefix}/HEAD",
                        Body=head,
                    )
                    break
            except ValueError:
                print("Invalid input")


class ManageBranch:
    def __init__(self, profile, bucket, prefix, branch) -> None:
        self.bucket = bucket
        self.prefix = prefix
        session = boto3.Session(profile_name=profile)
        self.s3 = register_s3_access_grants(session.client("s3", **s3_region_kwargs(session, bucket)), session)
        self.branch = branch
        if not self.get_branch_content():
            raise ValueError(f"Branch {self.branch} does not exist")

    def process_cmd(self, cmd):
        if cmd == "delete-branch":
            self.delete_branch()
        if cmd == "protect":
            self.protect_branch()
        if cmd == "unprotect":
            self.unprotect_branch()

    def delete_branch(self):
        objs = self.get_branch_content()
        resp = input(f"Delete {self.branch} branch [yes/no]: ")
        if resp.lower() == "yes":
            for o in objs:
                self.s3.delete_object(Bucket=self.bucket, Key=o["Key"])
            print(f"Branch {self.branch} has been deleted")
        else:
            print("Aborted")

    def get_branch_content(self) -> list[dict]:
        objs = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=f"{self.prefix}/refs/heads/{self.branch}/").get(
            "Contents", []
        )
        return objs

    def protect_branch(self):
        self.s3.put_object(
            Bucket=self.bucket,
            Key=f"{self.prefix}/refs/heads/{self.branch}/PROTECTED#",
        )
        print(f"Branch {self.branch} is now protected")

    def unprotect_branch(self):
        self.s3.delete_object(
            Bucket=self.bucket,
            Key=f"{self.prefix}/refs/heads/{self.branch}/PROTECTED#",
        )
        print(f"Branch {self.branch} is now unprotected")


def main():  # noqa: C901
    parser = argparse.ArgumentParser()
    parser.add_argument("command")
    parser.add_argument("remote", help="The remote s3 uri to analyze, including the AWS profile if used")
    parser.add_argument(
        "-d",
        "--delete-bundle",
        action="store_true",
        help="Delete the bundle instead of creating a new branch",
    )
    parser.add_argument(
        "--lock-ttl",
        type=int,
        default=DEFAULT_LOCK_TTL_SECONDS,
        help=f"Seconds after which a lock is considered stale (default: {DEFAULT_LOCK_TTL_SECONDS})",
    )
    parser.add_argument(
        "--delete-stale-locks",
        action="store_true",
        help="Delete stale lock files found during doctor run",
    )
    # Optional: "doctor" doesn't take a branch; delete-branch/protect/unprotect
    # validate it's present themselves (see the args.branch is None check below).
    parser.add_argument(
        "branch",
        type=str,
        nargs="?",
        default=None,
        help="Branch to delete from the remote",
    )
    args = parser.parse_args()
    remote = args.remote
    try:
        remote_url = get_remote_url(remote)
    except GitError as e:
        sys.stderr.write(f"fatal: {e}\n")
        sys.stderr.flush()
        sys.exit(1)

    uri_scheme, profile, bucket, prefix = parse_git_url(remote_url)
    try:
        bucket = resolve_bucket_alias(bucket)
    except BucketAliasError as e:
        sys.stderr.write(f"fatal: {e}\n")
        sys.stderr.flush()
        sys.exit(1)
    try:
        if args.command == "doctor":
            doctor = Doctor(
                profile,
                bucket,
                prefix,
                args.delete_bundle,
                args.lock_ttl,
                args.delete_stale_locks,
            )
            doctor.run()
        if args.command == "delete-branch" or args.command == "protect" or args.command == "unprotect":
            if args.branch is None:
                sys.stderr.write("fatal: --branch is required\n")
                sys.stderr.flush()
                sys.exit(1)
            try:
                manage_branch = ManageBranch(profile, bucket, prefix, args.branch)
                manage_branch.process_cmd(args.command)
            except ValueError as e:
                sys.stderr.write(f"fatal: {e}\n")
                sys.stderr.flush()
                sys.exit(1)

        sys.exit(0)

    except (
        ClientError,
        ProfileNotFound,
        CredentialRetrievalError,
        NoCredentialsError,
        UnknownCredentialError,
    ) as e:
        sys.stderr.write(f"fatal: invalid credentials {e}\n")
        sys.stderr.flush()
        sys.exit(1)
