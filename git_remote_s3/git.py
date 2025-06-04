# SPDX-FileCopyrightText: 2023-present Amazon.com, Inc. or its affiliates
#
# SPDX-License-Identifier: Apache-2.0

import subprocess
import sys
import re


class GitError(Exception):
    pass


def archive(*, folder: str, ref: str) -> str:
    """Archive the content of the folder into a repo.zip file

    Args:
        folder (str): the folder to archive
        ref (str): the ref to archive

    Returns:
        str: the path to the archive file
    """

    file_path = f"{folder}/repo.zip"
    result = subprocess.run(
        ["git", "archive", "--format", "zip", "--output", file_path, ref],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )

    if result.returncode == 0:
        return file_path
    else:
        raise GitError(result.stderr.decode("utf8"))


def bundle(*, folder: str, sha: str, ref: str) -> str:
    """Bundles the content of the folder into a sha.bundle file

    Args:
        folder (str): the folder to bundle
        sha (str): the sha of the bundle. A bundle is stored as sha.bundle
        ref (str): the ref to bundle

    Returns:
        str: the path to the bundle file
    """
    file_path = f"{folder}/{sha}.bundle"
    result = subprocess.run(
        ["git", "bundle", "create", file_path, ref],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )

    if result.returncode == 0:
        return file_path
    else:
        raise GitError(result.stderr.decode("utf8"))


def unbundle(*, folder: str, sha: str, ref: str):
    """Unbundles the content of the bundle referred by the sha

    Args:
        folder (str): the folder where the bundle is located
        sha (str): the sha of the bundle. A bundle is stored as sha.bundle
        ref (str): the ref to checkout after unbundling
    """
    subprocess.run(
        ["git", "bundle", "unbundle", f"{folder}/{sha}.bundle", ref],
        stdout=sys.stderr,
        check=True,
    )


def rev_parse(ref: str) -> str:
    """Gets the sha of a ref

    Args:
        ref (str): the ref to get the sha for

    Raises:
        Exception: if the ref is not found

    Returns:
        str: _description_
    """

    result = subprocess.run(["git", "rev-parse", ref], stdout=subprocess.PIPE)
    if result.returncode != 0:
        raise GitError(f"fatal: {ref} not found")
    sha = result.stdout.decode("utf8").strip()
    return sha


def is_ancestor(ancestor: str, descendant: str) -> bool:
    """Checks if the ancestor is an ancestor of the descendant

    Args:
        ancestor (str): the ancestor ref
        descendant (str): the descendant ref

    Returns:
        bool: true if the ancestor is an ancestor of the descendant
    """
    result = subprocess.run(
        ["git", "merge-base", "--is-ancestor", ancestor, descendant],
        stderr=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
    )
    return result.returncode == 0


def get_remote_url(remote: str) -> str:
    result = subprocess.run(
        ["git", "remote", "get-url", remote], stdout=subprocess.PIPE
    )
    if result.returncode != 0:
        raise GitError(f"fatal: {remote} not found")
    url = result.stdout.decode("utf8").strip()
    return url


# validate refname according to
# https://github.com/git/git/blob/406f326d271e0bacecdb00425422c5fa3f314930/refs.c#L170
def validate_ref_name(name: str) -> bool:
    return (
        re.search(
            r"(^\.)|(\.\.)|([:\?\[\\\^\~\s\*\]])|(\.lock$)|(/$)|(@\{)|([\x00-\x1f])",
            name,
        )
        is None
    )


def get_last_commit_message() -> str:
    result = subprocess.run(
        ["git", "log", "-1", "--pretty=%h %s"], stdout=subprocess.PIPE
    )
    if result.returncode != 0:
        raise GitError(f"fatal: an error as occurred")
    message = result.stdout.decode("utf8").strip()
    return message


def unbundle_stream(stream, *, ref: str, chunk_size: int = 1024 * 1024):
    """Pipe the bundle bytes from *stream* directly to `git bundle unbundle -`.

    Args:
        stream: File-like object (e.g. botocore StreamingBody) yielding bytes.
        ref:    The reference that `git bundle unbundle` should update.
        chunk_size: Size of chunks read from *stream*; defaults to 1 MiB.

    Raises:
        GitError: if the underlying git command exits with non-zero status.
    """

    import subprocess
    import sys

    proc = subprocess.Popen(
        ["git", "bundle", "unbundle", "-", ref],
        stdin=subprocess.PIPE,
        stdout=sys.stderr,
    )

    try:
        if hasattr(stream, "iter_chunks"):
            for chunk in stream.iter_chunks(chunk_size):
                if not chunk:
                    break
                proc.stdin.write(chunk)
        else:
            while True:
                chunk = stream.read(chunk_size)
                if not chunk:
                    break
                proc.stdin.write(chunk)
    finally:
        try:
            proc.stdin.close()
        except Exception:
            pass

    proc.wait()

    if proc.returncode != 0:
        raise GitError(f"git bundle unbundle failed (exit code {proc.returncode})")
