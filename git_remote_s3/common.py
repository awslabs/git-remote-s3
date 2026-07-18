# SPDX-FileCopyrightText: 2023-present Amazon.com, Inc. or its affiliates
#
# SPDX-License-Identifier: Apache-2.0

import functools
import re
import subprocess
from typing import Optional

import dns.exception
import dns.resolver

from .enums import UriScheme


def parse_git_url(url: str) -> tuple[UriScheme, str, str, str]:
    """Parses the elements in a s3:// remote origin URI

    Args:
        url (str): the URI to parse

    Returns:
        tuple[str, str, str, str]: uri scheme, prefix, bucket and profile extracted
        from the URI or None, None, None, None if the URI is invalid
    """
    if url is None:
        return None, None, None, None
    m = re.match(r"(s3|s3\+zip)://([^@]+@)?([a-z0-9][a-z0-9\.-]{2,62})/?(.+)?", url)
    if m is None or len(m.groups()) != 4:
        return None, None, None, None
    uri_scheme, profile, bucket, prefix = m.groups()
    if profile is not None:
        profile = profile[:-1]
    if prefix is not None:
        prefix = prefix.strip("/")
    if uri_scheme is not None:
        if uri_scheme == "s3":
            uri_scheme = UriScheme.S3
        if uri_scheme == "s3+zip":
            uri_scheme = UriScheme.S3_ZIP

    return uri_scheme, profile, bucket, prefix


BUCKET_ALIAS_TXT_PREFIX = "git-bucket="
BUCKET_ALIAS_CONFIG_KEY = "s3.dns-alias"


def _bucket_alias_opt_out_key(remote_name: Optional[str]) -> str:
    """Returns the git config key to disable aliasing for this remote.

    Falls back to the global key when the remote name is unknown or is a
    URL rather than a configured remote name.
    """
    if remote_name is not None and "://" not in remote_name:
        return f"remote.{remote_name}.s3-dns-alias"
    return BUCKET_ALIAS_CONFIG_KEY


class BucketAliasError(Exception):
    def __init__(self, host: str, reason: str, remote_name: Optional[str] = None):
        self.host = host
        self.reason = reason
        self.remote_name = remote_name
        super().__init__(
            f"cannot resolve bucket alias '{host}': {reason}. "
            f"Expected a DNS TXT record at '{host}' containing exactly one "
            f"value of the form '{BUCKET_ALIAS_TXT_PREFIX}<real-bucket-name>'. "
            f"To treat '{host}' as a literal bucket name instead, run: "
            f"git config {_bucket_alias_opt_out_key(remote_name)} false"
        )


@functools.lru_cache(maxsize=None)
def _git_config_bool(key: str) -> Optional[bool]:
    """Returns the boolean value of a git config key, or None if unset."""
    res = subprocess.run(
        ["git", "config", "--type=bool", "--get", key],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if res.returncode != 0:
        return None
    value = res.stdout.decode("utf-8").strip()
    if value == "true":
        return True
    if value == "false":
        return False
    return None


def bucket_alias_enabled(remote_name: Optional[str] = None) -> bool:
    """Returns whether DNS bucket alias resolution is enabled (default: True).

    Checks ``remote.<remote_name>.s3-dns-alias`` first when a remote name is
    known (skipped when remote_name is a URL rather than a configured remote
    name), then falls back to the global ``s3.dns-alias`` key.
    """
    if remote_name is not None and "://" not in remote_name:
        enabled = _git_config_bool(f"remote.{remote_name}.s3-dns-alias")
        if enabled is not None:
            return enabled
    enabled = _git_config_bool(BUCKET_ALIAS_CONFIG_KEY)
    if enabled is not None:
        return enabled
    return True


@functools.lru_cache(maxsize=None)
def resolve_bucket_alias(bucket: str, remote_name: Optional[str] = None) -> str:
    """Resolves a DNS-aliased bucket name to the real S3 bucket name.

    A bucket component containing at least one dot is treated as a DNS
    hostname (bucket names in this deployment never contain dots) and is
    resolved via a TXT lookup using the system resolver: the record at the
    hostname must contain exactly one value of the form
    ``git-bucket=<real-bucket-name>``. Results are cached per process.

    Resolution can be disabled per remote via the git config key
    ``remote.<remote_name>.s3-dns-alias`` (boolean, checked when a remote
    name is known) or globally via ``s3.dns-alias``; when disabled the
    bucket component is returned unchanged.

    Args:
        bucket (str): the bucket component parsed from the remote URI
        remote_name (str): the git remote name, when known; used to check
            the per-remote opt-out key and to build error messages

    Returns:
        str: the real bucket name, or bucket unchanged if it contains no
        dot or alias resolution is disabled via git config

    Raises:
        BucketAliasError: if the alias cannot be resolved to a bucket name
    """
    if bucket is None or "." not in bucket:
        return bucket
    if not bucket_alias_enabled(remote_name):
        return bucket
    try:
        answers = dns.resolver.resolve(bucket, "TXT")
    except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer):
        raise BucketAliasError(bucket, "no TXT record found", remote_name)
    except dns.exception.DNSException as e:
        raise BucketAliasError(bucket, f"DNS TXT lookup failed ({e})", remote_name)
    values = [
        txt.removeprefix(BUCKET_ALIAS_TXT_PREFIX)
        for txt in (b"".join(rdata.strings).decode("utf-8") for rdata in answers)
        if txt.startswith(BUCKET_ALIAS_TXT_PREFIX)
    ]
    if len(values) == 0:
        raise BucketAliasError(
            bucket,
            f"no '{BUCKET_ALIAS_TXT_PREFIX}' value found in TXT record",
            remote_name,
        )
    if len(values) > 1:
        raise BucketAliasError(
            bucket,
            f"found {len(values)} '{BUCKET_ALIAS_TXT_PREFIX}' values in TXT "
            f"record, expected exactly one",
            remote_name,
        )
    return values[0]


LFS_ALIAS_HOST = "lfs-alias.git-remote-s3.test"


def synthetic_lfs_url(bucket: str, prefix: str) -> str:
    """Builds the synthetic LFS endpoint URL for a given bucket and prefix.

    The URL is never contacted; it is purely a stable match key so that
    ``lfs.<url>.standalonetransferagent`` can be scoped per remote, and so
    git-lfs's HTTPS-shaped endpoint resolution short-circuits its SSH-style
    discovery for ``s3://`` URLs. The hostname uses the RFC 6761-reserved
    ``.test`` TLD to guarantee non-collision with any real host.
    """
    return f"https://{LFS_ALIAS_HOST}/{bucket}/{prefix}"
