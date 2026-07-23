# SPDX-FileCopyrightText: 2023-present Amazon.com, Inc. or its affiliates
#
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright 2026 FullDuplex Media
# Changed: Added DNS TXT bucket-alias resolution, bucket-region detection, and S3 Access Grants helpers.

import functools
import re
import subprocess
import sys

import dns.exception
import dns.resolver
from aws_s3_access_grants_boto3_plugin.s3_access_grants_plugin import (
    S3AccessGrantsPlugin,
)

from .enums import UriScheme


def parse_git_url(url: str | None) -> tuple[UriScheme | None, str | None, str | None, str | None]:
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
    # The regex constrains group 1 to exactly "s3" or "s3+zip".
    scheme = UriScheme.S3 if uri_scheme == "s3" else UriScheme.S3_ZIP

    return scheme, profile, bucket, prefix


def scoped_list_prefix(prefix: str) -> str:
    """Returns an S3 ListObjectsV2 Prefix scoped to exactly this repo.

    Appends a trailing slash so a repo prefix that is a string-prefix of a
    sibling repo (e.g. "core/cli" vs "core/climate") does not cross-list the
    sibling's objects. An empty prefix (bucket-root repo) stays "" so it
    lists the whole bucket instead of the never-matching "/".
    """
    prefix = prefix.rstrip("/")
    return f"{prefix}/" if prefix else ""


BUCKET_ALIAS_TXT_PREFIX = "git-bucket="
BUCKET_ALIAS_CONFIG_KEY = "s3.dns-alias"


def _bucket_alias_opt_out_key(remote_name: str | None) -> str:
    """Returns the git config key to disable aliasing for this remote.

    Falls back to the global key when the remote name is unknown or is a
    URL rather than a configured remote name.
    """
    if remote_name is not None and "://" not in remote_name:
        return f"remote.{remote_name}.s3-dns-alias"
    return BUCKET_ALIAS_CONFIG_KEY


class BucketAliasError(Exception):
    def __init__(self, host: str, reason: str, remote_name: str | None = None):
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


@functools.cache
def _git_config_bool(key: str) -> bool | None:
    """Returns the boolean value of a git config key, or None if unset."""
    res = subprocess.run(
        ["git", "config", "--type=bool", "--get", key],
        capture_output=True,
    )
    if res.returncode != 0:
        return None
    value = res.stdout.decode("utf-8").strip()
    if value == "true":
        return True
    if value == "false":
        return False
    return None


def bucket_alias_enabled(remote_name: str | None = None) -> bool:
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


@functools.cache
def resolve_bucket_alias(bucket: str, remote_name: str | None = None) -> str:
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
        raise BucketAliasError(bucket, "no TXT record found", remote_name) from None
    except dns.exception.DNSException as e:
        raise BucketAliasError(bucket, f"DNS TXT lookup failed ({e})", remote_name) from e
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
            f"found {len(values)} '{BUCKET_ALIAS_TXT_PREFIX}' values in TXT record, expected exactly one",
            remote_name,
        )
    return values[0]


_bucket_region_cache: dict[str, str | None] = {}


def resolve_bucket_region(session, bucket: str) -> str | None:
    """Returns the AWS region a bucket lives in, or None if undeterminable.

    HeadBucket reports the bucket's true region in the ``x-amz-bucket-region``
    response header (and, on botocore 1.43.x, the ``BucketRegion`` output field)
    even when the caller is unauthorized (403) or redirected (301), so region
    detection needs no bucket permission and never has to match the caller's
    default region. The result is cached per process keyed on bucket name
    (region is a property of the bucket, independent of the calling identity).

    On any failure to determine the region this returns None so callers proceed
    WITHOUT pinning a region (the pre-fork default-region behavior); region
    detection is never fatal.

    Args:
        session: the boto3 ``Session`` to build the probe client from.
        bucket: an already alias-resolved bucket name.

    Returns:
        the bucket's region name, or None if it cannot be determined.
    """
    if bucket is None:
        return None
    if bucket in _bucket_region_cache:
        return _bucket_region_cache[bucket]
    region = _detect_bucket_region(session, bucket)
    _bucket_region_cache[bucket] = region
    return region


def _detect_bucket_region(session, bucket: str) -> str | None:
    s3 = session.client("s3")
    try:
        response = s3.head_bucket(Bucket=bucket)
        region = response.get("BucketRegion")
        if region:
            return region
        headers = response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
        return headers.get("x-amz-bucket-region")
    except s3.exceptions.ClientError as x:
        # HeadBucket returns the region header even on a 301 redirect / 403, so
        # an error response is still authoritative for the bucket's region.
        headers = x.response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
        return headers.get("x-amz-bucket-region")
    except Exception:
        # Region detection must never be fatal; the real S3 call that follows
        # surfaces any credential/endpoint problem with a clearer message.
        return None


def s3_region_kwargs(session, bucket: str) -> dict:
    """Returns client kwargs that pin an S3 client/resource to the bucket region.

    Spread into ``session.client("s3", **...)`` or ``session.resource("s3", **...)``.
    Yields ``{"region_name": <region>}`` when the region is known, or ``{}`` when
    it cannot be determined (leaving boto3's default region resolution in place).
    """
    region = resolve_bucket_region(session, bucket)
    return {"region_name": region} if region else {}


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


_ACCESS_GRANTS_FALLBACK_NOTICE = (
    "git-remote-s3: S3 Access Grants unavailable; using direct S3 credentials. "
    "If you expected Access Grants, run 'git-s3 doctor' for details."
)

_access_grants_fallback_notified = False


def _notify_access_grants_fallback() -> None:
    """Emits a one-time notice that Access Grants fell back to direct S3 creds.

    The callers are a git remote helper and a git-lfs transfer agent whose
    stdout carries a wire protocol; any stray stdout byte corrupts it, so the
    notice must go to stderr (which git surfaces to the user). It is emitted at
    most once per process to avoid one line per object/operation.
    """
    global _access_grants_fallback_notified
    if _access_grants_fallback_notified:
        return
    _access_grants_fallback_notified = True
    print(_ACCESS_GRANTS_FALLBACK_NOTICE, file=sys.stderr, flush=True)


def _detect_access_grants_fallback(**kwargs) -> None:
    """before-sign.s3 handler that fires the one-time fallback notice.

    The plugin signals a successful Access Grants vend by setting
    ``request.context['signing']['request_credentials']`` in its own
    before-sign.s3 handler and leaves it unset on any fallback. This handler is
    registered after the plugin's, so botocore (which invokes same-event
    handlers in registration order) calls it once the plugin has decided; an
    unset value means a fallback occurred. The plugin's own fallback signal is a
    DEBUG record on the root logger, which cannot be observed without changing
    global logging levels, so this context check is used instead.
    """
    request = kwargs.get("request")
    if request is None:
        return
    if "request_credentials" not in request.context.get("signing", {}):
        _notify_access_grants_fallback()


def register_s3_access_grants(s3_client, session):
    """Registers the AWS S3 Access Grants plugin on an S3 client and returns it.

    Always registers with ``fallback_enabled=True`` so a single code path serves
    both credential models: profiles holding only ``s3:GetDataAccess`` get
    Access Grants vended credentials, while IAM-access-key profiles (which have
    no grant) transparently fall back to a direct S3 call. On the first fallback
    a one-time notice is emitted to stderr.

    The plugin is handed the same profile session the S3 client was built from
    via ``customer_session``. Without it the plugin resolves GetDataAccess (and
    its STS/s3control preflight) against the *default* botocore session, so a
    ``s3://profile@bucket/repo`` URL whose profile is not also the default would
    vend grants as the wrong identity. ``session._session`` is the underlying
    botocore Session that the plugin expects.

    Args:
        s3_client: a boto3 S3 client (or ``resource.meta.client``) built on an
            already alias-resolved bucket name.
        session: the boto3 ``Session`` the ``s3_client`` was created from.

    Returns:
        the same client, with the plugin and fallback detector registered.
    """
    plugin = S3AccessGrantsPlugin(s3_client, fallback_enabled=True, customer_session=session._session)
    plugin.register()
    s3_client.meta.events.register("before-sign.s3", _detect_access_grants_fallback)
    return s3_client


def register_s3_access_grants_strict(s3_client, session):
    """Registers the plugin with fallback DISABLED, for diagnostics only.

    The production helper (``register_s3_access_grants``) enables fallback so a
    missing grant silently drops to direct credentials — that silence is exactly
    what hides a misconfigured Access Grants path, including a failure in the
    plugin's ``GetAccessGrantsInstanceForPrefix`` preflight (a separate IAM action
    from ``GetDataAccess``) that it runs before vending. With fallback disabled the
    plugin re-raises that real error, so ``git-s3 doctor`` can report it instead of
    masking it.

    Args:
        s3_client: a boto3 S3 client built on an already alias-resolved bucket.
        session: the boto3 ``Session`` the ``s3_client`` was created from.

    Returns:
        the same client, with the fallback-disabled plugin registered.
    """
    plugin = S3AccessGrantsPlugin(s3_client, fallback_enabled=False, customer_session=session._session)
    plugin.register()
    return s3_client
