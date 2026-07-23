import subprocess

import pytest
from unittest.mock import patch

import dns.resolver

from git_remote_s3 import resolve_bucket_alias, BucketAliasError
from git_remote_s3.common import _git_config_bool


class TXTRecord:
    def __init__(self, *strings: bytes):
        self.strings = strings


@pytest.fixture(autouse=True)
def clear_alias_cache():
    resolve_bucket_alias.cache_clear()
    _git_config_bool.cache_clear()
    yield


def test_resolve_bucket_alias_no_dot_bypasses_dns():
    with patch("dns.resolver.resolve") as mock_resolve:
        assert resolve_bucket_alias("bucket-name") == "bucket-name"
        mock_resolve.assert_not_called()


def test_resolve_bucket_alias_none_bypasses_dns():
    with patch("dns.resolver.resolve") as mock_resolve:
        assert resolve_bucket_alias(None) is None
        mock_resolve.assert_not_called()


def test_resolve_bucket_alias_resolves_txt_record():
    with patch("dns.resolver.resolve") as mock_resolve:
        mock_resolve.return_value = [TXTRecord(b"git-bucket=real-bucket")]
        assert resolve_bucket_alias("etc.git.example.com") == "real-bucket"
        mock_resolve.assert_called_once_with("etc.git.example.com", "TXT")


def test_resolve_bucket_alias_ignores_other_txt_values():
    with patch("dns.resolver.resolve") as mock_resolve:
        mock_resolve.return_value = [
            TXTRecord(b"v=spf1 -all"),
            TXTRecord(b"git-bucket=real-bucket"),
            TXTRecord(b"unrelated=value"),
        ]
        assert resolve_bucket_alias("etc.git.example.com") == "real-bucket"


def test_resolve_bucket_alias_joins_multi_string_txt_value():
    with patch("dns.resolver.resolve") as mock_resolve:
        mock_resolve.return_value = [TXTRecord(b"git-bucket=real-", b"bucket")]
        assert resolve_bucket_alias("etc.git.example.com") == "real-bucket"


def test_resolve_bucket_alias_missing_record():
    with patch("dns.resolver.resolve") as mock_resolve:
        mock_resolve.side_effect = dns.resolver.NXDOMAIN
        with pytest.raises(BucketAliasError) as e:
            resolve_bucket_alias("etc.git.example.com")
        assert "no TXT record found" in str(e.value)
        assert "git-bucket=<real-bucket-name>" in str(e.value)


def test_resolve_bucket_alias_no_answer():
    with patch("dns.resolver.resolve") as mock_resolve:
        mock_resolve.side_effect = dns.resolver.NoAnswer
        with pytest.raises(BucketAliasError) as e:
            resolve_bucket_alias("etc.git.example.com")
        assert "no TXT record found" in str(e.value)


def test_resolve_bucket_alias_dns_failure():
    with patch("dns.resolver.resolve") as mock_resolve:
        mock_resolve.side_effect = dns.resolver.LifetimeTimeout
        with pytest.raises(BucketAliasError) as e:
            resolve_bucket_alias("etc.git.example.com")
        assert "DNS TXT lookup failed" in str(e.value)


def test_resolve_bucket_alias_no_git_bucket_value():
    with patch("dns.resolver.resolve") as mock_resolve:
        mock_resolve.return_value = [TXTRecord(b"v=spf1 -all")]
        with pytest.raises(BucketAliasError) as e:
            resolve_bucket_alias("etc.git.example.com")
        assert "no 'git-bucket=' value found" in str(e.value)
        assert "git-bucket=<real-bucket-name>" in str(e.value)


def test_resolve_bucket_alias_multiple_git_bucket_values():
    with patch("dns.resolver.resolve") as mock_resolve:
        mock_resolve.return_value = [
            TXTRecord(b"git-bucket=bucket-one"),
            TXTRecord(b"git-bucket=bucket-two"),
        ]
        with pytest.raises(BucketAliasError) as e:
            resolve_bucket_alias("etc.git.example.com")
        assert "expected exactly one" in str(e.value)


def test_resolve_bucket_alias_caches_per_process():
    with patch("dns.resolver.resolve") as mock_resolve:
        mock_resolve.return_value = [TXTRecord(b"git-bucket=real-bucket")]
        assert resolve_bucket_alias("etc.git.example.com") == "real-bucket"
        assert resolve_bucket_alias("etc.git.example.com") == "real-bucket"
        mock_resolve.assert_called_once()


def test_resolve_bucket_alias_opt_out_per_remote():
    config = {"remote.origin.s3-dns-alias": False}
    with patch("git_remote_s3.common._git_config_bool") as mock_config:
        mock_config.side_effect = config.get
        with patch("dns.resolver.resolve") as mock_resolve:
            assert resolve_bucket_alias("etc.git.example.com", "origin") == "etc.git.example.com"
            mock_resolve.assert_not_called()


def test_resolve_bucket_alias_opt_out_global():
    config = {"s3.dns-alias": False}
    with patch("git_remote_s3.common._git_config_bool") as mock_config:
        mock_config.side_effect = config.get
        with patch("dns.resolver.resolve") as mock_resolve:
            assert resolve_bucket_alias("etc.git.example.com") == "etc.git.example.com"
            mock_resolve.assert_not_called()
        mock_config.assert_called_once_with("s3.dns-alias")


def test_resolve_bucket_alias_opt_out_global_applies_to_named_remote():
    config = {"s3.dns-alias": False}
    with patch("git_remote_s3.common._git_config_bool") as mock_config:
        mock_config.side_effect = config.get
        with patch("dns.resolver.resolve") as mock_resolve:
            assert resolve_bucket_alias("etc.git.example.com", "origin") == "etc.git.example.com"
            mock_resolve.assert_not_called()


def test_resolve_bucket_alias_per_remote_overrides_global():
    config = {"remote.origin.s3-dns-alias": True, "s3.dns-alias": False}
    with patch("git_remote_s3.common._git_config_bool") as mock_config:
        mock_config.side_effect = config.get
        with patch("dns.resolver.resolve") as mock_resolve:
            mock_resolve.return_value = [TXTRecord(b"git-bucket=real-bucket")]
            assert resolve_bucket_alias("etc.git.example.com", "origin") == "real-bucket"


def test_resolve_bucket_alias_default_on_when_no_config():
    with patch("git_remote_s3.common._git_config_bool") as mock_config:
        mock_config.return_value = None
        with patch("dns.resolver.resolve") as mock_resolve:
            mock_resolve.return_value = [TXTRecord(b"git-bucket=real-bucket")]
            assert resolve_bucket_alias("etc.git.example.com", "origin") == "real-bucket"


def test_resolve_bucket_alias_url_remote_name_skips_per_remote_key():
    config = {"s3.dns-alias": False}
    with patch("git_remote_s3.common._git_config_bool") as mock_config:
        mock_config.side_effect = config.get
        with patch("dns.resolver.resolve") as mock_resolve:
            assert resolve_bucket_alias("etc.git.example.com", "s3://etc.git.example.com/repo") == "etc.git.example.com"
            mock_resolve.assert_not_called()
        mock_config.assert_called_once_with("s3.dns-alias")


def test_resolve_bucket_alias_error_includes_per_remote_opt_out_hint():
    with patch("git_remote_s3.common._git_config_bool") as mock_config:
        mock_config.return_value = None
        with patch("dns.resolver.resolve") as mock_resolve:
            mock_resolve.side_effect = dns.resolver.NXDOMAIN
            with pytest.raises(BucketAliasError) as e:
                resolve_bucket_alias("etc.git.example.com", "origin")
            assert "git config remote.origin.s3-dns-alias false" in str(e.value)


def test_resolve_bucket_alias_error_includes_global_opt_out_hint():
    with patch("git_remote_s3.common._git_config_bool") as mock_config:
        mock_config.return_value = None
        with patch("dns.resolver.resolve") as mock_resolve:
            mock_resolve.side_effect = dns.resolver.NXDOMAIN
            with pytest.raises(BucketAliasError) as e:
                resolve_bucket_alias("etc.git.example.com")
            assert "git config s3.dns-alias false" in str(e.value)


def test_git_config_bool_reads_via_git_config():
    with patch("git_remote_s3.common.subprocess.run") as mock_run:
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout=b"false\n", stderr=b"")
        assert _git_config_bool("s3.dns-alias") is False
        mock_run.assert_called_once_with(
            ["git", "config", "--type=bool", "--get", "s3.dns-alias"],
            capture_output=True,
        )


def test_git_config_bool_true_value():
    with patch("git_remote_s3.common.subprocess.run") as mock_run:
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout=b"true\n", stderr=b"")
        assert _git_config_bool("remote.origin.s3-dns-alias") is True


def test_git_config_bool_missing_key_is_unset():
    with patch("git_remote_s3.common.subprocess.run") as mock_run:
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=1, stdout=b"", stderr=b"")
        assert _git_config_bool("s3.dns-alias") is None


def test_git_config_bool_caches_per_process():
    with patch("git_remote_s3.common.subprocess.run") as mock_run:
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout=b"false\n", stderr=b"")
        assert _git_config_bool("s3.dns-alias") is False
        assert _git_config_bool("s3.dns-alias") is False
        mock_run.assert_called_once()
