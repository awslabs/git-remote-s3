import subprocess
import tempfile
import pytest
from unittest.mock import patch
from git_remote_s3 import S3Remote, UriScheme
from git_remote_s3.remote import maybe_install_lfs_agent


@pytest.fixture
def temp_git_repo(monkeypatch):
    with tempfile.TemporaryDirectory(prefix="git_remote_s3_autoinstall_") as repo:
        subprocess.run(
            ["git", "init", "-q", repo],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        monkeypatch.chdir(repo)
        for var in ("GIT_DIR", "GIT_WORK_TREE", "GIT_REMOTE_S3_AUTO_INSTALL_LFS"):
            monkeypatch.delenv(var, raising=False)
        yield repo


def _config_get(key: str):
    res = subprocess.run(
        ["git", "config", "--get", key],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    if res.returncode != 0:
        return None
    return res.stdout.decode("utf-8").strip() or None


def test_install_writes_both_keys_when_unset(temp_git_repo):
    maybe_install_lfs_agent("origin")
    assert _config_get("lfs.customtransfer.git-lfs-s3.path") == "git-lfs-s3"
    assert _config_get("lfs.standalonetransferagent") == "git-lfs-s3"


def test_install_is_idempotent(temp_git_repo):
    maybe_install_lfs_agent("origin")
    maybe_install_lfs_agent("origin")
    # --get returns the single configured value, not a duplicated one
    assert _config_get("lfs.standalonetransferagent") == "git-lfs-s3"
    res = subprocess.run(
        ["git", "config", "--get-all", "lfs.standalonetransferagent"],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    assert res.stdout.decode("utf-8").strip().splitlines() == ["git-lfs-s3"]


def test_existing_unscoped_agent_is_not_stomped(temp_git_repo):
    subprocess.run(
        ["git", "config", "--add", "lfs.standalonetransferagent", "some-other-agent"],
        check=True,
    )
    maybe_install_lfs_agent("origin")
    assert _config_get("lfs.standalonetransferagent") == "some-other-agent"
    assert _config_get("lfs.customtransfer.git-lfs-s3.path") is None


def test_per_remote_lfsurl_blocks_install(temp_git_repo):
    subprocess.run(
        [
            "git",
            "config",
            "--add",
            "remote.origin.lfsurl",
            "https://lfs-alias.git-remote-s3.test/bucket/prefix",
        ],
        check=True,
    )
    maybe_install_lfs_agent("origin")
    assert _config_get("lfs.standalonetransferagent") is None
    assert _config_get("lfs.customtransfer.git-lfs-s3.path") is None


def test_per_remote_lfsurl_for_different_remote_does_not_block(temp_git_repo):
    subprocess.run(
        [
            "git",
            "config",
            "--add",
            "remote.other.lfsurl",
            "https://lfs-alias.git-remote-s3.test/bucket/prefix",
        ],
        check=True,
    )
    maybe_install_lfs_agent("origin")
    assert _config_get("lfs.standalonetransferagent") == "git-lfs-s3"


@pytest.mark.parametrize("value", ["0", "false", "FALSE", "no", "No"])
def test_opt_out_env_var_skips_install(temp_git_repo, monkeypatch, value):
    monkeypatch.setenv("GIT_REMOTE_S3_AUTO_INSTALL_LFS", value)
    maybe_install_lfs_agent("origin")
    assert _config_get("lfs.standalonetransferagent") is None
    assert _config_get("lfs.customtransfer.git-lfs-s3.path") is None


def test_s3remote_init_with_remote_name_triggers_install(temp_git_repo):
    with patch("boto3.Session.client") as client_mock:
        client_mock.return_value.list_objects_v2.return_value = {"Contents": []}
        S3Remote(
            UriScheme.S3,
            None,
            "test_bucket",
            "test_prefix",
            remote_name="origin",
        )
    assert _config_get("lfs.standalonetransferagent") == "git-lfs-s3"


def test_s3remote_init_without_remote_name_does_not_install(temp_git_repo):
    with patch("boto3.Session.client") as client_mock:
        client_mock.return_value.list_objects_v2.return_value = {"Contents": []}
        S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")
    assert _config_get("lfs.standalonetransferagent") is None
    assert _config_get("lfs.customtransfer.git-lfs-s3.path") is None
