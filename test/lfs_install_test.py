# SPDX-FileCopyrightText: 2023-present Amazon.com, Inc. or its affiliates
#
# SPDX-License-Identifier: Apache-2.0

import subprocess
import pytest

from git_remote_s3 import lfs
from git_remote_s3.common import synthetic_lfs_url, LFS_ALIAS_HOST


def _git(args, cwd):
    res = subprocess.run(args, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert res.returncode == 0, res.stderr.decode()
    return res.stdout.decode().strip()


def _git_config_get_all(key, cwd):
    res = subprocess.run(
        ["git", "config", "--get-all", key],
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if res.returncode != 0:
        return []
    return [line for line in res.stdout.decode().splitlines() if line.strip()]


@pytest.fixture
def repo(tmp_path, monkeypatch):
    _git(["git", "init", "-q", "-b", "main", str(tmp_path)], cwd=tmp_path)
    _git(["git", "config", "user.email", "test@example.com"], cwd=tmp_path)
    _git(["git", "config", "user.name", "Test"], cwd=tmp_path)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_synthetic_lfs_url_is_deterministic():
    assert (
        synthetic_lfs_url("my-bucket", "path/to/repo")
        == f"https://{LFS_ALIAS_HOST}/my-bucket/path/to/repo"
    )


def test_synthetic_lfs_url_uses_reserved_tld():
    assert LFS_ALIAS_HOST.endswith(".test")


def test_install_bare_one_remote_writes_unscoped_config(repo, capsys):
    _git(["git", "remote", "add", "origin", "s3://bucket/repo"], cwd=repo)

    lfs.install()

    assert _git_config_get_all("lfs.customtransfer.git-lfs-s3.path", repo) == [
        "git-lfs-s3"
    ]
    assert _git_config_get_all("lfs.standalonetransferagent", repo) == ["git-lfs-s3"]
    captured = capsys.readouterr()
    assert "git-lfs-s3 installed" in captured.out
    assert "warning" not in captured.err.lower()


def test_install_bare_multiple_remotes_warns(repo, capsys):
    _git(["git", "remote", "add", "origin", "ssh://git@example.com/repo.git"], cwd=repo)
    _git(["git", "remote", "add", "s3", "s3://bucket/repo"], cwd=repo)

    lfs.install()

    assert _git_config_get_all("lfs.standalonetransferagent", repo) == ["git-lfs-s3"]
    captured = capsys.readouterr()
    assert "warning" in captured.err.lower()
    assert "--remote" in captured.err


def test_install_remote_nonexistent_exits(repo, capsys):
    with pytest.raises(SystemExit) as exc:
        lfs.install(remote_name="missing")
    assert exc.value.code == 1
    captured = capsys.readouterr()
    assert "not configured" in captured.err
    assert "missing" in captured.err


def test_install_remote_non_s3_exits(repo, capsys):
    _git(
        ["git", "remote", "add", "github", "ssh://git@github.com/example/repo.git"],
        cwd=repo,
    )

    with pytest.raises(SystemExit) as exc:
        lfs.install(remote_name="github")
    assert exc.value.code == 1
    captured = capsys.readouterr()
    assert "not an s3" in captured.err.lower()


def test_install_remote_s3_writes_scoped_config(repo, capsys):
    _git(["git", "remote", "add", "s3", "s3://bucket/repo"], cwd=repo)
    expected_url = synthetic_lfs_url("bucket", "repo")

    lfs.install(remote_name="s3")

    assert _git_config_get_all("remote.s3.lfsurl", repo) == [expected_url]
    assert _git_config_get_all(f"lfs.{expected_url}.standalonetransferagent", repo) == [
        "git-lfs-s3"
    ]
    assert _git_config_get_all("lfs.customtransfer.git-lfs-s3.path", repo) == [
        "git-lfs-s3"
    ]
    assert _git_config_get_all("lfs.standalonetransferagent", repo) == []
    captured = capsys.readouterr()
    assert "installed for remote 's3'" in captured.out
    assert expected_url in captured.out


def test_install_remote_s3_zip_writes_scoped_config(repo):
    _git(["git", "remote", "add", "s3", "s3+zip://bucket/repo"], cwd=repo)
    expected_url = synthetic_lfs_url("bucket", "repo")

    lfs.install(remote_name="s3")

    assert _git_config_get_all("remote.s3.lfsurl", repo) == [expected_url]
    assert _git_config_get_all(f"lfs.{expected_url}.standalonetransferagent", repo) == [
        "git-lfs-s3"
    ]


def test_install_remote_is_idempotent(repo):
    _git(["git", "remote", "add", "s3", "s3://bucket/repo"], cwd=repo)

    lfs.install(remote_name="s3")
    lfs.install(remote_name="s3")

    expected_url = synthetic_lfs_url("bucket", "repo")
    assert _git_config_get_all("remote.s3.lfsurl", repo) == [expected_url]
    assert _git_config_get_all(f"lfs.{expected_url}.standalonetransferagent", repo) == [
        "git-lfs-s3"
    ]
    assert _git_config_get_all("lfs.customtransfer.git-lfs-s3.path", repo) == [
        "git-lfs-s3"
    ]


def test_install_remote_refuses_to_overwrite_existing_lfsurl(repo, capsys):
    _git(["git", "remote", "add", "s3", "s3://bucket/repo"], cwd=repo)
    _git(
        [
            "git",
            "config",
            "remote.s3.lfsurl",
            "https://real-lfs.example.com/foo",
        ],
        cwd=repo,
    )

    with pytest.raises(SystemExit) as exc:
        lfs.install(remote_name="s3")
    assert exc.value.code == 1
    captured = capsys.readouterr()
    assert "already set" in captured.err
    assert "https://real-lfs.example.com/foo" in captured.err
    assert _git_config_get_all("remote.s3.lfsurl", repo) == [
        "https://real-lfs.example.com/foo"
    ]


def test_install_remote_warns_on_existing_unscoped_agent(repo, capsys):
    _git(["git", "remote", "add", "s3", "s3://bucket/repo"], cwd=repo)
    _git(
        ["git", "config", "lfs.standalonetransferagent", "git-lfs-s3"],
        cwd=repo,
    )

    lfs.install(remote_name="s3")

    captured = capsys.readouterr()
    assert "warning" in captured.err.lower()
    assert "lfs.standalonetransferagent" in captured.err
    expected_url = synthetic_lfs_url("bucket", "repo")
    assert _git_config_get_all(f"lfs.{expected_url}.standalonetransferagent", repo) == [
        "git-lfs-s3"
    ]
