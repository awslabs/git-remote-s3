# SPDX-FileCopyrightText: 2023-present Amazon.com, Inc. or its affiliates
#
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import patch

from git_remote_s3 import UriScheme
from git_remote_s3.manage import main


@pytest.fixture
def mocked_cli_chain():
    """Patches everything main() needs to reach argument-dependent branching
    without touching git, AWS, or a real bucket alias lookup."""
    with (
        patch("git_remote_s3.manage.get_remote_url") as get_remote_url,
        patch("git_remote_s3.manage.parse_git_url") as parse_git_url,
        patch("git_remote_s3.manage.resolve_bucket_alias") as resolve_bucket_alias,
        patch("git_remote_s3.manage.Doctor") as doctor_cls,
        patch("git_remote_s3.manage.ManageBranch") as manage_branch_cls,
    ):
        get_remote_url.return_value = "s3://profile@bucket/repo"
        parse_git_url.return_value = (UriScheme.S3, "profile", "bucket", "repo")
        resolve_bucket_alias.return_value = "bucket"
        yield doctor_cls, manage_branch_cls


def test_doctor_without_branch_parses_and_runs(mocked_cli_chain, monkeypatch):
    doctor_cls, manage_branch_cls = mocked_cli_chain
    monkeypatch.setattr("sys.argv", ["git-s3", "doctor", "s3://profile@bucket/repo"])

    with pytest.raises(SystemExit) as excinfo:
        main()

    assert excinfo.value.code == 0
    doctor_cls.assert_called_once_with("profile", "bucket", "repo", False, 60, False)
    doctor_cls.return_value.run.assert_called_once_with()
    manage_branch_cls.assert_not_called()


def test_delete_branch_without_branch_still_errors(mocked_cli_chain, monkeypatch, capsys):
    _, manage_branch_cls = mocked_cli_chain
    monkeypatch.setattr("sys.argv", ["git-s3", "delete-branch", "s3://profile@bucket/repo"])

    with pytest.raises(SystemExit) as excinfo:
        main()

    assert excinfo.value.code == 1
    assert "branch is required" in capsys.readouterr().err
    manage_branch_cls.assert_not_called()


def test_delete_branch_with_branch_still_works(mocked_cli_chain, monkeypatch):
    _, manage_branch_cls = mocked_cli_chain
    monkeypatch.setattr(
        "sys.argv",
        ["git-s3", "delete-branch", "s3://profile@bucket/repo", "mybranch"],
    )

    with pytest.raises(SystemExit) as excinfo:
        main()

    assert excinfo.value.code == 0
    manage_branch_cls.assert_called_once_with("profile", "bucket", "repo", "mybranch")
    manage_branch_cls.return_value.process_cmd.assert_called_once_with("delete-branch")


def test_doctor_accepts_options_before_and_after_uri(mocked_cli_chain, monkeypatch):
    doctor_cls, _ = mocked_cli_chain
    monkeypatch.setattr(
        "sys.argv",
        [
            "git-s3",
            "doctor",
            "--lock-ttl",
            "30",
            "s3://profile@bucket/repo",
            "--delete-stale-locks",
        ],
    )

    with pytest.raises(SystemExit) as excinfo:
        main()

    assert excinfo.value.code == 0
    doctor_cls.assert_called_once_with("profile", "bucket", "repo", False, 30, True)
