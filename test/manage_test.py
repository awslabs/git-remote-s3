import datetime
from io import BytesIO, StringIO

import pytest
from mock import patch

from git_remote_s3.manage import Doctor

SHA1 = "c105d19ba64965d2c9d3d3246e7269059ef8bb8a"
SHA2 = "c105d19ba64965d2c9d3d3246e7269059ef8bb8b"
NOW = datetime.datetime.now()


def contents(prefix, bundles=(SHA1,), head=True):
    items = [
        {"Key": f"{prefix}/refs/heads/main/{sha}.bundle", "LastModified": NOW}
        for sha in bundles
    ]
    if head:
        items.append({"Key": f"{prefix}/HEAD", "LastModified": NOW})
    return {"Contents": items}


def make_doctor(session_client_mock, prefix, listing, delete_bundle=False):
    s3 = session_client_mock.return_value
    s3.list_objects_v2.return_value = listing
    s3.get_object.return_value = {"Body": BytesIO(b"refs/heads/main\n")}
    return Doctor(None, "test_bucket", prefix, delete_bundle), s3


@pytest.mark.parametrize("prefix", ["my-repo", "team/my-repo", "org/team/my-repo"])
@patch("boto3.Session.client")
def test_analyze_repo_with_any_prefix_depth(session_client_mock, prefix):
    doctor, s3 = make_doctor(session_client_mock, prefix, contents(prefix))

    repos = doctor.analyze_repo()

    s3.list_objects_v2.assert_called_once_with(
        Bucket="test_bucket", Prefix=f"{prefix}/"
    )
    s3.get_object.assert_called_once_with(Bucket="test_bucket", Key=f"{prefix}/HEAD")
    assert repos == {
        prefix: {
            "refs": {
                "refs/heads/main": {
                    "protected": False,
                    "bundles": [{"sha": SHA1, "lastModified": NOW}],
                }
            },
            "HEAD": "refs/heads/main",
        }
    }


@patch("sys.stdout", new_callable=StringIO)
@patch("boto3.Session.client")
def test_run_does_not_rewrite_valid_head_with_nested_prefix(
    session_client_mock, stdout_mock
):
    doctor, s3 = make_doctor(
        session_client_mock, "team/my-repo", contents("team/my-repo")
    )

    with patch("builtins.input") as input_mock:
        doctor.run()

    input_mock.assert_not_called()
    s3.put_object.assert_not_called()
    assert "HEAD: Invalid" not in stdout_mock.getvalue()


@patch("sys.stdout", new_callable=StringIO)
@patch("boto3.Session.client")
def test_fix_multiple_bundles_deletes_existing_key_with_nested_prefix(
    session_client_mock, stdout_mock
):
    prefix = "team/my-repo"
    doctor, s3 = make_doctor(
        session_client_mock,
        prefix,
        contents(prefix, bundles=(SHA1, SHA2)),
        delete_bundle=True,
    )

    # keep the first bundle, then confirm
    with patch("builtins.input", side_effect=["1", ""]):
        doctor.fix_issues(doctor.analyze_repo())

    s3.delete_object.assert_any_call(
        Bucket="test_bucket", Key=f"{prefix}/refs/heads/main/{SHA2}.bundle"
    )
    s3.put_object.assert_not_called()


@patch("sys.stdout", new_callable=StringIO)
@patch("boto3.Session.client")
def test_fix_multiple_bundles_moves_existing_key_with_nested_prefix(
    session_client_mock, stdout_mock
):
    prefix = "team/my-repo"
    doctor, s3 = make_doctor(
        session_client_mock, prefix, contents(prefix, bundles=(SHA1, SHA2))
    )

    with patch("builtins.input", side_effect=["1", ""]):
        doctor.fix_issues(doctor.analyze_repo())

    copy_kwargs = s3.copy_object.call_args.kwargs
    assert copy_kwargs["CopySource"] == {
        "Bucket": "test_bucket",
        "Key": f"{prefix}/refs/heads/main/{SHA2}.bundle",
    }
    assert copy_kwargs["Key"].startswith(f"{prefix}/refs/heads/main_")
    s3.delete_object.assert_any_call(
        Bucket="test_bucket", Key=f"{prefix}/refs/heads/main/{SHA2}.bundle"
    )


@patch("sys.stdout", new_callable=StringIO)
@patch("boto3.Session.client")
def test_fix_head_writes_ref_relative_to_prefix(session_client_mock, stdout_mock):
    prefix = "team/my-repo"
    doctor, s3 = make_doctor(session_client_mock, prefix, contents(prefix, head=False))

    with patch("builtins.input", return_value="1"):
        doctor.run()

    s3.put_object.assert_called_once_with(
        Bucket="test_bucket", Key=f"{prefix}/HEAD", Body="refs/heads/main"
    )
