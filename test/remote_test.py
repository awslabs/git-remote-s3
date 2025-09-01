import botocore.client
from mock import patch
from io import StringIO, BytesIO
from git_remote_s3 import S3Remote, UriScheme
from botocore.exceptions import ClientError
import tempfile
import datetime
import botocore
import threading
from io import BytesIO

SHA1 = "c105d19ba64965d2c9d3d3246e7269059ef8bb8a"
SHA2 = "c105d19ba64965d2c9d3d3246e7269059ef8bb8b"
INVALID_SHA = "z45"
BUNDLE_SUFFIX = ".bundle"
MOCK_BUNDLE_CONTENT = b"MOCK_BUNDLE_CONTENT"
ARCHIVE_SUFFIX = ".zip"
MOCK_ARCHIVE_CONTENT = b"MOCK_ARCHIVE_CONTENT"
BRANCH = "pytest"


def create_list_objects_v2_mock(
    *,
    protected=False,
    no_head=False,
    branch=BRANCH,
    shas,
):
    def s3_list_objects_v2_mock(Prefix, **kwargs):
        content = []
        for s in shas:
            content.append(
                {
                    "Key": f"test_prefix/refs/heads/{branch}/{s}.bundle",
                    "LastModified": datetime.datetime.now(),
                }
            )
        if protected:
            content.append(
                {
                    "Key": f"test_prefix/refs/heads/{branch}/PROTECTED#",
                    "LastModified": datetime.datetime.now(),
                }
            )
        if not no_head:
            content.append(
                {
                    "Key": "test_prefix/HEAD",
                    "LastModified": datetime.datetime.now(),
                }
            )
        return {
            "Contents": [c for c in content if c["Key"].startswith(Prefix)],
            "NextContinuationToken": None,
        }

    return s3_list_objects_v2_mock


@patch("sys.stdout", new_callable=StringIO)
@patch("boto3.Session.client")
def test_cmd_list(session_client_mock, stdout_mock):
    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")

    session_client_mock.return_value.list_objects_v2.side_effect = (
        create_list_objects_v2_mock(shas=[SHA1])
    )
    session_client_mock.assert_called_once_with("s3")
    assert s3_remote.bucket == "test_bucket"
    assert s3_remote.prefix == "test_prefix"
    assert s3_remote.s3 == session_client_mock.return_value
    session_client_mock.return_value.get_object.return_value = {
        "Body": BytesIO(b"refs/heads/%b" % str.encode(BRANCH))
    }
    s3_remote.cmd_list()
    assert (
        f"@refs/heads/{BRANCH} HEAD\n{SHA1} refs/heads/{BRANCH}\n\n"
        == stdout_mock.getvalue()
    )


@patch("sys.stdout", new_callable=StringIO)
@patch("boto3.Session.client")
def test_list_refs(session_client_mock, stdout_mock):
    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "nested/test_prefix")

    session_client_mock.return_value.list_objects_v2.return_value = {
        "Contents": [
            {
                "Key": f"nested/test_prefix/refs/heads/{BRANCH}/{SHA1}.bundle",
                "LastModified": datetime.datetime.now(),
            },
            {
                "Key": f"nested/test_prefix/refs/tags/v1/{SHA1}.bundle",
                "LastModified": datetime.datetime.now(),
            },
        ]
    }

    session_client_mock.assert_called_once_with("s3")
    assert s3_remote.bucket == "test_bucket"
    assert s3_remote.prefix == "nested/test_prefix"
    assert s3_remote.s3 == session_client_mock.return_value
    refs = s3_remote.list_refs(bucket=s3_remote.bucket, prefix=s3_remote.prefix)
    assert len(refs) == 2
    assert f"refs/heads/{BRANCH}/{SHA1}.bundle" in refs
    assert f"refs/tags/v1/{SHA1}.bundle" in refs


@patch("sys.stdout", new_callable=StringIO)
@patch("boto3.Session.client")
def test_cmd_list_nested_prefix(session_client_mock, stdout_mock):
    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "nested/test_prefix")

    session_client_mock.return_value.list_objects_v2.return_value = {
        "Contents": [
            {
                "Key": f"nested/test_prefix/refs/heads/{BRANCH}/{SHA1}.bundle",
                "LastModified": datetime.datetime.now(),
            },
            {
                "Key": "nested/test_prefix/HEAD",
                "LastModified": datetime.datetime.now(),
            },
        ]
    }
    session_client_mock.assert_called_once_with("s3")
    assert s3_remote.bucket == "test_bucket"
    assert s3_remote.prefix == "nested/test_prefix"
    assert s3_remote.s3 == session_client_mock.return_value
    session_client_mock.return_value.get_object.return_value = {
        "Body": BytesIO(b"refs/heads/%b" % str.encode(BRANCH))
    }
    s3_remote.cmd_list()
    assert (
        f"@refs/heads/{BRANCH} HEAD\n{SHA1} refs/heads/{BRANCH}\n\n"
        == stdout_mock.getvalue()
    )


@patch("sys.stdout", new_callable=StringIO)
@patch("boto3.Session.client")
def test_cmd_list_no_head(session_client_mock, stdout_mock):
    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")

    session_client_mock.return_value.list_objects_v2.side_effect = (
        create_list_objects_v2_mock(shas=[SHA1], no_head=True)
    )

    def error(**kwargs):
        raise botocore.exceptions.ClientError(
            {"Error": {"Code": "NoSuchKey"}}, "get_object"
        )

    session_client_mock.return_value.get_object.side_effect = error
    session_client_mock.assert_called_once_with("s3")
    assert s3_remote.bucket == "test_bucket"
    assert s3_remote.prefix == "test_prefix"
    assert s3_remote.s3 == session_client_mock.return_value
    s3_remote.cmd_list()
    assert f"{SHA1} refs/heads/{BRANCH}\n\n" == stdout_mock.getvalue()


@patch("sys.stdout", new_callable=StringIO)
@patch("boto3.Session.client")
def test_cmd_list_with_head_not_exsting_ref(session_client_mock, stdout_mock):
    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")

    session_client_mock.return_value.list_objects_v2.side_effect = (
        create_list_objects_v2_mock(shas=[SHA1])
    )
    session_client_mock.return_value.get_object.return_value = {
        "Body": BytesIO(b"refs/heads/master")
    }
    session_client_mock.assert_called_once_with("s3")
    assert s3_remote.bucket == "test_bucket"
    assert s3_remote.prefix == "test_prefix"
    assert s3_remote.s3 == session_client_mock.return_value
    s3_remote.cmd_list()
    assert f"{SHA1} refs/heads/{BRANCH}\n\n" == stdout_mock.getvalue()


@patch("sys.stdout", new_callable=StringIO)
@patch("boto3.Session.client")
def test_cmd_list_protected_branch(session_client_mock, stdout_mock):
    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")

    session_client_mock.return_value.list_objects_v2.side_effect = (
        create_list_objects_v2_mock(protected=True, shas=[SHA1])
    )

    session_client_mock.return_value.get_object.return_value = {
        "Body": BytesIO(b"refs/heads/%b" % str.encode(BRANCH))
    }
    session_client_mock.assert_called_once_with("s3")
    assert s3_remote.bucket == "test_bucket"
    assert s3_remote.prefix == "test_prefix"
    assert s3_remote.s3 == session_client_mock.return_value
    s3_remote.cmd_list()
    assert (
        f"@refs/heads/{BRANCH} HEAD\n{SHA1} refs/heads/{BRANCH}\n\n"
        == stdout_mock.getvalue()
    )


@patch("git_remote_s3.git.is_ancestor")
@patch("git_remote_s3.git.rev_parse")
@patch("git_remote_s3.git.bundle")
@patch("boto3.Session.client")
def test_cmd_push_no_force_unprotected_ancestor(
    session_client_mock, bundle_mock, rev_parse_mock, is_ancestor_mock
):
    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")
    rev_parse_mock.return_value = SHA1
    temp_dir = tempfile.mkdtemp("test_temp")
    temp_file = tempfile.NamedTemporaryFile(dir=temp_dir, suffix=BUNDLE_SUFFIX)
    with open(temp_file.name, "wb") as f:
        f.write(MOCK_BUNDLE_CONTENT)
    bundle_mock.return_value = temp_file.name
    session_client_mock.return_value.list_objects_v2.side_effect = (
        create_list_objects_v2_mock(protected=True, shas=[SHA1])
    )
    is_ancestor_mock.return_value = True
    assert s3_remote.s3 == session_client_mock.return_value
    res = s3_remote.cmd_push(f"push refs/heads/{BRANCH}:refs/heads/{BRANCH}")
    put_calls = [c for c in session_client_mock.return_value.put_object.call_args_list if not c.kwargs["Key"].endswith(".lock")]
    assert len(put_calls) == 1
    del_calls = [c for c in session_client_mock.return_value.delete_object.call_args_list if not c.kwargs["Key"].endswith(".lock")]
    assert len(del_calls) == 1
    assert res == (f"ok refs/heads/{BRANCH}\n")


@patch("git_remote_s3.git.archive")
@patch("git_remote_s3.git.is_ancestor")
@patch("git_remote_s3.git.rev_parse")
@patch("git_remote_s3.git.bundle")
@patch("boto3.Session.client")
def test_cmd_push_no_force_unprotected_ancestor_s3_zip(
    session_client_mock, bundle_mock, rev_parse_mock, is_ancestor_mock, archive_mock
):
    s3_remote = S3Remote(UriScheme.S3_ZIP, None, "test_bucket", "test_prefix")
    rev_parse_mock.return_value = SHA1

    temp_dir = tempfile.mkdtemp("test_temp")
    temp_file = tempfile.NamedTemporaryFile(dir=temp_dir, suffix=BUNDLE_SUFFIX)
    with open(temp_file.name, "wb") as f:
        f.write(MOCK_BUNDLE_CONTENT)
    bundle_mock.return_value = temp_file.name

    temp_file_archive = tempfile.NamedTemporaryFile(dir=temp_dir, suffix=ARCHIVE_SUFFIX)
    with open(temp_file_archive.name, "wb") as f:
        f.write(MOCK_ARCHIVE_CONTENT)
    archive_mock.return_value = temp_file_archive.name

    session_client_mock.return_value.list_objects_v2.side_effect = (
        create_list_objects_v2_mock(protected=True, shas=[SHA1])
    )

    is_ancestor_mock.return_value = True

    assert s3_remote.s3 == session_client_mock.return_value

    res = s3_remote.cmd_push(f"push refs/heads/{BRANCH}:refs/heads/{BRANCH}")
    put_calls = [c for c in session_client_mock.return_value.put_object.call_args_list if not c.kwargs["Key"].endswith(".lock")]
    assert len(put_calls) == 2
    del_calls = [c for c in session_client_mock.return_value.delete_object.call_args_list if not c.kwargs["Key"].endswith(".lock")]
    assert len(del_calls) == 1
    assert res == (f"ok refs/heads/{BRANCH}\n")


@patch("git_remote_s3.git.is_ancestor")
@patch("git_remote_s3.git.rev_parse")
@patch("git_remote_s3.git.bundle")
@patch("boto3.Session.client")
def test_cmd_push_no_force_unprotected_no_ancestor(
    session_client_mock, bundle_mock, rev_parse_mock, is_ancestor_mock
):
    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")
    rev_parse_mock.return_value = SHA1
    temp_dir = tempfile.mkdtemp("test_temp")
    temp_file = tempfile.NamedTemporaryFile(dir=temp_dir, suffix=BUNDLE_SUFFIX)
    with open(temp_file.name, "wb") as f:
        f.write(MOCK_BUNDLE_CONTENT)
    bundle_mock.return_value = temp_file.name
    session_client_mock.return_value.list_objects_v2.side_effect = (
        create_list_objects_v2_mock(shas=[SHA2])
    )

    is_ancestor_mock.return_value = False
    assert s3_remote.s3 == session_client_mock.return_value
    res = s3_remote.cmd_push(f"push refs/heads/{BRANCH}:refs/heads/{BRANCH}")
    put_calls = [c for c in session_client_mock.return_value.put_object.call_args_list if not c.kwargs.get("Key", "").endswith(".lock")]
    assert len(put_calls) == 0
    assert session_client_mock.return_value.delete_object.call_count == 0
    assert res.startswith("error")


@patch("git_remote_s3.git.is_ancestor")
@patch("git_remote_s3.git.rev_parse")
@patch("git_remote_s3.git.bundle")
@patch("boto3.Session.client")
def test_cmd_push_force_no_ancestor(
    session_client_mock, bundle_mock, rev_parse_mock, is_ancestor_mock
):
    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")
    rev_parse_mock.return_value = SHA1
    temp_dir = tempfile.mkdtemp("test_temp")
    temp_file = tempfile.NamedTemporaryFile(dir=temp_dir, suffix=BUNDLE_SUFFIX)
    with open(temp_file.name, "wb") as f:
        f.write(MOCK_BUNDLE_CONTENT)
    bundle_mock.return_value = temp_file.name
    session_client_mock.return_value.list_objects_v2.side_effect = (
        create_list_objects_v2_mock(shas=[SHA2])
    )
    is_ancestor_mock.return_value = False
    assert s3_remote.s3 == session_client_mock.return_value
    res = s3_remote.cmd_push(f"push +refs/heads/{BRANCH}:refs/heads/{BRANCH}")
    put_calls = [c for c in session_client_mock.return_value.put_object.call_args_list if not c.kwargs["Key"].endswith(".lock")]
    assert len(put_calls) == 1
    del_calls = [c for c in session_client_mock.return_value.delete_object.call_args_list if not c.kwargs["Key"].endswith(".lock")]
    assert len(del_calls) == 1
    assert res.startswith("ok")


@patch("git_remote_s3.git.archive")
@patch("git_remote_s3.git.is_ancestor")
@patch("git_remote_s3.git.rev_parse")
@patch("git_remote_s3.git.bundle")
@patch("boto3.Session.client")
def test_cmd_push_force_no_ancestor_s3_zip(
    session_client_mock, bundle_mock, rev_parse_mock, is_ancestor_mock, archive_mock
):
    s3_remote = S3Remote(UriScheme.S3_ZIP, None, "test_bucket", "test_prefix")

    rev_parse_mock.return_value = SHA1

    temp_dir = tempfile.mkdtemp("test_temp")
    temp_file = tempfile.NamedTemporaryFile(dir=temp_dir, suffix=BUNDLE_SUFFIX)
    with open(temp_file.name, "wb") as f:
        f.write(MOCK_BUNDLE_CONTENT)
    bundle_mock.return_value = temp_file.name

    temp_file_archive = tempfile.NamedTemporaryFile(dir=temp_dir, suffix=ARCHIVE_SUFFIX)
    with open(temp_file_archive.name, "wb") as f:
        f.write(MOCK_ARCHIVE_CONTENT)
    archive_mock.return_value = temp_file_archive.name

    session_client_mock.return_value.list_objects_v2.side_effect = (
        create_list_objects_v2_mock(shas=[SHA2])
    )

    is_ancestor_mock.return_value = False

    assert s3_remote.s3 == session_client_mock.return_value

    res = s3_remote.cmd_push(f"push +refs/heads/{BRANCH}:refs/heads/{BRANCH}")
    put_calls = [c for c in session_client_mock.return_value.put_object.call_args_list if not c.kwargs["Key"].endswith(".lock")]
    assert len(put_calls) == 2
    del_calls = [c for c in session_client_mock.return_value.delete_object.call_args_list if not c.kwargs["Key"].endswith(".lock")]
    assert len(del_calls) == 1
    assert res.startswith("ok")


@patch("git_remote_s3.git.is_ancestor")
@patch("git_remote_s3.git.rev_parse")
@patch("git_remote_s3.git.bundle")
@patch("boto3.Session.client")
def test_cmd_push_force_no_ancestor_protected(
    session_client_mock, bundle_mock, rev_parse_mock, is_ancestor_mock
):
    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")
    rev_parse_mock.return_value = SHA1
    temp_dir = tempfile.mkdtemp("test_temp")
    temp_file = tempfile.NamedTemporaryFile(dir=temp_dir, suffix=BUNDLE_SUFFIX)
    with open(temp_file.name, "wb") as f:
        f.write(MOCK_BUNDLE_CONTENT)
    bundle_mock.return_value = temp_file.name
    session_client_mock.return_value.list_objects_v2.side_effect = (
        create_list_objects_v2_mock(protected=True, shas=[SHA2])
    )
    is_ancestor_mock.return_value = False
    assert s3_remote.s3 == session_client_mock.return_value
    res = s3_remote.cmd_push(f"push +refs/heads/{BRANCH}:refs/heads/{BRANCH}")
    assert session_client_mock.return_value.put_object.call_count == 0
    assert session_client_mock.return_value.delete_object.call_count == 0
    assert res.startswith("error")


@patch("git_remote_s3.git.is_ancestor")
@patch("git_remote_s3.git.rev_parse")
@patch("git_remote_s3.git.bundle")
@patch("boto3.Session.client")
def test_cmd_push_empty_bucket(
    session_client_mock, bundle_mock, rev_parse_mock, is_ancestor_mock
):
    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")
    rev_parse_mock.return_value = SHA1
    temp_dir = tempfile.mkdtemp("test_temp")
    temp_file = tempfile.NamedTemporaryFile(dir=temp_dir, suffix=BUNDLE_SUFFIX)
    with open(temp_file.name, "wb") as f:
        f.write(MOCK_BUNDLE_CONTENT)
    bundle_mock.return_value = temp_file.name

    session_client_mock.return_value.head_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey"}}, "head_object"
    )

    is_ancestor_mock.return_value = False
    assert s3_remote.s3 == session_client_mock.return_value
    res = s3_remote.cmd_push(f"push refs/heads/{BRANCH}:refs/heads/{BRANCH}")
    put_calls = [c for c in session_client_mock.return_value.put_object.call_args_list if not c.kwargs["Key"].endswith(".lock")]
    assert len(put_calls) == 2
    del_calls = [c for c in session_client_mock.return_value.delete_object.call_args_list if not c.kwargs["Key"].endswith(".lock")]
    assert len(del_calls) == 0
    assert res.startswith("ok")


@patch("git_remote_s3.git.archive")
@patch("git_remote_s3.git.is_ancestor")
@patch("git_remote_s3.git.rev_parse")
@patch("git_remote_s3.git.bundle")
@patch("boto3.Session.client")
def test_cmd_push_empty_bucket_s3_zip(
    session_client_mock,
    bundle_mock,
    rev_parse_mock,
    is_ancestor_mock,
    archive_mock,
):
    s3_remote = S3Remote(UriScheme.S3_ZIP, None, "test_bucket", "test_prefix")

    rev_parse_mock.return_value = SHA1

    temp_dir = tempfile.mkdtemp("test_temp")
    temp_file = tempfile.NamedTemporaryFile(dir=temp_dir, suffix=BUNDLE_SUFFIX)
    with open(temp_file.name, "wb") as f:
        f.write(MOCK_BUNDLE_CONTENT)
    bundle_mock.return_value = temp_file.name

    temp_file_archive = tempfile.NamedTemporaryFile(dir=temp_dir, suffix=ARCHIVE_SUFFIX)
    with open(temp_file_archive.name, "wb") as f:
        f.write(MOCK_ARCHIVE_CONTENT)
    archive_mock.return_value = temp_file_archive.name

    session_client_mock.return_value.head_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey"}}, "head_object"
    )

    is_ancestor_mock.return_value = False

    assert s3_remote.s3 == session_client_mock.return_value

    res = s3_remote.cmd_push(f"push refs/heads/{BRANCH}:refs/heads/{BRANCH}")
    put_calls = [c for c in session_client_mock.return_value.put_object.call_args_list if not c.kwargs["Key"].endswith(".lock")]
    assert len(put_calls) == 3
    del_calls = [c for c in session_client_mock.return_value.delete_object.call_args_list if not c.kwargs["Key"].endswith(".lock")]
    assert len(del_calls) == 0
    assert res.startswith("ok")


@patch("git_remote_s3.git.archive")
@patch("git_remote_s3.git.is_ancestor")
@patch("git_remote_s3.git.rev_parse")
@patch("git_remote_s3.git.bundle")
@patch("git_remote_s3.git.get_last_commit_message")
@patch("boto3.Session.client")
def test_cmd_push_s3_zip_put_object_params(
    session_client_mock,
    get_last_commit_message_mock,
    bundle_mock,
    rev_parse_mock,
    is_ancestor_mock,
    archive_mock,
):
    s3_remote = S3Remote(UriScheme.S3_ZIP, None, "test_bucket", "test_prefix")
    rev_parse_mock.return_value = SHA1
    get_last_commit_message_mock.return_value = "test commit message"

    temp_dir = tempfile.mkdtemp("test_temp")
    temp_file = tempfile.NamedTemporaryFile(dir=temp_dir, suffix=BUNDLE_SUFFIX)
    with open(temp_file.name, "wb") as f:
        f.write(MOCK_BUNDLE_CONTENT)
    bundle_mock.return_value = temp_file.name

    temp_file_archive = tempfile.NamedTemporaryFile(dir=temp_dir, suffix=ARCHIVE_SUFFIX)
    with open(temp_file_archive.name, "wb") as f:
        f.write(MOCK_ARCHIVE_CONTENT)
    archive_mock.return_value = temp_file_archive.name

    session_client_mock.return_value.list_objects_v2.side_effect = (
        create_list_objects_v2_mock(shas=[SHA2])
    )

    is_ancestor_mock.return_value = True

    s3_remote.cmd_push(f"push refs/heads/{BRANCH}:refs/heads/{BRANCH}")

    put_object_calls = [c for c in session_client_mock.return_value.put_object.call_args_list if not c.kwargs["Key"].endswith(".lock")]
    assert len(put_object_calls) == 2

    # Check bundle upload
    bundle_call = put_object_calls[0]
    assert bundle_call.kwargs["Bucket"] == "test_bucket"
    assert bundle_call.kwargs["Key"].endswith(".bundle")

    # Check zip upload
    zip_call = put_object_calls[1]
    assert zip_call.kwargs["Bucket"] == "test_bucket"
    assert zip_call.kwargs["Key"].endswith("repo.zip")
    assert (
        zip_call.kwargs["Metadata"]["codepipeline-artifact-revision-summary"]
        == "test commit message"
    )


@patch("git_remote_s3.git.is_ancestor")
@patch("git_remote_s3.git.rev_parse")
@patch("git_remote_s3.git.bundle")
@patch("boto3.Session.client")
def test_cmd_push_multiple_heads(
    session_client_mock, bundle_mock, rev_parse_mock, is_ancestor_mock
):
    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")
    rev_parse_mock.return_value = SHA1
    temp_dir = tempfile.mkdtemp("test_temp")
    temp_file = tempfile.NamedTemporaryFile(dir=temp_dir, suffix=BUNDLE_SUFFIX)
    with open(temp_file.name, "wb") as f:
        f.write(MOCK_BUNDLE_CONTENT)
    bundle_mock.return_value = temp_file.name
    session_client_mock.return_value.list_objects_v2.side_effect = (
        create_list_objects_v2_mock(shas=[SHA1, SHA2])
    )
    is_ancestor_mock.return_value = False
    assert s3_remote.s3 == session_client_mock.return_value
    res = s3_remote.cmd_push(f"push refs/heads/{BRANCH}:refs/heads/{BRANCH}")
    assert session_client_mock.return_value.put_object.call_count == 0
    assert session_client_mock.return_value.delete_object.call_count == 0
    assert res.startswith("error")


@patch("git_remote_s3.git.unbundle")
@patch("boto3.Session.client")
def test_cmd_fetch(session_client_mock, unbundle_mock):
    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")
    s3_remote.cmd_fetch(f"fetch {SHA1} refs/heads/{BRANCH}")

    unbundle_mock.assert_called_once()
    assert session_client_mock.return_value.download_file.call_count == 1


@patch("git_remote_s3.git.unbundle")
@patch("boto3.Session.client")
def test_cmd_fetch_same_ref(session_client_mock, unbundle_mock):
    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")
    s3_remote.cmd_fetch(f"fetch {SHA1} refs/heads/{BRANCH}")
    s3_remote.cmd_fetch(f"fetch {SHA1} refs/heads/{BRANCH}")
    unbundle_mock.assert_called_once()
    assert session_client_mock.return_value.download_file.call_count == 1


@patch("sys.stdout", new_callable=StringIO)
@patch("boto3.Session.client")
def test_cmd_option(session_client_mock, stdout_mock):
    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")
    s3_remote.cmd_option("option verbosity 2")
    assert stdout_mock.getvalue().startswith("ok\n")
    s3_remote.cmd_option("option concurrency 1")
    assert stdout_mock.getvalue().endswith("unsupported\n")


@patch("sys.stdout", new_callable=StringIO)
@patch("boto3.Session.client")
def test_cmd_capabilities(session_client_mock, stdout_mock):
    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")
    s3_remote.cmd_capabilities()
    assert "fetch" in stdout_mock.getvalue()
    assert "option" in stdout_mock.getvalue()
    assert "push" in stdout_mock.getvalue()


@patch("boto3.Session.client")
def test_cmd_push_delete(session_client_mock):
    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")

    session_client_mock.return_value.list_objects_v2.return_value = {
        "Contents": [
            {
                "Key": f"test_prefix/refs/heads/{BRANCH}/{SHA1}.bundle",
                "LastModified": datetime.datetime.now(),
            }
        ]
    }
    assert s3_remote.s3 == session_client_mock.return_value
    res = s3_remote.cmd_push(f"push :refs/heads/{BRANCH}")
    assert session_client_mock.return_value.delete_object.call_count == 1
    assert res == (f"ok refs/heads/{BRANCH}\n")


@patch("boto3.Session.client")
def test_cmd_push_delete_s3_zip(session_client_mock):
    s3_remote = S3Remote(UriScheme.S3_ZIP, None, "test_bucket", "test_prefix")

    session_client_mock.return_value.list_objects_v2.return_value = {
        "Contents": [
            {
                "Key": f"test_prefix/refs/heads/{BRANCH}/{SHA1}.bundle",
                "LastModified": datetime.datetime.now(),
            },
            {
                "Key": f"test_prefix/refs/heads/{BRANCH}/repo.zip",
                "LastModified": datetime.datetime.now(),
            },
        ]
    }
    assert s3_remote.s3 == session_client_mock.return_value
    res = s3_remote.cmd_push(f"push :refs/heads/{BRANCH}")
    assert session_client_mock.return_value.delete_object.call_count == 2
    assert res == (f"ok refs/heads/{BRANCH}\n")


@patch("boto3.Session.client")
def test_cmd_push_delete_fails_with_multiple_heads(session_client_mock):
    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")

    session_client_mock.return_value.list_objects_v2.return_value = {
        "Contents": [
            {
                "Key": f"test_prefix/refs/heads/{BRANCH}/{SHA1}.bundle",
                "LastModified": datetime.datetime.now(),
            },
            {
                "Key": f"test_prefix/refs/heads/{BRANCH}/{SHA2}.bundle",
                "LastModified": datetime.datetime.now(),
            },
        ]
    }
    assert s3_remote.s3 == session_client_mock.return_value
    res = s3_remote.cmd_push(f"push :refs/heads/{BRANCH}")
    assert session_client_mock.return_value.delete_object.call_count == 0
    assert res.startswith("error")


@patch("boto3.Session.client")
def test_cmd_push_delete_fails_with_multiple_heads_s3_zip(session_client_mock):
    s3_remote = S3Remote(UriScheme.S3_ZIP, None, "test_bucket", "test_prefix")

    session_client_mock.return_value.list_objects_v2.return_value = {
        "Contents": [
            {
                "Key": f"test_prefix/refs/heads/{BRANCH}/{SHA1}.bundle",
                "LastModified": datetime.datetime.now(),
            },
            {
                "Key": f"test_prefix/refs/heads/{BRANCH}/{SHA2}.bundle",
                "LastModified": datetime.datetime.now(),
            },
            {
                "Key": f"test_prefix/refs/heads/{BRANCH}/repo.zip",
                "LastModified": datetime.datetime.now(),
            },
        ]
    }
    assert s3_remote.s3 == session_client_mock.return_value
    res = s3_remote.cmd_push(f"push :refs/heads/{BRANCH}")
    assert session_client_mock.return_value.delete_object.call_count == 0
    assert res.startswith("error")


@patch("git_remote_s3.git.bundle")
@patch("git_remote_s3.git.rev_parse")
@patch("boto3.Session.client")
def test_simultaneous_pushes_single_bundle_remains(
    session_client_mock, rev_parse_mock, bundle_mock
):
    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")

    storage = {}
    lock_keys = []
    storage_lock = threading.Lock()

    def list_objects_v2_side_effect(Bucket, Prefix, **kwargs):
        with storage_lock:
            if Prefix.endswith("/LOCKS/"):
                contents = [{"Key": k, "LastModified": datetime.datetime.now()} for k in lock_keys]
            else:
                contents = [
                    {"Key": k, "LastModified": datetime.datetime.now()}
                    for k in storage.keys()
                    if k.startswith(Prefix)
                ]
        return {"Contents": contents, "NextContinuationToken": None}

    def put_object_side_effect(Bucket, Key, Body=None, **kwargs):
        with storage_lock:
            # Simulate S3 conditional writes for lock creation using If-None-Match
            if Key.endswith(".lock"):
                if kwargs.get("IfNoneMatch") == "*":
                    if Key in lock_keys:
                        raise botocore.exceptions.ClientError(
                            {
                                "ResponseMetadata": {"HTTPStatusCode": 412},
                                "Error": {"Code": "PreconditionFailed"},
                            },
                            "put_object",
                        )
                    lock_keys.append(Key)
                else:
                    lock_keys.append(Key)
            else:
                data = Body.read() if hasattr(Body, "read") else Body or b""
                storage[Key] = data
        return {}

    def delete_object_side_effect(Bucket, Key):
        with storage_lock:
            storage.pop(Key, None)
            try:
                lock_keys.remove(Key)
            except ValueError:
                pass
        return {}

    session_client_mock.return_value.list_objects_v2.side_effect = list_objects_v2_side_effect
    session_client_mock.return_value.put_object.side_effect = put_object_side_effect
    session_client_mock.return_value.delete_object.side_effect = delete_object_side_effect
    # Provide a concrete LastModified for lock head checks (non-stale)
    session_client_mock.return_value.head_object.side_effect = (
        lambda Bucket, Key: {"LastModified": datetime.datetime.now()}
    )

    def rev_parse_side_effect(local_ref: str):
        return SHA1 if "branch1" in local_ref else SHA2

    rev_parse_mock.side_effect = rev_parse_side_effect

    def bundle_side_effect(folder: str, sha: str, ref: str):
        temp_file = tempfile.NamedTemporaryFile(dir=folder, suffix=BUNDLE_SUFFIX, delete=False)
        with open(temp_file.name, "wb") as f:
            f.write(MOCK_BUNDLE_CONTENT)
        return temp_file.name

    bundle_mock.side_effect = bundle_side_effect

    remote_ref = f"refs/heads/{BRANCH}"

    t1 = threading.Thread(
        target=s3_remote.cmd_push, args=(f"push refs/heads/branch1:{remote_ref}",)
    )
    t2 = threading.Thread(
        target=s3_remote.cmd_push, args=(f"push refs/heads/branch2:{remote_ref}",)
    )

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    with storage_lock:
        bundles = [
            k
            for k in storage.keys()
            if k.startswith(f"test_prefix/{remote_ref}/") and k.endswith(".bundle")
        ]

    # Only one push should succeed due to per-ref locking; the other will fail to acquire lock
    assert len(bundles) == 1
    assert bundles[0].endswith(f"/{SHA1}.bundle") or bundles[0].endswith(f"/{SHA2}.bundle")


@patch("boto3.Session.client")
def test_acquire_lock_deletes_stale_and_reacquires(session_client_mock):
    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")

    # Ensure initial list call in constructor succeeds
    session_client_mock.return_value.list_objects_v2.return_value = {
        "Contents": [],
        "NextContinuationToken": None,
    }

    # Simulate existing lock causing first put to fail with 412, then succeed after delete
    attempts = {"count": 0}

    def put_object_side_effect(Bucket, Key, Body=None, IfNoneMatch=None, **kwargs):
        if Key.endswith(".lock") and IfNoneMatch == "*":
            if attempts["count"] == 0:
                attempts["count"] += 1
                raise botocore.exceptions.ClientError(
                    {
                        "ResponseMetadata": {"HTTPStatusCode": 412},
                        "Error": {"Code": "PreconditionFailed"},
                    },
                    "put_object",
                )
        return {}

    # Stale lock: last_modified far in the past
    def head_object_side_effect(Bucket, Key):
        return {"LastModified": datetime.datetime.now() - datetime.timedelta(seconds=120)}

    session_client_mock.return_value.put_object.side_effect = put_object_side_effect
    session_client_mock.return_value.head_object.side_effect = head_object_side_effect
    session_client_mock.return_value.delete_object.return_value = {}

    # Make TTL small enough so 120s old is stale
    s3_remote.lock_ttl_seconds = 60

    remote_ref = f"refs/heads/{BRANCH}"
    lock_key = s3_remote.acquire_lock(remote_ref)

    expected_lock_key = f"test_prefix/{remote_ref}/LOCK#.lock"
    assert lock_key == expected_lock_key

    # Verify delete was called exactly once for the stale lock
    delete_calls = [
        c for c in session_client_mock.return_value.delete_object.call_args_list if c.kwargs["Key"].endswith(".lock")
    ]
    assert len(delete_calls) == 1

    # Verify put was attempted at least twice (initial fail + reacquire)
    put_lock_calls = [
        c for c in session_client_mock.return_value.put_object.call_args_list if c.kwargs.get("Key", "").endswith(".lock")
    ]
    assert len(put_lock_calls) >= 2
