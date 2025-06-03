import botocore.client
from mock import patch
from io import StringIO, BytesIO
from git_remote_s3 import S3Remote, UriScheme
from botocore.exceptions import ClientError
import tempfile
import datetime
import botocore

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
    assert session_client_mock.return_value.put_object.call_count == 1
    assert session_client_mock.return_value.delete_object.call_count == 1
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
    assert session_client_mock.return_value.put_object.call_count == 2
    assert session_client_mock.return_value.delete_object.call_count == 1
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
    assert session_client_mock.return_value.put_object.call_count == 0
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
    assert session_client_mock.return_value.put_object.call_count == 1
    assert session_client_mock.return_value.delete_object.call_count == 1
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
    assert session_client_mock.return_value.put_object.call_count == 2
    assert session_client_mock.return_value.delete_object.call_count == 1
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
    assert session_client_mock.return_value.put_object.call_count == 2
    assert session_client_mock.return_value.delete_object.call_count == 0
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
    assert session_client_mock.return_value.put_object.call_count == 3
    assert session_client_mock.return_value.delete_object.call_count == 0
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

    res = s3_remote.cmd_push(f"push refs/heads/{BRANCH}:refs/heads/{BRANCH}")

    put_object_calls = session_client_mock.return_value.put_object.call_args_list
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
    session_client_mock.return_value.get_object.return_value = {
        "Body": BytesIO(MOCK_BUNDLE_CONTENT)
    }
    s3_remote.cmd_fetch(f"fetch {SHA1} refs/heads/{BRANCH}")

    unbundle_mock.assert_called_once()
    assert session_client_mock.return_value.get_object.call_count == 1


@patch("git_remote_s3.git.unbundle")
@patch("boto3.Session.client")
def test_cmd_fetch_same_ref(session_client_mock, unbundle_mock):
    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")
    session_client_mock.return_value.get_object.return_value = {
        "Body": BytesIO(MOCK_BUNDLE_CONTENT)
    }
    s3_remote.cmd_fetch(f"fetch {SHA1} refs/heads/{BRANCH}")
    s3_remote.cmd_fetch(f"fetch {SHA1} refs/heads/{BRANCH}")
    unbundle_mock.assert_called_once()
    assert session_client_mock.return_value.get_object.call_count == 1


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


@patch("git_remote_s3.git.unbundle")
@patch("boto3.Session.client")
def test_cmd_fetch_batch(session_client_mock, unbundle_mock):

    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")

    session_client_mock.return_value.get_object.return_value = {
        "Body": BytesIO(MOCK_BUNDLE_CONTENT)
    }

    batch_input = (
        f"{SHA1} refs/heads/{BRANCH}\n"
        f"{SHA2} refs/tags/v1\n"
        "\n"
    )

    with patch("sys.stdin", new=StringIO(batch_input)):
        s3_remote.cmd_fetch("fetch --stdin")

    assert unbundle_mock.call_count == 2
    assert session_client_mock.return_value.get_object.call_count == 2
