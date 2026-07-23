from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError
from unittest.mock import MagicMock, patch

from git_remote_s3 import S3Remote, UriScheme
from git_remote_s3 import common
from git_remote_s3.common import (
    register_s3_access_grants,
    resolve_bucket_region,
    s3_region_kwargs,
    scoped_list_prefix,
    _detect_access_grants_fallback,
)
from git_remote_s3.manage import Doctor


@pytest.fixture(autouse=True)
def reset_module_state():
    common._access_grants_fallback_notified = False
    common._bucket_region_cache.clear()
    yield
    common._access_grants_fallback_notified = False
    common._bucket_region_cache.clear()


def _fake_request(*, request_credentials=None, signing=True):
    context = {}
    if signing:
        context["signing"] = {}
        if request_credentials is not None:
            context["signing"]["request_credentials"] = request_credentials
    return SimpleNamespace(context=context)


@patch("git_remote_s3.common.S3AccessGrantsPlugin")
def test_register_s3_access_grants_registers_plugin_with_fallback(plugin_cls):
    client = MagicMock()
    session = MagicMock()

    returned = register_s3_access_grants(client, session)

    assert returned is client
    plugin_cls.assert_called_once_with(client, fallback_enabled=True, customer_session=session._session)
    plugin_cls.return_value.register.assert_called_once_with()
    client.meta.events.register.assert_called_once_with("before-sign.s3", _detect_access_grants_fallback)


@patch("git_remote_s3.common.S3AccessGrantsPlugin")
@patch("boto3.Session")
def test_s3remote_registers_plugin_on_its_client(session_cls, plugin_cls):
    session = session_cls.return_value
    client = session.client.return_value

    S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")

    plugin_cls.assert_called_once_with(client, fallback_enabled=True, customer_session=session._session)
    client.meta.events.register.assert_called_once_with("before-sign.s3", _detect_access_grants_fallback)


@patch("git_remote_s3.common.S3AccessGrantsPlugin")
@patch("boto3.Session")
def test_s3remote_auth_probe_uses_trailing_slash_prefix(session_cls, plugin_cls):
    # The existence-check probe must scope its listing to exactly this repo,
    # so a sibling repo whose prefix is a string-prefix of ours (e.g.
    # "core/climate" for "core/cli") is never cross-listed.
    client = session_cls.return_value.client.return_value

    S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")

    client.list_objects_v2.assert_called_once_with(Bucket="test_bucket", Prefix="test_prefix/")


@patch("git_remote_s3.common.S3AccessGrantsPlugin")
@patch("boto3.Session")
def test_s3remote_auth_probe_empty_prefix_lists_whole_bucket(session_cls, plugin_cls):
    # A bucket-root repo (prefix == "") must list Prefix="" rather than the
    # never-matching "/".
    client = session_cls.return_value.client.return_value

    S3Remote(UriScheme.S3, None, "test_bucket", "")

    client.list_objects_v2.assert_called_once_with(Bucket="test_bucket", Prefix="")


def test_scoped_list_prefix_empty():
    assert scoped_list_prefix("") == ""


def test_scoped_list_prefix_bare():
    assert scoped_list_prefix("core/cli") == "core/cli/"


def test_scoped_list_prefix_already_trailing_slash():
    assert scoped_list_prefix("core/cli/") == "core/cli/"


def test_fallback_notice_fires_once_to_stderr(capsys):
    request = _fake_request()

    _detect_access_grants_fallback(request=request)
    _detect_access_grants_fallback(request=request)
    _detect_access_grants_fallback(request=request)

    captured = capsys.readouterr()
    assert captured.out == ""
    lines = [line for line in captured.err.splitlines() if line]
    assert lines == [common._ACCESS_GRANTS_FALLBACK_NOTICE]


def test_no_notice_when_access_grants_creds_vended(capsys):
    request = _fake_request(request_credentials=object())

    _detect_access_grants_fallback(request=request)

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_no_notice_when_request_missing(capsys):
    _detect_access_grants_fallback(request=None)
    _detect_access_grants_fallback()

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def _session_with_head(*, head_return=None, head_error=None):
    session = MagicMock()
    client = session.client.return_value
    client.exceptions.ClientError = ClientError
    if head_error is not None:
        client.head_bucket.side_effect = head_error
    else:
        client.head_bucket.return_value = head_return
    return session


def _client_error(code, operation, headers=None, message="denied"):
    response = {"Error": {"Code": code, "Message": message}}
    if headers is not None:
        response["ResponseMetadata"] = {"HTTPHeaders": headers}
    return ClientError(response, operation)


def test_resolve_bucket_region_from_head_bucket_response():
    session = _session_with_head(head_return={"BucketRegion": "eu-west-1"})

    assert resolve_bucket_region(session, "bucket") == "eu-west-1"


def test_resolve_bucket_region_from_success_response_header():
    session = _session_with_head(
        head_return={"ResponseMetadata": {"HTTPHeaders": {"x-amz-bucket-region": "ap-south-1"}}}
    )

    assert resolve_bucket_region(session, "bucket") == "ap-south-1"


def test_resolve_bucket_region_from_client_error_header():
    err = _client_error("403", "HeadBucket", headers={"x-amz-bucket-region": "us-west-2"})
    session = _session_with_head(head_error=err)

    assert resolve_bucket_region(session, "bucket") == "us-west-2"


def test_resolve_bucket_region_returns_none_when_unavailable():
    err = _client_error("500", "HeadBucket", headers={})
    session = _session_with_head(head_error=err)

    assert resolve_bucket_region(session, "bucket") is None


def test_resolve_bucket_region_is_cached_per_process():
    session = _session_with_head(head_return={"BucketRegion": "eu-west-1"})

    resolve_bucket_region(session, "bucket")
    resolve_bucket_region(session, "bucket")

    session.client.return_value.head_bucket.assert_called_once()


def test_s3_region_kwargs_pins_when_region_known():
    session = _session_with_head(head_return={"BucketRegion": "eu-west-1"})

    assert s3_region_kwargs(session, "bucket") == {"region_name": "eu-west-1"}


def test_s3_region_kwargs_empty_when_region_unknown():
    err = _client_error("500", "HeadBucket", headers={})
    session = _session_with_head(head_error=err)

    assert s3_region_kwargs(session, "bucket") == {}


@patch("git_remote_s3.common.S3AccessGrantsPlugin")
@patch("boto3.Session")
def test_s3remote_pins_client_to_bucket_region(session_cls, plugin_cls):
    session = session_cls.return_value
    session.client.return_value.head_bucket.return_value = {"BucketRegion": "us-west-2"}

    S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")

    session.client.assert_any_call("s3", region_name="us-west-2")


def _doctor_with_probe(probe, prefix="prefix"):
    with (
        patch("boto3.Session"),
        patch("git_remote_s3.manage.register_s3_access_grants"),
        patch("git_remote_s3.manage.s3_region_kwargs", return_value={}),
        patch(
            "git_remote_s3.manage.register_s3_access_grants_strict",
            return_value=probe,
        ),
    ):
        doctor = Doctor(None, "bucket", prefix, False)
        doctor.check_access_grants()


def _probe_raising(error):
    probe = MagicMock()
    probe.exceptions.ClientError = ClientError
    probe.list_objects_v2.side_effect = error
    return probe


def test_doctor_maps_access_denied_on_instance_for_prefix(capsys):
    probe = _probe_raising(_client_error("AccessDenied", "GetAccessGrantsInstanceForPrefix"))

    _doctor_with_probe(probe)

    out = capsys.readouterr().out
    assert "not available" in out
    assert "GetAccessGrantsInstanceForPrefix failed: AccessDenied" in out
    assert "missing s3:GetAccessGrantsInstanceForPrefix" in out


def test_doctor_maps_access_denied_on_get_data_access(capsys):
    probe = _probe_raising(_client_error("AccessDenied", "GetDataAccess"))

    _doctor_with_probe(probe)

    out = capsys.readouterr().out
    assert "not available" in out
    assert "GetDataAccess failed: AccessDenied" in out
    assert "missing s3:GetDataAccess or has no matching grant" in out


def test_doctor_reports_ok_on_success(capsys):
    probe = MagicMock()
    probe.exceptions.ClientError = ClientError
    probe.list_objects_v2.return_value = {"Contents": []}

    _doctor_with_probe(probe)

    out = capsys.readouterr().out
    assert "Access Grants: OK" in out
    assert "s3://bucket/prefix/" in out
    probe.list_objects_v2.assert_called_once_with(Bucket="bucket", Prefix="prefix/")


def test_doctor_reports_ok_on_success_empty_prefix_no_stray_slash(capsys):
    probe = MagicMock()
    probe.exceptions.ClientError = ClientError
    probe.list_objects_v2.return_value = {"Contents": []}

    _doctor_with_probe(probe, prefix="")

    out = capsys.readouterr().out
    assert "Access Grants: OK" in out
    assert "s3://bucket/" in out
    assert "s3://bucket//" not in out
    probe.list_objects_v2.assert_called_once_with(Bucket="bucket", Prefix="")
