from mock import patch
from git_remote_s3.lfs import _lfs_only_url
import subprocess

REMOTE_ORIGIN = "origin"
REMOTE_UPSTREAM = "upstream"
EXPECTED_URL = "s3://bucket-name/path/to"
LFS_CONFIG_URL = "s3://lfsconfig-bucket/path"


@patch("git_remote_s3.lfs.subprocess.run")
def test_lfs_only_url_from_lfsconfig(mock_run):
    """Test that _lfs_only_url returns URL from .lfsconfig when available"""
    # Mock successful .lfsconfig lookup
    mock_result = subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=EXPECTED_URL.encode("utf-8"),
        stderr=b""
    )
    mock_run.return_value = mock_result
    
    result = _lfs_only_url(REMOTE_ORIGIN)
    
    assert result == EXPECTED_URL
    # Verify it was called with .lfsconfig first
    assert mock_run.call_count == 1
    call_args = mock_run.call_args[0][0]
    assert "--file" in call_args
    assert ".lfsconfig" in call_args
    assert f"remote.{REMOTE_ORIGIN}.lfsurl" in call_args


@patch("git_remote_s3.lfs.subprocess.run")
def test_lfs_only_url_from_git_config(mock_run):
    """Test that _lfs_only_url falls back to .git/config when .lfsconfig is not available"""
    # Mock .lfsconfig lookup failing, then .git/config succeeding
    def side_effect(*args, **kwargs):
        if "--file" in args[0] and ".lfsconfig" in args[0]:
            # First call: .lfsconfig fails
            return subprocess.CompletedProcess(
                args=args[0],
                returncode=1,
                stdout=b"",
                stderr=b""
            )
        else:
            # Second call: .git/config succeeds
            return subprocess.CompletedProcess(
                args=args[0],
                returncode=0,
                stdout=EXPECTED_URL.encode("utf-8"),
                stderr=b""
            )
    
    mock_run.side_effect = side_effect
    
    result = _lfs_only_url(REMOTE_ORIGIN)
    
    assert result == EXPECTED_URL
    # Verify both calls were made
    assert mock_run.call_count == 2
    # First call should be .lfsconfig
    first_call = mock_run.call_args_list[0][0][0]
    assert "--file" in first_call
    assert ".lfsconfig" in first_call
    # Second call should be .git/config
    second_call = mock_run.call_args_list[1][0][0]
    assert "--get" in second_call
    assert f"remote.{REMOTE_ORIGIN}.lfsurl" in second_call


@patch("git_remote_s3.lfs.subprocess.run")
def test_lfs_only_url_returns_none_when_not_found(mock_run):
    """Test that _lfs_only_url returns None when neither config has the URL"""
    # Mock both lookups failing
    mock_result = subprocess.CompletedProcess(
        args=[],
        returncode=1,
        stdout=b"",
        stderr=b""
    )
    mock_run.return_value = mock_result
    
    result = _lfs_only_url(REMOTE_ORIGIN)
    
    assert result is None
    # Verify both calls were made
    assert mock_run.call_count == 2


@patch("git_remote_s3.lfs.subprocess.run")
def test_lfs_only_url_handles_empty_stdout(mock_run):
    """Test that _lfs_only_url handles empty stdout even with returncode 0"""
    # Mock .lfsconfig returning success but empty stdout
    def side_effect(*args, **kwargs):
        if "--file" in args[0] and ".lfsconfig" in args[0]:
            return subprocess.CompletedProcess(
                args=args[0],
                returncode=0,
                stdout=b"",  # Empty stdout
                stderr=b""
            )
        else:
            # .git/config also returns empty
            return subprocess.CompletedProcess(
                args=args[0],
                returncode=0,
                stdout=b"",
                stderr=b""
            )
    
    mock_run.side_effect = side_effect
    
    result = _lfs_only_url(REMOTE_ORIGIN)
    
    assert result is None
    assert mock_run.call_count == 2


@patch("git_remote_s3.lfs.subprocess.run")
def test_lfs_only_url_handles_whitespace_in_url(mock_run):
    """Test that _lfs_only_url properly strips whitespace from URLs"""
    url_with_whitespace = "  s3://bucket-name/path/to  \n"
    expected_url = "s3://bucket-name/path/to"
    
    mock_result = subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=url_with_whitespace.encode("utf-8"),
        stderr=b""
    )
    mock_run.return_value = mock_result
    
    result = _lfs_only_url(REMOTE_ORIGIN)
    
    assert result == expected_url


@patch("git_remote_s3.lfs.subprocess.run")
def test_lfs_only_url_priority_lfsconfig_over_git_config(mock_run):
    """Test that .lfsconfig takes priority over .git/config even if both exist"""
    # Mock .lfsconfig succeeding (should return this, not .git/config)
    mock_result = subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=LFS_CONFIG_URL.encode("utf-8"),
        stderr=b""
    )
    mock_run.return_value = mock_result
    
    result = _lfs_only_url(REMOTE_ORIGIN)
    
    assert result == LFS_CONFIG_URL
    # Should only call .lfsconfig, not .git/config
    assert mock_run.call_count == 1


@patch("git_remote_s3.lfs.subprocess.run")
def test_lfs_only_url_with_different_remote_name(mock_run):
    """Test that _lfs_only_url works with different remote names"""
    mock_result = subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=EXPECTED_URL.encode("utf-8"),
        stderr=b""
    )
    mock_run.return_value = mock_result
    
    result = _lfs_only_url(REMOTE_UPSTREAM)
    
    assert result == EXPECTED_URL
    # Verify the remote name is used correctly in the config key
    call_args = mock_run.call_args[0][0]
    assert f"remote.{REMOTE_UPSTREAM}.lfsurl" in call_args

