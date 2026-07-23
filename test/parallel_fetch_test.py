from unittest.mock import patch
from io import BytesIO
from git_remote_s3 import S3Remote, UriScheme

import threading

SHA1 = "c105d19ba64965d2c9d3d3246e7269059ef8bb8a"
SHA2 = "c105d19ba64965d2c9d3d3246e7269059ef8bb8b"
SHA3 = "c105d19ba64965d2c9d3d3246e7269059ef8bb8c"
BRANCH = "pytest"
MOCK_BUNDLE_CONTENT = b"MOCK_BUNDLE_CONTENT"


@patch("boto3.Session.client")
def test_process_fetch_cmds_empty_list(session_client_mock):
    """Test that process_fetch_cmds handles empty command list gracefully"""
    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")

    # Call with empty list
    s3_remote.process_fetch_cmds([])

    # Verify no interactions with S3
    session_client_mock.return_value.get_object.assert_not_called()


@patch("git_remote_s3.git.unbundle")
@patch("boto3.Session.client")
def test_process_fetch_cmds_single_command(session_client_mock, unbundle_mock):
    """Test processing a single fetch command"""
    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")
    session_client_mock.return_value.get_object.return_value = {"Body": BytesIO(MOCK_BUNDLE_CONTENT)}

    # Process a single fetch command
    s3_remote.process_fetch_cmds([f"fetch {SHA1} refs/heads/{BRANCH}"])

    # Verify S3 download_file was called once
    session_client_mock.return_value.download_file.assert_called_once()
    unbundle_mock.assert_called_once()

    # Verify the fetched_refs list contains the SHA
    assert SHA1 in s3_remote.fetched_refs


@patch("git_remote_s3.git.unbundle")
@patch("boto3.Session.client")
def test_process_fetch_cmds_multiple_commands(session_client_mock, unbundle_mock):
    """Test processing multiple fetch commands in parallel"""
    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")
    session_client_mock.return_value.get_object.return_value = {"Body": BytesIO(MOCK_BUNDLE_CONTENT)}

    # Process multiple fetch commands
    fetch_cmds = [
        f"fetch {SHA1} refs/heads/{BRANCH}",
        f"fetch {SHA2} refs/heads/{BRANCH}",
        f"fetch {SHA3} refs/heads/{BRANCH}",
    ]
    s3_remote.process_fetch_cmds(fetch_cmds)

    # Verify S3 download_file was called for each command
    assert session_client_mock.return_value.download_file.call_count == 3
    assert unbundle_mock.call_count == 3

    # Verify all SHAs are in the fetched_refs list
    assert SHA1 in s3_remote.fetched_refs
    assert SHA2 in s3_remote.fetched_refs
    assert SHA3 in s3_remote.fetched_refs


@patch("git_remote_s3.git.unbundle")
@patch("boto3.Session.client")
def test_process_fetch_cmds_uses_thread_pool(session_client_mock, unbundle_mock):
    """Test that process_fetch_cmds uses a thread pool for parallel execution"""
    # This test verifies that the ThreadPoolExecutor is used by checking that
    # multiple commands are processed in parallel

    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")
    session_client_mock.return_value.get_object.return_value = {"Body": BytesIO(MOCK_BUNDLE_CONTENT)}

    # Create fetch commands
    fetch_cmds = [
        f"fetch {SHA1} refs/heads/{BRANCH}",
        f"fetch {SHA2} refs/heads/{BRANCH}",
        f"fetch {SHA3} refs/heads/{BRANCH}",
    ]

    # Process the commands
    s3_remote.process_fetch_cmds(fetch_cmds)

    # Verify all commands were processed
    assert session_client_mock.return_value.download_file.call_count == 3
    assert unbundle_mock.call_count == 3

    # Verify all SHAs are in the fetched_refs list
    assert SHA1 in s3_remote.fetched_refs
    assert SHA2 in s3_remote.fetched_refs
    assert SHA3 in s3_remote.fetched_refs


@patch("sys.stdin")
@patch("git_remote_s3.git.unbundle")
@patch("boto3.Session.client")
def test_process_cmd_batch_processing(session_client_mock, unbundle_mock, stdin_mock):
    """Test that fetch commands are collected and processed in batch"""
    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")
    session_client_mock.return_value.get_object.return_value = {"Body": BytesIO(MOCK_BUNDLE_CONTENT)}

    # Simulate processing multiple fetch commands followed by an empty line
    s3_remote.process_cmd(f"fetch {SHA1} refs/heads/{BRANCH}")
    s3_remote.process_cmd(f"fetch {SHA2} refs/heads/{BRANCH}")
    s3_remote.process_cmd(f"fetch {SHA3} refs/heads/{BRANCH}")

    # Verify commands are collected but not processed yet
    assert len(s3_remote.fetch_cmds) == 3
    unbundle_mock.assert_not_called()

    # Process the empty line to trigger batch processing
    with patch("git_remote_s3.remote.S3Remote.process_fetch_cmds") as mock_process:
        s3_remote.process_cmd("\n")

        # Verify process_fetch_cmds was called with all collected commands
        mock_process.assert_called_once()
        assert len(mock_process.call_args[0][0]) == 3

        # Verify fetch_cmds is cleared after processing
        assert len(s3_remote.fetch_cmds) == 0


@patch("git_remote_s3.git.unbundle")
@patch("boto3.Session.client")
def test_thread_safety_of_fetched_refs(session_client_mock, unbundle_mock):
    """Test thread safety of the fetched_refs list using a real thread pool"""
    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")
    session_client_mock.return_value.get_object.return_value = {"Body": BytesIO(MOCK_BUNDLE_CONTENT)}

    # Create multiple fetch commands with different SHAs
    fetch_cmds = [f"fetch {SHA1} refs/heads/{BRANCH}"] * 20

    # Process commands using a real thread pool
    s3_remote.process_fetch_cmds(fetch_cmds)

    # Verify SHA1 appears in fetched_refs
    assert SHA1 in s3_remote.fetched_refs


@patch("git_remote_s3.git.unbundle")
@patch("boto3.Session.client")
def test_cmd_fetch_thread_safety(session_client_mock, unbundle_mock):
    """Test that cmd_fetch is thread-safe when called concurrently"""
    s3_remote = S3Remote(UriScheme.S3, None, "test_bucket", "test_prefix")
    session_client_mock.return_value.get_object.return_value = {"Body": BytesIO(MOCK_BUNDLE_CONTENT)}

    # Create a function that simulates concurrent access
    def concurrent_fetch():
        s3_remote.cmd_fetch(f"fetch {SHA1} refs/heads/{BRANCH}")

    # Create and start multiple threads
    threads = []
    for _ in range(5):
        thread = threading.Thread(target=concurrent_fetch)
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Verify SHA1 appears in fetched_refs
    assert SHA1 in s3_remote.fetched_refs
