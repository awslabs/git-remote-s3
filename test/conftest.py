# SPDX-FileCopyrightText: 2023-present Amazon.com, Inc. or its affiliates
#
# SPDX-License-Identifier: Apache-2.0

import pytest

from git_remote_s3 import common


@pytest.fixture(autouse=True)
def _reset_bucket_region_cache():
    # resolve_bucket_region caches per process; isolate that state between tests
    # so a region resolved in one test does not suppress the HeadBucket probe in
    # the next.
    common._bucket_region_cache.clear()
    yield
    common._bucket_region_cache.clear()
