# Security Policy

## Supported Versions

This project is maintained on a rolling basis against the latest released version on
[PyPI](https://pypi.org/project/fduplex-git-remote-s3/). Security fixes are made against the `main` branch and
released as a new version; older versions are not separately patched.

## Reporting a Vulnerability

**Please do not open a public GitHub issue for security vulnerabilities.**

We prefer reports via GitHub's private vulnerability reporting feature:

1. Go to the [Security tab](https://github.com/fduplex/git-remote-s3/security) of this repository.
2. Click "Report a vulnerability" to open a private advisory.

If you are unable to use GitHub's private reporting, you may instead email **contact@fullduplex.media** with
details of the issue.

This is a small, single-maintainer open source project maintained on a best-effort basis. We don't commit to a
specific response-time SLA, but we take security reports seriously and will acknowledge receipt and work with
you on a fix and coordinated disclosure timeline.

## Scope

This repository (`fduplex-git-remote-s3` and the `git-remote-s3`, `git-lfs-s3`, and `git-s3` commands it
installs) is in scope. Vulnerabilities in upstream dependencies (e.g. `boto3`, `git`, `git-lfs`) should be
reported to their respective maintainers.
