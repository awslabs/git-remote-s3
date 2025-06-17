# git-remote-s3

This library enables to use Amazon S3 as a git remote and LFS server.

It provides an implementation of a [git remote helper](https://git-scm.com/docs/gitremote-helpers) to use S3 as a serverless Git server.

It also provide an implementation of the [git-lfs custom transfer](https://github.com/git-lfs/git-lfs/blob/main/docs/custom-transfers.md) to enable pushing LFS managed files to the same S3 bucket used as remote.

## Table of Contents

- [Installation](#installation)
- [Prerequisites](#prerequisites)
- [Security](#security)
  - [Data encryption](#data-encryption)
  - [Access control](#access-control)
- [Use S3 remotes](#use-s3-remotes)
  - [Create a new repo](#create-a-new-repo)
  - [Clone a repo](#clone-a-repo)
  - [Branches, etc.](#branches-etc)
  - [Using S3 remotes for submodules](#using-s3-remotes-for-submodules)
- [Repo as S3 Source for AWS CodePipeline](#repo-as-s3-source-for-aws-codepipeline)
  - [Archive file location](#archive-file-location)
  - [Example AWS CodePipeline source action config](#example-aws-codepipeline-source-action-config)
- [LFS](#lfs)
  - [Creating the repo and pushing](#creating-the-repo-and-pushing)
  - [Clone the repo](#clone-the-repo)
- [Notes about specific behaviors of Amazon S3 remotes](#notes-about-specific-behaviors-of-amazon-s3-remotes)
  - [Arbitrary Amazon S3 URIs](#arbitrary-amazon-s3-uris)
  - [Concurrent writes](#concurrent-writes)
- [Manage the Amazon S3 remote](#manage-the-amazon-s3-remote)
  - [Delete branches](#delete-branches)
  - [Protected branches](#protected-branches)
- [Under the hood](#under-the-hood)
  - [How S3 remote work](#how-s3-remote-work)
  - [How LFS work](#how-lfs-work)
  - [Debugging](#debugging)
- [Credits](#credits)

## Installation

`git-remote-s3` is a Python script and works with any Python version >= 3.9.

Run:

```
pip install git-remote-s3
```

## Prerequisites

Before you can use `git-remote-s3`, you must:

- Complete initial configuration:

  - Creating an AWS account
  - Configuring an IAM user or role

- Create an AWS S3 bucket (or have one already) in your AWS account.
- Attach a minimal policy to that user/role that allows the to the S3 bucket:

  ```json
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Sid": "S3ObjectAccess",
        "Effect": "Allow",
        "Action": ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"],
        "Resource": ["arn:aws:s3:::<BUCKET>/*"]
      },
      {
        "Sid": "S3ListAccess",
        "Effect": "Allow",
        "Action": ["s3:ListBucket"],
        "Resource": ["arn:aws:s3:::<BUCKET>"]
      }
    ]
  }
  ```

- Optional (but recommended) - use [SSE-KMS Bucket keys to encrypt the content of the bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucket-key.html), ensure the user/role create previously has the permission to access and use the key.

```json
{
  "Sid": "KMSAccess",
  "Effect": "Allow",
  "Action": ["kms:Decrypt", "kms:GenerateDataKey"],
  "Resource": ["arn:aws:kms:<REGION>:<ACCOUNT>:key/<KEY_ID>"]
}
```

- Install Python and its package manager, pip, if they are not already installed. To download and install the latest version of Python, [visit the Python website](https://www.python.org/).
- Install Git on your Linux, macOS, Windows, or Unix computer.
- Install the latest version of the AWS CLI on your Linux, macOS, Windows, or Unix computer. You can find instructions [here](https://docs.aws.amazon.com/cli/latest/userguide/installing.html).

## Security

### Data encryption

All data is encrypted at rest and in transit by default. To add an additional layer of security you can use customer managed KMS keys to encrypt the data at rest on the S3 bucket. We recommend to use [Bucket keys](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucket-key.html) to minimize the KMS costs.

### Access control

Access control to the remote is ensured via IAM permissions, and can be controlled at:

- bucket level
- prefix level (you can use prefixes to store multiple repos in the same S3 bucket thus minimizing the setup effort)
- KMS key level

If you store multiple repos in a single bucket but would like to separate permissions to access each repo, you can do so by modifying the resource definitions for the object related action to specify the repo prefix and by adding a condition to the ListBucket action to restrict the operation to matching prefixes (and by consequence the corresponding repo) :

```json
      {
        "Sid": "S3ObjectAccess",
        "Effect": "Allow",
        "Action": [
          "s3:PutObject",
          "s3:GetObject",
          "s3:DeleteObject"
        ],
        "Resource": ["arn:aws:s3:::<BUCKET>/<REPO>/*"]
      },
      {
        "Sid": "S3ListObjects",
        "Effect": "Allow",
        "Action": [
          "s3:ListBucket",
        ],
        "Condition": {
          "StringEquals": {
            "s3:prefix": "<REPO>"
          }
        },
        "Resource": ["arn:aws:s3:::<BUCKET>"]
      },
```

Using the condition key restricts the access operation to the content of the specific repo in the bucket.

## Use S3 remotes

### Create a new repo

S3 remotes are identified by the prefix `s3://` and at the bare minimum specify the name of the bucket. You can also provide a key prefix as in `s3://my-git-bucket/my-repo` and a profile `s3://my-profile@my-git-bucket/myrepo`.

```bash
mkdir my-repo
cd my-repo
git init
git remote add origin s3://my-git-bucket/my-repo
```

You can then add a file, commit and push the changes to the remote:

```bash
echo "Hello" > hello.txt
git add -A
git commit -a -m "hello"
git push --set-upstream origin main
```

The remote HEAD is set to track the branch that has been pushed first to the remote repo. To change the remote HEAD branch, delete the HEAD object `s3://<bucket>/<prefix>/HEAD` and then run `git-remote-s3 doctor s3://<bucket>/<prefix>`.

When you use `s3+zip://` instead of `s3://`, an additional zip archive named `repo.zip` is uploaded next to the `sha.bundle` file. This is for example useful if you want to use the Repo as a S3 Source for AWS CodePipeline, which expects a `.zip` file. The path on S3 when you push to the `main` branch is for example `refs/heads/main/repo.zip`. See [How S3 remote work](#how-s3-remote-work) for more details about the bundle file.

### Clone a repo

To clone the repo to another folder just use the normal git syntax using the s3 URI as remote:

```bash
git clone s3://my-git-bucket/my-repo my-repo-clone
```

### Branches, etc.

Creating branches and pushing them works as normal:

```bash
cd my-repo
git checkout -b new_branch
touch new_file.txt
git add -A
git commit -a -m "new file"
git push origin new_branch
```

All git operations that do not rely on communication with the server should work as usual (eg `git merge`)

### Using S3 remotes for submodules

If you have a repo that uses submodules also hosted on S3, you need to run the following command:

```
git config protocol.s3.allow always
```

Or, to enable globally:

```
git config --global protocol.s3.allow always
```

## Repo as S3 Source for AWS CodePipeline

[AWS CodePipeline](https://aws.amazon.com/codepipeline/) offers an [Amazon S3 source action](https://docs.aws.amazon.com/codepipeline/latest/userguide/integrations-action-type.html#integrations-source-s3)
as location for your source code and application files. But this requires to `upload the source files as a single ZIP file`.
As briefly mentioned in [Create a new repo](#create-a-new-repo), `git-remote-s3` can create and upload zip archives.
When you use `s3+zip` as URI Scheme when you add the remote, `git-remote-s3` will automatically upload an archive that can be used by AWS CodePipeline.

### Archive file location

Let's assume your bucket name is `my-git-bucket` and the repo is called `my-repo`. Run `git remote add origin s3+zip://my-git-bucket/my-repo` to use it as remote.
When you now commit your changes and push to the remote, an additional `repo.zip` file will be uploaded to the bucket.
For example, if you push to the `main` branch (`git push origin main`), the file is available under `s3://my-git-bucket/my-repo/refs/heads/main/repo.zip`.
When you push to a branch called `fix_a_bug` it's available under `s3://my-git-bucket/my-repo/refs/heads/fix_a_bug/repo.zip`.
And if you create and push a tag called `v1.0` it will be `s3://my-git-bucket/my-repo/refs/tags/v1.0/repo.zip`.

### Example AWS CodePipeline source action config

Your AWS CodePipeline Action configuration to trigger when you update your `main` branch:

- Action Provider: `Amazon S3`
- Bucket: `my-git-bucket`
- S3 object key: `my-repo/refs/heads/main/repo.zip`
- Change detection options: `AWS CodePipeline`

Visit [Tutorial: Create a simple pipeline (S3 bucket)](https://docs.aws.amazon.com/codepipeline/latest/userguide/tutorials-simple-s3.html) to learn more about a S3 bucket as source action.

## LFS

To use LFS you need to first install git-lfs. You can refer to the [official documentation](https://git-lfs.com/) on how to do this on your system.

Next, you need enable the S3 integration by running the following command in the repo folder:

```bash
git-lfs-s3 install
```

which is a short cut for:

```bash
git config --add lfs.customtransfer.git-lfs-s3.path git-lfs-s3
git config --add lfs.standalonetransferagent git-lfs-s3
```

### Creating the repo and pushing

Let's assume we want to store TIFF file in LFS.

```bash
mkdir lfs-repo
cd lfs-repo
git init
git lfs install
git-lfs-s3 install
git lfs track "*.tiff"
git add .gitattributes
<put file.tiff in the repo>
git add file.tiff
git commit -a -m "my first tiff file"
git remote add origin s3://my-git-bucket/lfs-repo
git push --set-upstream origin main
```

### Clone the repo

When cloning a repo using the S3 remote for LFS, `git-lfs` can't know how to fetch the files since we have yet to add the configuration.

It involves 2 extra steps.

```bash
% git clone s3://my-git-bucket/lfs-repo lfs-repo-clone
Error downloading object: file.tiff (54238cf): Smudge error: Error downloading file.tiff (54238cfaaaa42dda05da0e12bf8ee3156763fa35296085ccdef63b13a87837c5): batch request: ssh: Could not resolve hostname s3: Name or service not known: exit status 255
...
```

To fix:

```bash
cd lfs-repo-clone
git-lfs-s3 install
git reset --hard main
```

## Notes about specific behaviors of Amazon S3 remotes

### Arbitrary Amazon S3 URIs

An Amazon S3 URI for a valid bucket and an arbitrary prefix which does not contain the right structure under it, is considered valid.

`git ls-remote` returns an empty list and `git clone` clones an empty repository for which the S3 URI is set as remote origin.

```
% git clone s3://my-git-bucket/this-is-a-new-repo
Cloning into 'this-is-a-new-repo'...
warning: You appear to have cloned an empty repository.
% cd this-is-a-new-repo
% git remote -v
origin  s3://my-git-bucket/this-is-a-new-repo (fetch)
origin  s3://my-git-bucket/this-is-a-new-repo (push)
```

**Tip**: This behavior can be used to quickly create a new git repo.

### Concurrent writes

Due to the distributed nature of `git`, there might be cases (albeit rare) where 2 or more `git push` are executed at the same time by different user with their own modification of the same branch.

The git command executes the push in 2 steps:

1. first it checks if the remote reference is the correct ancestor for the commit being pushed
2. if that is correct it invokes the `git-remote-s3` command which writes the bundle to the S3 bucket at the `refs/heads/<branch>` path

In case two (or more) `git push` command are executed at the same time from different clients, at step 1 the same valid ref is fetched, hence both clients proceed with step 2, resulting in multiple bundles being stored in S3.

The branch has now multiple head references, and any subsequent `git push` fails with the error:

```
error: dst refspec refs/heads/<branch>> matches more than one
error: failed to push some refs to 's3://<bucket>/<prefix>'
```

To fix this issue, run the `git-remote-s3 doctor <s3-uri>` command. By default it will create a new branch for every bundle that should not be retained. The user can then checkout the branch locally and merge it to the original branch. If you want instead to remove the bundle, specify `--delete-bundle`.

## Manage the Amazon S3 remote

### Delete branches

To remove remote branches that are not used anymore you can use the `git-s3 delete-branch <s3uri> -b <branch_name>` command. This command deletes the bundle object(s) from Amazon S3 under the branch path.

### Protected branches

To protect/unprotect a branch run `git s3 protect <remote> <branch-name>` respectively `git s3 unprotect <remote> <branch-name>`.

## Under the hood

### How S3 remote work

Bundles are stored in the S3 bucket as `<prefix>/<ref>/<sha>.bundle`.

When listing remote ref (eg explicitly via `git ls-remote`) we list all the keys present under the given `<prefix>`.

When pushing a new ref (eg a commit), we get the sha of the ref, we bundle the ref via `git bundle create <sha>.bundle <ref>` and store it to S3 according the schema above.

If the push is successful, the code removes the previous bundle associated to the ref.

If two user concurrently push a commit based on the same current branch head to the remote both bundles would be written to the repo and the current bundle removed. No data is lost, but no further push will be possible until all bundles but one are removed.
For this you can use the `git s3 doctor <remote>` command.

### How LFS work

The LFS integration stores the file in the bucket defined by the remote URI, under a key `<prefix>/lfs/<oid>`, where oid is the unique identifier assigned by git-lfs to the file.

If an object with the same key already exists, git-lfs-s3 does not upload it again.

### Debugging

Use `--verbose` flag or set `transfer.verbosity=2` to print debug information when performing git operations:

```bash
git -c transfer.verbosity=2 push origin main
```

For early errors (like credential issues), use the environment variable:

```bash
GIT_REMOTE_S3_VERBOSE=1 git push origin main
```

Logs will be put to stderr.

For LFS operations you can enable and disable debug logging via `git-lfs-s3 enable-debug` and `git-lfs-s3 disable-debug` respectively. Logs are put in `.git/lfs/tmp/git-lfs-s3.log` in the repo.

## Credits

The git S3 integration was inspired by the work of Bryan Gahagan on [git-remote-s3](https://github.com/bgahagan/git-remote-s3).

The LFS implementation benefitted from [lfs-s3](https://github.com/nicolas-graves/lfs-s3) by [@nicolas-graves](https://github.com/nicolas-graves). If you do not need to use the git-remote-s3 transport you should use that project.
