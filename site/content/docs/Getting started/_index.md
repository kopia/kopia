
---
title: "Getting Started"
linkTitle: "Getting Started"
weight: 1
---

This guide will walk you through installing Kopia, creating, managing and restoring snapshots and defining snapshot policies. 

Make sure to familiarize yourself with Kopia [features](../features/) before following along.

## Installation

Kopia is distributed as a single command-line (CLI) binary called `kopia`. 

To install it follow the [Installation Guide](../installation/).

## Kopia UI

If you prefer a graphical user interface,
Kopia comes with a user-friendly desktop app for Windows, macOS, and Linux called `KopiaUI`which runs in the background and allows you to create snapshots, define policies and restore files quickly. More advanced features require the use of the CLI tool.

The Kopia UI is new and experimental. See the tutorial on Youtube:

{{< youtube sHJjSpasWIo >}}

## Setting Up Repository

Repository is a place where Kopia stores all its snapshot data. It's typically remote storage, such as [Google Cloud Storage](https://cloud.google.com/storage/), [Amazon S3](https://aws.amazon.com/s3/) or similar. You can also use any locally-mounted storage (using SMB, NFS or similar). For more details about repository see [Architecture](../architecture/).

To create a repository use one of the subcommands of `kopia repository create`. 

> NOTE: This guide focuses on simple scenarios, more command-line features are described in the [Command-Line Reference](../reference/command-line/).

When creating the repository must provide a password that will be used to encrypt all files. The password never leaves your machine and is never sent to the server.

**There's absolutely no way to recover contents of the repository if you forget the password. Remember to keep it secure!**

### Filesystem

To create a repository in a locally-mounted filesystem simply use:

```shell
$ kopia repository create filesystem --path /tmp/my-repository
```

We can examine the directory to see which files were created. As you can see Kopia uses sharded directory structure to optimize performance.

```shell
$ find /tmp/my-repository -type f
/tmp/my-repository/n1b/d00/aa56a13c1140142b39befb654a2.f
/tmp/my-repository/pea/8b5/e01d92618653ffbf2bb9961448d.f
/tmp/my-repository/kopia.repository.f
```

### Google Cloud Storage

To create a repository in Google Cloud Storage you need to provision a storage bucket and install local credentials that can access that bucket. To do so:

1. Create a storage bucket in [Google Cloud Console](https://console.cloud.google.com/storage/)
2. Install [Google Cloud SDK](https://cloud.google.com/sdk/)
3. Log in with credentials that have permissions to the bucket.

```shell
$ gcloud auth application-default login
```

After these preparations we can create Kopia repository (assuming bucket named `kopia-test-123`):

```shell
$ kopia repository create google --bucket kopia-test-123
```

At this point we should be able to confirm that Kopia has created the skeleton of the repository with 3
files in it:

```shell
$ gsutil ls gs://kopia-test-123
gs://kopia-test-123/kopia.repository
gs://kopia-test-123/n417ffc2adc8dbe93f1814eda3ba8a07c
gs://kopia-test-123/p78e034ac8b891168df97f9897d7ec316
```

## Connecting To Repository

To connect to a repository that already, simply use `kopia repository connect` instead of `kopia repository create`. You can connect as many computers as you like to any repository, even simultaneously.

For example:

```shell
$ kopia repository connect filesystem --path /tmp/my-repository
$ kopia repository connect google --bucket kopia-test-123
```

## Creating Initial Snapshot

Let's create our first snapshot. That's as simple as `kopia snapshot create`. 
We will create the snapshot of the source code of Kopia itself:

```shell
$ kopia snapshot create $HOME/Projects/github.com/kopia/kopia
```

After completion, Kopia prints the identifier of the root of the snapshot, which starts with `k`:

```
uploaded snapshot 9a622e33ab134ef440f76ed755f79c2f 
  (root kfe997567fb1cf8a13341e4ca11652f70) in 1m42.044883302s
```

## Incremental Snapshots

Let's take the snapshot again. Assuming we did not make any changes to the source code, the snapshot root
will be identical, because all object identifiers in Kopia are derived from contents of data

```
uploaded snapshot 8a45c3b079cf5e7b99fb855a3701607a
  (root kfe997567fb1cf8a13341e4ca11652f70) in 563.670362ms
```

Notice that snapshot creation was nearly instantenous - Kopia did not have to upload
almost any files to the repository, except tiny piece of metadata about the snapshot itself.

All snapshots in Kopia are always incremental - they will only upload files that are not in the repository yet, which saves storage space and upload time. This even applies to files that were moved or renamed. In fact if two computers have exactly the same file, it will still be stored only once.

## Managing Snapshots

We can see the history of snapshots of a directory using `kopia snapshot list`:

```
$ kopia snapshot list $HOME/Projects/github.com/kopia/kopia
jarek@jareks-mbp:/Users/jarek/Projects/Kopia
  2019-06-22 20:15:51 PDT kb9a8420bf6b8ea280d6637ad1adbd4c5 61.4 MB drwxr-xr-x files:12500 dirs:798 (latest-5)
  + 1 identical snapshots until 2019-06-22 20:15:57 PDT
  2019-06-22 20:21:39 PDT kbb7dd85a55ca79f282b59b57e5f9c479 61.4 MB drwxr-xr-x files:12500 dirs:798 (latest-3)
  2019-06-22 20:21:42 PDT ke2e07d38a8a902ad07eda5d2d0d3025d 61.4 MB drwxr-xr-x files:12500 dirs:798 (latest-2)
  + 1 identical snapshots until 2019-06-22 20:21:44 PDT
```

To compare contents of two snapshots use `kopia diff`:

```
$ kopia diff kb9a8420bf6b8ea280d6637ad1adbd4c5 ke2e07d38a8a902ad07eda5d2d0d3025d
changed ./content/docs/Getting started/_index.md at 2019-06-22 20:21:30.176230323 -0700 PDT (size 5346 -> 6098)
```

We can list the contents of the directory using `kopia ls`:

```
$ kopia ls -l kb9a8420bf6b8ea280d6637ad1adbd4c5
-rw-r--r--         6148 2019-06-22 19:01:45 PDT aea2fe8e5ed3104806957f48648c957e   .DS_Store
-rw-r--r--           78 2019-05-09 22:33:06 PDT c829f2205d0ba889ebb354464e14c97a   .gitignore
-rw-r--r--         1101 2019-05-09 22:33:06 PDT 5c4da68139ab0a92a56c334988c75e2a   CONTRIBUTING.md
-rw-r--r--        11357 2019-05-09 22:33:06 PDT 28614f260fab7463e3cd9c410a501c3f   LICENSE
-rw-r--r--         1613 2019-06-22 19:01:17 PDT 5c1f9d67a2b1e2d34fc121ba774266b4   Makefile
-rw-r--r--         2286 2019-05-09 22:33:06 PDT 83a5b758d8409550010786e254096606   README.md
drwxr-xr-x        11264 2019-05-09 22:33:06 PDT kc76b1a9ddf378f803f1710df1150ded6  assets/
drwxr-xr-x         6275 2019-06-02 23:08:14 PDT kf3b4b410df41570345dbc2a8043ee29b  cli2md/
-rw-r--r--         3749 2019-05-14 19:00:21 PDT 8c9e27bed2f577b31b07b07da4bdfffb   config.toml
drwxr-xr-x       879721 2019-06-22 20:15:45 PDT k24eb31a05b81d1a83c47c40a4f7b9f0e  content/
-rwxr-xr-x          727 2019-05-09 22:33:06 PDT 2c08f511019f1f5f45f889909c755a9b   deploy.sh
drwxr-xr-x         1838 2019-05-14 19:00:21 PDT k024f1106e0cd56e2c6611cf884a30894  layouts/
drwxr-xr-x     13682567 2019-06-22 18:57:48 PDT k181d6990e75dd783bd50dae36591622a  node_modules/
-rw-r--r--        94056 2019-06-22 18:57:49 PDT ed474fb638d2a3b1c528295d1586466a   package-lock.json
-rw-r--r--          590 2019-05-09 22:33:06 PDT ee85ae1f1cdb70bbd9e335be9762c251   package.json
drwxr-xr-x      7104710 2019-06-22 19:01:38 PDT keb814d92fe795b96795d5bdbfa816ad6  public/
drwxr-xr-x       904965 2019-06-22 20:13:56 PDT k7bf88a7ca076b03f0dafc93ab5fa2263  resources/
drwxr-xr-x     38701570 2019-06-01 20:11:32 PDT kdb9f41fc8db5c45b1aec06df001be995  themes/
```

For each directory entry, Kopia stores its name, size, attributes and object ID which has the contents of a file or directory.

To examine contents of files use `kopia show` passing the object identifier of either file or directory:

```shell
$ kopia show 8c9e27bed2f577b31b07b07da4bdfffb
```

Directories are stored as JSON objects, so it's possible to see their contents as if they were regular files (`-j` displays pretty-printed JSON)

```shell
$ kopia content show -j kb9a8420bf6b8ea280d6637ad1adbd4c5
```

Which returns:

```json
{
  "stream": "kopia:directory",
  "entries": [
    {
      "name": "assets",
      "type": "d",
      "mode": "0755",
      "mtime": "2019-05-14T18:24:15-07:00",
      "uid": 501,
      "gid": 20,
      "obj": "kc76b1a9ddf378f803f1710df1150ded6",
      "summ": {
        "size": 11264,
        "files": 2,
        "dirs": 3,
        "maxTime": "2019-05-09T22:33:06-07:00"
      }
    },
    ...
    {
      "name": "package.json",
      "type": "f",
      "mode": "0644",
      "size": 590,
      "mtime": "2019-05-09T22:33:06-07:00",
      "uid": 501,
      "gid": 20,
      "obj": "ee85ae1f1cdb70bbd9e335be9762c251"
    }
  ],
  "summary": {
    "size": 61414615,
    "files": 12500,
    "dirs": 798,
    "maxTime": "2019-06-22T20:15:45.301289096-07:00"
  }
}
```

## Mounting Snapshots

We can mount the directory in a local filesystem and examine it using regular file commands to examine the contents.
This is currently the recommended way of restoring files from snapshots.

```shell
$ mkdir /tmp/mnt
$ kopia mount kb9a8420bf6b8ea280d6637ad1adbd4c5 /tmp/mnt &
$ ls -l /tmp/mnt/
total 119992
-rw-r--r--  1 jarek  staff      1101 May  9 22:33 CONTRIBUTING.md
-rw-r--r--  1 jarek  staff     11357 May  9 22:33 LICENSE
-rw-r--r--  1 jarek  staff      1613 Jun 22 19:01 Makefile
-rw-r--r--  1 jarek  staff      2286 May  9 22:33 README.md
drwxr-xr-x  1 jarek  staff     11264 May  9 22:33 assets
drwxr-xr-x  1 jarek  staff      6275 Jun  2 23:08 cli2md
-rw-r--r--  1 jarek  staff      3749 May 14 19:00 config.toml
drwxr-xr-x  1 jarek  staff    879721 Jun 22 20:15 content
-rwxr-xr-x  1 jarek  staff       727 May  9 22:33 deploy.sh
drwxr-xr-x  1 jarek  staff      1838 May 14 19:00 layouts
drwxr-xr-x  1 jarek  staff  13682567 Jun 22 18:57 node_modules
-rw-r--r--  1 jarek  staff     94056 Jun 22 18:57 package-lock.json
-rw-r--r--  1 jarek  staff       590 May  9 22:33 package.json
drwxr-xr-x  1 jarek  staff   7104710 Jun 22 19:01 public
drwxr-xr-x  1 jarek  staff    904965 Jun 22 20:13 resources
drwxr-xr-x  1 jarek  staff  38701570 Jun  1 20:11 themes
$ umount /tmp/mnt
```

## Policies

Policies can be used to specify how the snapshots are taken and retained.

We can define:

- which files to ignore
- how many hourly, daily, weekly, monthly and yearly shapshots to maintain
- how frequently snapshots should be made

Each repository has a `global` policy, which contains defaults used when more specific policies are not defined.
We can examine it by using:

```
$ kopia policy show --global
Policy for (global):
Keep:
  Annual snapshots:    3           (defined for this target)
  Monthly snapshots:  24           (defined for this target)
  Weekly snapshots:   25           (defined for this target)
  Daily snapshots:    14           (defined for this target)
  Hourly snapshots:   48           (defined for this target)
  Latest snapshots:   10           (defined for this target)

Files policy:
  No ignore rules.
  Read ignore rules from files:
    .kopiaignore                   (defined for this target)
```

We can define the policy for a particular directory, by using `kopia policy set` command. For example to ignore two directories we can use:

```
$ kopia policy set --add-ignore public/ --add-ignore node_modules/ .
Setting policy for jarek@jareks-mbp:/Users/jarek/Projects/Kopia/site
 - adding public/ to ignored files
 - adding node_modules/ to ignored files
```

To set maximum number of weekly snapshots we might do:

```
$ kopia policy set --keep-weekly 30 .
Setting policy for jarek@jareks-mbp:/Users/jarek/Projects/Kopia/site
 - setting number of weekly backups to keep to 30.
```

Now when taking snapshot, the directories will be skipped.

To examine a policy for a particular directory use `kopia policy show`:

```
$ kopia policy show .
Policy for jarek@jareks-mbp:/Users/jarek/Projects/Kopia/site:
Keep:
  Annual snapshots:    3           inherited from (global)
  Monthly snapshots:  24           inherited from (global)
  Weekly snapshots:   30           (defined for this target)
  Daily snapshots:    14           inherited from (global)
  Hourly snapshots:   48           inherited from (global)
  Latest snapshots:   10           inherited from (global)

Files policy:
  Ignore rules:
    dist/                          (defined for this target)
    node_modules/                  (defined for this target)
    public/                        (defined for this target)
  Read ignore rules from files:
    .kopiaignore                   inherited from (global)
```

Finally to list all policies we can use `kopia policy list`:

```
$ kopia policy list
7898f47e36bad80a6d5d90f06ef16de6 (global)
63fc854c283ad63cafbca54eaa4509e9 jarek@jareks-mbp:/Users/jarek/Projects/Kopia/site
2339ab4739bb29688bf26a3a841cf68f jarek@jareks-mbp:/Users/jarek/Projects/Kopia/site/node_modules
```

## Examining Repository Structure

Kopia provides low-level commands to examine the contents of repository, perform maintenance actions and get deeper insight into how data is laid out.

### BLOBs

We can list the files in the repository using `kopia blob ls`, which shows how kopia manages snapshots. We can see that repository contents are groupped into Pack files (starting with `p`) and indexed using Index files (starting with `n`). Both index and pack files are encrypted, which makes it impossible to get data and metadata about snapshotted files without knowing the password.

```
$ kopia blob ls
kopia.repository                           636 2019-06-22 20:03:25 PDT
n16f6b7257610be4826396ce2be1fb302       667941 2019-06-22 20:05:30 PDT
n71861ad243bea4c2010001cf5cba1fbe          110 2019-06-22 20:03:29 PDT
p0b31020332e1fe97856faf8aa9c5cf4c     21015032 2019-06-22 20:05:13 PDT
p0df646619c8d1a25415b1b9dc9329d19     22659618 2019-06-22 20:04:36 PDT
p1bde010809b285b657cc130c34c01687     21606909 2019-06-22 20:04:30 PDT
p4dd3b780360b6889e172776fa65be836     23688119 2019-06-22 20:04:23 PDT
p549295a15c0f481ab46f4e4f18372bfc      6936758 2019-06-22 20:05:30 PDT
p61418c5b66c2ca06a51c4af3205e7006         4232 2019-06-22 20:03:29 PDT
p8a6e4f70c0fbbfda99b2e1100647606f     22930102 2019-06-22 20:04:43 PDT
p8f482528fff99ad5c23264873582545f     21261488 2019-06-22 20:05:21 PDT
p946d1fe528bde84d826cf177168d07b3     21015794 2019-06-22 20:04:15 PDT
pa0223f20fc6776b24d25b122cc9ac5f3     26497385 2019-06-22 20:05:07 PDT
pca01737879ac83f9e613c11f52d58349     21520893 2019-06-22 20:04:59 PDT
pcc4bc6fc2b960091755dc0c4704669f1     21656682 2019-06-22 20:05:27 PDT
pdec2a5b599acef1e1ce13ace25e914cd     21332440 2019-06-22 20:04:02 PDT
pf0239202fea978abf1cae4ca45d395b1     23756797 2019-06-22 20:03:54 PDT
pf1d1f00141f830ee34c89797e56011c2     23663212 2019-06-22 20:04:09 PDT
pfbaba06c70b6aed2c0ed3aa9c709dc47     22823458 2019-06-22 20:04:50 PDT
```

### Content-Addressable Block Storage

To list individual contents stored in the Repository use `kopia content list`:

```shell
$ kopia content list
00020136867452b90a3c65a029e7d08c
00029ee689919a13dbf2d588c0986530
00075db1b05e83ba529bc0e06de76ca0
00077bc1c333ddc475d66c45d4645210
0009d14c0a50ceca3519d522137b3a68
000df512dd55ac9749dd0670efa49c7d
001130562b64643ca255396a95a9f2a4
001375d9065eef57b0f21dfe11590227
0015561b6e70feff623eede49f95da73
00166bad1a69484992e32dbd2b869a8a
001828cc591d0bec1301b15266c5c530
...
kfec1215488eaf6acef4558e87ff343a9
kfec7264b36fabdf072de939f622b4452
kff0a0d64969cde306f4dd2c95ca2df6f
kff646ec33a3c24701aabb22f62d60e43
kff74067fa7a2bf14681aee73eb08330d
kff8824282ccc64f68b7b39aacdbb6ceb
kff99aac1cd37371cfc521753e2a1d424
m08ef40b314fb7f08c7be222a79485cc1
m259ec63a4a0137b7ce2801cc47012ffa
m5cf33f9416a435478dea4040b8049f51
m81bd005052e582e821df831c36138d76
m831980997bceabebefa095914a600a1b
m8401800f69795ed0137365c3e6f627bc
...
```

### Manifest Storage

To list manifests (snapshot manifests and policies) stored in repository use `kopia manifest list`

```
$ kopia manifest list
7898f47e36bad80a6d5d90f06ef16de6        170 2019-06-22 20:03:29 PDT type:policy policyType:global
9a622e33ab134ef440f76ed755f79c2f        802 2019-06-22 20:05:28 PDT type:snapshot hostname:jareks-mbp path:/Users/jarek/Projects/Kopia username:jarek
2d73b31af65d4ac7196641eeea9c475c        755 2019-06-22 20:15:53 PDT type:snapshot hostname:jareks-mbp path:/Users/jarek/Projects/Kopia/site username:jarek
8a45c3b079cf5e7b99fb855a3701607a        762 2019-06-22 20:15:58 PDT type:snapshot hostname:jareks-mbp path:/Users/jarek/Projects/Kopia/site username:jarek
ed30d264bcf795bd6648540fdeebdb31        761 2019-06-22 20:21:39 PDT type:snapshot hostname:jareks-mbp path:/Users/jarek/Projects/Kopia/site username:jarek
a1646120c7a2450cd9e77fd98369d260        761 2019-06-22 20:21:43 PDT type:snapshot hostname:jareks-mbp path:/Users/jarek/Projects/Kopia/site username:jarek
38f1987e1ea434e161111abce86212ed        761 2019-06-22 20:21:45 PDT type:snapshot hostname:jareks-mbp path:/Users/jarek/Projects/Kopia/site username:jarek
2339ab4739bb29688bf26a3a841cf68f         63 2019-06-22 21:19:41 PDT type:policy hostname:jareks-mbp path:/Users/jarek/Projects/Kopia/site/node_modules policyType:path username:jarek
856ed6f6cc5cd522d23718e6315cf51e        757 2019-06-22 21:20:37 PDT type:snapshot hostname:jareks-mbp path:/Users/jarek/Projects/Kopia/site username:jarek
63fc854c283ad63cafbca54eaa4509e9        102 2019-06-22 21:22:20 PDT type:policy hostname:jareks-mbp path:/Users/jarek/Projects/Kopia/site policyType:path username:jarek
```

To examine individual manifests use `kopia manifest show`:

```shell
$ kopia manifest show 2d73b31af65d4ac7196641eeea9c475c
```
```json
// id: 2d73b31af65d4ac7196641eeea9c475c
// length: 755
// modified: 2019-06-22 20:15:53 PDT
// label path:/Users/jarek/Projects/Kopia/site
// label type:snapshot
// label username:jarek
// label hostname:jareks-mbp
{
  "source": {
    "host": "jareks-mbp",
    "userName": "jarek",
    "path": "/Users/jarek/Projects/Kopia/site"
  },
  "description": "",
  "startTime": "2019-06-22T20:15:51.603328-07:00",
  "endTime": "2019-06-22T20:15:53.038247-07:00",
  "stats": {
    "content": {
      "readBytes": 863,
      "decryptedBytes": 799,
      "hashedBytes": 63685985,
      "readContents": 2,
      "hashedContents": 13315
    },
    "dirCount": 798,
    "fileCount": 12500,
    "totalSize": 61414615,
    "excludedFileCount": 0,
    "excludedTotalSize": 0,
    "excludedDirCount": 0,
    "cachedFiles": 0,
    "nonCachedFiles": 12500,
    "readErrors": 0
  },
  "rootEntry": {
    "name": "site",
    "type": "d",
    "mode": "0755",
    "mtime": "2019-06-22T19:01:45.936555202-07:00",
    "uid": 501,
    "gid": 20,
    "obj": "kb9a8420bf6b8ea280d6637ad1adbd4c5",
    "summ": {
      "size": 61414615,
      "files": 12500,
      "dirs": 798,
      "maxTime": "2019-06-22T20:15:45.301289096-07:00"
    }
  }
}
```

### Cache

For better performance Kopia maintains local cache directory where most-recently used blocks are stored.
You can examine the cache by using:

```
$ kopia cache info
/Users/jarek/Library/Caches/kopia/e470f963ef9528a1/contents: 3 files 7 KB (limit 5.2 GB)
/Users/jarek/Library/Caches/kopia/e470f963ef9528a1/indexes: 12 files 670.8 KB
/Users/jarek/Library/Caches/kopia/e470f963ef9528a1/metadata: 2006 files 3.9 MB (limit 0 B)
```

To clear the cache:

```
$ kopia cache clear
```

Finally to set caching parameters, such as maximum size of each cache use `kopia cache set`:

```
$ kopia cache set --metadata-cache-size-mb=500
21:38:25.024 [kopia/cli] changing metadata cache size to 500 MB
```
