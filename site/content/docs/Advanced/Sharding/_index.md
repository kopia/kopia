---
title: "Sharding"
linkTitle: "Sharding"
weight: 60
---

## Sharding

Sharding is a feature introduced in Kopia v0.9.0 that allows user to customize the file system structure of a repository. Sharded repository looks like this on disk:

```
│   .shards
│   kopia.maintenance.f
│
├───n00
│   └───255
│           6c89ca6a0337723cbf33b5a198d-s31ab5f6a5cf09672108.f
│
├───n4e
│   └───ad3
│           404342ae8b978e64b7bcfc7d6ff.f
│
├───p00
│   ├───003
│   │       237e0ac3607edbee743a26b0b34.f
│   │
│   ├───011
│   │       d5912b0673ffc2c192753c3d345.f
│   │       b9021cd457094b947de593d2e84.f
│   │
│   ├───023
│   │       a6fcfcf17352c79dc7d81e2656e-saae4f33c0b1ba0c410a.f
│   │
│   ├───03a
│   │       51e5deac78855eb30239d3be7d6.f
│   │       db5f6ba893482ea83b190651c49.f
│   │
```
Notice there are two levels of directories leading to all the data files at the third level. The actual blob hash can be re-constructed by prefixing the filename with all its parent directory names. For example, blob `p00003237e0ac3607edbee743a26b0b34` is stored at `<repo_root>/p00/003/237e0ac3607edbee743a26b0b34.f`.

If multiple blob hashes share the same prefix, they are placed in the same directory.

#### Motivation

Sharding is introduced to help improve performance of large repositories. It is common to have hundreds of thousands of data files for repositories over 1 TB. Not all file systems handle these many files in the same directory efficiently. By breaking them into multiple levels, with each only a few hundreds, the performance is thus improved.

For small repositories, sharding may not be necessary and can be turned off.

#### .shards

The layout is controlled by a ".shards" file located in every repository. It is a JSON file with the following structure:
```json
{
    "default": [2, 3],
    "maxNonShardedLength": 20,
    "overrides": [
        { "prefix": "p", "shards": [2, 2] },
        { "prefix": "q", "shards": [3] }
    ]
}
```
`default` is an integer array, that applies the sharding config to all non-overridden blobs. Each element in the array represents one level of directory, and the integer value specifies the length of the directory name. `[2, 3]` means "Take the first 5 characters of each blob hash, split into 2 and 3 as directory names, and put the remaining hash as filename". `[2, 2, 4]` for blob hash `abcdefghijklmn` will become `<repo_root>/ab/cd/efgh/ijklmn.f`.

`maxNonShardedLength` makes blob hashes with length less than its value always unsharded. With value 20, it means if a blob hash is less or equal to 20, such as `e213ff706a0d404e8320`, the file is ignored by the sharding process.

`overrides` is an array that allows user to customize the sharding config for specific prefix. Each entry contains an optional `prefix` for specifying [target blobs](/docs/advanced/storage-tiers/), and a required `shards` for specifying config.

When choosing the number, the rule of thumb is that larger value leads to more directories in that level, assuming the blob hash is evenly distributed (as they statistically should converge to, ignoring those pre-defined prefixes). Therefore if user notices too many files exist in directories given a `[2, 2]` config, it might be good idea to change it to `[3, 3]`. On the other hand, if too many directories are created, one should consider reducing the value.

#### Use

New repositories are created with some shard config, whose default value may change between Kopia version. If the default is undesirable, user can change it with [`blob shards modify`](/docs/reference/command-line/advanced/blob-shards-modify/) command. For example,
```shell
kopia blob shards modify --default-shards=0 --i-am-sure-kopia-is-not-running --path=<repo_root>
```
turns off sharding.

When creating or syncing to a repository backed by cloud storage, user can also consider disabling sharding if needed, since cloud storage by nature is distributed to begin with. Some `repository sync-to` commands come with `--flat` flag to achieve this:
```shell
kopia repository sync-to rclone --remote-path=<remote_repo> --flat
```
This guarantees the remote repository is flat (`"default": []`) regardless if the local repository is sharded or not.
