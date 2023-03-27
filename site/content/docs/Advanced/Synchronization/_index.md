---
title: "Synchronization"
linkTitle: "Synchronization"
weight: 55
---

## Synchronization

Maintaining multiple copies of a repository is important for disaster recovery scenarios. While cloud-based repositories often have better durability than local ones, a local repository copy may help speed up data recovery.

Kopia v0.6.0 adds support for automatic repository replication, which enables incremental copies of the currently connected repository to a separate storage location.

Any repository location can be used as target. For example:

```
$ kopia repository sync-to filesystem --path /dest/repository
$ kopia repository sync-to gcs --bucket my-bucket
```

The calling user must have read access to current repository and read/write access to the destination.

By default, synchronization does not perform any deletions in the destination location, even if the source file has been deleted.  Without deletions, the resulting repository will still be correct, but will not benefit from compaction and will run more slowly. To allow deletions, pass `--delete` option:

```
$ kopia repository sync-to filesystem --path /dest/repository --delete
```

When synchronizing to a filesystem location, it is important to check that the filesystem is mounted correctly. To ensure that Kopia will not unnecessarily download potentially large repositories when destination filesystem is accidentally unmounted, pass `--must-exist`:

```
$ kopia repository sync-to filesystem --path /dest/repository --must-exist
```
