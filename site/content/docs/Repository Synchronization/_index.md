---
title: "Repository Synchronization"
linkTitle: "Repository Synchronization"
weight: 46
---

Maintaining multiple copies of the repository is important from disaster recovery standpoint. While cloud-based repositories are often best from durability standpoint, local copy of the repository may help speed up data recovery.

Kopia v0.6.0 adds support for automatic repository replication, which performs automatic incremental copy of  currently connected repository to another storage location.

Any repository location can be used as target:

```
$ kopia repository sync-to filesystem --path /dest/repository
$ kopia repository sync-to gcs --bucket my-bucket
```

The calling user must have read access to current repository and read/write access to the destination.

By default synchronization does not perform any deletions in the destination location, even if the source file has been deleted.  Without deletions, the resulting repository will still be correct, but will not benefit from compaction and will run more slowly. To allow deletions, pass `--delete` option:

```
$ kopia repository sync-to filesystem --path /dest/repository --delete
```

When synchronizing to a filesystem location, it is important to check that the filesystem is mounted correctly. To ensure that Kopia will not unneccessarily download potentially large repositories when destination filesystem is accidentally unmounted, pass `--must-exist`:

```
$ kopia repository sync-to filesystem --path /dest/repository --must-exist
```
