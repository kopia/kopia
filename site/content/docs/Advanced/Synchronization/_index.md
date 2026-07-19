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

### Synchronizing to object-locked storage (immutable replicas)

When the destination supports object locking (for example an S3 bucket with Object Lock and versioning enabled), synchronization can apply object-lock retention to the copied repository blobs, creating an immutable replica of a repository whose primary storage does not support object locks (such as a local filesystem or SFTP):

```
$ kopia repository sync-to s3 --bucket my-bucket --retention-mode COMPLIANCE --retention-period 30d
```

The retention flags only affect the destination; the source repository is not modified. The destination bucket must be created with Object Lock (and versioning) enabled, and the destination storage must support object locking — otherwise the synchronization fails with an error. See [Ransomware Protection](../ransomware-protection/) for details, including the recommended retention mode and required permissions.

Because blobs that are already in sync are not re-uploaded, each synchronization run also extends the object locks on the blobs already present in the destination, so that immutability protection does not lapse between runs. This extension performs one retention-update API request per locked blob on every run. When synchronizing frequently, pass `--no-extend-object-locks` on most runs and let a less frequent run (for example, weekly) refresh the locks — but always schedule the refreshing runs with a safety margin of at least 24 hours before lock expiry.

Combining `--delete` with retention creates delete markers in the destination: locked historical versions remain retained, but recovering them requires version-aware (point-in-time) access. For immutable replicas, not passing `--delete` is recommended.

### Automation

For automated synchronization tasks, progress output can be suppressed using the `--no-progress` flag to provide clean output suitable for cron jobs and scripts.
