---
title: "Features"
linkTitle: "Features"
weight: 30
---

### Snapshots

Kopia uploads directories and files to remote storage called [Repository](../architecture/) and maintains a set of historical point-in-time snapshot records based on defined policies.

Kopia uses [content-addressable storage](https://en.wikipedia.org/wiki/Content-addressable storage) for snapshots, which has many benefits:

* Each snapshot is always incremental, no data included in previous snapshots is ever re-uploaded to the repository based on file content.

* Multiple copies of the same file will be stored once. This is known as de-duplication.

* After moving or renaming even large files, Kopia can recognize that they have the same content and won't need to upload them again.

* Multiple users or computers can share same repository: if users share the same files, they are also uploaded only once.

> NOTE: that there is currently no access control mechanism within repository - everybody with access to repository can see everyone's data.

Kopia also uses splitting based on [rolling hash](https://en.wikipedia.org/wiki/Rolling_hash), which allows efficient handling of changes to very large files. For example a virtual disk image which gets modified can also be efficiently snapshotted by only uploading the changed parts and not the entire file.

### Restore

To restore data, Kopia can mount contents of a Repository as a local disk and you can use normal file copy tools to perform full or selective restore.

### Encryption

All data is encrypted before it leaves your machine. Kopia uses state-of-the-art encryption algorithms, such as [AES-256](https://en.wikipedia.org/wiki/AES256) or [ChaCha20](https://en.wikipedia.org/wiki/ChaCha20).

All data is encrypted using per-content keys derived from the 256-bit master key stored in repository. Master key is encrypted with user-provided passphrase, which is never sent to any server.

### Compression

Kopia can compress your data to save extra storage and bandwidth. There are actually 3 compression methods available :

* [pgzip](https://github.com/klauspost/pgzip) : gzip is a very common compression algorithm. It was originally created as a replacement for the compress program used in early Unix systems.
Compression and decompression can be parallelized to speed up the process.

* [s2](https://github.com/klauspost/compress/tree/master/s2) : S2 is an extension of [Snappy](https://github.com/google/snappy). It's aimed for high throughput, which is why it features concurrent compression for bigger payloads.

* [zstd](https://github.com/klauspost/compress/tree/master/zstd) : [Zstandard](https://facebook.github.io/zstd/) is a real-time compression algorithm, providing high compression ratios. It offers a very wide range of compression / speed trade-off, while being backed by a very fast decoder. A high performance compression algorithm is implemented. For now focused on speed.

You can activate compression on a per directory basis

```shell
kopia policy set <path> --compression=<pgzip|s2|zstd>
```

or globally

```shell
kopia policy set --global --compression=<pgzip|s2|zstd>
```

### Policies

Policies can be used to define:

* set of files to snapshot - excluded files can be defined similar to `.gitignore`
* retention - how long to keep snapshots before expiring them
* scheduling - how frequently/when should snapshots be created

Policies can be applied to:

* `username@hostname:/path`
* `username@hostname`
* `@hostname`
* `global`

### Caching

Kopia maintains a local cache of recently accessed objects making it possible to quickly browse the repository contents without having to download from remote storage.

### Storage

Kopia performs all its operations client-side, without having to maintain dedicated server and supports a variety of storage providers, including cloud storage ([Google Cloud Storage](https://cloud.google.com/storage), [Amazon S3](https://aws.amazon.com/s3), [Wasabi](https://wasabi.com), [B2](https://www.backblaze.com/b2/cloud-storage.html), [Azure](https://azure.microsoft.com/fr-fr/services/storage/), or similar), [WebDAV](https://en.wikipedia.org/wiki/WebDAV)-compatible storage, sftp, http/s or any other remote storage mounted locally.

With Kopia you're in full control of your storage. You must provision, pay for and maintain storage with enough capacity to store your backup and enough availability to be able to recover data when needed. To avoid administrative overhead it's recommended to use one of the available cloud storage solutions, which provide excellent features for very reasonable price.

### Command Line Interface

Kopia has rich command-line interface for managing snapshots and policies, but also for low-level access to the underlying repository, including low-level data recovery.

### API Server

Kopia can be run in a server mode, in which case it exposes a HTTP API that can be used to build client tools that can trigger snapshots, get their status and access snapshotted data.

### Disaster Recovery

Kopia files can be often recovered even after partial loss of repository contents, because key index information and repository metadata is stored redundantly to prevent single points of failure.

