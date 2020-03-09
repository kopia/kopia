---
title: "Features"
linkTitle: "Features"
weight: 3
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

Kopia performs all its operations client-side, without having to maintain dedicated server and supports a variety of storage providers, including cloud storage ([Google Cloud Storage](https://cloud.google.com/storage), [Amazon S3](https://aws.amazon.com/s3), [Wasabi](https://wasabi.com) or similar), [WebDAV](https://en.wikipedia.org/wiki/WebDAV)-compatible storage or any other remote storage mounted locally.

With Kopia you're in full control of your storage. You must provision, pay for and maintain storage with enough capacity to store your backup and enough availability to be able to recover data when needed. To avoid administrative overhead it's recommended to use one of the available cloud storage solutions, which provide excellent features for very reasonable price.

### Command Line Interface

Kopia has rich command-line interface for managing snapshots and policies, but also for low-level access to the underlying repository, including low-level data recovery.

### API Server

Kopia can be run in a server mode, in which case it exposes a HTTP API that can be used to build client tools that can trigger snapshots, get their status and access snapshotted data.

### Disaster Recovery

Kopia files can be often recovered even after partial loss of repository contents, because key index information and repository metadata is stored redundantly to prevent single points of failure.

