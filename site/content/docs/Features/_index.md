---
title: "Features"
linkTitle: "Features"
weight: 30
---

### Backup Files and Directories using Snapshots

Kopia creates snapshots of the files and directories you designate and uploads theses snapshots to cloud/network/local storage called a [repository](../repositories/). Snapshots are maintained as a set of historical point-in-time records based on policies that you define.

Kopia uses [content-addressable storage](https://en.wikipedia.org/wiki/Content-addressable%20storage) for snapshots, which has many benefits:

* Each snapshot is always [incremental](https://www.techtarget.com/searchdatabackup/feature/Full-incremental-or-differential-How-to-choose-the-correct-backup-type). This means that all data is uploaded once to the repository based on file content, and a file is only re-uploaded to the repository if the file is modified. Kopia uses file splitting based on [rolling hash](https://en.wikipedia.org/wiki/Rolling_hash), which allows efficient handling of changes to very large files: any file that gets modified is efficiently snapshotted by only uploading the changed parts and not the entire file.

* Multiple copies of the same file will be stored once. This is known as [deduplication](https://en.wikipedia.org/wiki/Data_deduplication) and saves you a lot of storage space.

* After moving or renaming even large files, Kopia can recognize that they have the same content and won't need to upload them again.

* Multiple users or computers can share the same repository: if users share the same files, they are also uploaded only once.

> NOTE: that there is currently no access control mechanism within repository: everybody with access to the repository can see everyone's data, so be sure you trust the other users if you share a repository with someone else.

### Policies Control What and How Files/Directories are Saved in Snapshots

Kopia allows you to create an unlimited number of policies for each repository. Policies allow you to define what files/directories to backup in a snapshot and other features of a snapshot, including but not limited to:

* how frequently/when Kopia should automatically create snapshots of your data
* whether to exclude certain files/directories from snapshots, similar to `.gitignore`
* how long to retain a snapshot before expiring it and removing it from the repository
* whether and how to compress the files/directories being backed up

Policies can be applied at multiple different levels:

* `global` (i.e., the policy is applied to all snapshots for the repository)
* `username@hostname:/path` (i.e., the policy is applied only for the specific files/folders being backed up in that particular policy)
* `username@hostname` (i.e., the policy is applied for all policies belonging to the specific user)
* `@hostname` (i.e., the policy is applied to all policies belonging to the specific machine)

### Save Snapshots to Cloud, Network, or Local Storage

Kopia performs all its operations locally on your machine, meaning that you do not need to have any dedicated server to run your backups and you can save your snapshots to a variety of storage locations. Kopia supports network and local storage locations, of course, but also many cloud or remote storage locations, including but not limited to [Google Cloud Storage](https://cloud.google.com/storage), [Amazon S3](https://aws.amazon.com/s3) and all S3-compatible cloud storage such as [Wasabi](https://wasabi.com), [Backblaze B2](https://www.backblaze.com/b2/cloud-storage.html), [Microsoft Azure Blob Storage](https://azure.microsoft.com/fr-fr/services/storage/), [WebDAV](https://en.wikipedia.org/wiki/WebDAV)-compatible storage, any storage locations that allow you to connect via SFTP, and all storage locations supported by [Rclone](https://rclone.org/). Read the [repositories help page](https://kopia.io/docs/repositories/) for more information. 

With Kopia you're in full control of where to store your snapshots. You provision, pay for, and use whatever storage locations you desire. You can use multiple different storage locations if you want to. Note that different storage providers may operate slightly differently, so you need to make sure whatever storage location you use has enough capacity to store your backups and enough availability to be able to recover the data when needed. 

### Restore Snapshots Using Multiple Methods

To restore data, Kopia gives you three options: mount the contents of a snapshot as a local disk so that you can browse and copy files/directories from the snapshot as if the snapshot is a local directory on your machine; restore all files/directories contained in a snapshot to any local or network location that you designate; or selectively restore individual files from a snapshot.

### End-to-End 'Zero Knowledge' Encryption

All data is encrypted before it leaves your machine. Encryption is baked into the DNA of Kopia, and you cannot create a backup without using encryption. Kopia allows you to pick from two state-of-the-art encryption algorithms, [AES-256](https://en.wikipedia.org/wiki/AES256) and [ChaCha20](https://en.wikipedia.org/wiki/ChaCha20).

The data is encrypted using per-content keys which are derived from the 256-bit master key that is stored in the repository. The master key is encrypted with a passphrase you provide. This means that anyone that does not know the passphrase cannot access your backed up files and will not know what files/directories are contained in the snapshots that are saved in the repository. Importantly, the passphrase you provide is never sent to any server or anywhere outside your machine, and only you know your passphrase. In other words, Kopia provides your backups with end-to-end zero knowledge encryption. However, this also means that you cannot restore your files if you forget your passphrase. There is no way to recover or reset a forgotten passphrase.

### Compression

Kopia can [compress your data](https://kopia.io/docs/advanced/compression/) to save storage and bandwidth. Several compression methods are supported, including:

* [pgzip](https://github.com/klauspost/pgzip)

* [s2](https://github.com/klauspost/compress/tree/master/s2)

* [zstd](https://github.com/klauspost/compress/tree/master/zstd)

### Verifying Backup Validity and Consistency

Backing up data is great, but you also need to be able to restore that data when (if) the time arises. Kopia has built-in functions that enable you to verify the consistency/validity of your backed up files. You can run these consistency checks are frequently as you like (e.g., once a month, once a year, etc.). Read the [repository consistency](https://kopia.io/docs/advanced/consistency/) help docs for more information.

### Recovering Backed Up Data When There is Data Loss

Although never guaranteed, Kopia can often recover your files even if there is some partial data loss at your repository (e.g., a hard drive failure), because key index information and repository metadata is stored redundantly to prevent single points of failure. Note that Kopia cannot recover data where the actual backed up data file in the repository is corrupt, so make sure to regularly run repository consistency checks (see above discussion)!

### Regular Automatic Maintence of Repositories

Over time, repositories can get bloated to the point of decreased performance and waste of storage space. Kopia runs automatic maintence that ensures optimal performance and space usage. Read the [maintenance](https://kopia.io/docs/advanced/maintenance/) help docs for more information.

### Caching

Kopia maintains a local cache of recently accessed objects making it possible to quickly browse repository contents without having to download from the storage location (regardless of whether the storage is cloud, network, or local).

### Both Command Line and Graphical User Interfaces

Kopia has a rich command-line interface that allows you to create/connect to repositories, manage snapshots and policies, and provides low-level access to the underlying repository, including low-level data recovery. 

Do not want to use command-line? No problem. Kopia also comes with an official graphical user interface that allows you to easily create/connect to repositories, manage snapshots and policies, and restore data as needed.

### Server Mode with API Support

While Kopia is designed to backup individual machines and you absolutely do not need a server to run Kopia, Kopia can also be run in server mode, in which case the Kopia server exposes an HTTPS API that can be used to build client tools to trigger snapshots, get client status, and access snapshotted data.

### Speed

Kopia. Is. [Fast] (https://www.kasten.io/kubernetes/resources/blog/benchmarking-kopia-architecture-scale-and-performance).
