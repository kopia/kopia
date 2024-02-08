---
title: "Features"
linkTitle: "Features"
weight: 10
---

* [Backup Files and Directories Using Snapshots](#backup-files-and-directories-using-snapshots)
* [Policies Control What and How Files/Directories are Saved in Snapshots](#policies-control-what-and-how-filesdirectories-are-saved-in-snapshots)
* [Save Snapshots to Cloud, Network, or Local Storage](#save-snapshots-to-cloud-network-or-local-storage)
* [Restore Snapshots Using Multiple Methods](#restore-snapshots-using-multiple-methods)
* [End-to-End 'Zero Knowledge' Encryption](#end-to-end-zero-knowledge-encryption)
* [Compression](#compression)
* [Error Correction](#error-correction)
* [Verifying Backup Validity and Consistency](#verifying-backup-validity-and-consistency)
* [Recovering Backed Up Data When There is Data Loss](#recovering-backed-up-data-when-there-is-data-loss)
* [Regular Automatic Maintenance of Repositories](#regular-automatic-maintenance-of-repositories)
* [Caching](#caching)
* [Both Command Line and Graphical User Interfaces](#both-command-line-and-graphical-user-interfaces)
* [Optional Server Mode with API Support to Centrally Manage Backups of Multiple Machines](#optional-server-mode-with-api-support-to-centrally-manage-backups-of-multiple-machines)
* [Speed](#speed)

### Backup Files and Directories Using Snapshots

Kopia creates snapshots of the files and directories you designate, then [encrypts](#end-to-end-zero-knowledge-encryption) these snapshots before they leave your computer, and finally uploads these encrypted snapshots to cloud/network/local storage called a [repository](../repositories/). Snapshots are maintained as a set of historical point-in-time records based on [policies](#policies-control-what-and-how-filesdirectories-are-saved-in-snapshots) that you define.

Kopia uses [content-addressable storage](https://en.wikipedia.org/wiki/Content-addressable%20storage) for snapshots, which has many benefits:

* Each snapshot is always [incremental](https://www.techtarget.com/searchdatabackup/feature/Full-incremental-or-differential-How-to-choose-the-correct-backup-type). This means that all data is uploaded once to the repository based on file content, and a file is only re-uploaded to the repository if the file is modified. Kopia uses file splitting based on [rolling hash](https://en.wikipedia.org/wiki/Rolling_hash), which allows efficient handling of changes to very large files: any file that gets modified is efficiently snapshotted by only uploading the changed parts and not the entire file.

* Multiple copies of the same file will be stored once. This is known as [deduplication](https://en.wikipedia.org/wiki/Data_deduplication) and saves you a lot of storage space (i.e., saves you money).

* After moving or renaming even large files, Kopia can recognize that they have the same content and won't need to upload them again.

* Multiple users or computers can share the same repository: if different users have the same files, the files are uploaded only once as Kopia deduplicates content across the entire repository.

> NOTE: Kopia allows one password per repository, and there is currently no access control mechanism when sharing a repository with someone. If you share a repository with someone else, then you must also share your password with them and they will have access to your data. Therefore, make sure you trust all the other users/computers that you share a repository with!

### Policies Control What and How Files/Directories are Saved in Snapshots

Kopia allows you to create an unlimited number of policies for each repository. Policies allow you to define what files/directories to backup in a snapshot and other features of a snapshot, including but not limited to:

* how frequently/when Kopia should automatically create snapshots of your data
* whether to exclude [certain files/directories](../advanced/kopiaignore/) from snapshots
* how long to retain a snapshot before expiring it and removing it from the repository
* whether and how to compress the files/directories being backed up

Policies can be applied at multiple different levels:

* `global` (i.e., the policy is applied to all snapshots for the repository)
* `username@hostname:/path` (i.e., the policy is applied only for the specific files/folders being backed up in that particular policy)
* `username@hostname` (i.e., the policy is applied for all policies belonging to the specific user)
* `@hostname` (i.e., the policy is applied to all policies belonging to the specific machine)

### Save Snapshots to Cloud, Network, or Local Storage

Kopia performs all its operations locally on your machine, meaning that you do not need to have any dedicated server to run your backups and you can save your snapshots to a variety of storage locations. Kopia supports network and local storage locations, of course, but also many cloud or remote storage locations:

* **Amazon S3** and any **cloud storage that is compatible with S3**
* **Azure Blob Storage**
* **Backblaze B2**
* **Google Cloud Storage**
* Any remote server or cloud storage that supports **WebDAV**
* Any remote server or cloud storage that supports **SFTP**
* Some of the cloud storages supported by **Rclone**
  * Requires you to download and setup Rclone in addition to Kopia, but after that Kopia manages/runs Rclone for you
  * Rclone support is experimental: not all the cloud storages supported by Rclone have been tested to work with Kopia, and some may not work with Kopia; Kopia has been tested to work with **Dropbox**, **OneDrive**, and **Google Drive** through Rclone
* Your own server by setting up a [Kopia Repository Server](../repository-server/)

Read the [repositories help page](../repositories/) for more information on supported storage locations. 

With Kopia you’re in full control of where to store your snapshots; you pick the cloud storage you want to use. Kopia plays no role in selecting your storage locations. You must provision and pay (the storage provider) for whatever storage locations you want to use, and then tell Kopia what those storage locations are. The advantage of decoupling the software (i.e., Kopia) from storage is that you can use whatever storage locations you desire -– it makes no difference to Kopia what storage you use. You can even use multiple storage locations if you want to, and Kopia also supports backing up multiple machines to the same storage location. 

> NOTE: Different storage providers may operate slightly differently, so you need to make sure whatever storage location you use has enough capacity to store your backups and enough availability to be able to recover the data when needed. 

### Restore Snapshots Using Multiple Methods

To restore data, Kopia gives you three options: 

* mount the contents of a snapshot as a local disk so that you can browse and copy files/directories from the snapshot as if the snapshot is a local directory on your machine

* restore all files/directories contained in a snapshot to any local or network location that you designate

* selectively restore individual files from a snapshot

### End-to-End 'Zero Knowledge' Encryption

All data is encrypted before it leaves your machine. Encryption is baked into the DNA of Kopia, and you cannot create a backup without using encryption. Kopia allows you to pick from two state-of-the-art encryption algorithms, [AES-256](https://en.wikipedia.org/wiki/AES256) and [ChaCha20](https://en.wikipedia.org/wiki/ChaCha20).

Kopia encrypts both the content and the names of your backed up files/directories.

The data is encrypted using per-content keys which are derived from the 256-bit master key that is stored in the repository. The master key is encrypted with a password you provide. This means that anyone that does not know the password cannot access your backed up files and will not know what files/directories are contained in the snapshots that are saved in the repository. Importantly, the password you provide is never sent to any server or anywhere outside your machine, and only you know your password. In other words, Kopia provides your backups with end-to-end 'zero knowledge' encryption. However, this also means that you cannot restore your files if you forget your password: there is no way to recover a forgotten password because only you know it. (But you can [change your password](../faqs/#how-do-i-change-my-repository-password) if you are still connected to the repository that stores your snapshots.)

### Compression

Kopia can [compress your data](../advanced/compression/) to save storage and bandwidth. Several compression methods are supported, including:

* [pgzip](https://github.com/klauspost/pgzip)

* [s2](https://github.com/klauspost/compress/tree/master/s2)

* [zstd](https://github.com/klauspost/compress/tree/master/zstd)

### Error Correction

Kopia supports [Reed-Solomon error correction algorithm](../advanced/ecc/) to help prevent your snapshots from being corrupted by faulty hardware, such as bitflips or bitrot.

### Verifying Backup Validity and Consistency

Backing up data is great, but you also need to be able to restore that data when (if) the time arises. Kopia has built-in functions that enable you to verify the consistency/validity of your backed up files. You can run these consistency checks are frequently as you like (e.g., once a month, once a year, etc.). Read the [repository consistency](../advanced/consistency/) help docs for more information.

### Recovering Backed Up Data When There is Data Loss

Although never guaranteed, Kopia can often recover your files even if there is some partial data loss at your repository (e.g., a hard drive failure), because key index information and repository metadata is stored redundantly to prevent single points of failure. Note that Kopia cannot recover data where the actual backed up data file in the repository is corrupt, so make sure to regularly run repository consistency checks (see above discussion)!

### Regular Automatic Maintenance of Repositories

Over time, repositories can get bloated to the point of decreased performance and waste of storage space. Kopia runs automatic maintenance that ensures optimal performance and space usage. Read the [maintenance](../advanced/maintenance/) help docs for more information.

### Caching

Kopia maintains a local cache of recently accessed objects making it possible to quickly browse repository contents without having to download from the storage location (regardless of whether the storage is cloud, network, or local).

### Both Command Line and Graphical User Interfaces

Kopia has a rich [command-line interface](../installation/#two-variants-of-kopia) that gives you full access to all Kopia features, including allowing you to create/connect to repositories, manage snapshots and policies, and provides low-level access to the underlying repository, including low-level data recovery. 

Do not want to use command-line? No problem. Kopia also comes with a [powerful official graphical user interface](../installation/#two-variants-of-kopia) that allows you to easily create/connect to repositories, manage snapshots and policies, and restore data as needed.

### Optional Server Mode with API Support to Centrally Manage Backups of Multiple Machines

Kopia is designed to backup individual machines and you absolutely do not need a server to run Kopia. If you have a handful of machines, you can install and use Kopia on each of them individually, no problem. At the same time, Kopia can also be run in [server mode](../faqs/#what-is-a-kopia-repository-server) for those that are looking to centrally manage backups of multiple machines, in which case the Kopia server exposes an API that can be used to build client tools to do things like trigger snapshots, get client status, and access snapshotted data. Kopia's server mode makes it incredibly easy to centrally manage backups of multiple computers.

### Speed

Kopia. Is. [Fast](https://web.archive.org/web/20231202012341/https://www.kasten.io/kubernetes/resources/blog/benchmarking-kopia-architecture-scale-and-performance).
