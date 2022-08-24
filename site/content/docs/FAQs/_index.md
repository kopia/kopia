---
title: "Frequently Asked Questions"
linkTitle: "Frequently Asked Questions"
weight: 40
---

### Questions

* [What is a Snapshot?](#what-is-a-snapshot)
* [What is a Repository?](#what-is-a-repository)
* [What is a Policy?](#what-is-a-policy)
* [How to Restore My Backed Up Files/Directories?](#how-to-restore-my-backed-up-filesdirectories)
* [How Do I Enable Encryption?](#how-do-i-enable-encryption)
* [How Do I Enable Compression?](#how-do-i-enable-compression)
* [How Do I Enable Data Deduplication?](#how-do-i-enable-data-deduplication)
* [How Do I Change My Repository Password?](#how-do-i-change-my-repository-password)
* [Does Kopia Support Storage Classes, Like Amazon Glacier?](#does-kopia-support-storage-classes-like-amazon-glacier)
* [How Do I Decrease Kopia's CPU Usage?](#how-do-i-decrease-kopias-cpu-usage)
* [How Do I Decrease Kopia's Memory (RAM) Usage?](#how-do-i-decrease-kopias-memory-ram-usage)
* [What is a Kopia Repository Server?](#what-is-a-kopia-repository-server)

**Is your question not answered here? Please ask in the [Kopia discussion forums](https://kopia.discourse.group/) for help!**

### Answers

#### What is a Snapshot?

A `snapshot` is a [point-in-time backup](../features#backup-files-and-directories-using-snapshots) of your files/directories; each snapshot contains the files/directories that you can [restore when you need to](../features#restore-snapshots-using-multiple-methods).

#### What is a Repository?

A `repository` is the storage location where your snapshots are saved; Kopia supports [cloud/remote, network, and local storage locations](../features#save-snapshots-to-cloud-network-or-local-storage) and all repositories are [encrypted](../features/#end-to-end-zero-knowledge-encryption) with a password that you designate.

See the [repository help docs](../repository) for more information.

#### What is a Policy?

A `policy` is a set of rules that tells Kopia how to create/manage snapshots; this includes features such as [compression, snapshot retention, and scheduling when to take automatically snapshots](../features#policies-control-what-and-how-filesdirectories-are-saved-in-snapshots).

#### How to Restore My Backed Up Files/Directories?

Files/directories are restored from the snapshots you create. To restore the data you backed up in a snapshot, Kopia gives you three options: 

* mount the contents of a snapshot as a local disk so that you can browse and copy files/directories from the snapshot as if the snapshot is a local directory on your machine;

* restore all files/directories contained in a snapshot to any local or network location that you designate;

* or selectively restore individual files from a snapshot.

The [Getting Started Guide](../getting-started/) provides directions on how to restore files/directions [when using Kopia GUI](../getting-started/#restoring-filesdirectories-from-snapshots) and [when using Kopia CLI](../getting-started/#mounting-snapshots-and-restoring-filesdirectories-from-snapshots).

#### How Do I Enable Encryption?

Encryption is at the `repository` level, and Kopia encrypts all snapshots in all repositories by default. You should have been asked for a password when you created your `repository`. That password is used to encrypt your backups. (Do not forget it, there is no way to recover the password!)

By default, Kopia uses the `AES256-GCM-HMAC-SHA256` encryption algorithm for all repositories, but you can choose `CHACHA20-POLY1305-HMAC-SHA256` if you want to. Picking an encryption algorithm is done when you initially create a `repository`. In `KopiaUI`, to pick the `CHACHA20-POLY1305-HMAC-SHA256` encryption algorithm, you need to click the `Show Advanced Options` button at the screen where you enter your password when creating a new `repository`. For Kopia CLI users, you need to use the `--encryption=CHACHA20-POLY1305-HMAC-SHA256` option when [creating a `repository`](../getting-started/#creating-a-repository) with the [`kopia repository create` command](../reference/command-line/common/#commands-to-manipulate-repository).

Currently, encryption algorithms cannot be changed after a `repository` has been created.

#### How Do I Enable Compression?

Compression is controlled by [policies](../features#policies-control-what-and-how-filesdirectories-are-saved-in-snapshots) and is disabled by default. If compression is not working for a snapshot, it is likely that you have not enabled compression yet in your `policy`.

Enabling compression when using `KopiaUI` is easy; edit the `policy` you want to add compression to and pick a `Compression Algorithm` in the `Compression` section. Kopia CLI users need to use the [`kopia policy set`](..reference/command-line/common/policy-set/) command as shown in the [Getting Started Guide](../getting-started/#policies). You can set compression on a per-source-directory basis...

```shell
kopia policy set </path/to/source/directory/> --compression=<none|deflate-best-compression|deflate-best-speed|deflate-default|gzip|gzip-best-compression|gzip-best-speed|pgzip|pgzip-best-compression|pgzip-best-speed|s2-better|s2-default|s2-parallel-4|s2-parallel-8|zstd|zstd-better-compression|zstd-fastest>
```

...or globally for all source directories:

```shell
kopia policy set --global --compression=<none|deflate-best-compression|deflate-best-speed|deflate-default|gzip|gzip-best-compression|gzip-best-speed|pgzip|pgzip-best-compression|pgzip-best-speed|s2-better|s2-default|s2-parallel-4|s2-parallel-8|zstd|zstd-better-compression|zstd-fastest>
```
If you enable or disable compression or change the compression algorithm, the new setting is applied going forward and not reteroactively. In other words, Kopia will not modify the compression for files/directories already uploaded to your repository.

If you are unclear about what compression algorithm to use, `zstd` is considered one of the top algorithms currently.

#### How Do I Enable Data Deduplication?

[Data deduplication](../features/#backup-files-and-directories-using-snapshots) is enabled automatically by Kopia for all repositories, regardless of whether you use the GUI or CLI. You do not need to do anything.

#### How Do I Change My Repository Password?

You must use Kopia CLI if you want to change your `repository` password; changing password is not currently supported via Kopia GUI. The [`kopia repository change-password` command](../reference/command-line/common/repository-change-password/) is used to change your password. 

Before changing your password, you must be [connected to your `repository`](../getting-started/#connecting-to-repository). This means that you **can** reset your password if you forget your password AND you are still connected to your `repository`. But this also means that you **cannot** reset your password if you forget your password and you are NOT still connected to your `repository`, because you will need your current password to connect to the `repository`.

Remember to select a secure _repository password_. The password is used to [decrypt](../features/#end-to-end-zero-knowledge-encryption) and access the data in your snapshots.

#### Does Kopia Support Storage Classes, Like Amazon Glacier?

Yes. Please read the [storage classes guide](../advanced/storage-tiers) to learn more.

#### How Do I Decrease Kopia's CPU Usage?

It is difficult to know what is causing high CPU use on your machine without details about your unique issue (which you can post about [on the community forums](https://kopia.discourse.group/)). However, generally speaking, there are two `policy` settings you should change if you want to decrease the amount of CPU Kopia uses while creating snapshots:

1. Maximum Parallel Snapshots: This setting controls how many snapshots Kopia runs simultaneously. The lower this number, the less CPU Kopia will use when running multiple snapshots -- with the caveat that the total time it takes to run all your snapshots will be higher. This setting is not applicable if you only ever run one snapshot at a time.
    * Kopia CLI users can change this setting by running the [`kopia policy set --global --max-parallel-snapshots=#`](../reference/command-line/common/policy-set/) command, where `#` is the number of snapshots you want Kopia to run simultaneously.
    * Kopia GUI users can change this setting from the `Upload` tab when editing the `global` policy in `KopiaUI`.
2. Maximum Parallel File Reads: This setting controls how many files Kopia uploads simultaneously when running a snapshot. The lower this number, the less CPU Kopia will use when running a snapshot -- with the caveat that it will take longer to complete a snapshot. By default, Kopia sets this setting to the number of logical cores your machine's CPU has, so make sure to lower it to a smaller number than the default.
    * Kopia CLI users can change this setting by running the [`kopia policy set [target] --max-parallel-file-reads=#`](../reference/command-line/common/policy-set/) command, where `#` is the number of file uploads you want Kopia to run simultaneously. This setting can be set per policy or globally, in which case make sure to use `--global` in your `kopia policy set` command.
    * Kopia GUI users can change this setting from the `Upload` tab when editing policies in `KopiaUI`, either the `global` policy or individual policies.

An added benefit of decreasing these settings is that [Kopia's memory usage will also decrease](#how-do-i-decrease-kopias-memory-ram-usage).

#### How Do I Decrease Kopia's Memory (RAM) Usage?

It is difficult to know what is causing high memory use on your machine without details about your unique issue (which you can post about [on the community forums](https://kopia.discourse.group/)). However, generally speaking, [compression](#how-do-i-enable-compression) and parallelism (i.e., [simultaneous snapshots or simultaneous uploads](#how-do-i-decrease-kopias-cpu-usage)) are the two big culprits of memory usage in Kopia when creating snapshots. If you want to decrease the amount of memory used by Kopia, you should tweak these settings:

* Disabling compression will result in less memory usage than enabling compression. If you want to keep compression, [compression benchmarks](../advanced/compression/) suggest `s2`, `deflate`, and `gzip` are the most memory-friendly compression algorithms when backing up small files. When backing up large files, all the compression algorithms have similar memory usage. Thus, if your machine has low memory, try `s2`, `deflate`, or `gzip`. Read the FAQ on [enabling compression](#how-do-i-decrease-kopias-cpu-usage) to learn how to change or remove compression in Kopia.
* Decreasing the number of parallel snapshots and parallel file reads will decrease Kopia's memory usage because Kopia will run fewer simultaneous processes. Read the FAQ on [decreasing Kopia's CPU usage](#how-do-i-decrease-kopias-cpu-usage) to learn how to decrease parallelism in Kopia.

#### What is a Kopia Repository Server?

See the [Kopia Repository Server help docs](../repository-server) for more information.
