---
title: "Verifying Validity of Snapshots/Repositories and Repairing Corruption"
linkTitle: "Testing Validity of Backups and Fixing Corruption"
weight: 15
---

* [Verifying Validity of Snapshots/Repositories](#verifying-validity-of-snapshotsrepositories)
* [Repairing Corruption of Snapshots/Repositories](#repairing-corruption-of-snapshotsrepositories)

## Verifying Validity of Snapshots/Repositories

Backing up data is great, but you also need to be able to restore that data when (if) the time arises. That means you need to test snapshots regularly to ensure that a snapshot remains valid and has not become corrupt after you have created the snapshot. 

The gold standard for testing the validity of backups is to simply [restore the snapshot](../../getting-started/); if restore is successful, then the backup is valid. However, in many situations it is not feasible to conduct a full test restore of a snapshot, such as if you do not have enough local hard drive space to do a complete restore or if the egress costs of your cloud storage are extremely high (e.g., Amazon S3 charges $0.09 per GB you download). 

If you cannot, or do not want to, do a full test restore of your snapshots, Kopia enables you to verify the consistency/validity of your snapshots/repositories using the [`kopia snapshot verify` command](../../reference/command-line/common/snapshot-verify/). This command ensures that the directory structures in your repository are consistent by walking all files and directories in the snapshots from their roots; it verifies that the content manager index structures are correct and that every index entry is backed by an existing file. All of this is to ensure that your snapshots contain all the necessary blobs to restore each backed up file, should the need arise.

Both Kopia CLI and KopiaUI automatically run `kopia snapshot verify` during every [daily full maintenance](../maintenance/), so you do not need to run the command yourself. However, while `kopia snapshot verify` verifies the existence of all blobs necessary to restore your snapshots, the command stops short of downloading, decrypting, and decompressing the blobs. If you want Kopia to conduct what is essentially the equivalent of a test restore (i.e., verify the structure of your repository and the existence of all blobs AND download/decrypt/decompress all the files in your snapshots), then you need to use the `--verify-files-percent` option with the `kopia snapshot verify` command, such as `kopia snapshot verify --verify-files-percent=100`. `--verify-files-percent=100` tells Kopia to download/decrypt/decompress 100% of the files backed up in your repository. These files are downloaded temporarily and automatically discarded once Kopia has finished with each file, so you do not require a significant amount of hard drive space to use this command.

You can run

It is recommended to periodically verify repository by running:

```shell
$ kopia snapshot verify
```

This only verifies metadata and does not actually verify that data is intact and can be recovered, it is recommended to perform frequent partial verification, where each time the verification runs some percentage of files is downloaded and verifies. 

For example to verify metadata and 1% of data (so about 10GB for 1TB repositoy), use:

```shell
$ kopia snapshot verify --verify-files-percent=1
```

When running regularly, this will verify each file statistically every 100 days and will amortize the cost of large downloads over time.

## Repository Consistency

Kopia data structures and algorithms are designed to maintain data consistency even in the event of a crash or failure, but in rare cases the repository may still get corrupted, usually due to hardware or software misconfiguration.

This document describes common repository corruption scenarios, strategies for avoiding them and ways to recover.

Corruption is typically caused by one of three reasons:

* **misconfigured or unsupported filesystem** - Kopia requires certain properties of storage subsystems, including POSIX semantics, atomic writes (either native or emulated), strong read-after-write consistency and eventual list-after-write consistency.

  Many modern filesystems (especially networked ones) are not fully POSIX compliant and don't guarantee certain consistency in case of a crash, power outage, network partitions, high latency, etc. 

  Once such event occurs, it's possible for filesystem to misrepresent the set of files it is holding, possibly causing Kopia to assume certain files are unused. Kopia can compensate for such inconsistent behaviors for up to several minutes, but larger inconsistencies can lead to data loss.

  It is recommended to use the most mature filesystem available for an OS with full POSIX semantics and avoid emulated, layered or networked filesystems which may not be fully compliant.

* **silent data corruption after write** - most filesystems in use today are susceptible to silent data corruption (known as bit rot, bit flips, etc.) caused by bad hardware, radiation, etc..

  Modern filesystems such as ZFS or BTRFS use special encodings to be able to detect and recover from such issues, but they come with various trade-offs in terms of performance and capacity, so most filesystem deployments in common use today do not have these features. NOTE: RAID storage generally only provides increased availability and not bit rot protection.

  In cases a filesystem corruption is detected, there is very little Kopia can do about it and such event will lead to data loss. It is thus recommended to use the most reliable filesystem available or preferably use cloud storage which is generally not susceptible to such issues (although comes with a higher cost).

* **large clock skew** - Kopia requires _reasonable_ time synchronization between all clients and storage server, where clock skews of few minutes are tolerated, but bigger clock skews can lead to major inconsistencies, including Kopia deleting its own data because clocks indicate it is no longer needed.

  To protect against such issues, it's recommended to run NTP time synchronization which keeps local clock in sync with network time.

## Repairing Repository

Each type of data corruption is different, so there's not a single approach to data recovery.

There are few tips to try, which are generally safe to try:

1. Look for repository files with zero size and remove them. Kopia will never write such files and if they are present, it indicates filesystem data corruption. Simply removing them may work.

2. Try to open the repository from another computer. If that works, local cache may be corrupted and clearing that should fix the issue. Please do NOT clear the cache on the main computer where the corruption has manifested. Data in cache may help the recovery.

3. Look for files with unusual timestamps (like year 1970, year 2038 or similar), file extensions, etc and try to clean them up.

4. If a repository can't be opened but was working recently and maintenance has not run yet, it may be helpful to try to remove (or stash away) the most recently-written index files whose names start with `x` in the reverse timestamp order one by one until the issue is fixed. This will effectively roll back the repository writes to a prior state. Exercise caution when removing the files.

5. If the steps above do not help, report your issue on https://kopia.discourse.group or https://slack.kopia.io. Kopia has many low-level data recovery tools, but they should not be used by end users without guidance from developers.

>NOTE: Since all data corruption cases are unique, it's generally not recommended to attempt fixes recommended to other users even for possibly similar issues, since the particular fix method may not be applicable. 


