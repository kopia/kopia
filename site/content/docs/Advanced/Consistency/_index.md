---
title: "Verifying Validity of Snapshots/Repositories and Trying to Repair Any Corruption"
linkTitle: "Testing Validity of Backups and Fixing Corruption"
weight: 35
---

## Consistency 

Backing up data is great, but you also need to be able to restore that data when (if) the time arises. That means you need to test snapshots regularly to ensure that a snapshot remains valid and has not become corrupt after you have created the snapshot. (There are various different reasons why a snapshot may become corrupt; see the [corruption repair discussion below](#repairing-corruption-of-snapshotsrepositories) for more details.)

### Verifying Validity of Snapshots 

There are many verification methods, depending on what you need:

In the order of lowest- to highest-level:

1. [`kopia content verify`](../../reference/command-line/common/content-verify/) - will ensure that content manager index structures are correct and that every index entry is backed by an existing file

2. `kopia content verify --download-percent=10` - same as above, but will download 10% of random contents and ensure they can be decrypted properly

3. [`kopia snapshot verify`](../../reference/command-line/common/snapshot-verify/) - will ensure that directory structures in the repository are consistent by walking all files and directories in snapshots from their roots and performing equivalent of `kopia content verify` on all contents required to restore each file, but does not download the file. **This is done during every [daily full maintenance](../maintenance/), so you do not need to run the command yourself.**

4. `kopia snapshot verify --verify-files-percent 10` - same as above but will also download random 10% of all files, this ensures that decryption and decompression is correct.

In detail:

The gold standard for testing the validity of backups is to simply [restore the snapshot](../../getting-started/); if restore is successful, then the backup is valid. However, in many situations it is not feasible to conduct a full restore of a snapshot, such as if you do not have enough local hard drive space to do a full restore or if the egress costs of your cloud storage are extremely high (e.g., Amazon S3 charges $0.09 per GB you download). 

If you cannot, or do not want to, do a full test restore of your snapshots, Kopia enables you to verify the consistency/validity of your snapshots/repositories using the [`kopia snapshot verify` command](../../reference/command-line/common/snapshot-verify/). This command ensures that the directory structures in your repository are consistent by walking all files and directories in the snapshots from their roots; it verifies that the content manager index structures are correct and that every index entry is backed by an existing file. All of this is to ensure that your snapshots contain all the necessary metadata and blobs to restore each backed up file, should the need ever arise for you to restore your files.

Both Kopia CLI and KopiaUI automatically run `kopia snapshot verify` during every [daily full maintenance](../maintenance/), so you do not need to run the command yourself. However, while `kopia snapshot verify` verifies the existence of all blobs necessary to restore your snapshots, the command stops short of downloading, decrypting, and decompressing the blobs. This means that the command does not test whether blobs have been corrupted after they have been uploaded by Kopia, due to bit rot, bit flip, etc. 

If you want Kopia to test for bit rot and related data corruption issues and to conduct what is essentially the equivalent of a test restore (i.e., verify the structure of your repository and the existence of all blobs AND download/decrypt/decompress all the files in your snapshots), then you need to use the `--verify-files-percent` option with the `kopia snapshot verify` command, such as `kopia snapshot verify --verify-files-percent=100 --file-parallelism=10 --parallel=10`. `--verify-files-percent=100` tells Kopia to download/decrypt/decompress 100% of the files backed up in your repository. (The `--file-parallelism=10 --parallel=10` options are optional and tell Kopia to use parallelism to speed up the process. You can exclude these two options, if you so desire.) These files are downloaded temporarily and automatically discarded once Kopia has finished with each file, so you do not require a significant amount of hard drive space to use this command. If Kopia throws no errors while running this command, then your snapshots/repositories are valid. If Kopia does throw an error, then you may have a corruption issue, in which case you need to read the [corruption repair discussion below](#repairing-corruption-of-snapshotsrepositories).

You can run `kopia snapshot verify --verify-files-percent=100 --file-parallelism=10 --parallel=10` monthly or at whatever interval you desire. 

If you do not want Kopia to download 100% of your files, you can set `--verify-files-percent` to any percent you want. For example, `--verify-files-percent=1` will download a random selection of 1% of your data. If you are unable to, or do not want to, regularly run `kopia snapshot verify --verify-files-percent=100`, then it is recommended to at least run `kopia snapshot verify --verify-files-percent=1 --file-parallelism=10 --parallel=10` daily. If you run this command daily, statistically over the course of a year you have a roughly 98% likelihood to have tested 100% of your backed up data. 

Currently, `kopia snapshot verify --verify-files-percent=# --file-parallelism=10 --parallel=10` must be run via CLI. KopiaUI does not yet have the ability to run `kopia snapshot verify` with the `--verify-files-percent` option, so all KopiaUI users will need to run the command via CLI.

## Repairing Corruption of Snapshots

### How Corruption Happens

Kopia data structures and algorithms are designed to maintain data consistency even in the event of a crash or failure, but in rare cases a repository may still get corrupted, usually due to hardware or software misconfiguration. Corruption is typically caused by one of three reasons:

* **Misconfigured or unsupported filesystem** -- Kopia requires certain properties of storage subsystems, including POSIX semantics, atomic writes (either native or emulated), strong read-after-write consistency and eventual list-after-write consistency. Many modern filesystems (especially networked ones) are not fully POSIX compliant and don't guarantee certain consistency in case of a crash, power outage, network partitions, high latency, etc. Once such an event occurs, it's possible for the filesystem to misrepresent the set of files it is holding, possibly causing Kopia to assume certain files are unused. Kopia can compensate for such inconsistent behaviors for up to several minutes, but larger inconsistencies can lead to data loss.

> PRO TIP: It is recommended to use the most mature filesystem available for an OS with full POSIX semantics and avoid emulated, layered, or networked filesystems which may not be fully compliant. Alternatively, use one of the [cloud storage repositories](../../repositories/) supported by Kopia.

* **Silent data corruption after write** -- most filesystems in use today are susceptible to silent data corruption (known as bit rot, bit flips, etc.) caused by bad hardware, radiation, etc. Modern filesystems such as ZFS or BTRFS use special encodings to be able to detect and recover from such issues, but they come with various trade-offs in terms of performance and capacity, so most filesystem deployments in common use today do not have these features. (RAID storage generally only provides increased availability and not bit rot protection.) In cases when a filesystem corruption is detected, there is very little Kopia can do about it and such event will lead to data loss. It is thus recommended to use the most reliable filesystem available or preferably use [cloud storage](../../repositories/) which is generally better at preventing such issues.

* **Large clock skew** -- Kopia requires _reasonable_ time synchronization between all clients and storage servers, where clock skews of few minutes are tolerated, but bigger clock skews can lead to major inconsistencies, including Kopia deleting its own data because clocks indicate it is no longer needed. To protect against such issues, it's recommended to run NTP time synchronization on both clients and servers in order to ensure that local clock is up-to-date (if you are using [cloud storage](../../repositories/), you only need to worry about keeping the clock for your computers up-to-date).

### Repairing Corruption

Each type of data corruption is different, so there’s not a single approach to data recovery.

There are few tips to try, which are generally safe to try:

1. Look for repository files with zero size and remove them. Kopia will never write such files and if they are present, it indicates filesystem data corruption. Simply removing them may work.

2. Try to open the repository from another computer. If that works, local cache may be corrupted and clearing that should fix the issue. Please do NOT clear the cache on the main computer where the corruption has manifested. Data in cache may help the recovery.

3. Look for files with unusual timestamps (like year 1970, year 2038 or similar), file extensions, etc and try to clean them up.

4. If a repository can’t be opened but was working recently and maintenance has not run yet, it may be helpful to try to remove (or stash away) the most recently-written index files whose names start with `x` in the reverse timestamp order one by one until the issue is fixed. This will effectively roll back the repository writes to a prior state. Exercise caution when removing the files.

5. If the steps above do not help, report your issue on https://kopia.discourse.group or https://slack.kopia.io. Kopia has many low-level data recovery tools, but they should not be used by end users without guidance from developers.

> NOTE: Since all data corruption cases are unique, it’s generally not recommended to attempt fixes recommended to other users even for possibly similar issues, since the particular fix method may not be applicable.
