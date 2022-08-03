---
title: "Verifying Validity of Snapshots/Repositories and Trying to Repair Any Corruption"
linkTitle: "Testing Validity of Backups and Fixing Corruption"
weight: 5
---

* [Verifying Validity of Snapshots/Repositories](#verifying-validity-of-snapshotsrepositories)
* [Repairing Corruption of Snapshots/Repositories](#repairing-corruption-of-snapshotsrepositories)

## Verifying Validity of Snapshots/Repositories

Backing up data is great, but you also need to be able to restore that data when (if) the time arises. That means you need to test snapshots regularly to ensure that a snapshot remains valid and has not become corrupt after you have created the snapshot. (There are various different reasons why a snapshot may become corrupt; see the [corruption repair discussion below](#repairing-corruption-of-snapshotsrepositories) for more details.)

The gold standard for testing the validity of backups is to simply [restore the snapshot](../../getting-started/); if restore is successful, then the backup is valid. However, in many situations it is not feasible to conduct a full restore of a snapshot, such as if you do not have enough local hard drive space to do a full restore or if the egress costs of your cloud storage are extremely high (e.g., Amazon S3 charges $0.09 per GB you download). 

If you cannot, or do not want to, do a full test restore of your snapshots, Kopia enables you to verify the consistency/validity of your snapshots/repositories using the [`kopia snapshot verify` command](../../reference/command-line/common/snapshot-verify/). This command ensures that the directory structures in your repository are consistent by walking all files and directories in the snapshots from their roots; it verifies that the content manager index structures are correct and that every index entry is backed by an existing file. All of this is to ensure that your snapshots contain all the necessary metadata and blobs to restore each backed up file, should the need ever arise for you to restore your files.

Both Kopia CLI and KopiaUI automatically run `kopia snapshot verify` during every [daily full maintenance](../maintenance/), so you do not need to run the command yourself. However, while `kopia snapshot verify` verifies the existence of all blobs necessary to restore your snapshots, the command stops short of downloading, decrypting, and decompressing the blobs. This means that the command does not test whether blobs have been corrupted after they have been uploaded by Kopia, due to bit rot, bit flip, etc. 

If you want Kopia to test for bit rot and related data corruption issues and to conduct what is essentially the equivalent of a test restore (i.e., verify the structure of your repository and the existence of all blobs AND download/decrypt/decompress all the files in your snapshots), then you need to use the `--verify-files-percent` option with the `kopia snapshot verify` command, such as `kopia snapshot verify --verify-files-percent=100`. `--verify-files-percent=100` tells Kopia to download/decrypt/decompress 100% of the files backed up in your repository. These files are downloaded temporarily and automatically discarded once Kopia has finished with each file, so you do not require a significant amount of hard drive space to use this command. If Kopia throws no errors while running this command, then your snapshots/repositories are valid. If Kopia does throw an error, then you may have a corruption issue, in which case you need to read the [corruption repair discussion below](#repairing-corruption-of-snapshotsrepositories).

You can run `kopia snapshot verify --verify-files-percent=100` monthly or at whatever interval you desire. 

If you do not want Kopia to download 100% of your files, you can set `--verify-files-percent` to any percent you want. For example, `--verify-files-percent=1` will download a random selection of 1% of your data. If you are unable to, or do not want to, regularly run `kopia snapshot verify --verify-files-percent=100`, then it is recommended to at least run `kopia snapshot verify --verify-files-percent=1` daily. If you run this command daily, statistically over the course of a year you have a roughly 98% likelihood to have tested 100% of your backed up data. 

Currently, `kopia snapshot verify --verify-files-percent=#` must be run via CLI. KopiaUI does not yet have the ability to run `kopia snapshot verify` with the `--verify-files-percent` option, so all KopiaUI users will need to run the command via CLI.

## Repairing Corruption of Snapshots/Repositories

### How Corruption Happens

Kopia data structures and algorithms are designed to maintain data consistency even in the event of a crash or failure, but in rare cases a repository may still get corrupted, usually due to hardware or software misconfiguration. Corruption is typically caused by one of three reasons:

* **Misconfigured or unsupported filesystem** -- Kopia requires certain properties of storage subsystems, including POSIX semantics, atomic writes (either native or emulated), strong read-after-write consistency and eventual list-after-write consistency. Many modern filesystems (especially networked ones) are not fully POSIX compliant and don't guarantee certain consistency in case of a crash, power outage, network partitions, high latency, etc. Once such an event occurs, it's possible for the filesystem to misrepresent the set of files it is holding, possibly causing Kopia to assume certain files are unused. Kopia can compensate for such inconsistent behaviors for up to several minutes, but larger inconsistencies can lead to data loss.

> PRO TIP: It is recommended to use the most mature filesystem available for an OS with full POSIX semantics and avoid emulated, layered, or networked filesystems which may not be fully compliant. Alternatively, use one of the [cloud storage repositories](../../repositories/) supported by Kopia.

* **Silent data corruption after write** -- most filesystems in use today are susceptible to silent data corruption (known as bit rot, bit flips, etc.) caused by bad hardware, radiation, etc. Modern filesystems such as ZFS or BTRFS use special encodings to be able to detect and recover from such issues, but they come with various trade-offs in terms of performance and capacity, so most filesystem deployments in common use today do not have these features. (RAID storage generally only provides increased availability and not bit rot protection.) In cases when a filesystem corruption is detected, there is very little Kopia can do about it and such event will lead to data loss. It is thus recommended to use the most reliable filesystem available or preferably use [cloud storage](../../repositories/) which is generally better at preventing such issues.

* **Large clock skew** -- Kopia requires _reasonable_ time synchronization between all clients and storage servers, where clock skews of few minutes are tolerated, but bigger clock skews can lead to major inconsistencies, including Kopia deleting its own data because clocks indicate it is no longer needed. To protect against such issues, it's recommended to run NTP time synchronization on both clients and servers in order to ensure that local clock is up-to-date (if you are using [cloud storage](../../repositories/), you only need to worry about keeping the clock for your computers up-to-date).

### Repairing Corruption

Each type of data corruption is different, so there's not a single approach to data recovery. And, to be clear, there is no guarantee that you will be able to recover from corruption. Nonetheless, Kopia is designed to minimize corruption and give you the best shot at recovering your snapshots/repositories if there is corruption, so if you do run into a corruption issue, there are several things you can try:

1. Disable any automatic snapshotting or maintenance or any other Kopia function. You don't want Kopia making changes to your repository while you are working on it.

2. If possible, make a copy of your whole repository before you proceed further. This step is not necessary, but it is recommended because part of the corruption fixing process involves deleting blobs. Having a backup of your backups (yes, the irony is not lost on us) helps you easily revert any changes you make. 

3. Look for repository files with zero size and remove them. Kopia will never write such files and if they are present, it indicates data corruption. Simply removing them may work.

4. Try to open the repository from another computer. If that works, local cache may be corrupted and clearing that should fix the issue. Please do NOT clear the cache on the main computer where the corruption has manifested. Data in cache may help the recovery.

5. Look for files with unusual timestamps (like year 1970, year 2038, or similar), file extensions, etc. and delete them. Exercise caution when deleting files. The [`kopia blob delete` command](../../reference/command-line/advanced/blob-delete/) can be used to delete the files.

6. If a repository can't be opened but was working recently and maintenance has not run yet, it may be helpful to try to remove the most recently-written index files whose names start with `x` in the reverse timestamp order one by one until the issue is fixed. This will effectively roll back the repository writes to a prior state. Exercise caution when removing the files. The [`kopia blob delete` command](../../reference/command-line/advanced/blob-delete/) can be used to delete the files.

7. Look at the error that Kopia is throwing. Identify the name(s) of the corrupt blobs. 

  * If the name(s) start with `n`, `m`, `l` and `x`, then the corruption is likely with index files. Delete the corrupt blobs, run `kopia index recover --parallel=10 --commit`, and see if that fixes your issue. The [`kopia blob delete` command](../../reference/command-line/advanced/blob-delete/) can be used to delete blobs. If not, go back and delete the whole directory that shares the name with the blob. For example, if the corrupt blob's name is `xn25_04abe9d83a08a65c09bda238af191587-s91f147a798c55c73110-c1`, then find the directory that starts with `xn25` and delete it. Then, run `kopia index recover --parallel=10 --commit` and see if that fixes your issue. If that still does not fix your issues, try running `kopia index recover --parallel=10 --commit --delete-indexes` and see if that fixes your issue.

  * If the name(s) start with `q`, then the corruption is likely with metadata. Run `snapshot fix invalid-files --parallel=10 --commit` and see if that fixes your issue.

  * If the name(s) start with `p`, then the corruption may be in your indexes, metadata, or in the actual blob that stores your backed up files. First, try `kopia index recover --parallel=10 --commit`. Next, try `kopia index recover --parallel=10 --commit --delete-indexes`. If you are still having issues, try `snapshot fix invalid-files --parallel=10 --commit`. Finally, try `snapshot fix invalid-files --parallel=10 --commit --verify-files-percent=100` but note that this will download all your files, so it will take a while.
  
8. If all recovery attempts fail and you still have access to your original files, you can wipe the whole repository and start over with a clean snapshot. (Note that simply deleting snapshots from within Kopia does not wipe a repository. You need to manually delete all the files from the repository.) This allows Kopia to recreate clean backups without corruption, but keep in mind that if the corruption was caused by some hardware or software issue at your storage location, corruption may happen again.
  
If the steps above do not help, report your issue on https://kopia.discourse.group or https://slack.kopia.io. Kopia has many low-level data recovery tools, but they should not be used by end users without guidance from developers.

> NOTE: Since all data corruption cases are unique, it's generally not recommended to attempt fixes recommended to other users even for possibly similar issues, since the particular fix method may not be applicable to you.
