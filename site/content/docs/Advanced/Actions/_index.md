---
title: "Actions"
linkTitle: "Actions"
weight: 15
---

Starting with v0.8 Kopia supports running custom user-provided commands or scripts before and after snapshot root and also before/after individual folders as they get snapshotted. This supports scenarios such as:

- creating snapshots of filesystems that support it (e.g. ZFS)
- snapshotting databases or virtual machines as part of taking a snapshot
- sending notifications to users, etc.

Actions can optionally modify the directory to be snapshotted or redirect upload to another directory (typically a mountpoint representing filesystem snapshot).

### Enabling Actions

To reduce the security risk, actions are an opt-in feature and are not enabled by default.

Actions can be enabled globally at connection time or individually per snapshot:

1. When connecting to repository you can pass `--enable-actions` which will enable actions globally
   for the client.
2. You can override that decision when taking snapshot by passing `--force-enable-actions` or
   `--force-disable-actions` to enable or disable actions for the single snapshot session.

### Configuring actions

To set the script for a directory we can use `kopia policy set` on a directory.

For example:

```
$ kopia policy set /some/dir --before-folder-action /path/to/command
$ kopia policy set /some/dir --after-folder-action /path/to/command
$ kopia policy set /some/dir --before-snapshot-root-action /path/to/command
$ kopia policy set /some/dir --after-snapshot-root-action /path/to/command
```

>NOTE: Unlike all other policy options, `--before-folder-action` and `--after-folder-action` are not inherited and must be set explicitly on target folders, while `--before-snapshot-root-action` and `--after-snapshot-root-action` are interited from their parents and can be set at global, host, user or directory level.

Actions can be `essential` (must succeed, default behavior), `optional` (failures are tolerated) or `async` (kopia will start the action but not wait for it to finish). This can be set 
using `--action-command-mode`, for example:

```
$ kopia policy set /some/dir  --mode=async \
   --before-folder-action /usr/local/bin/notifier.sh
```

Each action has an associated timeout (by default 5 minutes), which specifies how long it will be allowed to run before Kopia kills the process. This can be overridden using `--action-command-timeout`:

```
$ kopia policy set /some/dir  --action-command-timeout=180s \
   --before-folder-action /usr/local/bin/notifier.sh
```

Finally, the action command itself can be stored in a repository, when `--persist-action-script` is passed. To prevent binaries from being stored, the maximum script length can be up to 32000 characters.

Scripts stored like this will be temporarily extracted to a local directory and executed using shell command, which is:

* On Linux and macOS: `sh -e /path/to/temporary/script/file.sh"
* On Windows: "cmd.exe /c C:\path\to\temporary\script\file.cmd"

On Unix, if the script has `#!` prefix, it will be executed directly, bypassing the `/bin/sh` shell.

### Action Environment

When kopia invokes `Before` actions, it passes the following parameters:

| Variable                 | Before | After | Description            |
| ------------------------ | ------ | ----- | ---------------------- |
| `KOPIA_SNAPSHOT_ID`      |  Yes   | Yes   | Random 64-bit number   |
| `KOPIA_SOURCE_PATH`      |  Yes   | Yes   | Path being snapshotted |

The action command can modify the contents source directory in place or it can request other directory
be snapshotted instead by printing a line to standard output:

```
KOPIA_SNAPSHOT_PATH=<new-directory>
```

This can be used to create point-in-time snapshots - see examples below.

The `After` action will receive the same parameters as `Before` plus the actual directory that was 
snapshotted (either `KOPIA_SOURCE_PATH` or `KOPIA_SNAPSHOT_PATH` if returned by the `Before` script).

## Examples

#### Dumping SQL databases before snapshotting:

This script invokes `mysqldump` to create a file called `dump.sql` in the directory
that's being snapshotted. This can be used to automate database backups.

```
#!/bin/sh
set -e
mysqldump SomeDatabase --result-file=$KOPIA_SOURCE_PATH/dump.sql
```

#### ZFS point-in-time snapshotting:

When snapshotting ZFS pools, we must first create a snapshot using `zfs snapshot`, mount it somewhere
and tell `kopia` to snapshot the mounted directory instead of the current one.

After snapshotting, we need to unmount and destroy the temporary snapshot using `zfs destroy` command:

Before:

```
#!/bin/sh
set -e
ZPOOL_NAME=tank
zfs snapshot $ZPOOL_NAME@$KOPIA_SNAPSHOT_ID
mkdir -p /mnt/$KOPIA_SNAPSHOT_ID
mount -t zfs $ZPOOL_NAME@$KOPIA_SNAPSHOT_ID /mnt/$KOPIA_SNAPSHOT_ID
echo KOPIA_SNAPSHOT_PATH: /mnt/$KOPIA_SNAPSHOT_ID
```

After:

```
#!/bin/sh
ZPOOL_NAME=tank
umount /mnt/$KOPIA_SNAPSHOT_ID
rmdir /mnt/$KOPIA_SNAPSHOT_ID
zfs destroy $ZPOOL_NAME@$KOPIA_SNAPSHOT_ID
```

#### Contributions Welcome

Those are just some initial ideas, we're certain more interesting types of actions will be developed using this mechanism, including Windows VSS snapshots, LVM snapshots, BTRFS Snapshots, notifications and more. 

If you have ideas for extending this mechanism, definitely [file an Issue on Github](https://github.com/kopia/kopia/issues).

If you develop a useful action script that you'd like to share with the communnity, we encourage you
to do so by sending us a pull request to add to this web page or you can put them in your own repository and we'll be happy to link it from here.

To get started, click 'Edit This Page' link.
