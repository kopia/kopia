---
title: "Actions"
linkTitle: "Actions"
weight: 45
---

## Actions

Starting with v0.8 Kopia supports running custom user-provided commands or scripts before and after snapshot root and also before/after individual folders as they get snapshotted. This supports scenarios such as:

- creating snapshots of filesystems that support it (e.g. ZFS)
- snapshotting databases or virtual machines as part of taking a snapshot
- sending notifications to users, etc.

Actions can optionally modify the directory to be snapshotted or redirect upload to another directory (typically a mountpoint representing filesystem snapshot).

### Enabling Actions

To reduce the security risk, actions are an opt-in feature and are not enabled by default.

When using Kopia CLI, actions can be enabled globally at connection time or individually per snapshot:

1. When connecting to repository you can pass `--enable-actions` which will enable actions globally
   for the client.
2. You can override that decision when taking snapshot by passing `--force-enable-actions` or
   `--force-disable-actions` to enable or disable actions for the single snapshot session.

When using KopiaUI, actions can be enabled globally by editing your repository.config (it is located in the `Config File` location in KopiaUI under `Repository`) and change `"enableActions": false` to `"enableActions": true`. Save the file and restart KopiaUI.

### Configuring actions

To set the script for a directory we can use `kopia policy set` on a directory.

For example:

```
$ kopia policy set /some/dir --before-folder-action /path/to/command
$ kopia policy set /some/dir --after-folder-action /path/to/command
$ kopia policy set /some/dir --before-snapshot-root-action /path/to/command
$ kopia policy set /some/dir --after-snapshot-root-action /path/to/command
```

>NOTE: Unlike all other policy options, `--before-folder-action` and `--after-folder-action` are not inherited and must be set explicitly on target folders, while `--before-snapshot-root-action` and `--after-snapshot-root-action` are inherited from their parents and can be set at global, host, user or directory level.

Actions can be `essential` (must succeed, default behavior), `optional` (failures are tolerated) or `async` (kopia will start the action but not wait for it to finish). This can be set
using `--action-command-mode`, for example:

```
$ kopia policy set /some/dir  --action-command-mode=async \
   --before-folder-action /usr/local/bin/notifier.sh
```

Each action has an associated timeout (by default 5 minutes), which specifies how long it will be allowed to run before Kopia kills the process. This can be overridden using `--action-command-timeout`:

```
$ kopia policy set /some/dir  --action-command-timeout=180s \
   --before-folder-action /usr/local/bin/notifier.sh
```

Finally, the action command itself can be stored in a repository, when `--persist-action-script` is passed. To prevent binaries from being stored, the maximum script length can be up to 32000 characters.

Scripts stored like this will be temporarily extracted to a local directory and executed using a shell command, which is:

* On Linux and macOS: `sh -e /path/to/temporary/script/file.sh`
* On Windows: `cmd.exe /c C:\path\to\temporary\script\file.cmd`

On Unix, if the script has `#!` prefix, it will be executed directly, bypassing the `/bin/sh` shell.

### Action Environment

When kopia invokes `Before` actions, it passes the following parameters:

| Variable                 | Description                               |
| ------------------------ | ----------------------------------------- |
| `KOPIA_ACTION`           | `before-folder` or `before-snapshot-root` |
| `KOPIA_SNAPSHOT_ID`      | Random 64-bit number                      |
| `KOPIA_SOURCE_PATH`      | Path being snapshotted                    |
| `KOPIA_SNAPSHOT_PATH`    | Path being snapshotted                    |
| `KOPIA_VERSION`          | Version of Kopia (e.g. `0.9.2`)           |

The action command can modify the contents source directory in place or it can request other directory
be snapshotted instead by printing a line to standard output:

```
KOPIA_SNAPSHOT_PATH=<new-directory>
```

This can be used to create point-in-time snapshots - see examples below.

The `After` action will receive similar parameters as `Before` plus the actual directory that was
snapshotted (either `KOPIA_SOURCE_PATH` or `KOPIA_SNAPSHOT_PATH` if returned by the `Before` script).

| Variable                 | Description                               |
| ------------------------ | ----------------------------------------- |
| `KOPIA_ACTION`           | `after-folder` or `after-snapshot-root`   |
| `KOPIA_SNAPSHOT_ID`      | Random 64-bit number                      |
| `KOPIA_SOURCE_PATH`      | Source path being snapshotted             |
| `KOPIA_SNAPSHOT_PATH`    | Actual path being snapshotted (returned by the _before_ action) |
| `KOPIA_VERSION`          | Version of Kopia (e.g. `0.9.2`)           |


## Examples

### Dumping SQL databases before snapshotting:

This script invokes `mysqldump` to create a file called `dump.sql` in the directory
that's being snapshotted. This can be used to automate database backups.

```shell
#!/bin/sh
set -e
mysqldump SomeDatabase --result-file=$KOPIA_SOURCE_PATH/dump.sql
```

### ZFS point-in-time snapshotting:

When snapshotting ZFS pools, we must first create a snapshot using `zfs snapshot`, mount it somewhere
and tell `kopia` to snapshot the mounted directory instead of the current one.

After snapshotting, we need to unmount and destroy the temporary snapshot using `zfs destroy` command:

Before:

```shell
#!/bin/sh
set -e
ZPOOL_NAME=tank
zfs snapshot $ZPOOL_NAME@$KOPIA_SNAPSHOT_ID
mkdir -p /mnt/$KOPIA_SNAPSHOT_ID
mount -t zfs $ZPOOL_NAME@$KOPIA_SNAPSHOT_ID /mnt/$KOPIA_SNAPSHOT_ID
echo KOPIA_SNAPSHOT_PATH=/mnt/$KOPIA_SNAPSHOT_ID
```

After:

```shell
#!/bin/sh
ZPOOL_NAME=tank
umount /mnt/$KOPIA_SNAPSHOT_ID
rmdir /mnt/$KOPIA_SNAPSHOT_ID
zfs destroy $ZPOOL_NAME@$KOPIA_SNAPSHOT_ID
```

### LVM Snapshots:

When snapshotting filesystems using LVM snapshots, we must first create a LVM snapshot using `lvcreate` and mount the filesystem inside the LVM snapshot somewhere.
Then we tell `kopia` to snapshot the mounted directory instead of the current one.
Make sure to match the snapshot size with your requirements. The snapshot grows with the delta to the origin logical volume.
You also need to make sure to have enough free space in your volume group, otherwise the snapshot creation will fail.

After snapshotting, we need to unmount and remove the temporary logical volume using `lvremove` command:

Before:

```shell
#!/bin/sh
set -e
VG_NAME=vg0
LV_NAME=lv-root
SNAPSHOT_SIZE=10G
lvcreate -L ${SNAPSHOT_SIZE} -s -n $KOPIA_SNAPSHOT_ID $VG_NAME/$LV_NAME
mkdir -p /mnt/$KOPIA_SNAPSHOT_ID
mount /dev/$VG_NAME/$KOPIA_SNAPSHOT_ID /mnt/$KOPIA_SNAPSHOT_ID
echo KOPIA_SNAPSHOT_PATH=/mnt/$KOPIA_SNAPSHOT_ID
```

After:

```shell
#!/bin/sh
VG_NAME=vg0
umount /mnt/$KOPIA_SNAPSHOT_ID
rmdir /mnt/$KOPIA_SNAPSHOT_ID
lvremove -f $VG_NAME/$KOPIA_SNAPSHOT_ID
```

### Windows shadow copy

When backing up files opened with exclusive lock in Windows, Kopia would fail the snapshot task because it can't read the file content.
One of the popular solutions is taking a [shadow copy](https://en.wikipedia.org/wiki/Shadow_Copy) of the storage volume and ask Kopia to backup that instead.

In this example, we will use [PowerShell](https://github.com/PowerShell/PowerShell/) to take a shadow copy in the "before" action of the target directory and clean everything up in the "after" action.
The script also self-elevates as administrator (required to take shadow copy) if Kopia is ran with an unprivileged account.

Make sure `powershell` is reachable in the PATH environment variable.

before.ps1:

```powershell
if ($args.Length -eq 0) {
    $kopiaSnapshotId = $env:KOPIA_SNAPSHOT_ID
    $kopiaSourcePath = $env:KOPIA_SOURCE_PATH
} else {
    $kopiaSnapshotId = $args[0]
    $kopiaSourcePath = $args[1]
}

$sourceDrive = Split-Path -Qualifier $kopiaSourcePath
$sourcePath = Split-Path -NoQualifier $kopiaSourcePath
# use Kopia snapshot ID as mount point name for extra caution for duplication
$mountPoint = "${PSScriptRoot}\${kopiaSnapshotId}"

if (([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] 'Administrator')) {
    $shadowId = (Invoke-CimMethod -ClassName Win32_ShadowCopy -MethodName Create -Arguments @{ Volume = "${sourceDrive}\" }).ShadowID
    $shadowDevice = (Get-CimInstance -ClassName Win32_ShadowCopy | Where-Object { $_.ID -eq $shadowId }).DeviceObject
    if (-not $shadowDevice) {
        # fail the Kopia snapshot early if shadow copy was not created
        exit 1
    }

    cmd /c mklink /d $mountPoint "${shadowDevice}\"
} else {
    $proc = Start-Process 'powershell' '-f', $MyInvocation.MyCommand.Path, $kopiaSnapshotId, $kopiaSourcePath -PassThru -Verb RunAs -WindowStyle Hidden -Wait
    if ($proc.ExitCode) {
        exit $proc.ExitCode
    }
}

Write-Output "KOPIA_SNAPSHOT_PATH=${mountPoint}${sourcePath}"
```

after.ps1:

```powershell
if ($args.Length -eq 0) {
    $kopiaSnapshotId = $env:KOPIA_SNAPSHOT_ID
} else {
    $kopiaSnapshotId = $args[0]
}

if (([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] 'Administrator')) {
    $mountPoint = Get-Item "${PSScriptRoot}\${kopiaSnapshotId}"
    $mountedVolume = $mountPoint.Target

    cmd /c rmdir $mountPoint
    Get-CimInstance -ClassName Win32_ShadowCopy | Where-Object { "$($_.DeviceObject)\" -eq "\\?\${mountedVolume}" } | Remove-CimInstance
} else {
    Start-Process 'powershell' '-f', $MyInvocation.MyCommand.Path, $kopiaSnapshotId -Verb RunAs -WindowStyle Hidden -Wait
    if ($proc.ExitCode) {
        exit $proc.ExitCode
    }
}
```

To install the actions:

```shell
kopia policy set <target_dir> --before-folder-action "powershell -WindowStyle Hidden -File <path_to_script>\before.ps1"
kopia policy set <target_dir> --after-folder-action  "powershell -WindowStyle Hidden -File <path_to_script>\after.ps1"
```

### Contributions Welcome

Those are just some initial ideas, we're certain more interesting types of actions will be developed using this mechanism, including LVM snapshots, BTRFS Snapshots, notifications and more.

If you have ideas for extending this mechanism, definitely [file an Issue on Github](https://github.com/kopia/kopia/issues).

If you develop a useful action script that you'd like to share with the community, we encourage you
to do so by sending us a pull request to add to this web page or you can put them in your own repository and we'll be happy to link it from here.

To get started, click 'Edit This Page' link.
