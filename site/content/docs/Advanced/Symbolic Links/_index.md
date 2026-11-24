---
title: "Symbolic Links"
linkTitle: "Symbolic Links"
weight: 75
---

## Symbolic Links in Snapshots

By default, Kopia stores a symbolic link (symlink) in a snapshot as a symlink: the snapshot records where the link points, but not the contents of the file or directory it points to. Restoring the snapshot recreates the link.

This keeps the snapshot small and preserves the links as they are, but it does not capture the data when a directory consists mostly of links to files stored somewhere else, for example a share that is assembled from links into a storage pool.

### Following Symbolic Links

The `followSymlinks` files policy setting makes Kopia snapshot what a link points to instead of the link itself:

* a link to a file is stored as a regular file with the name of the link and the contents of the file it points to
* a link to a directory is stored as a directory with the name of the link and the contents of the directory it points to, including its subdirectories

To turn it on for a directory that you snapshot:

```shell
kopia policy set /path/to/directory --follow-symlinks=true
```

To turn it off again, or to inherit the setting from a parent policy:

```shell
kopia policy set /path/to/directory --follow-symlinks=false
kopia policy set /path/to/directory --follow-symlinks=inherit
```

The setting is shown by `kopia policy show` as `Follow symbolic links` and is off by default, so existing snapshots are not affected.

The setting applies to the directory the policy is defined for and everything below it. A policy defined for a subdirectory keeps the setting unless that policy sets `--follow-symlinks` itself, and a policy defined for a single link can turn it on or off for that link.

Restoring a snapshot taken with this setting produces regular files and directories, not links.

### Things to Know

* **Data from outside the directory is included.** A link can point anywhere the user running Kopia can read, so the snapshot can contain data that is not located under the directory being snapshotted.
* **Do not enable this where untrusted users can create links.** Kopia follows a link with its own permissions, so a link created by another user can bring a file into the snapshot that the user who created the link could not read, for example when Kopia runs as root. Anyone who can read the snapshot can then read that file.
* **A target reached through several links is stored once but listed under each name.** Kopia stores identical content only once in the repository, but each path appears in the snapshot as a separate file or directory.
* **Links that cannot be followed stay symlinks.** A link whose target does not exist, a link to something that is neither a file nor a directory (such as a named pipe or a device), and a link to a directory that Kopia is already scanning (a loop, such as a link to a parent directory) are stored as symlinks, as if the setting was off, and the snapshot does not fail. Kopia logs a warning for a loop and a debug message for the other cases.
* **Metadata comes from the target.** The size, modification time, permissions and owner of a followed link are those of the file or directory it points to. The first snapshot after turning the setting on reads the files behind the links again, because the previous snapshot recorded links.
* **Ignore rules see the target.** A rule such as `logs/` that only matches directories matches a link to a directory when links are followed. The `max file size` setting compares the size of the target.
* **`--one-file-system` applies to the target.** When both settings are on, a link whose target is on another filesystem is not included.
* **Mount the targets.** When Kopia runs in a container or another environment where the link targets are not available at the paths the links use, the links cannot be followed. Make the targets available at the same paths.

### Windows

Symbolic links created with `mklink` or `New-Item -ItemType SymbolicLink` are followed like on other platforms. Directory junctions are not symbolic links and are not followed. Kopia treats them as entries of unknown type, which are skipped by default.
