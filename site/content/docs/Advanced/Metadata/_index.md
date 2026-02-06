---
title: "File Metadata"
linkTitle: "File Metadata"
weight: 25
---

## File Metadata

When Kopia creates snapshots, it captures and stores metadata about each file and directory. This page documents what metadata is preserved during backup and what is not.

### Metadata That Is Backed Up

Kopia stores the following metadata for each file, directory, and symbolic link:

| Metadata | Description | Notes |
|----------|-------------|-------|
| **Name** | File or directory name | Always preserved |
| **Type** | Entry type (file, directory, or symlink) | Always preserved |
| **Permissions** | Unix permission bits (rwx for owner, group, others) | Includes setuid, setgid, and sticky bits |
| **Modification Time** | Last modification timestamp (mtime) | Stored in UTC |
| **File Size** | Size of file content in bytes | Directories show aggregate size |
| **User ID (UID)** | Numeric user ID of the owner | Unix/Linux/macOS only; Windows returns 0 |
| **Group ID (GID)** | Numeric group ID | Unix/Linux/macOS only; Windows returns 0 |
| **Symlink Target** | Target path for symbolic links | Preserved exactly as-is |

#### Directory Summary

For directories, Kopia also stores aggregate summary information:

- Total file count
- Total directory count
- Total symbolic link count
- Total size of all files
- Maximum modification time among all entries
- Error counts and failed entry information (if any)

### Metadata That Is NOT Backed Up

The following metadata is **not** captured or preserved by Kopia:

| Metadata | Description |
|----------|-------------|
| **Extended Attributes (xattr)** | User-defined metadata (e.g., `user.*`, `system.*`, `security.*`) |
| **Access Control Lists (ACLs)** | Fine-grained permission controls beyond basic Unix permissions |
| **SELinux Contexts** | Security-Enhanced Linux labels and contexts |
| **Access Time (atime)** | Last access timestamp |
| **Change Time (ctime)** | Inode change timestamp |
| **Creation Time (birthtime)** | File creation timestamp (where supported) |
| **Immutable Flags** | File attributes like immutable (`chattr +i`) or append-only |
| **Hard Link Information** | Inode relationships; hard links are stored as separate files |
| **Sparse File Metadata** | Sparse file hole information (content is preserved, but restored as regular files) |
| **Windows Alternate Data Streams** | Additional NTFS data streams beyond the primary stream |
| **Windows ACLs** | Windows-specific access control entries |
| **macOS Resource Forks** | Legacy macOS file metadata (resource fork data) |
| **macOS Finder Flags** | Finder-specific attributes (labels, locked status, etc.) |

### Platform-Specific Behavior

#### Unix/Linux/macOS

- **Ownership**: UID and GID are captured from the source filesystem and can be restored using `chown`. Root privileges may be required to restore ownership.
- **Permissions**: Full permission bits including setuid, setgid, and sticky bits are preserved and restored using `chmod`.
- **Symbolic Links**: Use `lchown` and `lutimes` for ownership and timestamp operations (note: Linux does not support permission bits on symlinks).

#### Windows

- **Ownership**: UID and GID are reported as 0 and are not captured.
- **Chown**: Ownership restoration is not attempted on Windows.
- **Permissions**: Basic permission bits are captured, but Windows ACLs are not preserved.
- **VSS Snapshots**: Kopia supports backing up from Windows Volume Shadow Copy paths.

### Restoration Options

When restoring files, you can optionally skip certain metadata restoration using the following flags with `kopia snapshot restore`:

| Flag | Effect |
|------|--------|
| `--skip-owners` | Do not restore UID/GID ownership |
| `--skip-permissions` | Do not restore permission bits |
| `--skip-times` | Do not restore modification times |
| `--ignore-permission-errors` | Continue on permission errors instead of failing |

### Viewing Metadata

You can view the metadata stored for files in a snapshot:

```shell
# List files with metadata
kopia ls -l <snapshot-id>

# View raw directory structure as JSON
kopia content show -j <directory-object-id>
```

The JSON output includes all stored metadata fields:

```json
{
  "name": "example.txt",
  "type": "f",
  "mode": "0644",
  "size": 1024,
  "mtime": "2024-01-15T10:30:00Z",
  "uid": 1000,
  "gid": 1000,
  "obj": "abc123..."
}
```

### Implications

When planning your backup strategy, consider these implications:

1. **ACLs and Extended Attributes**: If your files rely on ACLs or extended attributes for security or application functionality, you will need supplementary backup solutions for this metadata.

2. **Hard Links**: Restoring a snapshot with hard-linked files will create separate copies, potentially increasing disk usage compared to the original.

3. **Cross-Platform Restores**: When restoring backups from Unix to Windows (or vice versa), ownership information may not transfer meaningfully.

4. **Permission Requirements**: Restoring files with original ownership (UID/GID) typically requires root/administrator privileges.
