# Storage Recovery Implementation Guide

## Overview
This PR implements a robust mechanism to handle "No space left on device" (ENOSPC) conditions in Kopia. By introducing a **Storage Reserve** and an **Automatic Emergency Mode**, Kopia can now recover from a completely full disk without requiring manual file deletions by the user.

## Architecture

### 1. `internal/storagereserve`
A new package dedicated to managing the repository's safety buffer.
- **Reserve Blob (`kopia.reserve`)**: A 500MB blob (by default) that acts as emergency "working capital."
- **Memory-Efficient Creation**: Uses a custom stream generator to create large blobs without loading them into RAM.
- **Dynamic Headspace Rule**: To prevent "ghost" files (failed writes that still take space), the reserve is only created or restored if the disk has enough space for the file **plus 10% of the total volume capacity** as breathing room.

### 2. Emergency Recovery Mode
Triggered automatically when a critical operation detects that the storage reserve is missing and cannot be maintained.

#### Detection and Activation
- **In Maintenance**: `RunExclusive` proactively calls `storagereserve.Ensure`. If this fails with `ErrInsufficientSpace` or `ENOSPC`, the system enters **Emergency Mode** and sets the `LowSpace` flag in `RunParameters`.
- **In Snapshot Deletion**: The `delete` command uses a sacrificial logic: it tries to ensure the reserve, but if it fails, it **deletes the existing reserve** to grant immediate space for the deletion metadata.

#### Emergency Behavior
When `LowSpace` is active:
- **Sacrificial Buffer**: The `kopia.reserve` file is deleted immediately.
- **Forced Physical GC**: Garbage Collection is automatically downgraded to **`safety=none`**. This bypasses the 24-hour safety margin and physically purges deleted snapshot data from the disk.
- **Prioritized Tasks**: Maintenance skips space-consuming optimizations (like index compaction or rewriting) and focuses solely on reclaiming space via `snapshot-gc` and `quick-delete-blobs`.

## Operational Guards

### `snapshot create`
- Proactively checks for the reserve.
- If the reserve is missing and the disk is too full to recreate it, the snapshot job is **blocked**. This prevents the repository from reaching a state where even recovery manifests cannot be written.

### `repo connect` / `repo create`
- Automatically initializes the reserve file.
- `connect` performs a "lazy creation," meaning older repositories will automatically gain a reserve the first time a newer client connects to them (if space is available).

## User Experience
- **Transparent Recovery**: Users don't need to know about `--safety=none` or manual blob deletion. They simply run `maintenance run` or `snapshot delete`, and Kopia "rescues" itself.
- **Clear Logging**: Emergency actions are loudly logged (e.g., `"Emergency mode detected: forcing safety=none to reclaim space immediately"`).

## Verification Performed
The implementation was verified using a 2GiB virtual disk simulation:
1.  Repository initialized (Reserve created).
2.  Disk filled to 100% via a successful 1GiB snapshot + a failed second snapshot.
3.  **Recovery Phase 1**: Deleted the successful snapshot. The logic successfully sacrificed the 500MB reserve to allow the deletion manifest to be written.
4.  **Recovery Phase 2**: Ran maintenance. The system entered Emergency Mode, detected the low space, forced `safety=none`, and physically reclaimed 1.1GB of space.
5.  **Recreation**: Verified the 500MB reserve was automatically restored once space became available.
