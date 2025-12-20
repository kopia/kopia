# Kopia Storage Recovery Project

## Overview
This project implements a native recovery mechanism for Kopia when the underlying storage repository runs out of space. It uses a "Storage Reserve" strategy to ensure that maintenance and deletion operations have enough working space to complete even when the disk is technically full.

## The Problem
Kopia's architecture (CABS - Content Addressable Blob Storage) requires writing new data (indexes and manifests) to perform deletions and maintenance. When storage is 100% full:
- `snapshot delete` fails because it cannot write the "tombstone" manifest.
- `maintenance` fails because it cannot write the updated maintenance schedule or new consolidated index blobs.
- Users are trapped in a loop where they cannot free space because they have no space to run the cleanup.

## The Solution: Storage Reserve
We implement a reserved space mechanism (default ~500MB) that acts as emergency "working capital" for the repository.

### 1. The Reserve File
- A file named `kopia.reserve` is created in the repository root during `repo connect` or `repo create`.
- Managed by a dedicated internal package: `internal/storagereserve`.

### 2. Emergency Recovery Path
When a write operation fails with `ENOSPC` (No space left on device):
1. **Detection:** The error is caught at the repository/maintenance layer.
2. **Activation:** Kopia enters "Emergency Recovery Mode".
3. **Space Reclamation:** The `kopia.reserve` file is deleted, immediately granting ~500MB of free space.
4. **Prioritized Maintenance:** 
   - Maintenance runs with high priority on GC (Garbage Collection).
   - Space-filling operations (like creating new snapshots) are blocked.
   - Non-essential maintenance tasks are deferred.
5. **Restoration:** Once space is freed, the `kopia.reserve` file is automatically recreated.

### 3. Guarding Critical Tasks
- `snapshot create` and other space-filling tasks will check for the existence of the reserve.
- If the reserve is missing and cannot be recreated, these tasks will be blocked to prevent total exhaustion.

## Future Refinements
### 1. Memory-Backed Temporary Files
As a "last resort" safety net, `internal/tempfile` could be refactored to return an interface instead of `*os.File`. If disk-based temporary file creation fails with `ENOSPC` even after the reserve is deleted, Kopia could fall back to an in-memory buffer (e.g., `bytes.Buffer`). This would be particularly useful for:
- **`internal/bigmap`**: Used during Garbage Collection to track live objects. Allowing this to run in RAM would ensure GC can complete even if the underlying OS temporary directory is also full.

## Goal
1. Implement `internal/storagereserve` package.
2. Integrate reserve creation into `repo.Initialize`.
3. Implement "Emergency Mode" logic in `repo/maintenance`.
4. Add guards to `cli/command_snapshot_create.go` and `cli/command_snapshot_delete.go`.