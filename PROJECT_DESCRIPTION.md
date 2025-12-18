# Kopia Storage Recovery Project

## Overview
This project aims to implement a native recovery mechanism for Kopia when the underlying storage repository runs out of space. 

## The Problem
Currently (as of v0.10.4), when Kopia's storage repository becomes full, the application enters an unrecoverable state where:
- All operations fail due to inability to create temporary files
- `snapshot delete` commands cannot execute
- `maintenance` operations cannot run to clean up and free space
- Users are forced to manually delete repository files, risking data corruption

## Current Behavior
When storage is exhausted, Kopia throws errors like:
```
ERROR error updating maintenance schedule: unable to complete PutBlobInPath despite 10 retries, 
last error: cannot create temporary file: There is not enough space on the disk. 
```

This affects all operations including:
- Snapshot deletion
- Maintenance runs
- Any command requiring temporary file creation

## Expected Behavior
Kopia should be able to gracefully handle out-of-space conditions by:
- Allowing snapshot deletion operations to proceed without requiring temp space
- Enabling maintenance operations to clean up and reclaim storage
- Providing a native recovery path that doesn't require manual file deletion

## Goal
Implement a recovery mechanism that allows Kopia to: 
1. Delete snapshots even when storage is full
2. Execute maintenance operations to free up space
3. Recover gracefully without manual intervention or data loss

## Reference
- Original Issue: [kopia/kopia#1738](https://github.com/kopia/kopia/issues/1738)
- Discussed with Jarek on Slack prior to issue creation