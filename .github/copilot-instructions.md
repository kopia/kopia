Git repository for Kopia, a backup/restore tool that creates encrypted snapshots and saves them to remote storage, such as object stores and remote file systems.

- Primary language: Go
- Contains: 1,000+ Go files.
- Build System: GNU Make with cross-platform support (Windows/Linux/macOS/ARM)

## Relevant Source Directories

- Root `/`:
   - `Makefile` - Primary build system (Make)
   - `go.mod` / `go.sum` - Go module dependencies
   - `README.md` - General project information

- `cli/**`: CLI command implementations (~200 command files)
   - Each command is in a separate file (e.g., `command_snapshot_create.go`)
   - Uses kingpin v2 for command-line parsing
   - Main entry via `app.go`

- `repo/**`: Repository management and storage backends
   - `repo/blob/` - Storage provider implementations (object stores, filesystem, ...)
   - `repo/content/` - Content-addressable storage interface
   - `repo/format/` - Repository format and versioning
   - `repo/manifest/` - Manifest management
   - `repo/object/` - Byte stream I/O storage interface

- `fs/`: Filesystem abstraction layer
   - `fs/localfs/` - Local filesystem implementation
   - Supports snapshots, restore, and filesystem walking

- `snapshot/`: Snapshot creation and management
   - `snapshot/snapshotmaintenance/` - Snapshot GC and maintenance
   - `snapshot/upload/` - Upload logic and parallelization

- `internal/**`: Internal packages (~75 subdirectories)
   - Utilities and shared code not for external use
   - Examples: cache, crypto, compression, auth, server, etc.

- `tests/**`: Integration and end-to-end tests
