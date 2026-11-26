# Search names in snapshots

Use `kopia snapshot search` when you remember a file or directory name but do
not know which backup contains it:

```console
kopia snapshot search MyImportantDocument.docx
kopia snapshot search report --ignore-case
kopia snapshot search report --ignore-case --max-results=20
```

The pattern is a **literal substring of the entry name**, not a glob, regular
expression, full path or file-content query. Matching is case-sensitive unless
`--ignore-case` is supplied. Directories and symbolic links are included, and
symbolic links are not followed. An empty pattern is rejected.

By default, the command searches all complete snapshots visible to the connected
repository, including sources from other users and hosts when the repository
allows access. Use `--snapshot ID` to select one snapshot; repeat the flag to
select several. Repeating an ID does not duplicate results. Add `--incomplete`
to include unfinished snapshots.

Results are ordered by snapshot time, newest first, then by snapshot ID for
equal times. Entries within a snapshot follow its directory iteration order.
Each text line contains the snapshot ID, its `user@host:source` and a quoted
path relative to the snapshot root. `.` represents the root itself. Identical
paths in different snapshots are separate results, as are identical directory
contents archived at different paths.

For scripts, request a JSON array:

```console
kopia snapshot search report --ignore-case --json
kopia snapshot search report --snapshot SNAPSHOT_ID --json
```

Each object contains `snapshotID`, `source`, `startTime`, `path`, `type`
(`file`, `directory` or `symlink`), `objectID` and `size`. The snapshot ID and path
identify a match even when several backups share the same object. A search with
no matches succeeds and emits an empty JSON array. `--max-results` limits the
total number of matches across all selected snapshots; its default of `0` is
unlimited. Negative limits and invalid snapshot IDs fail.

The search reads directory metadata without mounting snapshots, restoring files
or downloading file contents. It reuses complete subtree results by directory
object ID in a bounded in-memory cache. Large result sets continue to stream
after the cache fills. Searches still need to read uncached directory metadata,
so the first search of a large remote repository can take time.

Cancellation and unreadable snapshot metadata stop the command with an error.
Results already emitted may be partial; scripts should check the exit status.
