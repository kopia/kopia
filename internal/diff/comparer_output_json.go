package diff

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"
)

// ComparerOutputJSON is a basic standard output compare output.
type ComparerOutputJSON struct {
	out     io.Writer
	entries []interface{}
}

func (coj *ComparerOutputJSON) add(entryType string, data map[string]interface{}) {
	entry := map[string]interface{}{
		"type": entryType,
	}

	for k, v := range data {
		entry[k] = v
	}

	coj.entries = append(coj.entries, entry)
}

// AddDirectory is called when a directory was added.
func (coj *ComparerOutputJSON) AddDirectory(path string) {
	coj.add("add_directory", map[string]interface{}{"path": path})
}

// RemoveDirectory is called when a directory was removed.
func (coj *ComparerOutputJSON) RemoveDirectory(path string) {
	coj.add("remove_directory", map[string]interface{}{"path": path})
}

// ChangeDirNondir is called when a directory was replaced by a non-directory.
func (coj *ComparerOutputJSON) ChangeDirNondir(path string) {
	coj.add("change_dir_nondir", map[string]interface{}{"path": path})
}

// ChangeNondirDir is called when a non-directory was replaced by a directory.
func (coj *ComparerOutputJSON) ChangeNondirDir(path string) {
	coj.add("change_nondir_dir", map[string]interface{}{"path": path})
}

// ChangeFile is called when the properties of a file were changed.
func (coj *ComparerOutputJSON) ChangeFile(path string, modTime time.Time, size1, size2 int64) {
	coj.add("change_file", map[string]interface{}{"path": path, "size1": size1, "size2": size2})
}

// AddFile is called when a file of certain size was added.
func (coj *ComparerOutputJSON) AddFile(path string, size int64) {
	coj.add("add_file", map[string]interface{}{"path": path, "size": size})
}

// RemoveFile is called when a file of certain size was removed.
func (coj *ComparerOutputJSON) RemoveFile(path string, size int64) {
	coj.add("remove_file", map[string]interface{}{"path": path, "size": size})
}

// RunDiffCommand runs the given prepared command for the purpose of comparing two files.
func (coj *ComparerOutputJSON) RunDiffCommand(path1, path2 string, cmd *exec.Cmd) {
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()

	if err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			// this is the only kind of error cmd.Output() returns
			coj.add("diff", map[string]interface{}{
				"path1":     path1,
				"path2":     path2,
				"exit_code": exitError.ExitCode(),
				"output":    fmt.Sprintf("%v", string(out)),
			})
		} else {
			coj.add("diff", map[string]interface{}{
				"path1":  path1,
				"path2":  path2,
				"error":  fmt.Sprintf("%v", err),
				"output": fmt.Sprintf("%v", string(out)),
			})
		}
	} else {
		coj.add("diff", map[string]interface{}{
			"path1":     path1,
			"path2":     path2,
			"exit_code": 0,
			"output":    fmt.Sprintf("%v", string(out)),
		})
	}
}

// NotExistSource is called when the path does not exist in the source, but exists in the destination.
func (coj *ComparerOutputJSON) NotExistSource(path string) {
	coj.add("not_exist_source", map[string]interface{}{"path": path})
}

// NotExistDest is called when the path does not exist in the destination, but exists in the source.
func (coj *ComparerOutputJSON) NotExistDest(path string) {
	coj.add("not_exist_dest", map[string]interface{}{"path": path})
}

// ModesDiffer is called when the permission flags of the path differ.
func (coj *ComparerOutputJSON) ModesDiffer(path string, mode1, mode2 os.FileMode) {
	coj.add("modes_differ", map[string]interface{}{"path": path, "mode1": mode1, "mode2": mode2})
}

// SizesDiffer is called when the size of the file differs.
func (coj *ComparerOutputJSON) SizesDiffer(path string, size1, size2 int64) {
	coj.add("sizes_differ", map[string]interface{}{"path": path, "size1": size1, "size2": size2})
}

// ModTimeDiffer is called when the modification time of the path differs.
func (coj *ComparerOutputJSON) ModTimeDiffer(path string, modtime1, modtime2 time.Time) {
	coj.add("mod_time_differ", map[string]interface{}{"path": path, "modtime1": modtime1, "modtime2": modtime2})
}

// OwnerDiffer is called when the owner user id of the path differs.
func (coj *ComparerOutputJSON) OwnerDiffer(path string, userID1, userID2 uint32) {
	coj.add("owner_differ", map[string]interface{}{"path": path, "user_id1": userID1, "useR_id2": userID2})
}

// GroupDiffer is called when the owner group of the path differs.
func (coj *ComparerOutputJSON) GroupDiffer(path string, groupID1, groupID2 uint32) {
	coj.add("group_differ", map[string]interface{}{"path": path, "group_id1": groupID1, "group_id2": groupID2})
}

// Close the output; no further calls shall be issued.
func (coj *ComparerOutputJSON) Close() {
	jsonRoot := map[string]interface{}{"entries": coj.entries}
	if err := json.NewEncoder(coj.out).Encode(jsonRoot); err != nil {
		fmt.Fprintf(coj.out, "{\"error\": 1}")
	}
}
