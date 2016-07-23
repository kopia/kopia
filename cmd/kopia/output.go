package main

import (
	"fmt"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
)

func listDirectory(dir fs.Directory) error {
	entries, err := dir.Readdir()
	if err != nil {
		return err
	}
	for _, e := range entries {
		m := e.Metadata()
		var oid string
		if m.ObjectID.Type().IsStored() {
			oid = string(m.ObjectID)
		} else if m.ObjectID.Type() == repo.ObjectIDTypeBinary {
			oid = "<inline binary>"
		} else if m.ObjectID.Type() == repo.ObjectIDTypeText {
			oid = "<inline text>"
		}
		info := fmt.Sprintf("%v %9d %v %-30s %v", m.FileMode, m.FileSize, m.ModTime.Local().Format("02 Jan 06 15:04:05"), m.Name, oid)
		fmt.Println(info)
	}
	return nil
}
