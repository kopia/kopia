package main

import (
	"fmt"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
)

func listDirectory(dir fs.Directory) {
	for _, e := range dir {
		var oid string
		if e.ObjectID.Type().IsStored() {
			oid = string(e.ObjectID)
		} else if e.ObjectID.Type() == repo.ObjectIDTypeBinary {
			oid = "<inline binary>"
		} else if e.ObjectID.Type() == repo.ObjectIDTypeText {
			oid = "<inline text>"
		}
		info := fmt.Sprintf("%v %9d %v %-30s %v", e.FileMode, e.FileSize, e.ModTime.Local().Format("02 Jan 06 15:04:05"), e.Name, oid)
		fmt.Println(info)
	}
}
