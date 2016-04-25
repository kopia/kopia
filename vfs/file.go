package vfs

import (
	"io/ioutil"

	"golang.org/x/net/context"
)

// fileNode implements both Node and Handle.
type fileNode struct {
	node
}

func (f *fileNode) ReadAll(ctx context.Context) ([]byte, error) {
	reader, err := f.manager.open(f.ObjectID)
	if err != nil {
		return nil, err
	}

	return ioutil.ReadAll(reader)
}
