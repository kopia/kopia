package dir

import "github.com/kopia/kopia/content"

type HashCache interface {
	Put(path string, oid content.ObjectID)
	Get(path string) content.ObjectID
}

type nullHashCache struct {
}

func (hc *nullHashCache) Put(path string, oid content.ObjectID) {
}

func (hc *nullHashCache) Get(path string) content.ObjectID {
	return ""
}
