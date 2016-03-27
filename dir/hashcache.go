package dir

import "github.com/kopia/kopia/content"

type cachedListing struct {
	Listing

	objectID content.ObjectID
}

type hashCache struct {
}

func (hc hashCache) GetCachedListing(path string) cachedListing {
	return cachedListing{}
}
