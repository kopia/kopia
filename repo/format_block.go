package repo

import (
	"encoding/json"
	"fmt"

	"github.com/kopia/kopia/auth"
	"github.com/kopia/kopia/metadata"
	"github.com/kopia/kopia/storage"
)

type formatBlock struct {
	auth.SecurityOptions
	metadata.Format
}

func readFormaBlock(st storage.Storage) (*formatBlock, error) {
	f := &formatBlock{}

	b, err := st.GetBlock(metadata.MetadataBlockPrefix+"format", 0, -1)
	if err != nil {
		return nil, fmt.Errorf("unable to read format block: %v", err)
	}

	if err := json.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("invalid format block: %v", err)
	}
	return f, err
}
