package blob_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/repo/blob"
)

type myConfig struct {
	Field int `json:"someField"`
}

type myStorage struct {
	blob.Storage

	cfg    *myConfig
	create bool
}

func TestRegistry(t *testing.T) {
	blob.AddSupportedStorage("mystorage", myConfig{Field: 3}, func(c context.Context, mc *myConfig, isCreate bool) (blob.Storage, error) {
		return &myStorage{cfg: mc, create: isCreate}, nil
	})

	st, err := blob.NewStorage(context.Background(), blob.ConnectionInfo{
		Type: "mystorage",
		Config: &myConfig{
			Field: 4,
		},
	}, true)

	require.NoError(t, err)
	require.IsType(t, (*myStorage)(nil), st)
	require.Equal(t, 4, st.(*myStorage).cfg.Field)
	require.True(t, st.(*myStorage).create)

	_, err = blob.NewStorage(context.Background(), blob.ConnectionInfo{
		Type: "unknownstorage",
		Config: &myConfig{
			Field: 3,
		},
	}, false)

	require.Error(t, err)
}

func TestConnectionInfo(t *testing.T) {
	blob.AddSupportedStorage("mystorage2", myConfig{}, func(c context.Context, mc *myConfig, isCreate bool) (blob.Storage, error) {
		return &myStorage{cfg: mc}, nil
	})

	ci := blob.ConnectionInfo{
		Type: "mystorage2",
		Config: &myConfig{
			Field: 4,
		},
	}

	var ci2 blob.ConnectionInfo

	var buf bytes.Buffer

	require.NoError(t, json.NewEncoder(&buf).Encode(ci))
	require.NoError(t, json.NewDecoder(bytes.NewReader(buf.Bytes())).Decode(&ci2))
	require.Equal(t, ci, ci2)

	invalidJSON := []string{
		`[1,2,3]`,
		`{"type":"no-such-type","config":{}}`,
		`{"type":"mystorage2","config":3}`,
	}

	for _, tc := range invalidJSON {
		require.Error(t, json.NewDecoder(bytes.NewReader([]byte(tc))).Decode(&ci))
	}
}
