package repo

import (
	"context"
	"io/ioutil"
	"os"
	"reflect"
	"strings"

	"github.com/kopia/kopia/auth"
	"github.com/kopia/kopia/blob"
	"github.com/kopia/kopia/blob/filesystem"

	"testing"
)

func TestMetadataManager(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "metadata")
	if err != nil {
		t.Errorf("can't create temp dir: %v", err)
		return
	}
	defer os.RemoveAll(tmpDir)

	st := mustCreateFileStorage(t, tmpDir)

	creds, err := auth.Password("foo.bar.baz.foo.bar.baz")
	if err != nil {
		t.Errorf("can't create password credentials: %v", err)
		return
	}

	otherCreds, err := auth.Password("foo.bar.baz.foo.bar.baz0")
	if err != nil {
		t.Errorf("can't create password credentials: %v", err)
		return
	}

	if err := Initialize(st, nil, creds); err != nil {
		t.Errorf("can't initialize repository: %v", err)
		return
	}

	v1, err := newMetadataManager(st, creds)
	if err != nil {
		t.Errorf("can't open first metadata manager: %v", err)
		return
	}

	v2, err := newMetadataManager(st, creds)
	if err != nil {
		t.Errorf("can't open second metadata manager: %v", err)
		return
	}

	cfg, err := v1.connectionConfiguration()
	if err != nil {
		t.Errorf("error getting token1 %v", err)
	}

	cfg2, err := v2.connectionConfiguration()
	if err != nil {
		t.Errorf("error getting token2 %v", err)
	}

	if !reflect.DeepEqual(cfg, cfg2) {
		t.Errorf("configurations are different: %+v vs %+v", cfg, cfg2)
	}

	_, err = newMetadataManager(st, otherCreds)
	if err == nil {
		t.Errorf("unexpectedly opened repository with invalid credentials")
		return
	}

	if err := v1.PutMetadata("foo", []byte("test1")); err != nil {
		t.Errorf("error putting: %v", err)
	}
	if err := v2.PutMetadata("bar", []byte("test2")); err != nil {
		t.Errorf("error putting: %v", err)
	}
	if err := v1.PutMetadata("baz", []byte("test3")); err != nil {
		t.Errorf("error putting: %v", err)
	}

	// Verify contents of metadata items for both managers.
	for _, v := range []*MetadataManager{v1, v2} {
		assertMetadataItem(t, v, "foo", "test1")
		assertMetadataItem(t, v, "bar", "test2")
		assertMetadataItem(t, v, "baz", "test3")
		assertMetadataItemNotFound(t, v, "x")

		assertMetadataItems(t, v, "x", nil)
		assertMetadataItems(t, v, "f", []string{"foo"})
		assertMetadataItems(t, v, "ba", []string{"bar", "baz"})
		assertMetadataItems(t, v, "be", nil)
		assertMetadataItems(t, v, "baz", []string{"baz"})
		assertMetadataItems(t, v, "bazx", nil)

		assertReservedName(t, v, formatBlockID)
		assertReservedName(t, v, repositoryConfigBlockID)
	}

	v1.RemoveMetadata("bar")

	for _, v := range []*MetadataManager{v1, v2} {
		assertMetadataItem(t, v, "foo", "test1")
		assertMetadataItemNotFound(t, v, "bar")
		assertMetadataItem(t, v, "baz", "test3")

		assertMetadataItems(t, v, "x", nil)
		assertMetadataItems(t, v, "f", []string{"foo"})
		assertMetadataItems(t, v, "ba", []string{"baz"})
		assertMetadataItems(t, v, "be", nil)
		assertMetadataItems(t, v, "baz", []string{"baz"})
		assertMetadataItems(t, v, "bazx", nil)
	}
}

func assertMetadataItem(t *testing.T, v *MetadataManager, itemID string, expectedData string) {
	b, err := v.GetMetadata(itemID)
	if err != nil {
		t.Errorf("error getting item %v: %v", itemID, err)
	}

	bs := string(b)
	if bs != expectedData {
		t.Errorf("invalid data for '%v': expected: %v but got %v", itemID, expectedData, bs)
	}
}

func assertMetadataItemNotFound(t *testing.T, v *MetadataManager, itemID string) {
	result, err := v.GetMetadata(itemID)
	if err != ErrMetadataNotFound {
		t.Errorf("invalid error getting item %v: %v (result=%v), but expected %v", itemID, err, result, ErrMetadataNotFound)
	}
}

func assertReservedName(t *testing.T, v *MetadataManager, itemID string) {
	_, err := v.GetMetadata(itemID)
	assertReservedNameError(t, "Get", itemID, err)
	assertReservedNameError(t, "Put", itemID, v.PutMetadata(itemID, nil))
	assertReservedNameError(t, "Remove", itemID, v.RemoveMetadata(itemID))
}

func assertReservedNameError(t *testing.T, method string, itemID string, err error) {
	if err == nil {
		t.Errorf("call did not fail: %v(%v)", method, itemID)
		return
	}

	if !strings.Contains(err.Error(), "invalid metadata item name") {
		t.Errorf("call did not fail the right way: %v(%v), was: %v", method, itemID, err)
	}
}

func assertMetadataItems(t *testing.T, v *MetadataManager, prefix string, expected []string) {
	res, err := v.ListMetadata(prefix, -1)
	if err != nil {
		t.Errorf("error listing items beginning with %v: %v", prefix, err)
	}

	if !reflect.DeepEqual(expected, res) {
		t.Errorf("unexpected items returned for prefix '%v': %v, but expected %v", prefix, res, expected)
	}
}

func mustCreateFileStorage(t *testing.T, path string) blob.Storage {
	os.MkdirAll(path, 0700)
	s, err := filesystem.New(context.Background(), &filesystem.Options{
		Path: path,
	})
	if err != nil {
		t.Errorf("can't create file storage: %v", err)
	}
	return s
}
