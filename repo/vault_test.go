package repo

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/kopia/kopia/auth"
	"github.com/kopia/kopia/blob"
	"github.com/kopia/kopia/blob/filesystem"

	"testing"
)

func TestNonColocatedVault(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "vault")
	if err != nil {
		t.Errorf("can't create temp dir: %v", err)
		return
	}
	defer os.RemoveAll(tmpDir)

	verifyVault(
		t,
		filepath.Join(tmpDir, "vlt"),
		filepath.Join(tmpDir, "repo"))
}

func TestColocatedVault(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "vault")
	if err != nil {
		t.Errorf("can't create temp dir: %v", err)
		return
	}
	//defer os.RemoveAll(tmpDir)

	// vault and repository in one storage
	verifyVault(t, tmpDir, tmpDir)
}

func verifyVault(t *testing.T, vaultPath string, repoPath string) {
	vaultStorage := mustCreateFileStorage(t, vaultPath)

	vaultCreds, err := auth.Password("foo.bar.baz.foo.bar.baz")
	if err != nil {
		t.Errorf("can't create password credentials: %v", err)
		return
	}

	otherVaultCreds, err := auth.Password("foo.bar.baz.foo.bar.baz0")
	if err != nil {
		t.Errorf("can't create password credentials: %v", err)
		return
	}

	vaultFormat := &VaultFormat{
		EncryptionAlgorithm: "AES256_GCM",
	}

	repoFormat := &Format{
		Version:      1,
		MaxBlockSize: 1000000,
		ObjectFormat: "UNENCRYPTED_HMAC_SHA256",
		Secret:       []byte{1, 2, 3},
	}

	v1, err := Create(vaultStorage, vaultFormat, vaultCreds, repoFormat)
	if err != nil {
		t.Errorf("can't create vault: %v", err)
		return
	}

	v2, err := Open(vaultStorage, vaultCreds)
	if err != nil {
		t.Errorf("can't open vault: %v", err)
		return
	}

	cfg, err := v1.Config()
	if err != nil {
		t.Errorf("error getting token1 %v", err)
	}

	cfg2, err := v2.Config()
	if err != nil {
		t.Errorf("error getting token2 %v", err)
	}

	if !reflect.DeepEqual(cfg, cfg2) {
		t.Errorf("configurations are different: %+v vs %+v", cfg, cfg2)
	}

	_, err = Open(vaultStorage, otherVaultCreds)
	if err == nil {
		t.Errorf("unexpectedly opened vault with invalid credentials")
		return
	}

	if err := v1.Put("foo", []byte("test1")); err != nil {
		t.Errorf("error putting: %v", err)
	}
	if err := v2.Put("bar", []byte("test2")); err != nil {
		t.Errorf("error putting: %v", err)
	}
	if err := v1.Put("baz", []byte("test3")); err != nil {
		t.Errorf("error putting: %v", err)
	}

	// Verify contents of vault items for both created and opened vault.
	for _, v := range []*Vault{v1, v2} {
		rf := v.RepoConfig.Format
		if !reflect.DeepEqual(rf, repoFormat) {
			t.Errorf("invalid repository format: %v, but got %v", repoFormat, rf)
		}
		assertVaultItem(t, v, "foo", "test1")
		assertVaultItem(t, v, "bar", "test2")
		assertVaultItem(t, v, "baz", "test3")
		assertVaultItemNotFound(t, v, "x")

		assertVaultItems(t, v, "x", nil)
		assertVaultItems(t, v, "f", []string{"foo"})
		assertVaultItems(t, v, "ba", []string{"bar", "baz"})
		assertVaultItems(t, v, "be", nil)
		assertVaultItems(t, v, "baz", []string{"baz"})
		assertVaultItems(t, v, "bazx", nil)

		assertReservedName(t, v, formatBlockID)
		assertReservedName(t, v, repositoryConfigBlockID)
	}

	v1.Remove("bar")

	for _, v := range []*Vault{v1, v2} {
		assertVaultItem(t, v, "foo", "test1")
		assertVaultItemNotFound(t, v, "bar")
		assertVaultItem(t, v, "baz", "test3")

		assertVaultItems(t, v, "x", nil)
		assertVaultItems(t, v, "f", []string{"foo"})
		assertVaultItems(t, v, "ba", []string{"baz"})
		assertVaultItems(t, v, "be", nil)
		assertVaultItems(t, v, "baz", []string{"baz"})
		assertVaultItems(t, v, "bazx", nil)
	}
}

func assertVaultItem(t *testing.T, v *Vault, itemID string, expectedData string) {
	b, err := v.Get(itemID)
	if err != nil {
		t.Errorf("error getting item %v: %v", itemID, err)
	}

	bs := string(b)
	if bs != expectedData {
		t.Errorf("invalid data for '%v': expected: %v but got %v", itemID, expectedData, bs)
	}
}

func assertVaultItemNotFound(t *testing.T, v *Vault, itemID string) {
	result, err := v.Get(itemID)
	if err != ErrItemNotFound {
		t.Errorf("invalid error getting item %v: %v (result=%v), but expected %v", itemID, err, result, ErrItemNotFound)
	}
}

func assertReservedName(t *testing.T, v *Vault, itemID string) {
	_, err := v.Get(itemID)
	assertReservedNameError(t, "Get", itemID, err)
	assertReservedNameError(t, "Put", itemID, v.Put(itemID, nil))
	assertReservedNameError(t, "Remove", itemID, v.Remove(itemID))
}

func assertReservedNameError(t *testing.T, method string, itemID string, err error) {
	if err == nil {
		t.Errorf("call did not fail: %v(%v)", method, itemID)
		return
	}

	if !strings.Contains(err.Error(), "invalid vault item name") {
		t.Errorf("call did not fail the right way: %v(%v), was: %v", method, itemID, err)
	}
}

func assertVaultItems(t *testing.T, v *Vault, prefix string, expected []string) {
	res, err := v.List(prefix, -1)
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
