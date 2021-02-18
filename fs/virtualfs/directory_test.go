package virtualfs

import (
	"context"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

const (
	defaultPermissions os.FileMode = 0777
	dirPermissions     os.FileMode = defaultPermissions | os.ModeDir
)

func TestAddDir(t *testing.T) {
	t.Log("Add root directory")

	rootDir, err := NewDirectory("root")
	expectSuccess(t, err)

	t.Log("Add sub-directory d1")

	dir, err := rootDir.AddDir("d1", defaultPermissions)
	expectSuccess(t, err)
	checkEquality(t, dir.Name(), "d1")

	t.Log("Add duplicate sub-directory d1")

	_, err = rootDir.AddDir("d1", defaultPermissions)
	expectFailure(t, err)

	t.Log("Add sub-directory with invalid name /d2")

	_, err = rootDir.AddDir("/d2", defaultPermissions)
	expectFailure(t, err)
}

func TestAddAllDirs(t *testing.T) {
	t.Log("Add root directory")

	rootDir, err := NewDirectory("root")
	expectSuccess(t, err)

	t.Log("Add a directory: root/d1")

	subdir, err := rootDir.AddAllDirs("d1", defaultPermissions)
	expectSuccess(t, err)
	checkEquality(t, subdir.Name(), "d1")

	d1 := verifyAndGetSubdir(t, rootDir, "d1")

	t.Log("Add a sub-dir under an existing directory: root/d1/d2")

	subdir, err = rootDir.AddAllDirs("d1/d2", defaultPermissions)
	expectSuccess(t, err)
	checkEquality(t, subdir.Name(), "d2")

	_ = verifyAndGetSubdir(t, d1, "d2")

	t.Log("Add third/fourth level dirs: root/d1/d3/d4")

	subdir, err = rootDir.AddAllDirs("d1/d3/d4", defaultPermissions)
	expectSuccess(t, err)
	checkEquality(t, subdir.Name(), "d4")

	d3 := verifyAndGetSubdir(t, d1, "d3")

	_ = verifyAndGetSubdir(t, d3, "d4")

	t.Log("Add a directory under a file (expect failure): root/f1/d6")

	f, err := AddFileWithContent(rootDir, "f1", []byte("test"), defaultPermissions, defaultPermissions)
	expectSuccess(t, err)
	checkEquality(t, f.Name(), "f1")

	_, err = rootDir.AddAllDirs("f1/d6", defaultPermissions)
	expectFailure(t, err)
}

func TestAddFile(t *testing.T) {
	t.Log("Add root directory")

	rootDir, err := NewDirectory("root")
	expectSuccess(t, err)

	t.Log("Add file with stdin source: root/f1")

	f, err := AddFileWithStdinSource(rootDir, "f1", defaultPermissions, defaultPermissions)
	expectSuccess(t, err)

	if f == nil {
		t.Fatal("expected file, got nil")
	}

	checkEquality(t, f.Name(), "f1")

	t.Log("Add file with stdin source at third level: root/d1/f2")

	f, err = AddFileWithStdinSource(rootDir, "d1/f2", defaultPermissions, defaultPermissions)
	expectSuccess(t, err)

	if f == nil {
		t.Fatal("expected file, got nil")
	}

	checkEquality(t, f.Name(), "f2")

	d1 := verifyAndGetSubdir(t, rootDir, "d1")

	e, err := d1.Child(context.Background(), "f2")
	expectSuccess(t, err)

	if e == nil {
		t.Fatal("expected child entry, got nil")
	}

	t.Log("Add file with content at third level: root/d2/f3")

	f, err = AddFileWithContent(rootDir, "d2/f3", []byte("test"), defaultPermissions, defaultPermissions)
	expectSuccess(t, err)

	if f == nil {
		t.Fatal("expected file, got nil")
	}

	checkEquality(t, f.Name(), "f3")

	d2 := verifyAndGetSubdir(t, rootDir, "d2")

	e, err = d2.Child(context.Background(), "f3")
	expectSuccess(t, err)

	if e == nil {
		t.Fatal("expected child entry, got nil")
	}
}

func TestAddFileWithStdinSource(t *testing.T) {
	// Create a temporary file with test data
	content := []byte("TempFile Content")

	tempFile, err := ioutil.TempFile("", "file-with-stdin-source")
	if err != nil {
		t.Fatalf("temp file create failed with %v", err)
	}

	defer os.Remove(tempFile.Name())

	if _, err = tempFile.Write(content); err != nil {
		t.Fatalf("temp file write failed with %v", err)
	}

	if _, err = tempFile.Seek(0, 0); err != nil {
		t.Fatalf("temp file seek failed with %v", err)
	}

	t.Log("Add a root directory")

	rootDir, err := NewDirectory("root")
	expectSuccess(t, err)

	// Add file with stdin source
	initialStdin := os.Stdin

	defer func() {
		os.Stdin = initialStdin
	}()

	os.Stdin = tempFile

	t.Log("Add a file with stdin source")

	f, err := AddFileWithStdinSource(rootDir, "f1", defaultPermissions, defaultPermissions)
	expectSuccess(t, err)

	// Read and compare data
	t.Log("Open virtual file")

	r, err := f.Open(context.TODO())
	expectSuccess(t, err)

	defer r.Close()

	result := make([]byte, len(content))

	t.Log("Read virtual file")

	if _, err = r.Read(result); err != nil {
		t.Fatalf("reading virtual file failed with %v", err)
	}

	checkEquality(t, result, content)

	if err = tempFile.Close(); err != nil {
		t.Fatalf("temp file close failed with %v", err)
	}
}

func expectSuccess(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatalf("expected success, failed with %v", err)
	}
}

func expectFailure(t *testing.T, err error) {
	t.Helper()

	if err == nil {
		t.Fatal("expected failure, but got none")
	}
}

func checkEquality(t *testing.T, got, want interface{}) {
	t.Helper()

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Did not get expected output: (actual) %v != %v (expected)", got, want)
	}
}

func verifyAndGetSubdir(t *testing.T, rootDir *Directory, subdirName string) *Directory {
	t.Helper()

	sub, err := rootDir.Subdir(subdirName)
	expectSuccess(t, err)

	if sub == nil {
		t.Fatal("expected subdir, got nil")
	}

	checkEquality(t, sub.Name(), subdirName)
	checkEquality(t, sub.Mode(), dirPermissions)

	return sub
}
