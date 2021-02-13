package repo_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"math/rand"
	"runtime/debug"
	"testing"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/object"
)

func TestWriters(t *testing.T) {
	cases := []struct {
		data     []byte
		objectID object.ID
	}{
		{
			[]byte("the quick brown fox jumps over the lazy dog"),
			"345acef0bcf82f1daf8e49fab7b7fac7ec296c518501eabea3645b99345a4e08",
		},
		{make([]byte, 100), "1d804f1f69df08f3f59070bf962de69433e3d61ac18522a805a84d8c92741340"}, // 100 zero bytes
	}

	ctx := testlogging.Context(t)

	for _, c := range cases {
		var env repotesting.Environment
		defer env.Setup(t).Close(ctx, t)

		writer := env.RepositoryWriter.NewObjectWriter(ctx, object.WriterOptions{})
		if _, err := writer.Write(c.data); err != nil {
			t.Fatalf("write error: %v", err)
		}

		result, err := writer.Result()
		if err != nil {
			t.Errorf("error getting writer results for %v, expected: %v", c.data, c.objectID.String())
			continue
		}

		if !objectIDsEqual(result, c.objectID) {
			t.Errorf("incorrect result for %v, expected: %v got: %v", c.data, c.objectID.String(), result.String())
		}

		env.RepositoryWriter.ContentManager().Flush(ctx)
	}
}

func objectIDsEqual(o1, o2 object.ID) bool {
	return o1 == o2
}

func TestWriterCompleteChunkInTwoWrites(t *testing.T) {
	var env repotesting.Environment

	ctx := testlogging.Context(t)
	defer env.Setup(t).Close(ctx, t)

	b := make([]byte, 100)
	writer := env.RepositoryWriter.NewObjectWriter(ctx, object.WriterOptions{})
	writer.Write(b[0:50])
	writer.Write(b[0:50])
	result, err := writer.Result()

	if result != "1d804f1f69df08f3f59070bf962de69433e3d61ac18522a805a84d8c92741340" {
		t.Errorf("unexpected result: %v err: %v", result, err)
	}
}

func TestPackingSimple(t *testing.T) {
	var env repotesting.Environment

	ctx := testlogging.Context(t)
	defer env.Setup(t).Close(ctx, t)

	content1 := "hello, how do you do?"
	content2 := "hi, how are you?"
	content3 := "thank you!"

	oid1a := writeObject(ctx, t, env.RepositoryWriter, []byte(content1), "packed-object-1a")
	oid1b := writeObject(ctx, t, env.RepositoryWriter, []byte(content1), "packed-object-1b")
	oid2a := writeObject(ctx, t, env.RepositoryWriter, []byte(content2), "packed-object-2a")
	oid2b := writeObject(ctx, t, env.RepositoryWriter, []byte(content2), "packed-object-2b")

	oid3a := writeObject(ctx, t, env.RepositoryWriter, []byte(content3), "packed-object-3a")
	oid3b := writeObject(ctx, t, env.RepositoryWriter, []byte(content3), "packed-object-3b")
	verify(ctx, t, env.RepositoryWriter, oid1a, []byte(content1), "packed-object-1")
	verify(ctx, t, env.RepositoryWriter, oid2a, []byte(content2), "packed-object-2")
	oid2c := writeObject(ctx, t, env.RepositoryWriter, []byte(content2), "packed-object-2c")
	oid1c := writeObject(ctx, t, env.RepositoryWriter, []byte(content1), "packed-object-1c")

	env.RepositoryWriter.ContentManager().Flush(ctx)

	if got, want := oid1a.String(), oid1b.String(); got != want {
		t.Errorf("oid1a(%q) != oid1b(%q)", got, want)
	}

	if got, want := oid1a.String(), oid1c.String(); got != want {
		t.Errorf("oid1a(%q) != oid1c(%q)", got, want)
	}

	if got, want := oid2a.String(), oid2b.String(); got != want {
		t.Errorf("oid2(%q)a != oidb(%q)", got, want)
	}

	if got, want := oid2a.String(), oid2c.String(); got != want {
		t.Errorf("oid2(%q)a != oidc(%q)", got, want)
	}

	if got, want := oid3a.String(), oid3b.String(); got != want {
		t.Errorf("oid3a(%q) != oid3b(%q)", got, want)
	}

	env.VerifyBlobCount(t, 3)

	env.MustReopen(t)

	verify(ctx, t, env.RepositoryWriter, oid1a, []byte(content1), "packed-object-1")
	verify(ctx, t, env.RepositoryWriter, oid2a, []byte(content2), "packed-object-2")
	verify(ctx, t, env.RepositoryWriter, oid3a, []byte(content3), "packed-object-3")

	if err := env.RepositoryWriter.ContentManager().CompactIndexes(ctx, content.CompactOptions{MaxSmallBlobs: 1}); err != nil {
		t.Errorf("optimize error: %v", err)
	}

	env.MustReopen(t)

	verify(ctx, t, env.RepositoryWriter, oid1a, []byte(content1), "packed-object-1")
	verify(ctx, t, env.RepositoryWriter, oid2a, []byte(content2), "packed-object-2")
	verify(ctx, t, env.RepositoryWriter, oid3a, []byte(content3), "packed-object-3")

	if err := env.RepositoryWriter.ContentManager().CompactIndexes(ctx, content.CompactOptions{MaxSmallBlobs: 1}); err != nil {
		t.Errorf("optimize error: %v", err)
	}

	env.MustReopen(t)

	verify(ctx, t, env.RepositoryWriter, oid1a, []byte(content1), "packed-object-1")
	verify(ctx, t, env.RepositoryWriter, oid2a, []byte(content2), "packed-object-2")
	verify(ctx, t, env.RepositoryWriter, oid3a, []byte(content3), "packed-object-3")
}

func TestHMAC(t *testing.T) {
	var env repotesting.Environment

	ctx := testlogging.Context(t)
	defer env.Setup(t).Close(ctx, t)

	c := bytes.Repeat([]byte{0xcd}, 50)

	w := env.RepositoryWriter.NewObjectWriter(ctx, object.WriterOptions{})
	w.Write(c)
	result, err := w.Result()

	if result.String() != "367352007ee6ca9fa755ce8352347d092c17a24077fd33c62f655574a8cf906d" {
		t.Errorf("unexpected result: %v err: %v", result.String(), err)
	}
}

func TestUpgrade(t *testing.T) {
	var env repotesting.Environment

	ctx := testlogging.Context(t)
	defer env.Setup(t).Close(ctx, t)

	if err := env.RepositoryWriter.Upgrade(ctx); err != nil {
		t.Errorf("upgrade error: %v", err)
	}

	if err := env.RepositoryWriter.Upgrade(ctx); err != nil {
		t.Errorf("2nd upgrade error: %v", err)
	}
}

func TestReaderStoredBlockNotFound(t *testing.T) {
	var env repotesting.Environment

	ctx := testlogging.Context(t)
	defer env.Setup(t).Close(ctx, t)

	objectID, err := object.ParseID("Ddeadbeef")
	if err != nil {
		t.Errorf("cannot parse object ID: %v", err)
	}

	reader, err := env.RepositoryWriter.OpenObject(ctx, objectID)
	if !errors.Is(err, object.ErrObjectNotFound) || reader != nil {
		t.Errorf("unexpected result: reader: %v err: %v", reader, err)
	}
}

func writeObject(ctx context.Context, t *testing.T, rep repo.RepositoryWriter, data []byte, testCaseID string) object.ID {
	t.Helper()

	w := rep.NewObjectWriter(ctx, object.WriterOptions{})
	if _, err := w.Write(data); err != nil {
		t.Fatalf("can't write object %q - write failed: %v", testCaseID, err)
	}

	oid, err := w.Result()
	if err != nil {
		t.Fatalf("can't write object %q - result failed: %v", testCaseID, err)
	}

	return oid
}

func verify(ctx context.Context, t *testing.T, rep repo.Repository, objectID object.ID, expectedData []byte, testCaseID string) {
	t.Helper()

	reader, err := rep.OpenObject(ctx, objectID)
	if err != nil {
		t.Errorf("cannot get reader for %v (%v): %v %v", testCaseID, objectID, err, string(debug.Stack()))
		return
	}

	for i := 0; i < 20; i++ {
		sampleSize := int(rand.Int31n(300))
		seekOffset := int(rand.Int31n(int32(len(expectedData))))

		if seekOffset+sampleSize > len(expectedData) {
			sampleSize = len(expectedData) - seekOffset
		}

		if sampleSize > 0 {
			got := make([]byte, sampleSize)

			if offset, err := reader.Seek(int64(seekOffset), 0); err != nil || offset != int64(seekOffset) {
				t.Errorf("seek error: %v offset=%v expected:%v", err, offset, seekOffset)
			}

			if n, err := reader.Read(got); err != nil || n != sampleSize {
				t.Errorf("invalid data: n=%v, expected=%v, err:%v", n, sampleSize, err)
			}

			expected := expectedData[seekOffset : seekOffset+sampleSize]

			if !bytes.Equal(expected, got) {
				t.Errorf("incorrect data read for %v: expected: %x, got: %x", testCaseID, expected, got)
			}
		}
	}
}

func TestFormats(t *testing.T) {
	ctx := testlogging.Context(t)

	makeFormat := func(hashAlgo string) func(*repo.NewRepositoryOptions) {
		return func(n *repo.NewRepositoryOptions) {
			n.BlockFormat.Hash = hashAlgo
			n.BlockFormat.HMACSecret = []byte("key")
			n.ObjectFormat.Splitter = "FIXED-1M"
		}
	}

	cases := []struct {
		format func(*repo.NewRepositoryOptions)
		oids   map[string]object.ID
	}{
		{
			format: func(n *repo.NewRepositoryOptions) {
			},
			oids: map[string]object.ID{
				"": "b613679a0814d9ec772f95d778c35fc5ff1697c493715653c6c712144292c5ad",
				"The quick brown fox jumps over the lazy dog": "fb011e6154a19b9a4c767373c305275a5a69e8b68b0b4c9200c383dced19a416",
			},
		},
		{
			format: makeFormat("HMAC-SHA256"),
			oids: map[string]object.ID{
				"The quick brown fox jumps over the lazy dog": "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8",
			},
		},
		{
			format: makeFormat("HMAC-SHA256-128"),
			oids: map[string]object.ID{
				"The quick brown fox jumps over the lazy dog": "f7bc83f430538424b13298e6aa6fb143",
			},
		},
	}

	for caseIndex, c := range cases {
		var env repotesting.Environment

		opts := repotesting.Options{NewRepositoryOptions: c.format}
		defer env.Setup(t, opts).Close(ctx, t)

		for k, v := range c.oids {
			bytesToWrite := []byte(k)
			w := env.RepositoryWriter.NewObjectWriter(ctx, object.WriterOptions{})
			w.Write(bytesToWrite)

			oid, err := w.Result()
			if err != nil {
				t.Errorf("error: %v", err)
			}

			if !objectIDsEqual(oid, v) {
				t.Errorf("invalid oid for #%v\ngot:\n%#v\nexpected:\n%#v", caseIndex, oid.String(), v.String())
			}

			rc, err := env.RepositoryWriter.OpenObject(ctx, oid)
			if err != nil {
				t.Errorf("open failed: %v", err)
				continue
			}

			bytesRead, err := ioutil.ReadAll(rc)
			if err != nil {
				t.Errorf("error reading: %v", err)
			}

			if !bytes.Equal(bytesRead, bytesToWrite) {
				t.Errorf("data mismatch, read:%x vs written:%v", bytesRead, bytesToWrite)
			}
		}
	}
}

func TestWriterScope(t *testing.T) {
	var env repotesting.Environment

	ctx := context.Background()
	defer env.Setup(t, repotesting.Options{}).Close(ctx, t)

	rep := env.Repository // read-only

	lw := rep.(repo.RepositoryWriter)

	// w1, w2, w3 are indepdendent sessions.
	w1, err := rep.NewWriter(ctx, repo.WriteSessionOptions{Purpose: "writer1"})
	must(t, err)

	defer w1.Close(ctx)

	w2, err := rep.NewWriter(ctx, repo.WriteSessionOptions{Purpose: "writer2"})
	must(t, err)

	defer w2.Close(ctx)

	w3, err := rep.NewWriter(ctx, repo.WriteSessionOptions{Purpose: "writer3"})
	must(t, err)

	defer w3.Close(ctx)

	o1Data := []byte{1, 2, 3}
	o2Data := []byte{2, 3, 4}
	o3Data := []byte{3, 4, 5}
	o4Data := []byte{4, 5, 6}

	o1 := writeObject(ctx, t, w1, o1Data, "o1")
	o2 := writeObject(ctx, t, w2, o2Data, "o2")
	o3 := writeObject(ctx, t, w3, o3Data, "o3")
	o4 := writeObject(ctx, t, lw, o4Data, "o4")

	// each writer can see their own data but not others, except for 'lw' and 'rep' which are
	// one and the same.
	verify(ctx, t, w1, o1, o1Data, "o1-w1")
	verifyNotFound(ctx, t, w2, o1, "o1-w2")
	verifyNotFound(ctx, t, w3, o1, "o1-w3")
	verifyNotFound(ctx, t, lw, o1, "o1-lw")
	verifyNotFound(ctx, t, rep, o1, "o1-rep")

	verifyNotFound(ctx, t, w1, o2, "o2-w1")
	verify(ctx, t, w2, o2, o2Data, "o2-w2")
	verifyNotFound(ctx, t, w3, o2, "o2-w3")
	verifyNotFound(ctx, t, lw, o2, "o2-lw")
	verifyNotFound(ctx, t, rep, o2, "o2-rep")

	verifyNotFound(ctx, t, w1, o3, "o3-w1")
	verifyNotFound(ctx, t, w2, o3, "o3-w2")
	verify(ctx, t, w3, o3, o3Data, "o3-w3")
	verifyNotFound(ctx, t, lw, o3, "o3-lw")
	verifyNotFound(ctx, t, rep, o3, "o3-rep")

	verifyNotFound(ctx, t, w1, o4, "o4-w1")
	verifyNotFound(ctx, t, w2, o4, "o4-w2")
	verifyNotFound(ctx, t, w2, o3, "o4-ww")
	verify(ctx, t, lw, o4, o4Data, "o4-lw")
	verify(ctx, t, rep, o4, o4Data, "o4-rep") // rep == lw so read is immediately visible

	must(t, w1.Flush(ctx))

	// after flushing w1, everybody else can now see o1
	verify(ctx, t, w1, o1, o1Data, "o1-w1")
	verify(ctx, t, w2, o1, o1Data, "o1-w2")
	verify(ctx, t, w3, o1, o1Data, "o1-w3")
	verify(ctx, t, lw, o1, o1Data, "o1-lw")
	verify(ctx, t, rep, o1, o1Data, "o1-rep")

	must(t, w2.Flush(ctx))

	// after flushing w2, everybody else can now see o2
	verify(ctx, t, w1, o2, o2Data, "o2-w1")
	verify(ctx, t, w2, o2, o2Data, "o2-w2")
	verify(ctx, t, w3, o2, o2Data, "o2-w3")
	verify(ctx, t, lw, o2, o2Data, "o2-lw")
	verify(ctx, t, rep, o2, o2Data, "o2-rep")

	must(t, w3.Flush(ctx))
	must(t, lw.Flush(ctx))

	verify(ctx, t, w1, o3, o3Data, "o3-w1")
	verify(ctx, t, w2, o3, o3Data, "o3-w2")
	verify(ctx, t, w3, o3, o3Data, "o3-w3")
	verify(ctx, t, lw, o3, o3Data, "o3-lw")
	verify(ctx, t, rep, o3, o3Data, "o3-rep")

	verify(ctx, t, w1, o4, o4Data, "o4-w1")
	verify(ctx, t, w2, o4, o4Data, "o4-w2")
	verify(ctx, t, w3, o4, o4Data, "o4-w3")
	verify(ctx, t, lw, o4, o4Data, "o4-lw")
	verify(ctx, t, rep, o4, o4Data, "o3-rep")
}

func TestWriteSessionFlushOnSuccess(t *testing.T) {
	var env repotesting.Environment

	ctx := context.Background()
	defer env.Setup(t, repotesting.Options{}).Close(ctx, t)

	var oid object.ID

	must(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(w repo.RepositoryWriter) error {
		oid = writeObject(ctx, t, w, []byte{1, 2, 3}, "test-1")
		return nil
	}))

	verify(ctx, t, env.Repository, oid, []byte{1, 2, 3}, "test-1")
}

func TestWriteSessionNoFlushOnFailure(t *testing.T) {
	var env repotesting.Environment

	ctx := context.Background()
	defer env.Setup(t, repotesting.Options{}).Close(ctx, t)

	var oid object.ID

	someErr := errors.New("some error")
	err := repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(w repo.RepositoryWriter) error {
		oid = writeObject(ctx, t, w, []byte{1, 2, 3}, "test-1")
		return someErr
	})

	if !errors.Is(err, someErr) {
		t.Fatalf("invalid error: %v want %v", err, someErr)
	}

	verifyNotFound(ctx, t, env.Repository, oid, "test-1")
}

func TestWriteSessionFlushOnFailure(t *testing.T) {
	var env repotesting.Environment

	ctx := context.Background()
	defer env.Setup(t, repotesting.Options{}).Close(ctx, t)

	var oid object.ID

	someErr := errors.New("some error")
	err := repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{
		FlushOnFailure: true,
	}, func(w repo.RepositoryWriter) error {
		oid = writeObject(ctx, t, w, []byte{1, 2, 3}, "test-1")
		return someErr
	})

	if !errors.Is(err, someErr) {
		t.Fatalf("invalid error: %v want %v", err, someErr)
	}

	verify(ctx, t, env.Repository, oid, []byte{1, 2, 3}, "test-1")
}

func verifyNotFound(ctx context.Context, t *testing.T, rep repo.Repository, objectID object.ID, testCaseID string) {
	t.Helper()

	_, err := rep.OpenObject(ctx, objectID)
	if !errors.Is(err, object.ErrObjectNotFound) {
		t.Fatalf("expected not found for %v, got %v", testCaseID, err)
		return
	}
}

func must(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatal(err)
	}
}
