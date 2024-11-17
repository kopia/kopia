package repo_test

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/crypto"
	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/metricid"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/servertesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/beforeop"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/content/indexblob"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/object"
)

func (s *formatSpecificTestSuite) TestWriters(t *testing.T) {
	cases := []struct {
		data     []byte
		objectID object.ID
	}{
		{
			[]byte("the quick brown fox jumps over the lazy dog"),
			mustParseObjectID(t, "f65fc4107863281faaeb7087197c05ad59457362607330c665c86c852c5e5906"),
		},
		{make([]byte, 100), mustParseObjectID(t, "bfa2b4b9421671ab2b5bfa8c90ee33607784a27e452b08556509ef9bd47a37c6")}, // 100 zero bytes
	}

	for _, c := range cases {
		ctx, env := repotesting.NewEnvironment(t, s.formatVersion)

		writer := env.RepositoryWriter.NewObjectWriter(ctx, object.WriterOptions{MetadataCompressor: "zstd-fastest"})
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

func (s *formatSpecificTestSuite) TestWriterCompleteChunkInTwoWrites(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, s.formatVersion)

	b := make([]byte, 100)
	writer := env.RepositoryWriter.NewObjectWriter(ctx, object.WriterOptions{MetadataCompressor: "zstd-fastest"})
	writer.Write(b[0:50])
	writer.Write(b[0:50])
	result, err := writer.Result()

	if result != mustParseObjectID(t, "bfa2b4b9421671ab2b5bfa8c90ee33607784a27e452b08556509ef9bd47a37c6") {
		t.Errorf("unexpected result: %v err: %v", result, err)
	}
}

func (s *formatSpecificTestSuite) TestPackingSimple(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, s.formatVersion)

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

	env.VerifyBlobCount(t, 4)

	env.MustReopen(t)

	verify(ctx, t, env.RepositoryWriter, oid1a, []byte(content1), "packed-object-1")
	verify(ctx, t, env.RepositoryWriter, oid2a, []byte(content2), "packed-object-2")
	verify(ctx, t, env.RepositoryWriter, oid3a, []byte(content3), "packed-object-3")

	if err := env.RepositoryWriter.ContentManager().CompactIndexes(ctx, indexblob.CompactOptions{MaxSmallBlobs: 1}); err != nil {
		t.Errorf("optimize error: %v", err)
	}

	env.MustReopen(t)

	verify(ctx, t, env.RepositoryWriter, oid1a, []byte(content1), "packed-object-1")
	verify(ctx, t, env.RepositoryWriter, oid2a, []byte(content2), "packed-object-2")
	verify(ctx, t, env.RepositoryWriter, oid3a, []byte(content3), "packed-object-3")

	if err := env.RepositoryWriter.ContentManager().CompactIndexes(ctx, indexblob.CompactOptions{MaxSmallBlobs: 1}); err != nil {
		t.Errorf("optimize error: %v", err)
	}

	env.MustReopen(t)

	verify(ctx, t, env.RepositoryWriter, oid1a, []byte(content1), "packed-object-1")
	verify(ctx, t, env.RepositoryWriter, oid2a, []byte(content2), "packed-object-2")
	verify(ctx, t, env.RepositoryWriter, oid3a, []byte(content3), "packed-object-3")
}

func (s *formatSpecificTestSuite) TestHMAC(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, s.formatVersion)

	c := bytes.Repeat([]byte{0xcd}, 50)

	w := env.RepositoryWriter.NewObjectWriter(ctx, object.WriterOptions{MetadataCompressor: "zstd-fastest"})
	w.Write(c)
	result, err := w.Result()

	if result.String() != "e37e93ba74e074ad1366ee2f032ee9c3a5b81ec82c140b053c1a4e6673d5d9d9" {
		t.Errorf("unexpected result: %v err: %v", result.String(), err)
	}
}

func (s *formatSpecificTestSuite) TestReaderStoredBlockNotFound(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, s.formatVersion)

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

	w := rep.NewObjectWriter(ctx, object.WriterOptions{MetadataCompressor: "zstd-fastest"})
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

	for range 20 {
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
				"": mustParseObjectID(t, "0c2d44dc80de21b71d4219623082f5dc253fe9bb54e48b0fc90e118f8e6cf419"),
				"The quick brown fox jumps over the lazy dog": mustParseObjectID(t, "6bbb74fef0699e516fb96252d8280c1c7f3492e12de9ec6d79c3c9c39b7b0063"),
			},
		},
		{
			format: makeFormat("HMAC-SHA256"),
			oids: map[string]object.ID{
				"The quick brown fox jumps over the lazy dog": mustParseObjectID(t, "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"),
			},
		},
		{
			format: makeFormat("HMAC-SHA256-128"),
			oids: map[string]object.ID{
				"The quick brown fox jumps over the lazy dog": mustParseObjectID(t, "f7bc83f430538424b13298e6aa6fb143"),
			},
		},
	}

	for caseIndex, c := range cases {
		ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant, repotesting.Options{NewRepositoryOptions: c.format})

		for k, v := range c.oids {
			bytesToWrite := []byte(k)
			w := env.RepositoryWriter.NewObjectWriter(ctx, object.WriterOptions{MetadataCompressor: "zstd-fastest"})
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

			bytesRead, err := io.ReadAll(rc)
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
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	rep := env.Repository // read-only

	lw := rep.(repo.RepositoryWriter)

	// w1, w2, w3 are independent sessions.
	_, w1, err := rep.NewWriter(ctx, repo.WriteSessionOptions{Purpose: "writer1"})
	require.NoError(t, err)

	defer w1.Close(ctx)

	_, w2, err := rep.NewWriter(ctx, repo.WriteSessionOptions{Purpose: "writer2"})
	require.NoError(t, err)

	defer w2.Close(ctx)

	_, w3, err := rep.NewWriter(ctx, repo.WriteSessionOptions{Purpose: "writer3"})
	require.NoError(t, err)

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

	require.NoError(t, w1.Flush(ctx))

	// after flushing w1, everybody else can now see o1
	verify(ctx, t, w1, o1, o1Data, "o1-w1")
	verify(ctx, t, w2, o1, o1Data, "o1-w2")
	verify(ctx, t, w3, o1, o1Data, "o1-w3")
	verify(ctx, t, lw, o1, o1Data, "o1-lw")
	verify(ctx, t, rep, o1, o1Data, "o1-rep")

	require.NoError(t, w2.Flush(ctx))

	// after flushing w2, everybody else can now see o2
	verify(ctx, t, w1, o2, o2Data, "o2-w1")
	verify(ctx, t, w2, o2, o2Data, "o2-w2")
	verify(ctx, t, w3, o2, o2Data, "o2-w3")
	verify(ctx, t, lw, o2, o2Data, "o2-lw")
	verify(ctx, t, rep, o2, o2Data, "o2-rep")

	require.NoError(t, w3.Flush(ctx))
	require.NoError(t, lw.Flush(ctx))

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

func TestInitializeWithBlobCfgRetentionBlob(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant, repotesting.Options{
		NewRepositoryOptions: func(n *repo.NewRepositoryOptions) {
			n.RetentionMode = blob.Governance
			n.RetentionPeriod = time.Hour * 24
		},
	})

	var d gather.WriteBuffer
	defer d.Close()

	// verify that the blobcfg retention blob is created
	require.NoError(t, env.RepositoryWriter.BlobStorage().GetBlob(ctx, format.KopiaBlobCfgBlobID, 0, -1, &d))
	require.NoError(t, env.RepositoryWriter.FormatManager().ChangePassword(ctx, "new-password"))
	// verify that the blobcfg retention blob is created and is different after
	// password-change
	require.NoError(t, env.RepositoryWriter.BlobStorage().GetBlob(ctx, format.KopiaBlobCfgBlobID, 0, -1, &d))

	// verify that we cannot re-initialize the repo even after password change
	require.EqualError(t, repo.Initialize(testlogging.Context(t), env.RootStorage(), nil, env.Password),
		"repository already initialized")

	// backup blobcfg blob
	{
		// backup & corrupt the blobcfg blob
		d.Reset()
		require.NoError(t, env.RepositoryWriter.BlobStorage().GetBlob(ctx, format.KopiaBlobCfgBlobID, 0, -1, &d))
		corruptedData := d.Dup()
		corruptedData.Append([]byte("bad bits"))
		require.NoError(t, env.RepositoryWriter.BlobStorage().PutBlob(ctx, format.KopiaBlobCfgBlobID, corruptedData.Bytes(), blob.PutOptions{}))

		// verify that we error out on corrupted blobcfg blob
		_, err := repo.Open(ctx, env.ConfigFile(), env.Password, &repo.Options{})
		require.ErrorContains(t, err, "invalid repository password")

		// restore the original blob
		require.NoError(t, env.RepositoryWriter.BlobStorage().PutBlob(ctx, format.KopiaBlobCfgBlobID, d.Bytes(), blob.PutOptions{}))
	}

	// verify that we'd hard-fail on unexpected errors on blobcfg blob-puts
	// when creating a new repository
	require.EqualError(t,
		repo.Initialize(testlogging.Context(t),
			beforeop.NewWrapper(
				env.RootStorage(),
				// GetBlob callback
				func(ctx context.Context, id blob.ID) error {
					if id == format.KopiaBlobCfgBlobID {
						return errors.New("unexpected error")
					}
					// simulate not-found for format-blob
					if id == format.KopiaRepositoryBlobID {
						return blob.ErrBlobNotFound
					}
					return nil
				}, nil, nil, nil,
			),
			nil,
			env.Password,
		),
		"unexpected error when checking for blobcfg blob: unexpected error")

	// verify that we'd consider the repository corrupted if we were able to
	// read the blobcfg blob but failed to read the format blob
	require.EqualError(t,
		repo.Initialize(testlogging.Context(t),
			beforeop.NewWrapper(
				env.RootStorage(),
				// GetBlob callback
				func(ctx context.Context, id blob.ID) error {
					// simulate not-found for format-blob but let blobcfg
					// blob appear as pre-existing
					if id == format.KopiaBlobCfgBlobID {
						return nil
					}
					if id == format.KopiaRepositoryBlobID {
						return blob.ErrBlobNotFound
					}
					return nil
				}, nil, nil, nil,
			),
			nil,
			env.Password,
		),
		"possible corruption: blobcfg blob exists, but format blob is not found")

	// verify that we consider the repository corrupted if we were unable to
	// write the blobcfg blob
	require.EqualError(t,
		repo.Initialize(testlogging.Context(t),
			beforeop.NewWrapper(
				env.RootStorage(),
				// GetBlob callback
				func(ctx context.Context, id blob.ID) error {
					// simulate not-found for format-blob and blobcfg blob
					if id == format.KopiaBlobCfgBlobID || id == format.KopiaRepositoryBlobID {
						return blob.ErrBlobNotFound
					}
					return nil
				},
				nil, nil,
				// PutBlob callback
				func(ctx context.Context, id blob.ID, _ *blob.PutOptions) error {
					if id == format.KopiaBlobCfgBlobID {
						return errors.New("unexpected error")
					}
					return nil
				},
			),
			&repo.NewRepositoryOptions{
				RetentionMode:   blob.Governance,
				RetentionPeriod: 24 * time.Hour,
			},
			env.Password,
		),
		"unable to write blobcfg blob: PutBlob() failed for \"kopia.blobcfg\": unexpected error")

	// verify that we always read/fail on the repository blob first before the
	// blobcfg blob
	require.EqualError(t,
		repo.Initialize(testlogging.Context(t),
			beforeop.NewWrapper(
				env.RootStorage(),
				// GetBlob callback
				func(ctx context.Context, id blob.ID) error {
					// simulate not-found for format-blob and blobcfg blob
					if id == format.KopiaRepositoryBlobID {
						return errors.New("unexpected error")
					}
					return nil
				},
				nil, nil, nil,
			),
			nil,
			env.Password,
		),
		"unexpected error when checking for format blob: unexpected error")
}

func TestInitializeWithNoRetention(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant, repotesting.Options{})

	// Verify that the blobcfg blob is created even if the retention settings
	// are not supplied.
	var b gather.WriteBuffer
	defer b.Close()
	require.NoError(t, env.RepositoryWriter.BlobStorage().GetBlob(ctx, format.KopiaBlobCfgBlobID, 0, -1, &b))
}

func TestObjectWritesWithRetention(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant, repotesting.Options{
		NewRepositoryOptions: func(n *repo.NewRepositoryOptions) {
			n.RetentionMode = blob.Governance
			n.RetentionPeriod = time.Hour * 24
		},
	})

	writer := env.RepositoryWriter.NewObjectWriter(ctx, object.WriterOptions{MetadataCompressor: "zstd-fastest"})
	_, err := writer.Write([]byte("the quick brown fox jumps over the lazy dog"))
	require.NoError(t, err)

	_, err = writer.Result()
	require.NoError(t, err)

	env.RepositoryWriter.ContentManager().Flush(ctx)

	var prefixesWithRetention []string

	versionedMap := env.RootStorage().(cache.Storage)

	for _, prefix := range content.PackBlobIDPrefixes {
		prefixesWithRetention = append(prefixesWithRetention, string(prefix))
	}

	prefixesWithRetention = append(prefixesWithRetention, indexblob.V0IndexBlobPrefix, epoch.EpochManagerIndexUberPrefix,
		format.KopiaRepositoryBlobID, format.KopiaBlobCfgBlobID)

	// make sure that we cannot set mtime on the kopia objects created due to the
	// retention time constraint
	require.NoError(t, versionedMap.ListBlobs(ctx, "", func(it blob.Metadata) error {
		for _, prefix := range prefixesWithRetention {
			if strings.HasPrefix(string(it.BlobID), prefix) {
				_, err = versionedMap.TouchBlob(ctx, it.BlobID, 0)
				require.Error(t, err, "expected error while touching blob %s", it.BlobID)
				return nil
			}
		}
		_, err = versionedMap.TouchBlob(ctx, it.BlobID, 0)
		require.NoError(t, err, "unexpected error while touching blob %s", it.BlobID)
		return nil
	}))
}

func TestWriteSessionFlushOnSuccess(t *testing.T) {
	var beforeFlushCount, afterFlushCount atomic.Int32

	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant, repotesting.Options{
		OpenOptions: func(o *repo.Options) {
			o.BeforeFlush = append(o.BeforeFlush, func(ctx context.Context, w repo.RepositoryWriter) error {
				beforeFlushCount.Add(1)
				w.OnSuccessfulFlush(func(ctx context.Context, w repo.RepositoryWriter) error {
					afterFlushCount.Add(1)
					return nil
				})
				return nil
			})
		},
	})

	var oid object.ID

	require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		oid = writeObject(ctx, t, w, []byte{1, 2, 3}, "test-1")
		return nil
	}))

	require.EqualValues(t, 1, beforeFlushCount.Load())
	require.EqualValues(t, 1, afterFlushCount.Load())

	verify(ctx, t, env.Repository, oid, []byte{1, 2, 3}, "test-1")

	someErr := errors.New("some error")

	require.ErrorIs(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		oid = writeObject(ctx, t, w, []byte{1, 2, 3, 4}, "test-2")
		return someErr
	}), someErr)

	require.EqualValues(t, 1, beforeFlushCount.Load())
	require.EqualValues(t, 1, afterFlushCount.Load())

	require.ErrorIs(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{
		FlushOnFailure: true,
	}, func(ctx context.Context, w repo.RepositoryWriter) error {
		oid = writeObject(ctx, t, w, []byte{1, 2, 3, 4, 5}, "test-3")
		return someErr
	}), someErr)

	require.EqualValues(t, 2, beforeFlushCount.Load())
	require.EqualValues(t, 2, afterFlushCount.Load())
}

func TestWriteSessionFlushOnSuccessClient(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant, repotesting.Options{})

	apiServerInfo := servertesting.StartServer(t, env, true)

	var beforeFlushCount, afterFlushCount atomic.Int32

	ctx2, cancel := context.WithCancel(testlogging.Context(t))
	defer cancel()

	rep, err := servertesting.ConnectAndOpenAPIServer(t, ctx2, apiServerInfo, repo.ClientOptions{
		Username: servertesting.TestUsername,
		Hostname: servertesting.TestHostname,
	}, content.CachingOptions{
		CacheDirectory: testutil.TempDirectory(t),
	}, servertesting.TestPassword, &repo.Options{
		BeforeFlush: []repo.RepositoryWriterCallback{
			func(ctx context.Context, w repo.RepositoryWriter) error {
				beforeFlushCount.Add(1)
				w.OnSuccessfulFlush(func(ctx context.Context, w repo.RepositoryWriter) error {
					afterFlushCount.Add(1)
					return nil
				})
				return nil
			},
		},
	})

	require.NoError(t, err)

	defer rep.Close(ctx) //nolint:errcheck,staticcheck

	var oid object.ID

	require.NoError(t, repo.WriteSession(ctx, rep, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		oid = writeObject(ctx, t, w, []byte{1, 2, 3}, "test-1")
		return nil
	}))

	require.EqualValues(t, 1, beforeFlushCount.Load())
	require.EqualValues(t, 1, afterFlushCount.Load())

	verify(ctx, t, rep, oid, []byte{1, 2, 3}, "test-1")

	someErr := errors.New("some error")

	require.ErrorIs(t, repo.WriteSession(ctx, rep, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		oid = writeObject(ctx, t, w, []byte{1, 2, 3, 4}, "test-2")
		return someErr
	}), someErr)

	require.EqualValues(t, 1, beforeFlushCount.Load())
	require.EqualValues(t, 1, afterFlushCount.Load())

	require.ErrorIs(t, repo.WriteSession(ctx, rep, repo.WriteSessionOptions{
		FlushOnFailure: true,
	}, func(ctx context.Context, w repo.RepositoryWriter) error {
		oid = writeObject(ctx, t, w, []byte{1, 2, 3, 4, 5}, "test-3")
		return someErr
	}), someErr)

	t.Logf("-----")

	require.EqualValues(t, 2, beforeFlushCount.Load())
	require.EqualValues(t, 2, afterFlushCount.Load())
}

func (s *formatSpecificTestSuite) TestWriteSessionNoFlushOnFailure(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, s.formatVersion)

	var oid object.ID

	someErr := errors.New("some error")
	err := repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		oid = writeObject(ctx, t, w, []byte{1, 2, 3}, "test-1")
		return someErr
	})

	if !errors.Is(err, someErr) {
		t.Fatalf("invalid error: %v want %v", err, someErr)
	}

	verifyNotFound(ctx, t, env.Repository, oid, "test-1")
}

func (s *formatSpecificTestSuite) TestWriteSessionFlushOnFailure(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, s.formatVersion)

	var oid object.ID

	someErr := errors.New("some error")
	err := repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{
		FlushOnFailure: true,
	}, func(ctx context.Context, w repo.RepositoryWriter) error {
		oid = writeObject(ctx, t, w, []byte{1, 2, 3}, "test-1")
		return someErr
	})

	if !errors.Is(err, someErr) {
		t.Fatalf("invalid error: %v want %v", err, someErr)
	}

	verify(ctx, t, env.Repository, oid, []byte{1, 2, 3}, "test-1")
}

func (s *formatSpecificTestSuite) TestChangePassword(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, s.formatVersion)
	if s.formatVersion == format.FormatVersion1 {
		require.Error(t, env.RepositoryWriter.FormatManager().ChangePassword(ctx, "new-password"))
	} else {
		require.NoError(t, env.RepositoryWriter.FormatManager().ChangePassword(ctx, "new-password"))

		r, err := repo.Open(ctx, env.RepositoryWriter.ConfigFilename(), "new-password", nil)
		require.NoError(t, err)
		r.Close(ctx)
	}
}

func TestMetrics_CompressibleData(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)
	_ = ctx

	ms := env.RepositoryMetrics().Snapshot(false)

	require.EqualValues(t, 0, ensureMapEntry(t, ms.Counters, "content_write_bytes"))

	var (
		inputData = bytes.Repeat([]byte{1, 2, 3, 4}, 100)
		count     = 0
		oid       object.ID
	)

	for ensureMapEntry(t, env.RepositoryMetrics().Snapshot(false).Counters, "content_write_duration_nanos") < 5e6 {
		w := env.RepositoryWriter.NewObjectWriter(ctx, object.WriterOptions{
			Compressor:         "gzip",
			MetadataCompressor: "zstd-fastest",
		})
		w.Write(inputData)

		var err error

		oid, err = w.Result()
		require.NoError(t, err)

		count++
	}

	ms = env.RepositoryMetrics().Snapshot(false)
	require.EqualValues(t, count*len(inputData), ensureMapEntry(t, ms.Counters, "content_write_bytes"))
	require.EqualValues(t, count*len(inputData), ensureMapEntry(t, ms.Counters, "content_hashed_bytes"))
	require.EqualValues(t, len(inputData), ensureMapEntry(t, ms.Counters, "content_compression_attempted_bytes"))

	// this is what 100x{1,2,3,4} compresses down to using gzip, it's also
	// the number of bytes that go into encryption.
	const compressedByteCount = 36

	const encryptionOverhead = 28

	require.EqualValues(t, compressedByteCount, ensureMapEntry(t, ms.Counters, "content_after_compression_bytes"))
	require.EqualValues(t, len(inputData), ensureMapEntry(t, ms.Counters, "content_compressible_bytes"))
	require.EqualValues(t, 0, ensureMapEntry(t, ms.Counters, "content_non_compressible_bytes"))
	require.EqualValues(t, len(inputData)-compressedByteCount, ensureMapEntry(t, ms.Counters, "content_compression_savings_bytes"))
	require.EqualValues(t, compressedByteCount+encryptionOverhead, ensureMapEntry(t, ms.Counters, "content_encrypted_bytes"))

	r, err := env.RepositoryWriter.OpenObject(ctx, oid)
	require.NoError(t, err)

	data, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, inputData, data)

	ms = env.RepositoryMetrics().Snapshot(false)
	require.EqualValues(t, len(inputData), ensureMapEntry(t, ms.Counters, "content_read_bytes"))
	require.EqualValues(t, compressedByteCount, ensureMapEntry(t, ms.Counters, "content_decompressed_bytes"))
	require.EqualValues(t, compressedByteCount+encryptionOverhead, ensureMapEntry(t, ms.Counters, "content_decrypted_bytes"))
}

func ensureMapEntry[T any](t *testing.T, m map[string]T, key string) T {
	t.Helper()

	actual, got := m[key]
	require.True(t, got, key)

	return actual
}

func TestAllRegistryMetricsAreMapped(t *testing.T) {
	_, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	snap := env.RepositoryMetrics().Snapshot(false)

	for s := range snap.Counters {
		require.Contains(t, metricid.Counters.NameToIndex, s)
	}

	for s := range snap.DurationDistributions {
		require.Contains(t, metricid.DurationDistributions.NameToIndex, s)
	}

	for s := range snap.SizeDistributions {
		require.Contains(t, metricid.SizeDistributions.NameToIndex, s)
	}
}

func TestDeriveKey(t *testing.T) {
	testPurpose := []byte{0, 0, 0, 0}
	testKeyLength := 8
	masterKey := []byte("01234567890123456789012345678901")
	uniqueID := []byte("a5ba5d2da4b14b518b9501b64b5d87ca")

	j := format.KopiaRepositoryJSON{
		UniqueID:               uniqueID,
		KeyDerivationAlgorithm: format.DefaultKeyDerivationAlgorithm,
	}

	formatEncryptionKeyFromPassword, err := j.DeriveFormatEncryptionKeyFromPassword(repotesting.DefaultPasswordForTesting)
	require.NoError(t, err)

	validV1KeyDerivedFromPassword := crypto.DeriveKeyFromMasterKey(formatEncryptionKeyFromPassword, uniqueID, testPurpose, testKeyLength)
	validV2KeyDerivedFromMasterKey := crypto.DeriveKeyFromMasterKey(masterKey, uniqueID, testPurpose, testKeyLength)

	setup := func(v format.Version) repo.DirectRepositoryWriter {
		_, env := repotesting.NewEnvironment(t, v, repotesting.Options{
			NewRepositoryOptions: func(nro *repo.NewRepositoryOptions) {
				nro.BlockFormat.MasterKey = masterKey
				nro.UniqueID = uniqueID
			},
		})

		return env.RepositoryWriter
	}

	setupUpgraded := func(v1, v2 format.Version) repo.DirectRepositoryWriter {
		ctx, env := repotesting.NewEnvironment(t, v1, repotesting.Options{
			NewRepositoryOptions: func(nro *repo.NewRepositoryOptions) {
				// do not set nro.BlockFormat.MasterKey
				nro.UniqueID = uniqueID
				nro.FormatBlockKeyDerivationAlgorithm = format.DefaultKeyDerivationAlgorithm
			},
		})

		// prepare upgrade
		dw1Upgraded := env.Repository.(repo.DirectRepositoryWriter)
		cf := dw1Upgraded.ContentReader().ContentFormat()

		mp, mperr := cf.GetMutableParameters(ctx)
		require.NoError(t, mperr)

		feat, err := dw1Upgraded.FormatManager().RequiredFeatures(ctx)
		require.NoError(t, err)

		// perform upgrade
		mp.Version = v2

		blobCfg, err := dw1Upgraded.FormatManager().BlobCfgBlob(ctx)
		require.NoError(t, err)

		require.NoError(t, dw1Upgraded.FormatManager().SetParameters(ctx, mp, blobCfg, feat))

		return env.MustConnectOpenAnother(t).(repo.DirectRepositoryWriter)
	}

	// we verify that repositories started on V1 will continue to derive keys from
	// password (which can't be changed) and not from the master key.
	cases := []struct {
		desc       string
		dw         repo.DirectRepositoryWriter
		wantFormat format.Version
		wantKey    []byte
	}{
		{"v1", setup(format.FormatVersion1), format.FormatVersion1, validV1KeyDerivedFromPassword},
		{"v1-v2", setupUpgraded(format.FormatVersion1, format.FormatVersion2), format.FormatVersion2, validV1KeyDerivedFromPassword},
		{"v1-v3", setupUpgraded(format.FormatVersion1, format.FormatVersion3), format.FormatVersion3, validV1KeyDerivedFromPassword},
		{"v2", setup(format.FormatVersion2), format.FormatVersion2, validV2KeyDerivedFromMasterKey},
		{"v3", setup(format.FormatVersion3), format.FormatVersion3, validV2KeyDerivedFromMasterKey},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			mp, err := tc.dw.FormatManager().GetMutableParameters(testlogging.Context(t))
			require.NoError(t, err)

			require.Equal(t, tc.wantFormat, mp.Version)
			require.Equal(t, tc.wantKey, tc.dw.DeriveKey(testPurpose, testKeyLength))
		})
	}
}

func verifyNotFound(ctx context.Context, t *testing.T, rep repo.Repository, objectID object.ID, testCaseID string) {
	t.Helper()

	_, err := rep.OpenObject(ctx, objectID)
	if !errors.Is(err, object.ErrObjectNotFound) {
		t.Fatalf("expected not found for %v, got %v", testCaseID, err)
		return
	}
}

func mustParseObjectID(t *testing.T, s string) object.ID {
	t.Helper()

	id, err := object.ParseID(s)
	require.NoError(t, err)

	return id
}
