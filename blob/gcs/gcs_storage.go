// Package gcs implements Storage based on Google Cloud Storage bucket.
package gcs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"google.golang.org/api/iterator"

	"github.com/efarrer/iothrottler"
	"github.com/kopia/kopia/blob"
	"golang.org/x/oauth2"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

const (
	gcsStorageType = "gcs"
)

type gcsStorage struct {
	Options

	ctx           context.Context
	storageClient *storage.Client
	bucket        *storage.BucketHandle

	downloadThrottler *iothrottler.IOThrottlerPool
	uploadThrottler   *iothrottler.IOThrottlerPool
}

func (gcs *gcsStorage) BlockSize(b string) (int64, error) {
	oh := gcs.bucket.Object(gcs.getObjectNameString(b))
	a, err := oh.Attrs(context.Background())
	if err != nil {
		return 0, translateError(err)
	}

	return a.Size, nil
}

func (gcs *gcsStorage) GetBlock(b string) ([]byte, error) {
	reader, err := gcs.bucket.Object(gcs.getObjectNameString(b)).NewReader(gcs.ctx)
	if err != nil {
		return nil, translateError(err)
	}
	defer reader.Close()

	return ioutil.ReadAll(reader)
}

func translateError(err error) error {
	switch err {
	case nil:
		return nil
	case storage.ErrObjectNotExist:
		return blob.ErrBlockNotFound
	case storage.ErrObjectNotExist:
		return blob.ErrBlockNotFound
	default:
		return err
	}
}

func (gcs *gcsStorage) PutBlock(b string, data []byte, options blob.PutOptions) error {
	writer := gcs.bucket.Object(gcs.getObjectNameString(b)).NewWriter(gcs.ctx)
	throttledWriter, err := gcs.uploadThrottler.AddWriter(writer)
	if err != nil {
		return err
	}
	n, err := throttledWriter.Write(data)
	if err != nil {
		return translateError(err)
	}
	if n != len(data) {
		return writer.CloseWithError(errors.New("truncated write"))
	}

	return translateError(writer.Close())
}

func (gcs *gcsStorage) DeleteBlock(b string) error {
	return translateError(gcs.bucket.Object(gcs.getObjectNameString(b)).Delete(gcs.ctx))
}

func (gcs *gcsStorage) getObjectNameString(b string) string {
	return gcs.Prefix + string(b)
}

func (gcs *gcsStorage) ListBlocks(prefix string) (chan blob.BlockMetadata, blob.CancelFunc) {
	ch := make(chan blob.BlockMetadata, 100)
	cancelled := make(chan bool)

	go func() {
		defer close(ch)

		lst := gcs.bucket.Objects(gcs.ctx, &storage.Query{
			Prefix: gcs.getObjectNameString(prefix),
		})

		oa, err := lst.Next()
		for err == nil {
			bm := blob.BlockMetadata{
				BlockID:   oa.Name[len(gcs.Prefix):],
				Length:    oa.Size,
				TimeStamp: oa.Created,
			}
			select {
			case ch <- bm:
			case <-cancelled:
				return
			}
			oa, err = lst.Next()
		}

		if err != iterator.Done {
			select {
			case ch <- blob.BlockMetadata{Error: translateError(err)}:
				return
			case <-cancelled:
				return
			}
		}
	}()

	return ch, func() {
		close(cancelled)
	}
}

func (gcs *gcsStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   gcsStorageType,
		Config: &gcs.Options,
	}
}

func (gcs *gcsStorage) Close() error {
	gcs.storageClient.Close()
	return nil
}

func (gcs *gcsStorage) String() string {
	return fmt.Sprintf("gcs://%v/%v", gcs.BucketName, gcs.Prefix)
}

func (gcs *gcsStorage) SetThrottle(downloadBytesPerSecond, uploadBytesPerSecond int) error {
	gcs.downloadThrottler.SetBandwidth(toBandwidth(downloadBytesPerSecond))
	gcs.uploadThrottler.SetBandwidth(toBandwidth(uploadBytesPerSecond))
	return nil
}

func toBandwidth(bytesPerSecond int) iothrottler.Bandwidth {
	if bytesPerSecond <= 0 {
		return iothrottler.Unlimited
	}

	return iothrottler.Bandwidth(bytesPerSecond) * iothrottler.BytesPerSecond
}

func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	t := oauth2.Token{}
	err = json.NewDecoder(f).Decode(&t)
	return &t, err
}

func saveToken(file string, token *oauth2.Token) {
	f, err := os.Create(file)
	if err != nil {
		log.Printf("Warning: failed to cache oauth token: %v", err)
		return
	}
	defer f.Close()
	json.NewEncoder(f).Encode(token)
}

// New creates new Google Cloud Storage-backed storage with specified options:
//
// - the 'BucketName' field is required and all other parameters are optional.
//
// By default the connection reuses credentials managed by (https://cloud.google.com/sdk/),
// but this can be disabled by setting IgnoreDefaultCredentials to true.
func New(ctx context.Context, opt *Options) (blob.Storage, error) {
	var cliOpts []option.ClientOption

	if sa := opt.ServiceAccountCredentials; sa != "" {
		cliOpts = append(cliOpts, option.WithServiceAccountFile(sa))
	}

	cli, err := storage.NewClient(ctx, cliOpts...)
	if err != nil {
		return nil, err
	}

	if opt.BucketName == "" {
		return nil, errors.New("bucket name must be specified")
	}

	return &gcsStorage{
		Options:           *opt,
		ctx:               ctx,
		storageClient:     cli,
		bucket:            cli.Bucket(opt.BucketName),
		downloadThrottler: iothrottler.NewIOThrottlerPool(iothrottler.Unlimited),
		uploadThrottler:   iothrottler.NewIOThrottlerPool(iothrottler.Unlimited),
	}, nil
}

func init() {
	blob.AddSupportedStorage(
		gcsStorageType,
		func() interface{} {
			return &Options{}
		},
		func(ctx context.Context, o interface{}) (blob.Storage, error) {
			return New(ctx, o.(*Options))
		})
}

var _ blob.ConnectionInfoProvider = &gcsStorage{}
var _ blob.Throttler = &gcsStorage{}
