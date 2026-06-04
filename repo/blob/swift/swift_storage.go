package swift

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"strings"

	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/objects"
	"github.com/gophercloud/gophercloud/v2/pagination"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/readonly"
	"github.com/kopia/kopia/repo/blob/retrying"
)

const (
	swiftStorageType = "swift"
	listLimit        = 1000
)

type swiftStorage struct {
	Options
	blob.DefaultProviderImplementation

	cli *gophercloud.ServiceClient
}

func (s *swiftStorage) GetBlob(ctx context.Context, b blob.ID, offset, length int64, output blob.OutputBuffer) error {
	if offset < 0 {
		return blob.ErrInvalidRange
	}

	output.Reset()

	if length == 0 {
		bm, err := s.GetMetadata(ctx, b)
		if err != nil {
			return err
		}

		if offset > bm.Length {
			return blob.ErrInvalidRange
		}

		return blob.EnsureLengthExactly(output.Length(), length) //nolint:wrapcheck
	}

	opts := objects.DownloadOpts{}
	if length > 0 {
		if offset+length < offset {
			return blob.ErrInvalidRange
		}

		opts.Range = fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	}

	result := objects.Download(ctx, s.cli, s.ContainerName, s.getObjectNameString(b), opts)
	if result.Err != nil {
		return errors.Wrap(translateError(result.Err), "Download")
	}

	defer result.Body.Close() //nolint:errcheck

	if err := iocopy.JustCopy(output, result.Body); err != nil {
		return errors.Wrap(err, "copy blob")
	}

	return blob.EnsureLengthExactly(output.Length(), length) //nolint:wrapcheck
}

func (s *swiftStorage) GetMetadata(ctx context.Context, b blob.ID) (blob.Metadata, error) {
	result := objects.Get(ctx, s.cli, s.ContainerName, s.getObjectNameString(b), nil)

	header, err := result.Extract()
	if err != nil {
		return blob.Metadata{}, errors.Wrap(translateError(err), "Get")
	}

	return blob.Metadata{
		BlobID:    b,
		Length:    header.ContentLength,
		Timestamp: header.LastModified,
	}, nil
}

func (s *swiftStorage) PutBlob(ctx context.Context, b blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	switch {
	case opts.HasRetentionOptions():
		return errors.Wrap(blob.ErrUnsupportedPutBlobOption, "blob-retention")
	case !opts.SetModTime.IsZero():
		return blob.ErrSetTimeUnsupported
	}

	r := data.Reader()
	defer r.Close() //nolint:errcheck

	createOpts := objects.CreateOpts{
		Content:       r,
		ContentLength: int64(data.Length()),
		ContentType:   "application/x-kopia",
	}

	if opts.DoNotRecreate {
		createOpts.IfNoneMatch = "*"
	}

	if err := translateError(objects.Create(ctx, s.cli, s.ContainerName, s.getObjectNameString(b), createOpts).Err); err != nil {
		return errors.Wrap(err, "Create")
	}

	if opts.GetModTime != nil {
		bm, err := s.GetMetadata(ctx, b)
		if err != nil {
			return err
		}

		*opts.GetModTime = bm.Timestamp
	}

	return nil
}

func (s *swiftStorage) DeleteBlob(ctx context.Context, b blob.ID) error {
	err := translateError(objects.Delete(ctx, s.cli, s.ContainerName, s.getObjectNameString(b), nil).Err)
	if errors.Is(err, blob.ErrBlobNotFound) {
		return nil
	}

	return errors.Wrap(err, "Delete")
}

func (s *swiftStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	pager := objects.List(s.cli, s.ContainerName, objects.ListOpts{
		Limit:  listLimit,
		Prefix: s.getObjectNameString(prefix),
	})

	return errors.Wrap(pager.EachPage(ctx, func(_ context.Context, page pagination.Page) (bool, error) {
		items, err := objects.ExtractInfo(page)
		if err != nil {
			return false, err
		}

		for _, item := range items {
			if item.Subdir != "" || !strings.HasPrefix(item.Name, s.Prefix) {
				continue
			}

			if err := callback(blob.Metadata{
				BlobID:    s.toBlobID(item.Name),
				Length:    item.Bytes,
				Timestamp: item.LastModified,
			}); err != nil {
				return false, err
			}
		}

		return true, nil
	}), "List")
}

func (s *swiftStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   swiftStorageType,
		Config: &s.Options,
	}
}

func (s *swiftStorage) DisplayName() string {
	return fmt.Sprintf("Swift: %v %v", s.AuthURL, s.ContainerName)
}

func (s *swiftStorage) String() string {
	return fmt.Sprintf("swift://%s/%s", s.ContainerName, s.Prefix)
}

func (s *swiftStorage) getObjectNameString(b blob.ID) string {
	return s.Prefix + string(b)
}

func (s *swiftStorage) toBlobID(objectName string) blob.ID {
	return blob.ID(objectName[len(s.Prefix):])
}

func (o *Options) authOptions() gophercloud.AuthOptions {
	return gophercloud.AuthOptions{
		IdentityEndpoint: o.AuthURL,

		Username: o.Username,
		UserID:   o.UserID,
		Password: o.Password,

		DomainName: o.DomainName,
		DomainID:   o.DomainID,
		TenantName: o.TenantName,
		TenantID:   o.TenantID,

		TokenID: o.Token,

		ApplicationCredentialID:     o.ApplicationCredentialID,
		ApplicationCredentialName:   o.ApplicationCredentialName,
		ApplicationCredentialSecret: o.ApplicationCredentialSecret,

		AllowReauth: o.Token == "",
	}
}

func translateError(err error) error {
	if err == nil {
		return nil
	}

	var reauthErr *gophercloud.ErrErrorAfterReauthentication
	if errors.As(err, &reauthErr) {
		return translateError(reauthErr.ErrOriginal)
	}

	var unableReauthErr *gophercloud.ErrUnableToReauthenticate
	if errors.As(err, &unableReauthErr) {
		return translateError(unableReauthErr.ErrOriginal)
	}

	switch {
	case gophercloud.ResponseCodeIs(err, http.StatusUnauthorized):
		return blob.ErrInvalidCredentials
	case gophercloud.ResponseCodeIs(err, http.StatusForbidden):
		return blob.ErrInvalidCredentials
	case gophercloud.ResponseCodeIs(err, http.StatusNotFound):
		return blob.ErrBlobNotFound
	case gophercloud.ResponseCodeIs(err, http.StatusRequestedRangeNotSatisfiable):
		return blob.ErrInvalidRange
	case gophercloud.ResponseCodeIs(err, http.StatusPreconditionFailed):
		return blob.ErrBlobAlreadyExists
	default:
		return err
	}
}

func availabilityFromString(v string) (gophercloud.Availability, error) {
	switch strings.ToLower(v) {
	case "", "public", "publicurl":
		return gophercloud.AvailabilityPublic, nil
	case "internal", "internalurl":
		return gophercloud.AvailabilityInternal, nil
	case "admin", "adminurl":
		return gophercloud.AvailabilityAdmin, nil
	default:
		return "", errors.Errorf("unsupported OpenStack endpoint availability %q", v)
	}
}

func getCustomTransport(opt *Options) (*http.Transport, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone() //nolint:forcetypeassert

	if opt.DoNotVerifyTLS {
		//nolint:gosec
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}

		return transport, nil
	}

	if len(opt.RootCA) != 0 {
		rootcas := x509.NewCertPool()
		if ok := rootcas.AppendCertsFromPEM(opt.RootCA); !ok {
			return nil, errors.New("cannot parse provided CA")
		}

		tlsConfig := transport.TLSClientConfig
		if tlsConfig == nil {
			tlsConfig = &tls.Config{}
		} else {
			tlsConfig = tlsConfig.Clone()
		}

		tlsConfig.RootCAs = rootcas
		transport.TLSClientConfig = tlsConfig
	}

	return transport, nil
}

// New creates new OpenStack Swift-backed storage with specified options.
func New(ctx context.Context, opt *Options, isCreate bool) (blob.Storage, error) {
	_ = isCreate

	if opt.ContainerName == "" {
		return nil, errors.New("container name must be specified")
	}

	if opt.AuthURL == "" {
		return nil, errors.New("auth URL must be specified")
	}

	availability, err := availabilityFromString(opt.Availability)
	if err != nil {
		return nil, err
	}

	provider, err := openstack.NewClient(opt.AuthURL)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create OpenStack client")
	}

	provider.UserAgent.Prepend(blob.ApplicationID)

	provider.HTTPClient.Transport, err = getCustomTransport(opt)
	if err != nil {
		return nil, err
	}

	if err := openstack.Authenticate(ctx, provider, opt.authOptions()); err != nil {
		return nil, errors.Wrap(translateError(err), "unable to authenticate OpenStack client")
	}

	cli, err := openstack.NewObjectStorageV1(provider, gophercloud.EndpointOpts{
		Region:       opt.Region,
		Availability: availability,
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to create Swift object storage client")
	}

	var st blob.Storage = &swiftStorage{
		Options: *opt,
		cli:     cli,
	}

	if opt.ReadOnly {
		st = readonly.NewWrapper(st)
	}

	return retrying.NewWrapper(st), nil
}

func init() {
	blob.AddSupportedStorage(swiftStorageType, Options{}, New)
}
