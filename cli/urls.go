package cli

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/kopia/kopia/blob"

	fsstorage "github.com/kopia/kopia/blob/filesystem"
	gcsstorage "github.com/kopia/kopia/blob/gcs"
	"github.com/kopia/kopia/blob/webdav"
)

func newStorageFromURL(ctx context.Context, urlString string) (blob.Storage, error) {
	if strings.HasPrefix(urlString, "/") {
		urlString = "file://" + urlString
	}

	u, err := url.Parse(urlString)
	if err != nil {
		return nil, err
	}

	switch u.Scheme {
	case "file":
		var fso fsstorage.Options
		if err := parseFilesystemURL(&fso, u); err != nil {
			return nil, err
		}

		return fsstorage.New(ctx, &fso)

	case "gs", "gcs":
		var gcso gcsstorage.Options
		if err := parseGoogleCloudStorageURL(&gcso, u); err != nil {
			return nil, err
		}
		return gcsstorage.New(ctx, &gcso)

	case "http", "https":
		var wdo webdav.Options

		if err := parseWebDAVOptions(&wdo, u); err != nil {
			return nil, err
		}
		return webdav.New(ctx, &wdo)

	default:
		return nil, fmt.Errorf("unrecognized storage type: %v", u.Scheme)
	}
}

func parseFilesystemURL(fso *fsstorage.Options, u *url.URL) error {
	if u.Opaque != "" {
		fso.Path = u.Opaque
	} else {
		fso.Path = u.Path
	}
	if v := connectOwnerUID; v != "" {
		fso.FileUID = getIntPtrValue(v, 10)
	}
	if v := connectOwnerGID; v != "" {
		fso.FileGID = getIntPtrValue(v, 10)
	}
	if v := connectFileMode; v != "" {
		fso.FileMode = getFileModeValue(v, 8)
	}
	if v := connectDirMode; v != "" {
		fso.DirectoryMode = getFileModeValue(v, 8)
	}
	return nil
}

func parseGoogleCloudStorageURL(gcso *gcsstorage.Options, u *url.URL) error {
	gcso.BucketName = u.Host
	gcso.Prefix = u.Path
	gcso.ServiceAccountCredentials = connectCredentialsFile
	gcso.ReadOnly = connectReadOnly
	gcso.MaxUploadSpeedBytesPerSecond = connectMaxUploadSpeedBytesPerSecond
	gcso.MaxDownloadSpeedBytesPerSecond = connectMaxDownloadSpeedBytesPerSecond

	return nil
}

func parseWebDAVOptions(wdo *webdav.Options, u *url.URL) error {
	u2 := *u
	u2.User = nil
	wdo.URL = u2.String()
	if ui := u.User; ui != nil {
		wdo.Username = ui.Username()
		if p, ok := ui.Password(); ok {
			wdo.Password = p
		} else {
			pass, err := askPass("Enter WebDAV password: ")
			if err != nil {
				return err
			}

			wdo.Password = pass
		}
	}
	return nil
}
func getIntPtrValue(value string, base int) *int {
	if int64Val, err := strconv.ParseInt(value, base, 32); err == nil {
		intVal := int(int64Val)
		return &intVal
	}

	return nil
}

func getFileModeValue(value string, def os.FileMode) os.FileMode {
	if uint32Val, err := strconv.ParseUint(value, 8, 32); err == nil {
		return os.FileMode(uint32Val)
	}

	return def
}
