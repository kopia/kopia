package main

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/kopia/kopia/blob"
)

func newStorageFromURL(urlString string) (blob.Storage, error) {
	if strings.HasPrefix(urlString, "/") {
		urlString = "file://" + urlString
	}

	u, err := url.Parse(urlString)
	if err != nil {
		return nil, err
	}

	switch u.Scheme {
	case "file":
		var fso blob.FSStorageOptions
		if err := parseFilesystemURL(&fso, u); err != nil {
			return nil, err
		}

		return blob.NewFSStorage(&fso)

	case "gs", "gcs":
		var gcso blob.GCSStorageOptions
		if err := parseGoogleCloudStorageURL(&gcso, u); err != nil {
			return nil, err
		}
		return blob.NewGCSStorage(&gcso)

	default:
		return nil, fmt.Errorf("unrecognized storage type: %v", u.Scheme)
	}
}

func parseFilesystemURL(fso *blob.FSStorageOptions, u *url.URL) error {
	if u.Opaque != "" {
		fso.Path = u.Opaque
	} else {
		fso.Path = u.Path
	}
	fso.FileUID = getIntPtrValue(u, "uid", 10)
	fso.FileGID = getIntPtrValue(u, "gid", 10)
	fso.FileMode = getFileModeValue(u, "filemode", 0)
	fso.DirectoryMode = getFileModeValue(u, "dirmode", 0)
	if s := u.Query().Get("shards"); s != "" {
		parts := strings.Split(s, ".")
		shards := make([]int, len(parts))
		for i, p := range parts {
			var err error
			shards[i], err = strconv.Atoi(p)
			if err != nil {
				return err
			}
		}
		fso.DirectoryShards = shards
	}
	return nil
}

func parseGoogleCloudStorageURL(gcso *blob.GCSStorageOptions, u *url.URL) error {
	gcso.BucketName = u.Host
	gcso.Prefix = u.Path
	return nil
}

func getIntPtrValue(u *url.URL, name string, base int) *int {
	if value := u.Query().Get(name); value != "" {
		if int64Val, err := strconv.ParseInt(value, base, 32); err == nil {
			intVal := int(int64Val)
			return &intVal
		}
	}

	return nil
}

func getFileModeValue(u *url.URL, name string, def os.FileMode) os.FileMode {
	if value := u.Query().Get(name); value != "" {
		if uint32Val, err := strconv.ParseUint(value, 8, 32); err == nil {
			return os.FileMode(uint32Val)
		}
	}

	return def
}

func getStringValue(u *url.URL, name string, def string) string {
	if value := u.Query().Get(name); value != "" {
		return value
	}

	return def
}
