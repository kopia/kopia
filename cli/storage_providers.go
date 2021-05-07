package cli

import (
	"context"

	"github.com/alecthomas/kingpin"

	"github.com/kopia/kopia/repo/blob"
)

type storageProviderServices interface {
	setPasswordFromToken(pwd string)
}

type storageFlags interface {
	setup(sps storageProviderServices, cmd *kingpin.CmdClause)
	connect(ctx context.Context, isNew bool) (blob.Storage, error)
}

type storageProvider struct {
	name        string
	description string
	newFlags    func() storageFlags
}

var storageProviders = []storageProvider{
	{"from-config", "the provided configuration file", func() storageFlags { return &storageFromConfigFlags{} }},

	{"azure", "an Azure blob storage", func() storageFlags { return &storageAzureFlags{} }},
	{"b2", "a B2 bucket", func() storageFlags { return &storageB2Flags{} }},
	{"filesystem", "a filesystem", func() storageFlags { return &storageFilesystemFlags{} }},
	{"gcs", "a Google Cloud Storage bucket", func() storageFlags { return &storageGCSFlags{} }},
	{"rclone", "an rclone-based provided", func() storageFlags { return &storageRcloneFlags{} }},
	{"s3", "an S3 bucket", func() storageFlags { return &storageS3Flags{} }},
	{"sftp", "an SFTP storage", func() storageFlags { return &storageSFTPFlags{} }},
	{"webdav", "a WebDAV storage", func() storageFlags { return &storageWebDAVFlags{} }},
}
