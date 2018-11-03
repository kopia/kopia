package snapshotfs

import "github.com/kopia/kopia/snapshot"

// UploadProgress is invoked by by uploader to report status of file and directory uploads.
type UploadProgress interface {
	Progress(path string, numFiles int, pathCompleted, pathTotal int64, stats *snapshot.Stats)
	UploadFinished()
}

type nullUploadProgress struct {
}

func (p *nullUploadProgress) Progress(path string, numFiles int, pathCompleted, pathTotal int64, stats *snapshot.Stats) {
}

func (p *nullUploadProgress) UploadFinished() {
}

var _ UploadProgress = (*nullUploadProgress)(nil)
