package upload

import "github.com/kopia/kopia/snapshot"

// Progress is invoked by by uploader to report status of file and directory uploads.
type Progress interface {
	Progress(path string, numFiles int, pathCompleted, pathTotal int64, stats *snapshot.Stats)
	UploadFinished()
}

type nullUploadProgress struct {
}

func (p *nullUploadProgress) Progress(path string, numFiles int, pathCompleted, pathTotal int64, stats *snapshot.Stats) {
}

func (p *nullUploadProgress) UploadFinished() {
}

var _ Progress = (*nullUploadProgress)(nil)
