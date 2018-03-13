package snapshot

// UploadProgress is invoked by by uploader to report status of file and directory uploads.
type UploadProgress interface {
	Progress(path string, pathCompleted, pathTotal int64, stats *Stats)
	UploadFinished()
}

type nullUploadProgress struct {
}

func (p *nullUploadProgress) Progress(path string, pathCompleted, pathTotal int64, stats *Stats) {
}

func (p *nullUploadProgress) UploadFinished() {
}

var _ UploadProgress = (*nullUploadProgress)(nil)
