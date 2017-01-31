package snapshot

// UploadProgress is invoked by by uploader to report status of file and directory uploads.
type UploadProgress interface {
	Cached(path string, length int64)

	Started(path string, length int64)
	Progress(path string, completed int64, total int64)
	Finished(path string, length int64, err error)

	StartedDir(path string)
	FinishedDir(path string)
}

type nullUploadProgress struct {
}

func (p *nullUploadProgress) Cached(path string, length int64) {
}

func (p *nullUploadProgress) Started(path string, length int64) {
}

func (p *nullUploadProgress) Progress(path string, completed int64, total int64) {
}

func (p *nullUploadProgress) Finished(path string, length int64, err error) {
}

func (p *nullUploadProgress) StartedDir(path string) {
}

func (p *nullUploadProgress) FinishedDir(path string) {
}
