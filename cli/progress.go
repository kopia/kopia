package cli

import (
	"os"
	"sync"
	"time"

	"github.com/cheggaaa/pb"
	"github.com/kopia/kopia/snapshot"
)

type uploadProgress struct {
	currentDir string
	mu         sync.Mutex
	bar        *pb.ProgressBar
}

func (p *uploadProgress) Progress(path string, dirCompleted, dirTotal int64, stats *snapshot.Stats) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.currentDir != path || p.bar == nil {
		p.currentDir = path
		if p.bar != nil {
			p.bar.Finish()
			p.bar = nil
		}

		p.bar = pb.New64(dirTotal).Prefix("  " + shortenPath(path))
		p.bar.Output = os.Stderr
		p.bar.SetRefreshRate(time.Second)
		p.bar.ShowSpeed = true
		p.bar.ShowTimeLeft = true
		p.bar.SetUnits(pb.U_BYTES)
		p.bar.Start()
	}

	p.bar.Set64(dirCompleted)
}

func (p *uploadProgress) UploadFinished() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.bar != nil {
		p.bar.Finish()
		p.bar = nil
	}
}

func shortenPath(s string) string {
	if len(s) < 60 {
		return s
	}

	return s[0:30] + "..." + s[len(s)-27:]
}

var _ snapshot.UploadProgress = &uploadProgress{}
