package main

import (
	"log"
	"path/filepath"
	"time"

	"github.com/cheggaaa/pb"

	"github.com/kopia/kopia/internal/units"

	"github.com/kopia/kopia/fs/repofs"
)

type uploadProgress struct {
	dirLevel     int
	outputPrefix string

	bar *pb.ProgressBar
}

func (p *uploadProgress) Cached(path string, length int64) {
	log.Printf(p.outputPrefix+"Cached: %v %v", path, units.BytesString(length))
}

func (p *uploadProgress) StartedDir(path string) {
	log.Printf("Processing directory: %v", path)
	p.dirLevel++
	p.outputPrefix = p.outputPrefix + "  "
}

func (p *uploadProgress) FinishedDir(path string) {
	log.Printf("Finished directory: %v", path)
	p.outputPrefix = p.outputPrefix[2:]
	p.dirLevel--
}

func (p *uploadProgress) Started(path string, length int64) {
	//log.Printf("STARTED %v %v", path, units.BytesString(length))
	p.bar = pb.New64(length).Prefix(p.outputPrefix + filepath.Base(path))
	p.bar.SetRefreshRate(time.Second)
	p.bar.ShowSpeed = true
	p.bar.ShowTimeLeft = true
	p.bar.SetUnits(pb.U_BYTES)
	p.bar.Start()
}

func (p *uploadProgress) Finished(path string, length int64, err error) {
	if p.bar != nil {
		p.bar.Finish()
		p.bar = nil
	}
	//log.Printf("FINISHED %v %v", path, units.BytesString(length))
}

func (p *uploadProgress) Progress(path string, completed, total int64) {
	//log.Printf("PROGRESS %v %v/%v", path, units.BytesString(completed), units.BytesString(total))
	p.bar.Set64(completed)
}

var up repofs.UploadProgress = &uploadProgress{}
