package cli

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/snapshot"
)

type singleProgress struct {
	desc      string
	startTime time.Time
	progress  int64
	total     int64
}

func (p *singleProgress) update(progress, total int64) {
	p.total = total
	p.progress = progress
}

func (p *singleProgress) toString(details bool) string {
	if p.total == 0 {
		return fmt.Sprintf("empty %v", p.desc)
	}

	dur := time.Since(p.startTime)
	extraInfo := ""

	if dur > 1*time.Second && details {
		extraInfo = " " + units.BitsPerSecondsString(8*float64(p.progress)/time.Since(p.startTime).Seconds())
	}

	if p.progress == p.total {
		return fmt.Sprintf("completed %v %v",
			p.desc,
			units.BytesStringBase10(p.progress),
		)
	}

	return fmt.Sprintf("processing %v %v of %v (%v%%)%v",
		p.desc,
		units.BytesStringBase10(p.progress),
		units.BytesStringBase10(p.total),
		100*p.progress/p.total,
		extraInfo,
	)
}

type multiProgress struct {
	mu    sync.Mutex
	items []*singleProgress
}

func (mp *multiProgress) findLocked(desc string) (progress *singleProgress, ordinal int) {
	for i, p := range mp.items {
		if p.desc == desc {
			return p, i
		}
	}

	return nil, 0
}

func (mp *multiProgress) Report(desc string, progress, total int64) {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	found, foundPos := mp.findLocked(desc)

	if found != nil && found.progress == progress && found.total == total {
		// do not print redundant progress
		return
	}

	if found == nil {
		found = &singleProgress{
			desc:      desc,
			startTime: time.Now(),
		}
		foundPos = len(mp.items)
		mp.items = append(mp.items, found)
	}

	found.update(progress, total)

	var segments []string
	for i, p := range mp.items {
		segments = append(segments, p.toString(i > 0))
	}

	if found.progress >= found.total && foundPos == len(segments)-1 {
		mp.items = append(mp.items[0:foundPos], mp.items[foundPos+1:]...)

		if len(segments) > 0 {
			log.Notice(segments[len(segments)-1])
		}
	} else if len(segments) > 0 {
		log.Info(segments[len(segments)-1])
	}
}

func (mp *multiProgress) Progress(path string, numFiles int, dirCompleted, dirTotal int64, stats *snapshot.Stats) {
	mp.Report(
		fmt.Sprintf("directory '%v' (%v files)", shortenPath(strings.TrimPrefix(path, "./")), numFiles),
		dirCompleted,
		dirTotal)
}

func (mp *multiProgress) UploadFinished() {
}

func shortenPath(s string) string {
	if len(s) < 60 {
		return s
	}

	return s[0:30] + "..." + s[len(s)-27:]
}

var cliProgress = &multiProgress{}
