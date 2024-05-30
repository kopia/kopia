package cli

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/repodiag"
	"github.com/kopia/kopia/repo/blob"
)

type logSessionInfo struct {
	id        string
	startTime time.Time
	endTime   time.Time
	segments  []blob.Metadata
	totalSize int64
}

type logSelectionCriteria struct {
	all         bool
	latest      int
	youngerThan time.Duration
	olderThan   time.Duration
}

func (c *logSelectionCriteria) setup(cmd *kingpin.CmdClause) {
	cmd.Flag("all", "Show all logs").BoolVar(&c.all)
	cmd.Flag("latest", "Include last N logs, by default the last one is shown").Short('n').IntVar(&c.latest)
	cmd.Flag("younger-than", "Include logs younger than X (e.g. '1h')").DurationVar(&c.youngerThan)
	cmd.Flag("older-than", "Include logs older than X (e.g. '1h')").DurationVar(&c.olderThan)
}

func (c *logSelectionCriteria) any() bool {
	return c.all || c.latest > 0 || c.youngerThan > 0 || c.olderThan > 0
}

func (c *logSelectionCriteria) filterLogSessions(allSessions []*logSessionInfo) []*logSessionInfo {
	if c.all {
		return allSessions
	}

	if c.youngerThan > 0 {
		allSessions = filterLogSessions(allSessions, func(ls *logSessionInfo) bool {
			return clock.Now().Sub(ls.startTime) < c.youngerThan
		})
	}

	if c.olderThan > 0 {
		allSessions = filterLogSessions(allSessions, func(ls *logSessionInfo) bool {
			return clock.Now().Sub(ls.startTime) > c.olderThan
		})
	}

	if c.latest > 0 && len(allSessions) > c.latest {
		allSessions = allSessions[len(allSessions)-c.latest:]
	}

	return allSessions
}

func getLogSessions(ctx context.Context, st blob.Reader) ([]*logSessionInfo, error) {
	sessions := map[string]*logSessionInfo{}

	var allSessions []*logSessionInfo

	if err := st.ListBlobs(ctx, repodiag.LogBlobPrefix, func(bm blob.Metadata) error {
		parts := strings.Split(string(bm.BlobID), "_")

		//nolint:mnd
		if len(parts) < 8 {
			log(ctx).Errorf("invalid part count: %v skipping unrecognized log: %v", len(parts), bm.BlobID)
			return nil
		}

		id := parts[2] + "_" + parts[3]

		startTime, err := strconv.ParseInt(parts[4], 10, 64)
		if err != nil {
			log(ctx).Errorf("invalid start time - skipping unrecognized log: %v", bm.BlobID)

			//nolint:nilerr
			return nil
		}

		endTime, err := strconv.ParseInt(parts[5], 10, 64)
		if err != nil {
			log(ctx).Errorf("invalid end time - skipping unrecognized log: %v", bm.BlobID)

			//nolint:nilerr
			return nil
		}

		s := sessions[id]
		if s == nil {
			s = &logSessionInfo{
				id: id,
			}
			sessions[id] = s
			allSessions = append(allSessions, s)
		}

		if t := time.Unix(startTime, 0); s.startTime.IsZero() || t.Before(s.startTime) {
			s.startTime = t
		}

		if t := time.Unix(endTime, 0); t.After(s.endTime) {
			s.endTime = t
		}

		s.segments = append(s.segments, bm)
		s.totalSize += bm.Length

		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "error listing logs")
	}

	for _, s := range allSessions {
		sort.Slice(s.segments, func(i, j int) bool {
			return s.segments[i].Timestamp.Before(s.segments[j].Timestamp)
		})
	}

	// sort sessions by start time
	sort.Slice(allSessions, func(i, j int) bool {
		return allSessions[i].segments[0].Timestamp.Before(allSessions[j].segments[0].Timestamp)
	})

	return allSessions, nil
}

func filterLogSessions(logs []*logSessionInfo, predicate func(l *logSessionInfo) bool) []*logSessionInfo {
	var result []*logSessionInfo

	for _, l := range logs {
		if predicate(l) {
			result = append(result, l)
		}
	}

	return result
}
