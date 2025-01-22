package notifytemplate_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/notification"
	"github.com/kopia/kopia/notification/notifydata"
	"github.com/kopia/kopia/notification/notifytemplate"
	"github.com/kopia/kopia/snapshot"
)

var defaultTestOptions = notifytemplate.Options{
	Timezone: time.UTC,
}

var altTestOptions = notifytemplate.Options{
	Timezone:   time.FixedZone("PST", -8*60*60),
	TimeFormat: time.RFC1123,
}

func TestNotifyTemplate_generic_error(t *testing.T) {
	args := notification.MakeTemplateArgs(&notifydata.ErrorInfo{
		Operation:        "Some Operation",
		OperationDetails: "Some Operation Details",
		ErrorMessage:     "error message",
		ErrorDetails:     "error details",
		StartTime:        time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC),
		EndTime:          time.Date(2020, 1, 2, 3, 4, 6, 7, time.UTC),
	})

	args.EventTime = time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC)
	args.Hostname = "some-host"

	verifyTemplate(t, "generic-error.txt", ".default", args, defaultTestOptions)
	verifyTemplate(t, "generic-error.html", ".default", args, defaultTestOptions)
	verifyTemplate(t, "generic-error.txt", ".alt", args, altTestOptions)
	verifyTemplate(t, "generic-error.html", ".alt", args, altTestOptions)
}

func TestNotifyTemplate_snapshot_report(t *testing.T) {
	args := notification.MakeTemplateArgs(&notifydata.MultiSnapshotStatus{
		Snapshots: []*notifydata.ManifestWithError{
			{
				// normal snapshot with positive deltas
				Manifest: snapshot.Manifest{
					Source:    snapshot.SourceInfo{Host: "some-host", UserName: "some-user", Path: "/some/path"},
					StartTime: fs.UTCTimestamp(time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC).UnixNano()),
					EndTime:   fs.UTCTimestamp(time.Date(2020, 1, 2, 3, 4, 6, 120000000, time.UTC).UnixNano()),
					RootEntry: &snapshot.DirEntry{
						DirSummary: &fs.DirectorySummary{
							TotalFileCount: 123,
							TotalFileSize:  456,
							TotalDirCount:  33,
							FailedEntries: []*fs.EntryWithError{
								{
									EntryPath: "/some/path",
									Error:     "some error",
								},
								{
									EntryPath: "/some/path2",
									Error:     "some error",
								},
							},
						},
					},
				},
				Previous: &snapshot.Manifest{
					Source:    snapshot.SourceInfo{Host: "some-host", UserName: "some-user", Path: "/some/path"},
					StartTime: fs.UTCTimestamp(time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC).UnixNano()),
					EndTime:   fs.UTCTimestamp(time.Date(2020, 1, 2, 3, 4, 6, 120000000, time.UTC).UnixNano()),
					RootEntry: &snapshot.DirEntry{
						DirSummary: &fs.DirectorySummary{
							TotalFileCount: 100,
							TotalFileSize:  400,
							TotalDirCount:  30,
						},
					},
				},
			},
			{
				// normal snapshot with positive deltas
				Manifest: snapshot.Manifest{
					Source:    snapshot.SourceInfo{Host: "some-host", UserName: "some-user", Path: "/some/path"},
					StartTime: fs.UTCTimestamp(time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC).UnixNano()),
					EndTime:   fs.UTCTimestamp(time.Date(2020, 1, 2, 3, 4, 6, 120000000, time.UTC).UnixNano()),
					RootEntry: &snapshot.DirEntry{
						DirSummary: &fs.DirectorySummary{
							TotalFileCount: 123,
							TotalFileSize:  456,
							TotalDirCount:  33,
							FailedEntries: []*fs.EntryWithError{
								{
									EntryPath: "/some/path",
									Error:     "some error",
								},
								{
									EntryPath: "/some/path2",
									Error:     "some error",
								},
							},
						},
					},
				},
				Previous: &snapshot.Manifest{
					Source:    snapshot.SourceInfo{Host: "some-host", UserName: "some-user", Path: "/some/path"},
					StartTime: fs.UTCTimestamp(time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC).UnixNano()),
					EndTime:   fs.UTCTimestamp(time.Date(2020, 1, 2, 3, 4, 6, 120000000, time.UTC).UnixNano()),
					RootEntry: &snapshot.DirEntry{
						DirSummary: &fs.DirectorySummary{
							TotalFileCount: 200,
							TotalFileSize:  500,
							TotalDirCount:  40,
						},
					},
				},
			},
			{
				// no previous snapshot
				Manifest: snapshot.Manifest{
					Source:    snapshot.SourceInfo{Host: "some-host", UserName: "some-user", Path: "/some/path2"},
					StartTime: fs.UTCTimestamp(time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC).UnixNano()),
					EndTime:   fs.UTCTimestamp(time.Date(2020, 1, 2, 3, 4, 6, 120000000, time.UTC).UnixNano()),
					RootEntry: &snapshot.DirEntry{
						DirSummary: &fs.DirectorySummary{
							TotalFileCount: 123,
							TotalFileSize:  456,
							TotalDirCount:  33,
							FailedEntries: []*fs.EntryWithError{
								{
									EntryPath: "/some/path",
									Error:     "some error",
								},
								{
									EntryPath: "/some/path2",
									Error:     "some error",
								},
							},
						},
					},
				},
			},
			{
				Error: "some top-level error",
				Manifest: snapshot.Manifest{
					Source: snapshot.SourceInfo{Host: "some-host", UserName: "some-user", Path: "/some/other/path"},
				},
			},
		},
	})

	args.EventTime = time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC)
	args.Hostname = "some-host"

	verifyTemplate(t, "snapshot-report.txt", ".default", args, defaultTestOptions)
	verifyTemplate(t, "snapshot-report.html", ".default", args, defaultTestOptions)
	verifyTemplate(t, "snapshot-report.txt", ".alt", args, altTestOptions)
	verifyTemplate(t, "snapshot-report.html", ".alt", args, altTestOptions)
}

func TestNotifyTemplate_snapshot_report_single_success(t *testing.T) {
	args := notification.MakeTemplateArgs(&notifydata.MultiSnapshotStatus{
		Snapshots: []*notifydata.ManifestWithError{
			{
				// normal snapshot with positive deltas
				Manifest: snapshot.Manifest{
					Source:    snapshot.SourceInfo{Host: "some-host", UserName: "some-user", Path: "/some/path"},
					StartTime: fs.UTCTimestamp(time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC).UnixNano()),
					EndTime:   fs.UTCTimestamp(time.Date(2020, 1, 2, 3, 4, 6, 120000000, time.UTC).UnixNano()),
					RootEntry: &snapshot.DirEntry{
						DirSummary: &fs.DirectorySummary{
							TotalFileCount: 123,
							TotalFileSize:  456,
							TotalDirCount:  33,
							FailedEntries: []*fs.EntryWithError{
								{
									EntryPath: "/some/path",
									Error:     "some error",
								},
								{
									EntryPath: "/some/path2",
									Error:     "some error",
								},
							},
						},
					},
				},
				Previous: &snapshot.Manifest{
					Source:    snapshot.SourceInfo{Host: "some-host", UserName: "some-user", Path: "/some/path"},
					StartTime: fs.UTCTimestamp(time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC).UnixNano()),
					EndTime:   fs.UTCTimestamp(time.Date(2020, 1, 2, 3, 4, 6, 120000000, time.UTC).UnixNano()),
					RootEntry: &snapshot.DirEntry{
						DirSummary: &fs.DirectorySummary{
							TotalFileCount: 100,
							TotalFileSize:  400,
							TotalDirCount:  30,
						},
					},
				},
			},
		},
	})

	args.EventTime = time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC)
	args.Hostname = "some-host"

	verifyTemplate(t, "snapshot-report.txt", ".success", args, defaultTestOptions)
	verifyTemplate(t, "snapshot-report.html", ".success", args, defaultTestOptions)
}

func verifyTemplate(t *testing.T, embeddedTemplateName, expectedSuffix string, args interface{}, opt notifytemplate.Options) {
	t.Helper()

	tmpl, err := notifytemplate.GetEmbeddedTemplate(embeddedTemplateName)
	require.NoError(t, err)

	tt, err := notifytemplate.ParseTemplate(tmpl, opt)
	require.NoError(t, err)

	var buf bytes.Buffer

	require.NoError(t, tt.Execute(&buf, args))

	actualFileName := filepath.Join("testdata", embeddedTemplateName+expectedSuffix+".actual")
	require.NoError(t, os.WriteFile(actualFileName, buf.Bytes(), 0o644))

	expectedFileName := filepath.Join("testdata", embeddedTemplateName+expectedSuffix+".expected")

	wantBytes, err := os.ReadFile(expectedFileName)
	require.NoError(t, err)

	want := string(wantBytes)

	assert.Equal(t, want, buf.String())

	if want == buf.String() {
		require.NoError(t, os.Remove(actualFileName))
	}
}
