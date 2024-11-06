package notifytemplate_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/notification"
	"github.com/kopia/kopia/notification/notifydata"
	"github.com/kopia/kopia/notification/notifytemplate"
	"github.com/kopia/kopia/snapshot"
)

var testOptions = notifytemplate.Options{
	Timezone:   time.UTC,
	TimeFormat: time.RFC3339,
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

	verifyTemplate(t, "generic-error.txt", args)
	verifyTemplate(t, "generic-error.html", args)
}

func TestNotifyTemplate_snapshot_report(t *testing.T) {
	args := notification.MakeTemplateArgs(&notifydata.MultiSnapshotStatus{
		Snapshots: []*notifydata.ManifestWithError{
			{
				Manifest: snapshot.Manifest{
					Source:    snapshot.SourceInfo{Host: "some-host", UserName: "some-user", Path: "/some/path"},
					StartTime: fs.UTCTimestamp(time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC).UnixNano()),
					EndTime:   fs.UTCTimestamp(time.Date(2020, 1, 2, 3, 4, 6, 7, time.UTC).UnixNano()),
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

	verifyTemplate(t, "snapshot-report.txt", args)
	verifyTemplate(t, "snapshot-report.html", args)
}

func verifyTemplate(t *testing.T, embeddedTemplateName string, args interface{}) {
	t.Helper()

	tmpl, err := notifytemplate.GetEmbeddedTemplate(embeddedTemplateName)
	require.NoError(t, err)

	tt, err := notifytemplate.ParseTemplate(tmpl, testOptions)
	require.NoError(t, err)

	var buf bytes.Buffer

	require.NoError(t, tt.Execute(&buf, args))

	actualFileName := filepath.Join("testdata", embeddedTemplateName+".actual")
	require.NoError(t, os.WriteFile(actualFileName, buf.Bytes(), 0o644))

	expectedFileName := filepath.Join("testdata", embeddedTemplateName+".expected")

	wantBytes, err := os.ReadFile(expectedFileName)
	require.NoError(t, err)

	want := string(wantBytes)
	require.NotEmpty(t, want)

	require.Equal(t, want, buf.String())

	require.NoError(t, os.Remove(actualFileName))
}
