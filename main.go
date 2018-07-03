/*
Command-line tool for creating and accessing backups.

Usage:

  $ kopia [<flags>] <subcommand> [<args> ...]

Use 'kopia help' to see more details.
*/
package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kopia/kopia/internal/ospath"

	"github.com/mattn/go-colorable"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/repo"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"gopkg.in/alecthomas/kingpin.v2"

	_ "github.com/kopia/kopia/storage/filesystem/cli"
	_ "github.com/kopia/kopia/storage/gcs/cli"
	_ "github.com/kopia/kopia/storage/s3/cli"
	_ "github.com/kopia/kopia/storage/webdav/cli"
)

var logLevels = []string{"debug", "info", "warning", "error"}
var (
	logFile        = cli.App().Flag("log-file", "Log file name.").String()
	logDir         = cli.App().Flag("log-dir", "Directory where log files should be written.").Envar("KOPIA_LOG_DIR").Default(ospath.LogsDir()).String()
	logDirMaxFiles = cli.App().Flag("log-dir-max-files", "Maximum number of log files to retain").Envar("KOPIA_LOG_DIR_MAX_FILES").Default("1000").Hidden().Int()
	logDirMaxAge   = cli.App().Flag("log-dir-max-age", "Maximum age of log files to retain").Envar("KOPIA_LOG_DIR_MAX_AGE").Hidden().Duration()
	logLevel       = cli.App().Flag("log-level", "Console log level").Default("info").Enum(logLevels...)
	fileLogLevel   = cli.App().Flag("file-log-level", "File log level").Default("debug").Enum(logLevels...)
)

const logFileNamePrefix = "kopia-"
const logFileNameSuffix = ".log"

func initializeLogging(ctx *kingpin.ParseContext) error {
	zerolog.TimeFieldFormat = "2006-01-02T15:04:05.000000"

	var logWriters []io.Writer
	var logFileName string
	var symlinkName string

	if lfn := *logFile; lfn != "" {
		var err error
		logFileName, err = filepath.Abs(lfn)
		if err != nil {
			return err
		}
	}

	var shouldSweepLogs bool
	if logFileName == "" && *logDir != "" {
		logBaseName := fmt.Sprintf("%v%v-%v%v", logFileNamePrefix, time.Now().Format("20060102-150405"), os.Getpid(), logFileNameSuffix)
		logFileName = filepath.Join(*logDir, logBaseName)
		symlinkName = "kopia.latest.log"
		if *logDirMaxAge > 0 || *logDirMaxFiles > 0 {
			shouldSweepLogs = true
		}
	}

	if logFileName != "" {
		logFileDir := filepath.Dir(logFileName)
		logFileBaseName := filepath.Base(logFileName)
		if err := os.MkdirAll(logFileDir, 0700); err != nil {
			fmt.Fprintln(os.Stderr, "Unable to create logs directory:", err)
		}

		logWriters = append(logWriters,
			levelFilter(
				*fileLogLevel,
				zerolog.ConsoleWriter{Out: &onDemandLogWriter{
					logDir:          logFileDir,
					logFileBaseName: logFileBaseName,
					symlinkName:     symlinkName,
				}, NoColor: true}))
	}

	logWriters = append(logWriters,
		levelFilter(
			*logLevel,
			zerolog.ConsoleWriter{Out: colorable.NewColorableStderr()}))

	log.Logger = zerolog.New(zerolog.MultiLevelWriter(logWriters...)).With().Timestamp().Logger()

	if shouldSweepLogs {
		go sweepLogDir(*logDir, *logDirMaxFiles, *logDirMaxAge)
	}

	return nil
}

func sweepLogDir(dirname string, maxCount int, maxAge time.Duration) {
	var timeCutoff time.Time
	if maxAge > 0 {
		timeCutoff = time.Now().Add(-maxAge)
	}
	if maxCount == 0 {
		maxCount = math.MaxInt32
	}
	log.Debug().Msgf("log file time cut-off: %v max count: %v", timeCutoff, maxCount)

	entries, err := ioutil.ReadDir(dirname)
	if err != nil {
		log.Warn().Err(err).Msg("unable to read log directory")
		return
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].ModTime().After(entries[j].ModTime())
	})

	var cnt = 0

	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), logFileNamePrefix) {
			continue
		}

		if !strings.HasSuffix(e.Name(), logFileNameSuffix) {
			continue
		}

		cnt++
		if cnt > maxCount || e.ModTime().Before(timeCutoff) {
			if err = os.Remove(filepath.Join(dirname, e.Name())); err != nil {
				log.Warn().Err(err).Msg("unable to remove log file")
			}
		}
	}
}

var usageTemplate = `{{define "FormatCommand"}}\
{{if .FlagSummary}} {{.FlagSummary}}{{end}}\
{{range .Args}} {{if not .Required}}[{{end}}<{{.Name}}>{{if .Value|IsCumulative}}...{{end}}{{if not .Required}}]{{end}}{{end}}\
{{end}}\
{{define "FormatCommandList"}}\
{{range .}}\
{{if not .Hidden}}\
{{.Depth|Indent}}{{.Name}}{{if .Default}}*{{end}}{{template "FormatCommand" .}}
{{template "FormatCommandList" .Commands}}\
{{end}}\
{{end}}\
{{end}}\
{{define "FormatUsage"}}\
{{template "FormatCommand" .}}{{if .Commands}} <command> [<args> ...]{{end}}
{{if .Help}}
{{.Help|Wrap 0}}\
{{end}}\
{{end}}\
{{if .Context.SelectedCommand}}\
usage: {{.App.Name}} {{.Context.SelectedCommand}}{{template "FormatUsage" .Context.SelectedCommand}}
{{else}}\
usage: {{.App.Name}}{{template "FormatUsage" .App}}
{{end}}\
{{if .Context.Flags}}\
Flags:
{{.Context.Flags|FlagsToTwoColumns|FormatTwoColumns}}
{{end}}\
{{if .Context.Args}}\
Args:
{{.Context.Args|ArgsToTwoColumns|FormatTwoColumns}}
{{end}}\
{{if .Context.SelectedCommand}}\
{{if .Context.SelectedCommand.Commands}}\
Subcommands:
  {{.Context.SelectedCommand}}
{{template "FormatCommandList" .Context.SelectedCommand.Commands}}
{{end}}\
{{else if .App.Commands}}\
Commands (use --help-full to list all commands):

{{template "FormatCommandList" .App.Commands}}
{{end}}\
`

func levelFilter(level string, writer io.Writer) zerolog.LevelWriter {
	switch level {
	case "debug":
		return levelWriter{zerolog.DebugLevel, writer}
	case "info":
		return levelWriter{zerolog.InfoLevel, writer}
	case "warning":
		return levelWriter{zerolog.WarnLevel, writer}
	case "error":
		return levelWriter{zerolog.ErrorLevel, writer}
	default:
		return levelWriter{zerolog.Disabled, writer}
	}
}

type levelWriter struct {
	level  zerolog.Level
	writer io.Writer
}

func (l levelWriter) WriteLevel(level zerolog.Level, b []byte) (int, error) {
	if level >= l.level {
		return l.writer.Write(b)
	}

	return len(b), nil
}

func (l levelWriter) Write(b []byte) (int, error) {
	return l.writer.Write(b)
}

type onDemandLogWriter struct {
	logDir          string
	logFileBaseName string
	symlinkName     string

	f    *os.File
	once sync.Once
}

func (w *onDemandLogWriter) Write(b []byte) (int, error) {
	var err error

	w.once.Do(func() {
		lf := filepath.Join(w.logDir, w.logFileBaseName)
		w.f, err = os.Create(lf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "unable to open log file: %v\n", err)
			return
		}

		if w.symlinkName != "" {
			symlink := filepath.Join(w.logDir, w.symlinkName)
			_ = os.Remove(symlink)                     // best-effort remove
			_ = os.Symlink(w.logFileBaseName, symlink) // best-effort symlink
		}
	})

	if w.f == nil {
		return len(b), nil
	}

	return w.f.Write(b)
}

func main() {
	app := cli.App()
	app.Version(repo.BuildVersion + " build: " + repo.BuildInfo)
	app.PreAction(initializeLogging)
	app.UsageTemplate(usageTemplate)
	kingpin.MustParse(app.Parse(os.Args[1:]))
}
