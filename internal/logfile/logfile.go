// Package logfile manages log files.
package logfile

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/fatih/color"
	logging "github.com/op/go-logging"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/ospath"
	"github.com/kopia/kopia/repo/content"
	repologging "github.com/kopia/kopia/repo/logging"
)

var contentLogFormat = logging.MustStringFormatter(
	`%{time:2006-01-02 15:04:05.0000000} %{message}`)

var fileLogFormat = logging.MustStringFormatter(
	`%{time:2006-01-02 15:04:05.000} %{level:.1s} [%{shortfile}] %{message}`)

var logLevels = []string{"debug", "info", "warning", "error"}
var (
	logFile        = cli.App().Flag("log-file", "Override log file.").String()
	contentLogFile = cli.App().Flag("content-log-file", "Override content log file.").Hidden().String()

	logDir                = cli.App().Flag("log-dir", "Directory where log files should be written.").Envar("KOPIA_LOG_DIR").Default(ospath.LogsDir()).String()
	logDirMaxFiles        = cli.App().Flag("log-dir-max-files", "Maximum number of log files to retain").Envar("KOPIA_LOG_DIR_MAX_FILES").Default("1000").Hidden().Int()
	logDirMaxAge          = cli.App().Flag("log-dir-max-age", "Maximum age of log files to retain").Envar("KOPIA_LOG_DIR_MAX_AGE").Hidden().Default("720h").Duration()
	contentLogDirMaxFiles = cli.App().Flag("content-log-dir-max-files", "Maximum number of content log files to retain").Envar("KOPIA_CONTENT_LOG_DIR_MAX_FILES").Default("5000").Hidden().Int()
	contentLogDirMaxAge   = cli.App().Flag("content-log-dir-max-age", "Maximum age of content log files to retain").Envar("KOPIA_CONTENT_LOG_DIR_MAX_AGE").Default("720h").Hidden().Duration()
	logLevel              = cli.App().Flag("log-level", "Console log level").Default("info").Enum(logLevels...)
	fileLogLevel          = cli.App().Flag("file-log-level", "File log level").Default("debug").Enum(logLevels...)
	forceColor            = cli.App().Flag("force-color", "Force color output").Hidden().Envar("KOPIA_FORCE_COLOR").Bool()
	disableColor          = cli.App().Flag("disable-color", "Disable color output").Hidden().Envar("KOPIA_DISABLE_COLOR").Bool()
	consoleLogTimestamps  = cli.App().Flag("console-timestamps", "Log timestamps to stderr.").Hidden().Default("false").Envar("KOPIA_CONSOLE_TIMESTAMPS").Bool()
)

var log = repologging.GetContextLoggerFunc("kopia")

const (
	logFileNamePrefix = "kopia-"
	logFileNameSuffix = ".log"
)

// Initialize is invoked as part of command execution to create log file just before it's needed.
func Initialize(ctx *kingpin.ParseContext) error {
	if *logDir == "" {
		return nil
	}

	now := clock.Now()

	suffix := "unknown"
	if c := ctx.SelectedCommand; c != nil {
		suffix = strings.ReplaceAll(c.FullCommand(), " ", "-")
	}

	// activate backends
	logging.SetBackend(
		setupConsoleBackend(),
		setupLogFileBackend(now, suffix),
		setupContentLogFileBackend(now, suffix),
	)

	if *forceColor {
		color.NoColor = false
	}

	if *disableColor {
		color.NoColor = true
	}

	return nil
}

func setupConsoleBackend() logging.Backend {
	var (
		prefix         = "%{color}"
		suffix         = "%{message}%{color:reset}"
		maybeTimestamp = "%{time:15:04:05.000} "
	)

	if !*consoleLogTimestamps {
		maybeTimestamp = ""
	}

	l := logging.AddModuleLevel(logging.NewBackendFormatter(
		logging.NewLogBackend(os.Stderr, "", 0),
		logging.MustStringFormatter(prefix+maybeTimestamp+suffix)))

	// do not output content logs to the console
	l.SetLevel(logging.CRITICAL, content.FormatLogModule)

	// log everything else at a level specified using --log-level
	l.SetLevel(logLevelFromFlag(*logLevel), "")

	return l
}

func setupLogFileBasedLogger(now time.Time, subdir, suffix, logFileOverride string, maxFiles int, maxAge time.Duration) logging.Backend {
	var logFileName, symlinkName string

	if logFileOverride != "" {
		var err error

		logFileName, err = filepath.Abs(logFileOverride)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Unable to resolve logs path", err)
		}
	}

	if logFileName == "" {
		logBaseName := fmt.Sprintf("%v%v-%v-%v%v", logFileNamePrefix, now.Format("20060102-150405"), os.Getpid(), suffix, logFileNameSuffix)
		logFileName = filepath.Join(*logDir, subdir, logBaseName)
		symlinkName = "latest.log"
	}

	logDir := filepath.Dir(logFileName)
	logFileBaseName := filepath.Base(logFileName)

	if err := os.MkdirAll(logDir, 0o700); err != nil {
		fmt.Fprintln(os.Stderr, "Unable to create logs directory:", err)
	}

	// do not scrub directory if custom log file has been provided.
	if logFileOverride == "" && shouldSweepLog(maxFiles, maxAge) {
		go sweepLogDir(context.TODO(), logDir, maxFiles, maxAge)
	}

	return &onDemandBackend{
		logDir:          logDir,
		logFileBaseName: logFileBaseName,
		symlinkName:     symlinkName,
	}
}

func setupLogFileBackend(now time.Time, suffix string) logging.Backend {
	l := logging.AddModuleLevel(
		logging.NewBackendFormatter(
			setupLogFileBasedLogger(now, "cli-logs", suffix, *logFile, *logDirMaxFiles, *logDirMaxAge),
			fileLogFormat))

	// do not output content logs to the regular log file
	l.SetLevel(logging.CRITICAL, content.FormatLogModule)

	// log everything else at a level specified using --file-level
	l.SetLevel(logLevelFromFlag(*fileLogLevel), "")

	return l
}

func setupContentLogFileBackend(now time.Time, suffix string) logging.Backend {
	l := logging.AddModuleLevel(
		logging.NewBackendFormatter(
			setupLogFileBasedLogger(now, "content-logs", suffix, *contentLogFile, *contentLogDirMaxFiles, *contentLogDirMaxAge),
			contentLogFormat))

	// only log content entries
	l.SetLevel(logging.DEBUG, content.FormatLogModule)

	// do not log anything else
	l.SetLevel(logging.CRITICAL, "")

	return l
}

func shouldSweepLog(maxFiles int, maxAge time.Duration) bool {
	return maxFiles > 0 || maxAge > 0
}

func sweepLogDir(ctx context.Context, dirname string, maxCount int, maxAge time.Duration) {
	var timeCutoff time.Time
	if maxAge > 0 {
		timeCutoff = clock.Now().Add(-maxAge)
	}

	if maxCount == 0 {
		maxCount = math.MaxInt32
	}

	entries, err := ioutil.ReadDir(dirname)
	if err != nil {
		log(ctx).Warningf("unable to read log directory: %v", err)
		return
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].ModTime().After(entries[j].ModTime())
	})

	cnt := 0

	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), logFileNamePrefix) {
			continue
		}

		if !strings.HasSuffix(e.Name(), logFileNameSuffix) {
			continue
		}

		cnt++

		if cnt > maxCount || e.ModTime().Before(timeCutoff) {
			if err = os.Remove(filepath.Join(dirname, e.Name())); err != nil && !os.IsNotExist(err) {
				log(ctx).Warningf("unable to remove log file: %v", err)
			}
		}
	}
}

func logLevelFromFlag(levelString string) logging.Level {
	switch levelString {
	case "debug":
		return logging.DEBUG
	case "info":
		return logging.INFO
	case "warning":
		return logging.WARNING
	case "error":
		return logging.ERROR
	default:
		return logging.CRITICAL
	}
}

type onDemandBackend struct {
	logDir          string
	logFileBaseName string
	symlinkName     string

	backend logging.Backend
	once    sync.Once
}

func (w *onDemandBackend) Log(level logging.Level, depth int, rec *logging.Record) error {
	w.once.Do(func() {
		lf := filepath.Join(w.logDir, w.logFileBaseName)
		f, err := os.Create(lf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "unable to open log file: %v\n", err)
			return
		}

		w.backend = logging.NewLogBackend(f, "", 0)

		if w.symlinkName != "" {
			symlink := filepath.Join(w.logDir, w.symlinkName)
			_ = os.Remove(symlink)                     // best-effort remove
			_ = os.Symlink(w.logFileBaseName, symlink) // best-effort symlink
		}
	})

	if w.backend == nil {
		return errors.New("no backend")
	}

	return w.backend.Log(level, depth+1, rec)
}
