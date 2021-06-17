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

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/ospath"
	"github.com/kopia/kopia/repo/content"
	repologging "github.com/kopia/kopia/repo/logging"
)

const logsDirMode = 0o700

var contentLogFormat = logging.MustStringFormatter(
	`%{time:2006-01-02 15:04:05.0000000} %{message}`)

var fileLogFormat = logging.MustStringFormatter(
	`%{time:2006-01-02 15:04:05.000} %{level:.1s} [%{shortfile}] %{message}`)

// warning is for backwards compatibility, same as error.
var logLevels = []string{"debug", "info", "warning", "error"}

type loggingFlags struct {
	logFile               string
	contentLogFile        string
	logDir                string
	logDirMaxFiles        int
	logDirMaxAge          time.Duration
	contentLogDirMaxFiles int
	contentLogDirMaxAge   time.Duration
	logLevel              string
	fileLogLevel          string
	forceColor            bool
	disableColor          bool
	consoleLogTimestamps  bool
}

func (c *loggingFlags) setup(app *kingpin.Application) {
	app.Flag("log-file", "Override log file.").StringVar(&c.logFile)
	app.Flag("content-log-file", "Override content log file.").Hidden().StringVar(&c.contentLogFile)

	app.Flag("log-dir", "Directory where log files should be written.").Envar("KOPIA_LOG_DIR").Default(ospath.LogsDir()).StringVar(&c.logDir)
	app.Flag("log-dir-max-files", "Maximum number of log files to retain").Envar("KOPIA_LOG_DIR_MAX_FILES").Default("1000").Hidden().IntVar(&c.logDirMaxFiles)
	app.Flag("log-dir-max-age", "Maximum age of log files to retain").Envar("KOPIA_LOG_DIR_MAX_AGE").Hidden().Default("720h").DurationVar(&c.logDirMaxAge)
	app.Flag("content-log-dir-max-files", "Maximum number of content log files to retain").Envar("KOPIA_CONTENT_LOG_DIR_MAX_FILES").Default("5000").Hidden().IntVar(&c.contentLogDirMaxFiles)
	app.Flag("content-log-dir-max-age", "Maximum age of content log files to retain").Envar("KOPIA_CONTENT_LOG_DIR_MAX_AGE").Default("720h").Hidden().DurationVar(&c.contentLogDirMaxAge)
	app.Flag("log-level", "Console log level").Default("info").EnumVar(&c.logLevel, logLevels...)
	app.Flag("file-log-level", "File log level").Default("debug").EnumVar(&c.fileLogLevel, logLevels...)
	app.Flag("force-color", "Force color output").Hidden().Envar("KOPIA_FORCE_COLOR").BoolVar(&c.forceColor)
	app.Flag("disable-color", "Disable color output").Hidden().Envar("KOPIA_DISABLE_COLOR").BoolVar(&c.disableColor)
	app.Flag("console-timestamps", "Log timestamps to stderr.").Hidden().Default("false").Envar("KOPIA_CONSOLE_TIMESTAMPS").BoolVar(&c.consoleLogTimestamps)

	app.PreAction(c.initialize)
}

// Attach attaches logging flags to the provided application.
func Attach(app *kingpin.Application) {
	lf := &loggingFlags{}
	lf.setup(app)
}

var log = repologging.GetContextLoggerFunc("kopia")

const (
	logFileNamePrefix = "kopia-"
	logFileNameSuffix = ".log"
)

// initialize is invoked as part of command execution to create log file just before it's needed.
func (c *loggingFlags) initialize(ctx *kingpin.ParseContext) error {
	if c.logDir == "" {
		return nil
	}

	now := clock.Now()

	suffix := "unknown"
	if c := ctx.SelectedCommand; c != nil {
		suffix = strings.ReplaceAll(c.FullCommand(), " ", "-")
	}

	// activate backends
	logging.SetBackend(
		multiLogger{
			c.setupConsoleBackend(),
			c.setupLogFileBackend(now, suffix),
			c.setupContentLogFileBackend(now, suffix),
		},
	)

	if c.forceColor {
		color.NoColor = false
	}

	if c.disableColor {
		color.NoColor = true
	}

	return nil
}

type multiLogger []logging.Backend

func (m multiLogger) Log(l logging.Level, calldepth int, rec *logging.Record) error {
	// use clock.Now() which can be overridden in e2e tests.
	rec.Time = clock.Now()

	for _, child := range m {
		// Shallow copy of the record for the formatted cache on Record and get the
		// record formatter from the backend.
		r2 := *rec
		child.Log(l, calldepth, &r2) //nolint:errcheck
	}

	return nil
}

func (c *loggingFlags) setupConsoleBackend() logging.Backend {
	var (
		prefix         = "%{color}"
		suffix         = "%{message}%{color:reset}"
		maybeTimestamp = "%{time:15:04:05.000} "
	)

	if c.disableColor {
		prefix = ""
		suffix = "%{message}"
	}

	if !c.consoleLogTimestamps {
		maybeTimestamp = ""
	}

	l := logging.AddModuleLevel(logging.NewBackendFormatter(
		logging.NewLogBackend(os.Stderr, "", 0),
		logging.MustStringFormatter(prefix+maybeTimestamp+suffix)))

	// do not output content logs to the console
	l.SetLevel(logging.CRITICAL, content.FormatLogModule)

	// log everything else at a level specified using --log-level
	l.SetLevel(logLevelFromFlag(c.logLevel), "")

	return l
}

func (c *loggingFlags) setupLogFileBasedLogger(now time.Time, subdir, suffix, logFileOverride string, maxFiles int, maxAge time.Duration) logging.Backend {
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
		logFileName = filepath.Join(c.logDir, subdir, logBaseName)
		symlinkName = "latest.log"
	}

	logDir := filepath.Dir(logFileName)
	logFileBaseName := filepath.Base(logFileName)

	if err := os.MkdirAll(logDir, logsDirMode); err != nil {
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

func (c *loggingFlags) setupLogFileBackend(now time.Time, suffix string) logging.Backend {
	l := logging.AddModuleLevel(
		logging.NewBackendFormatter(
			c.setupLogFileBasedLogger(now, "cli-logs", suffix, c.logFile, c.logDirMaxFiles, c.logDirMaxAge),
			fileLogFormat))

	// do not output content logs to the regular log file
	l.SetLevel(logging.CRITICAL, content.FormatLogModule)

	// log everything else at a level specified using --file-level
	l.SetLevel(logLevelFromFlag(c.fileLogLevel), "")

	return l
}

func (c *loggingFlags) setupContentLogFileBackend(now time.Time, suffix string) logging.Backend {
	l := logging.AddModuleLevel(
		logging.NewBackendFormatter(
			c.setupLogFileBasedLogger(now, "content-logs", suffix, c.contentLogFile, c.contentLogDirMaxFiles, c.contentLogDirMaxAge),
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
		log(ctx).Errorf("unable to read log directory: %v", err)
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
				log(ctx).Errorf("unable to remove log file: %v", err)
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

	// nolint:wrapcheck
	return w.backend.Log(level, depth+1, rec)
}
