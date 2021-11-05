// Package logfile manages log files.
package logfile

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/fatih/color"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/ospath"
	"github.com/kopia/kopia/internal/zaplogutil"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/logging"
)

const logsDirMode = 0o700

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
	fileLogLocalTimezone  bool
	jsonLogFile           bool
	jsonLogConsole        bool
	forceColor            bool
	disableColor          bool
	consoleLogTimestamps  bool

	cliApp *cli.App
}

func (c *loggingFlags) setup(cliApp *cli.App, app *kingpin.Application) {
	app.Flag("log-file", "Override log file.").StringVar(&c.logFile)
	app.Flag("content-log-file", "Override content log file.").Hidden().StringVar(&c.contentLogFile)

	app.Flag("log-dir", "Directory where log files should be written.").Envar("KOPIA_LOG_DIR").Default(ospath.LogsDir()).StringVar(&c.logDir)
	app.Flag("log-dir-max-files", "Maximum number of log files to retain").Envar("KOPIA_LOG_DIR_MAX_FILES").Default("1000").Hidden().IntVar(&c.logDirMaxFiles)
	app.Flag("log-dir-max-age", "Maximum age of log files to retain").Envar("KOPIA_LOG_DIR_MAX_AGE").Hidden().Default("720h").DurationVar(&c.logDirMaxAge)
	app.Flag("content-log-dir-max-files", "Maximum number of content log files to retain").Envar("KOPIA_CONTENT_LOG_DIR_MAX_FILES").Default("5000").Hidden().IntVar(&c.contentLogDirMaxFiles)
	app.Flag("content-log-dir-max-age", "Maximum age of content log files to retain").Envar("KOPIA_CONTENT_LOG_DIR_MAX_AGE").Default("720h").Hidden().DurationVar(&c.contentLogDirMaxAge)
	app.Flag("log-level", "Console log level").Default("info").EnumVar(&c.logLevel, logLevels...)
	app.Flag("json-log-console", "JSON log file").Hidden().BoolVar(&c.jsonLogConsole)
	app.Flag("json-log-file", "JSON log file").Hidden().BoolVar(&c.jsonLogFile)
	app.Flag("file-log-level", "File log level").Default("debug").EnumVar(&c.fileLogLevel, logLevels...)
	app.Flag("file-log-local-tz", "When logging to a file, use local timezone").Hidden().Envar("KOPIA_FILE_LOG_LOCAL_TZ").BoolVar(&c.fileLogLocalTimezone)
	app.Flag("force-color", "Force color output").Hidden().Envar("KOPIA_FORCE_COLOR").BoolVar(&c.forceColor)
	app.Flag("disable-color", "Disable color output").Hidden().Envar("KOPIA_DISABLE_COLOR").BoolVar(&c.disableColor)
	app.Flag("console-timestamps", "Log timestamps to stderr.").Hidden().Default("false").Envar("KOPIA_CONSOLE_TIMESTAMPS").BoolVar(&c.consoleLogTimestamps)

	app.PreAction(c.initialize)
	c.cliApp = cliApp
}

// Attach attaches logging flags to the provided application.
func Attach(cliApp *cli.App, app *kingpin.Application) {
	lf := &loggingFlags{}
	lf.setup(cliApp, app)
}

var log = logging.Module("kopia")

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
	if c.fileLogLocalTimezone {
		now = now.Local()
	} else {
		now = now.UTC()
	}

	suffix := "unknown"
	if c := ctx.SelectedCommand; c != nil {
		suffix = strings.ReplaceAll(c.FullCommand(), " ", "-")
	}

	rootLogger := zap.New(zapcore.NewTee(
		c.setupConsoleCore(),
		c.setupLogFileCore(now, suffix),
	), zap.WithClock(zaplogutil.Clock))

	contentLogger := zap.New(c.setupContentLogFileBackend(now, suffix), zap.WithClock(zaplogutil.Clock)).Sugar()

	c.cliApp.SetLoggerFactory(func(module string) logging.Logger {
		if module == content.FormatLogModule {
			return contentLogger
		}

		return rootLogger.Named(module).Sugar()
	})

	if c.forceColor {
		color.NoColor = false
	}

	if c.disableColor {
		color.NoColor = true
	}

	return nil
}

func (c *loggingFlags) setupConsoleCore() zapcore.Core {
	ec := &zapcore.EncoderConfig{
		LevelKey:         "l",
		MessageKey:       "m",
		LineEnding:       zapcore.DefaultLineEnding,
		EncodeTime:       zapcore.RFC3339NanoTimeEncoder,
		EncodeDuration:   zapcore.StringDurationEncoder,
		EncodeCaller:     zapcore.ShortCallerEncoder,
		ConsoleSeparator: " ",
	}

	if c.consoleLogTimestamps {
		ec.TimeKey = "t"

		if c.jsonLogConsole {
			ec.EncodeTime = zapcore.RFC3339NanoTimeEncoder
		} else {
			// always log local timestamps to the console, not UTC
			ec.EncodeTime = zaplogutil.TimezoneAdjust(zapcore.TimeEncoderOfLayout("15:04:05.000"), true)
		}
	}

	if c.jsonLogConsole {
		ec.EncodeLevel = zapcore.CapitalLevelEncoder

		ec.NameKey = "n"
		ec.EncodeName = zapcore.FullNameEncoder
	} else {
		ec.EncodeLevel = func(l zapcore.Level, pae zapcore.PrimitiveArrayEncoder) {
			if l == zap.InfoLevel {
				// info log does not have a prefix.
				return
			}

			if c.disableColor {
				zapcore.CapitalLevelEncoder(l, pae)
			} else {
				zapcore.CapitalColorLevelEncoder(l, pae)
			}
		}
	}

	return zapcore.NewCore(
		c.jsonOrConsoleEncoder(ec, c.jsonLogConsole),
		zapcore.AddSync(c.cliApp.Stderr()),
		logLevelFromFlag(c.logLevel),
	)
}

func (c *loggingFlags) setupLogFileBasedLogger(now time.Time, subdir, suffix, logFileOverride string, maxFiles int, maxAge time.Duration) zapcore.WriteSyncer {
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

	return &onDemandFile{
		logDir:          logDir,
		logFileBaseName: logFileBaseName,
		symlinkName:     symlinkName,
	}
}

func (c *loggingFlags) setupLogFileCore(now time.Time, suffix string) zapcore.Core {
	return zapcore.NewCore(
		c.jsonOrConsoleEncoder(&zapcore.EncoderConfig{
			TimeKey:          "t",
			MessageKey:       "m",
			NameKey:          "n",
			LevelKey:         "l",
			EncodeName:       zapcore.FullNameEncoder,
			EncodeLevel:      zapcore.CapitalLevelEncoder,
			EncodeTime:       zaplogutil.TimezoneAdjust(zaplogutil.PreciseTimeEncoder, c.fileLogLocalTimezone),
			EncodeDuration:   zapcore.StringDurationEncoder,
			ConsoleSeparator: " ",
		}, c.jsonLogFile),
		c.setupLogFileBasedLogger(now, "cli-logs", suffix, c.logFile, c.logDirMaxFiles, c.logDirMaxAge),
		logLevelFromFlag(c.fileLogLevel),
	)
}

func (c *loggingFlags) jsonOrConsoleEncoder(ec *zapcore.EncoderConfig, isJSON bool) zapcore.Encoder {
	if isJSON {
		return zapcore.NewJSONEncoder(*ec)
	}

	return zapcore.NewConsoleEncoder(*ec)
}

func (c *loggingFlags) setupContentLogFileBackend(now time.Time, suffix string) zapcore.Core {
	return zapcore.NewCore(
		zapcore.NewConsoleEncoder(zapcore.EncoderConfig{
			TimeKey:          "t",
			MessageKey:       "m",
			NameKey:          "n",
			EncodeTime:       zaplogutil.TimezoneAdjust(zaplogutil.PreciseTimeEncoder, false),
			EncodeDuration:   zapcore.StringDurationEncoder,
			ConsoleSeparator: " ",
		}),
		c.setupLogFileBasedLogger(now, "content-logs", suffix, c.contentLogFile, c.contentLogDirMaxFiles, c.contentLogDirMaxAge),
		zap.DebugLevel)
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

	entries, err := os.ReadDir(dirname)
	if err != nil {
		log(ctx).Errorf("unable to read log directory: %v", err)
		return
	}

	fileInfos := make([]os.FileInfo, 0, len(entries))

	for _, e := range entries {
		info, err2 := e.Info()
		if os.IsNotExist(err2) {
			// we lost the race, the file was deleted since it was listed.
			continue
		}

		if err2 != nil {
			log(ctx).Errorf("unable to read file info: %v", err2)
			return
		}

		fileInfos = append(fileInfos, info)
	}

	sort.Slice(fileInfos, func(i, j int) bool {
		return fileInfos[i].ModTime().After(fileInfos[j].ModTime())
	})

	cnt := 0

	for _, fi := range fileInfos {
		if !strings.HasPrefix(fi.Name(), logFileNamePrefix) {
			continue
		}

		if !strings.HasSuffix(fi.Name(), logFileNameSuffix) {
			continue
		}

		cnt++

		if cnt > maxCount || fi.ModTime().Before(timeCutoff) {
			if err = os.Remove(filepath.Join(dirname, fi.Name())); err != nil && !os.IsNotExist(err) {
				log(ctx).Errorf("unable to remove log file: %v", err)
			}
		}
	}
}

func logLevelFromFlag(levelString string) zapcore.LevelEnabler {
	switch levelString {
	case "debug":
		return zap.DebugLevel
	case "info":
		return zap.InfoLevel
	case "warning":
		return zap.WarnLevel
	case "error":
		return zap.ErrorLevel
	default:
		return zap.FatalLevel
	}
}

type onDemandFile struct {
	logDir          string
	logFileBaseName string
	symlinkName     string

	f *os.File

	once sync.Once
}

func (w *onDemandFile) Sync() error {
	if w.f == nil {
		return nil
	}

	// nolint:wrapcheck
	return w.f.Sync()
}

func (w *onDemandFile) Write(b []byte) (int, error) {
	w.once.Do(func() {
		lf := filepath.Join(w.logDir, w.logFileBaseName)
		f, err := os.Create(lf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "unable to open log file: %v\n", err)
			return
		}

		w.f = f

		if w.symlinkName != "" {
			symlink := filepath.Join(w.logDir, w.symlinkName)
			_ = os.Remove(symlink)                     // best-effort remove
			_ = os.Symlink(w.logFileBaseName, symlink) // best-effort symlink
		}
	})

	if w.f == nil {
		return 0, nil
	}

	// nolint:wrapcheck
	return w.f.Write(b)
}
