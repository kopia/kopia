package cli

import (
	"context"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	rpprof "runtime/pprof"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/prometheus/common/expfmt"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo"
)

// DirMode is the directory mode for output directories.
const DirMode = 0o700

//nolint:gochecknoglobals
var metricsPushFormats = map[string]expfmt.Format{
	"text":               expfmt.FmtText,
	"proto-text":         expfmt.FmtProtoText,
	"proto-delim":        expfmt.FmtProtoDelim,
	"proto-compact":      expfmt.FmtProtoCompact,
	"open-metrics":       expfmt.FmtOpenMetrics_1_0_0,
	"open-metrics-0.0.1": expfmt.FmtOpenMetrics_0_0_1,
}

type observabilityFlags struct {
	enablePProf         bool
	metricsListenAddr   string
	metricsPushAddr     string
	metricsJob          string
	metricsPushInterval time.Duration
	metricsGroupings    []string
	metricsPushUsername string
	metricsPushPassword string
	metricsPushFormat   string
	metricsOutputDir    string
	outputFilePrefix    string
	pprofDir            string

	enableJaeger bool

	stopPusher chan struct{}
	pusherWG   sync.WaitGroup

	traceProvider *trace.TracerProvider
}

func (c *observabilityFlags) setup(svc appServices, app *kingpin.Application) {
	app.Flag("metrics-listen-addr", "Expose Prometheus metrics on a given host:port").Hidden().StringVar(&c.metricsListenAddr)
	app.Flag("enable-pprof", "Expose pprof handlers").Hidden().BoolVar(&c.enablePProf)

	// push gateway parameters
	app.Flag("metrics-push-addr", "Address of push gateway").Envar(svc.EnvName("KOPIA_METRICS_PUSH_ADDR")).Hidden().StringVar(&c.metricsPushAddr)
	app.Flag("metrics-push-interval", "Frequency of metrics push").Envar(svc.EnvName("KOPIA_METRICS_PUSH_INTERVAL")).Hidden().Default("5s").DurationVar(&c.metricsPushInterval)
	app.Flag("metrics-push-job", "Job ID for to push gateway").Envar(svc.EnvName("KOPIA_METRICS_JOB")).Hidden().Default("kopia").StringVar(&c.metricsJob)
	app.Flag("metrics-push-grouping", "Grouping for push gateway").Envar(svc.EnvName("KOPIA_METRICS_PUSH_GROUPING")).Hidden().StringsVar(&c.metricsGroupings)
	app.Flag("metrics-push-username", "Username for push gateway").Envar(svc.EnvName("KOPIA_METRICS_PUSH_USERNAME")).Hidden().StringVar(&c.metricsPushUsername)
	app.Flag("metrics-push-password", "Password for push gateway").Envar(svc.EnvName("KOPIA_METRICS_PUSH_PASSWORD")).Hidden().StringVar(&c.metricsPushPassword)

	app.Flag("enable-jaeger-collector", "Emit OpenTelemetry traces to Jaeger collector").Hidden().Envar(svc.EnvName("KOPIA_ENABLE_JAEGER_COLLECTOR")).BoolVar(&c.enableJaeger)

	var formats []string

	for k := range metricsPushFormats {
		formats = append(formats, k)
	}

	sort.Strings(formats)

	app.Flag("metrics-push-format", "Format to use for push gateway").Envar(svc.EnvName("KOPIA_METRICS_FORMAT")).Hidden().EnumVar(&c.metricsPushFormat, formats...)

	app.Flag("metrics-directory", "Directory where the metrics should be saved when kopia exits. A file per process execution will be created in this directory").Hidden().StringVar(&c.metricsOutputDir)

	//nolint:lll
	app.Flag("pprof-directory", "Directory to dump pprof data at the end of the process execution. The profiling settings can be modified using the default GODEBUG environment variable mechanism (see https://pkg.go.dev/runtime@master#hdr-Environment_Variables for additional information.)").Hidden().StringVar(&c.pprofDir)

	app.PreAction(c.initialize)
}

func (c *observabilityFlags) initialize(ctx *kingpin.ParseContext) error {
	if c.metricsOutputDir == "" && c.pprofDir == "" {
		return nil
	}

	// write to a separate file per command and process execution to avoid
	// conflicts with previously created files
	command := "unknown"
	if cmd := ctx.SelectedCommand; cmd != nil {
		command = strings.ReplaceAll(cmd.FullCommand(), " ", "-")
	}

	c.outputFilePrefix = clock.Now().Format("20060102-150405-") + command

	return nil
}

func (c *observabilityFlags) startMetrics(ctx context.Context) error {
	c.maybeStartListener(ctx)

	if err := c.maybeStartMetricsPusher(ctx); err != nil {
		return err
	}

	if c.metricsOutputDir != "" {
		c.metricsOutputDir = filepath.Clean(c.metricsOutputDir)

		// ensure the metrics output dir can be created
		if err := os.MkdirAll(c.metricsOutputDir, DirMode); err != nil {
			return errors.Wrapf(err, "could not create metrics output directory: %s", c.metricsOutputDir)
		}
	}

	if err := c.maybeStartPprofDumper(ctx); err != nil {
		return err
	}

	return c.maybeStartTraceExporter()
}

// Starts observability listener when a listener address is specified.
func (c *observabilityFlags) maybeStartListener(ctx context.Context) {
	if c.metricsListenAddr == "" {
		return
	}

	m := mux.NewRouter()
	initPrometheus(m)

	if c.enablePProf {
		m.HandleFunc("/debug/pprof/", pprof.Index)
		m.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		m.HandleFunc("/debug/pprof/profile", pprof.Profile)
		m.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		m.HandleFunc("/debug/pprof/trace", pprof.Trace)
		m.HandleFunc("/debug/pprof/{cmd}", pprof.Index) // special handling for Gorilla mux, see https://stackoverflow.com/questions/30560859/cant-use-go-tool-pprof-with-an-existing-server/71032595#71032595
	}

	log(ctx).Infof("starting prometheus metrics on %v", c.metricsListenAddr)

	go http.ListenAndServe(c.metricsListenAddr, m) //nolint:errcheck,gosec
}

func (c *observabilityFlags) maybeStartMetricsPusher(ctx context.Context) error {
	if c.metricsPushAddr == "" {
		return nil
	}

	c.stopPusher = make(chan struct{})
	c.pusherWG.Add(1)

	pusher := push.New(c.metricsPushAddr, c.metricsJob)

	pusher.Gatherer(prometheus.DefaultGatherer)

	for _, g := range c.metricsGroupings {
		const nParts = 2

		parts := strings.SplitN(g, ":", nParts)
		if len(parts) != nParts {
			return errors.Errorf("grouping must be name:value")
		}

		name := parts[0]
		val := parts[1]

		pusher.Grouping(name, val)
	}

	if c.metricsPushUsername != "" {
		pusher.BasicAuth(c.metricsPushUsername, c.metricsPushPassword)
	}

	if c.metricsPushFormat != "" {
		pusher.Format(metricsPushFormats[c.metricsPushFormat])
	}

	log(ctx).Infof("starting prometheus pusher on %v every %v", c.metricsPushAddr, c.metricsPushInterval)
	c.pushOnce(ctx, "initial", pusher)

	go c.pushPeriodically(ctx, pusher)

	return nil
}

func (c *observabilityFlags) maybeStartTraceExporter() error {
	if !c.enableJaeger {
		return nil
	}

	// Create the Jaeger exporter
	se, err := jaeger.New(jaeger.WithCollectorEndpoint())
	if err != nil {
		return errors.Wrap(err, "unable to create Jaeger exporter")
	}

	if se != nil {
		r := resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("kopia"),
			semconv.ServiceVersionKey.String(repo.BuildVersion),
		)

		tp := trace.NewTracerProvider(
			trace.WithBatcher(se),
			trace.WithResource(r),
		)

		otel.SetTracerProvider(tp)

		c.traceProvider = tp
	}

	return nil
}

func (c *observabilityFlags) maybeStartPprofDumper(ctx context.Context) error {
	if c.pprofDir == "" {
		return nil
	}

	// ensure upfront that the pprof output dir can be created
	c.pprofDir = filepath.Clean(c.pprofDir)
	if err := os.MkdirAll(c.pprofDir, DirMode); err != nil {
		return errors.Wrapf(err, "could not create pprof output directory: %s", c.pprofDir)
	}

	return nil
}

func (c *observabilityFlags) stopMetrics(ctx context.Context) {
	if c.stopPusher != nil {
		close(c.stopPusher)

		c.pusherWG.Wait()
	}

	if c.traceProvider != nil {
		if err := c.traceProvider.Shutdown(ctx); err != nil {
			log(ctx).Warnf("unable to shutdown trace provicer: %v", err)
		}
	}

	if c.metricsOutputDir != "" {
		filename := filepath.Join(c.metricsOutputDir, c.outputFilePrefix+".prom")

		if err := prometheus.WriteToTextfile(filename, prometheus.DefaultGatherer); err != nil {
			log(ctx).Warnf("unable to write metrics file '%s': %v", filename, err)
		}
	}

	if c.pprofDir != "" {
		runtime.GC() // get up-to-date statistics

		for _, p := range rpprof.Profiles() {
			func() {
				fname := filepath.Clean(filepath.Join(c.pprofDir, p.Name()+".pprof"))

				f, err := os.Create(fname)
				if err != nil {
					log(ctx).Warnf("unable to create profile output file '%s': %v", fname, err)
				}

				defer func() {
					if err := f.Close(); err != nil {
						log(ctx).Warnf("unable to close profile output file '%s': %v", fname, err)
					}
				}()

				if err := p.WriteTo(f, 0); err != nil {
					log(ctx).Warnf("unable to write profile to file '%s': %v", fname, err)
				}
			}()
		}
	}
}

func (c *observabilityFlags) pushPeriodically(ctx context.Context, p *push.Pusher) {
	defer c.pusherWG.Done()

	ticker := time.NewTicker(c.metricsPushInterval)

	for {
		select {
		case <-ticker.C:
			c.pushOnce(ctx, "periodic", p)

		case <-c.stopPusher:
			ticker.Stop()
			c.pushOnce(ctx, "final", p)

			return
		}
	}
}

func (c *observabilityFlags) pushOnce(ctx context.Context, kind string, p *push.Pusher) {
	log(ctx).Debugw("pushing prometheus metrics", "kind", kind)

	if err := p.Push(); err != nil {
		log(ctx).Debugw("error pushing prometheus metrics", "kind", kind, "err", err)
	}
}
