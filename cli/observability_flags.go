package cli

import (
	"context"
	"net/http"
	"net/http/pprof"
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

	"github.com/kopia/kopia/repo"
)

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
}

func (c *observabilityFlags) startMetrics(ctx context.Context) error {
	if c.metricsListenAddr != "" {
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

	if c.metricsPushAddr != "" {
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
	}

	se, err := c.getSpanExporter()
	if err != nil {
		return err
	}

	r := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String("kopia"),
		semconv.ServiceVersionKey.String(repo.BuildVersion),
	)

	if se != nil {
		tp := trace.NewTracerProvider(
			trace.WithBatcher(se),
			trace.WithResource(r),
		)

		otel.SetTracerProvider(tp)

		c.traceProvider = tp
	}

	return nil
}

func (c *observabilityFlags) getSpanExporter() (trace.SpanExporter, error) {
	if c.enableJaeger {
		// Create the Jaeger exporter
		exp, err := jaeger.New(jaeger.WithCollectorEndpoint())
		if err != nil {
			return nil, errors.Wrap(err, "unable to create Jaeger exporter")
		}

		return exp, nil
	}

	return nil, nil
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
