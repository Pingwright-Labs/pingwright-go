// Package pingwright sends a Go service's logs, metrics and errors to
// Pingwright (https://pingwright.app).
//
// It is a thin layer over OpenTelemetry. [Init] configures an OTLP/HTTP log
// exporter and metric exporter that point at https://ingest.pingwright.app,
// and installs a [log/slog] logger that writes locally and to Pingwright at
// the same time. Errors are log records that carry the OpenTelemetry
// exception attributes (exception.type, exception.message,
// exception.stacktrace); Pingwright groups them into issues.
//
//	shutdown, err := pingwright.Init(ctx, pingwright.Options{
//		Key:         os.Getenv("PINGWRIGHT_KEY"),
//		Service:     "billing-api",
//		Environment: "production",
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer shutdown(context.Background())
//
//	slog.Info("started") // goes to stderr and to Pingwright
//
// Without a key (neither [Options.Key] nor PINGWRIGHT_KEY), Init prints one
// warning and runs in local mode: logging, [CaptureError], [Recover] and
// [Middleware] keep working, and nothing is sent. This keeps development
// setups and tests free of configuration.
//
// Pingwright does not store traces, so none are exported. Trace and span ids
// from the context (set by your own tracer, or by [Middleware] from an
// incoming traceparent header) are attached to every log record.
package pingwright

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

// Version is the version of this library, reported as the instrumentation
// scope version.
const Version = "0.1.0"

// DefaultEndpoint is where telemetry goes unless [Options.Endpoint] or
// PINGWRIGHT_ENDPOINT says otherwise.
const DefaultEndpoint = "https://ingest.pingwright.app"

// LevelFatal is the level [Recover] uses for a panic that crashes the
// process. It is exported as OpenTelemetry severity FATAL (21).
const LevelFatal = slog.LevelError + 4

const (
	scopeName      = "github.com/Pingwright-Labs/pingwright-go"
	exportTimeout  = 10 * time.Second
	metricInterval = 30 * time.Second
	panicFlush     = 5 * time.Second
)

// ErrAlreadyInitialized is returned by a second [Init] before the first one's
// shutdown function has run.
var ErrAlreadyInitialized = errors.New("pingwright: already initialized")

var keyPattern = regexp.MustCompile(`^pwi_[0-9a-f]{32}$`)

// Options configures [Init]. The zero value is valid: it reads the key from
// PINGWRIGHT_KEY and sends logs at Info and above, errors and metrics.
type Options struct {
	// Key is the ingest key, "pwi_" followed by 32 hex characters.
	// Falls back to PINGWRIGHT_KEY. With neither, Init runs in local mode.
	Key string

	// Service is service.name. Falls back to OTEL_SERVICE_NAME, then to the
	// executable's name.
	Service string

	// Environment is deployment.environment.name, for example "production".
	Environment string

	// Version is service.version. Optional.
	Version string

	// Endpoint is the ingest base URL. Falls back to PINGWRIGHT_ENDPOINT,
	// then to DefaultEndpoint. "/v1/logs" and "/v1/metrics" are appended.
	Endpoint string

	// DisableLogs stops sending log records. Errors are still sent unless
	// DisableErrors is also set.
	DisableLogs bool

	// DisableErrors stops CaptureError, Recover and Middleware from sending
	// exception records. They still log locally (and, with logs on, send a
	// plain error log without exception attributes, so no issue is made).
	DisableErrors bool

	// DisableMetrics stops all metrics, including runtime and HTTP metrics.
	DisableMetrics bool

	// DisableRuntimeMetrics stops the Go runtime metrics (memory, GC,
	// goroutines) that are otherwise collected every 30 seconds.
	DisableRuntimeMetrics bool

	// LogLevel is the minimum level written locally and sent. The default
	// (the zero value) is slog.LevelInfo. Errors are always sent.
	LogLevel slog.Level

	// Output receives local log output. Defaults to os.Stderr. Use
	// io.Discard to log to Pingwright only.
	Output io.Writer

	// TextOutput writes local output with slog's text format instead of JSON.
	TextOutput bool

	// KeepDefaultLogger leaves slog.Default alone. By default Init calls
	// slog.SetDefault(pingwright.Logger()), so slog.Info and the log package
	// go to Pingwright too.
	KeepDefaultLogger bool

	// ResourceAttributes are added to every log record and metric, for
	// example {"k8s.cluster.name": "eu-1"}. Service, Environment and Version
	// win over the same keys here. OTEL_RESOURCE_ATTRIBUTES is also read.
	ResourceAttributes map[string]string
}

type client struct {
	handler    slog.Handler // Logger(): local + logs exporter
	errHandler slog.Handler // CaptureError/Recover: local + logs exporter, no level filter
	exceptions bool         // add exception.* attributes
	lp         *sdklog.LoggerProvider
	mp         *sdkmetric.MeterProvider
}

var (
	mu      sync.Mutex // serializes Init and shutdown
	current atomic.Pointer[client]
)

func noopShutdown(context.Context) error { return nil }

// Init configures logs, metrics and errors for the process and returns a
// function that flushes and stops them. Call it once at startup and defer
// the returned function; it is safe to call more than once.
//
// A second Init before shutdown returns ErrAlreadyInitialized. After
// shutdown, Init may be called again. The returned shutdown function is
// never nil, even when err is not.
//
// Init sets the global OpenTelemetry MeterProvider, so libraries that use
// otel.Meter report through Pingwright too. It does not touch the global
// TracerProvider or propagator.
func Init(ctx context.Context, opts Options) (shutdown func(context.Context) error, err error) {
	mu.Lock()
	defer mu.Unlock()
	if current.Load() != nil {
		return noopShutdown, ErrAlreadyInitialized
	}

	out := opts.Output
	if out == nil {
		out = os.Stderr
	}
	local := newLocalHandler(out, opts.LogLevel, opts.TextOutput)
	c := &client{handler: local, errHandler: local, exceptions: true}

	key := firstNonEmpty(opts.Key, os.Getenv("PINGWRIGHT_KEY"))
	if key == "" {
		c.install(opts)
		slog.New(local).Warn("pingwright: no key (Options.Key or PINGWRIGHT_KEY); logs, metrics and errors stay local")
		return c.shutdown, nil
	}
	if !keyPattern.MatchString(key) {
		return noopShutdown, errors.New(`pingwright: the key must be "pwi_" followed by 32 lowercase hex characters`)
	}

	endpoint := strings.TrimRight(firstNonEmpty(opts.Endpoint, os.Getenv("PINGWRIGHT_ENDPOINT"), DefaultEndpoint), "/")
	if u, err := url.Parse(endpoint); err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return noopShutdown, fmt.Errorf("pingwright: invalid endpoint %q: want an http(s) URL", endpoint)
	}

	res, err := newResource(ctx, opts)
	if err != nil {
		return noopShutdown, err
	}
	headers := map[string]string{"Authorization": "Bearer " + key}

	if !opts.DisableLogs || !opts.DisableErrors {
		exp, err := otlploghttp.New(ctx,
			otlploghttp.WithEndpointURL(endpoint+"/v1/logs"),
			otlploghttp.WithHeaders(headers),
			otlploghttp.WithCompression(otlploghttp.GzipCompression),
			otlploghttp.WithTimeout(exportTimeout),
		)
		if err != nil {
			return noopShutdown, fmt.Errorf("pingwright: creating log exporter: %w", err)
		}
		c.lp = sdklog.NewLoggerProvider(
			sdklog.WithResource(res),
			sdklog.WithProcessor(sdklog.NewBatchProcessor(exp)),
		)
		otlp := otelslog.NewHandler(scopeName, otelslog.WithLoggerProvider(c.lp), otelslog.WithVersion(Version))
		if !opts.DisableLogs {
			c.handler = slog.NewMultiHandler(local, minLevel{level: opts.LogLevel, Handler: otlp})
		}
		if !opts.DisableErrors {
			c.errHandler = slog.NewMultiHandler(local, otlp)
		} else {
			c.errHandler = c.handler
			c.exceptions = false
		}
	}

	if !opts.DisableMetrics {
		exp, err := otlpmetrichttp.New(ctx,
			otlpmetrichttp.WithEndpointURL(endpoint+"/v1/metrics"),
			otlpmetrichttp.WithHeaders(headers),
			otlpmetrichttp.WithCompression(otlpmetrichttp.GzipCompression),
			otlpmetrichttp.WithTimeout(exportTimeout),
		)
		if err != nil {
			_ = c.stop(ctx)
			return noopShutdown, fmt.Errorf("pingwright: creating metric exporter: %w", err)
		}
		c.mp = sdkmetric.NewMeterProvider(
			sdkmetric.WithResource(res),
			sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp,
				sdkmetric.WithInterval(metricInterval),
				sdkmetric.WithTimeout(exportTimeout),
			)),
		)
		if !opts.DisableRuntimeMetrics {
			if err := runtime.Start(runtime.WithMeterProvider(c.mp)); err != nil {
				_ = c.stop(ctx)
				return noopShutdown, fmt.Errorf("pingwright: starting runtime metrics: %w", err)
			}
		}
		otel.SetMeterProvider(c.mp)
	}

	c.install(opts)
	return c.shutdown, nil
}

func (c *client) install(opts Options) {
	current.Store(c)
	if !opts.KeepDefaultLogger {
		slog.SetDefault(root)
	}
}

// shutdown flushes pending logs and metrics and stops the exporters. Later
// calls return nil. After it, Logger writes to stderr only.
func (c *client) shutdown(ctx context.Context) error {
	mu.Lock()
	defer mu.Unlock()
	if !current.CompareAndSwap(c, nil) {
		return nil
	}
	return c.stop(ctx)
}

func (c *client) stop(ctx context.Context) error {
	var errs []error
	if c.lp != nil {
		errs = append(errs, c.lp.Shutdown(ctx))
	}
	if c.mp != nil {
		errs = append(errs, c.mp.Shutdown(ctx))
	}
	return errors.Join(errs...)
}

func (c *client) flush(ctx context.Context) {
	if c == nil {
		return
	}
	if c.lp != nil {
		_ = c.lp.ForceFlush(ctx)
	}
	if c.mp != nil {
		_ = c.mp.ForceFlush(ctx)
	}
}

// Meter returns a meter from Pingwright's MeterProvider. Before Init, or with
// metrics off, it returns the global otel meter, which binds to Pingwright's
// provider once Init runs.
func Meter(name string, opts ...metric.MeterOption) metric.Meter {
	if c := current.Load(); c != nil && c.mp != nil {
		return c.mp.Meter(name, opts...)
	}
	return otel.GetMeterProvider().Meter(name, opts...)
}

func newResource(ctx context.Context, opts Options) (*resource.Resource, error) {
	var kv []attribute.KeyValue
	for k, v := range opts.ResourceAttributes {
		kv = append(kv, attribute.String(k, v))
	}
	if opts.Service != "" {
		kv = append(kv, attribute.String("service.name", opts.Service))
	}
	if opts.Environment != "" {
		kv = append(kv, attribute.String("deployment.environment.name", opts.Environment))
	}
	if opts.Version != "" {
		kv = append(kv, attribute.String("service.version", opts.Version))
	}
	res, err := resource.New(ctx,
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithFromEnv(), // OTEL_SERVICE_NAME, OTEL_RESOURCE_ATTRIBUTES
		resource.WithAttributes(kv...),
	)
	if err != nil && !errors.Is(err, resource.ErrPartialResource) {
		return nil, fmt.Errorf("pingwright: building resource: %w", err)
	}
	if v, ok := res.Set().Value("service.name"); !ok || v.AsString() == "" {
		res, err = resource.Merge(res, resource.NewSchemaless(attribute.String("service.name", filepath.Base(os.Args[0]))))
		if err != nil {
			return nil, fmt.Errorf("pingwright: building resource: %w", err)
		}
	}
	return res, nil
}

func firstNonEmpty(s ...string) string {
	for _, v := range s {
		if v != "" {
			return v
		}
	}
	return ""
}
