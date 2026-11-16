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
//	defer func() {
//		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//		defer cancel()
//		_ = shutdown(ctx)
//	}()
//
//	slog.Info("started") // goes to stderr and to Pingwright
//
// Without a key (neither [Options.Key] nor PINGWRIGHT_KEY), Init prints one
// warning and runs in local mode: logging, [CaptureError], [Recover] and
// [Middleware] keep working, and nothing is sent. This keeps development
// setups and tests free of configuration.
//
// The library never panics in your code and never blocks it on the network:
// records are batched and exported in the background, export failures are
// written (rate-limited) to the local output only, and shutdown returns by
// the context's deadline, or after 5 seconds if the context has none.
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
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

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
	scopeName       = "github.com/Pingwright-Labs/pingwright-go"
	exportTimeout   = 10 * time.Second
	metricInterval  = 30 * time.Second
	panicFlush      = 5 * time.Second
	shutdownTimeout = 5 * time.Second
	maxKeyLen       = 256
)

// ErrAlreadyInitialized is returned by a second [Init] before the first one's
// shutdown function has run.
var ErrAlreadyInitialized = errors.New("pingwright: already initialized")

// Options configures [Init]. The zero value is valid: it reads the key from
// PINGWRIGHT_KEY and sends logs at Info and above, errors and metrics.
type Options struct {
	// Key is the ingest key, starting with "pwi_". Falls back to
	// PINGWRIGHT_KEY. With neither, Init runs in local mode. Surrounding
	// whitespace is trimmed; a key that is blank or contains whitespace or
	// control characters makes Init fail. A key of any other unexpected
	// shape gets one local warning, and the server decides.
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
	// exception records. They still log locally, with the stack (and, with
	// logs on, send a plain error log without exception attributes, so no
	// issue is made).
	DisableErrors bool

	// DisableMetrics stops all metrics, including runtime and HTTP metrics.
	DisableMetrics bool

	// DisableRuntimeMetrics stops the Go runtime metrics (memory, GC,
	// goroutines) that are otherwise collected every 30 seconds.
	DisableRuntimeMetrics bool

	// LogLevel is the minimum level written locally and sent, for example
	// slog.LevelDebug or a *slog.LevelVar to change it at run time. nil
	// means slog.LevelInfo. Errors are always sent.
	LogLevel slog.Leveler

	// Output receives local log output. Defaults to os.Stderr. Use
	// io.Discard to log to Pingwright only. Ignored when Handler is set.
	Output io.Writer

	// TextOutput writes local output with slog's text format instead of JSON.
	// Ignored when Handler is set.
	TextOutput bool

	// Handler, when set, receives local log output instead of the built-in
	// JSON or text handler on Output. Its own level applies locally;
	// LogLevel still decides what is sent.
	Handler slog.Handler

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
	local      slog.Handler // local output only
	handler    slog.Handler // Logger(): local + logs exporter
	errExport  slog.Handler // CaptureError/Recover export side; nil when nothing is sent
	exceptions bool         // errExport gets exception.* attributes (else error.type only)
	lp         *sdklog.LoggerProvider
	mp         *sdkmetric.MeterProvider
	prevEH     otel.ErrorHandler // restored on shutdown; nil when not replaced
}

var (
	mu      sync.Mutex // serializes Init and shutdown
	current atomic.Pointer[client]
)

func noopShutdown(context.Context) error { return nil }

// Init configures logs, metrics and errors for the process and returns a
// function that flushes and stops them. Call it once at startup and defer
// the returned function with a context that has a deadline (without one,
// it gives up after 5 seconds); it is safe to call more than once.
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

	level := opts.LogLevel
	if isNilLeveler(level) {
		level = slog.LevelInfo
	}
	local := localHandler(opts, level)
	c := &client{local: local, handler: local, exceptions: true}

	key := firstNonEmpty(opts.Key, os.Getenv("PINGWRIGHT_KEY"))
	if key == "" {
		c.install(opts)
		slog.New(local).Warn("pingwright: no key (Options.Key or PINGWRIGHT_KEY); logs, metrics and errors stay local")
		return c.shutdown, nil
	}
	key, warning, err := checkKey(key)
	if err != nil {
		return noopShutdown, err
	}

	endpoint := strings.TrimRight(firstNonEmpty(opts.Endpoint, os.Getenv("PINGWRIGHT_ENDPOINT"), DefaultEndpoint), "/")
	u, err := url.Parse(endpoint)
	if err != nil {
		return noopShutdown, errors.New("pingwright: invalid endpoint: want an http(s) URL")
	}
	if (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return noopShutdown, fmt.Errorf("pingwright: invalid endpoint %q: want an http(s) URL", u.Redacted())
	}
	if warning != "" {
		slog.New(local).Warn(warning)
	}
	if u.Scheme == "http" && !isLoopback(u.Hostname()) {
		slog.New(local).Warn("pingwright: the endpoint uses plain http to a non-local host; the key is sent unencrypted", "endpoint", u.Redacted())
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
			sdklog.WithProcessor(sdklog.NewBatchProcessor(exp, sdklog.WithExportTimeout(exportTimeout))),
		)
		otlp := otelslog.NewHandler(scopeName, otelslog.WithLoggerProvider(c.lp), otelslog.WithVersion(Version))
		var logs slog.Handler
		if !opts.DisableLogs {
			logs = minLevel{level: level, Handler: otlp}
			c.handler = slog.NewMultiHandler(local, logs)
		}
		if !opts.DisableErrors {
			c.errExport = otlp
		} else {
			c.errExport = logs // plain error logs, if logs are on
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

	// Export failures go to the local output only: the default handler
	// writes to the log package, which Init routes back into the exporter.
	c.prevEH = otel.GetErrorHandler()
	if c.prevEH != otelErrors {
		otel.SetErrorHandler(otelErrors)
	}
	c.install(opts)
	return c.shutdown, nil
}

// checkKey trims key and rejects one that can't be sent as a header value.
// It returns a warning for a key of an unexpected shape, which is still
// used: the server decides.
func checkKey(key string) (string, string, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return "", "", errors.New("pingwright: the key is blank")
	}
	if strings.IndexFunc(key, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return "", "", errors.New("pingwright: the key contains whitespace or control characters")
	}
	if !strings.HasPrefix(key, "pwi_") || len(key) > maxKeyLen {
		return key, `pingwright: the key does not look like an ingest key ("pwi_..."); sending anyway`, nil
	}
	return key, "", nil
}

func isLoopback(host string) bool {
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (c *client) install(opts Options) {
	current.Store(c)
	if !opts.KeepDefaultLogger {
		slog.SetDefault(root)
	}
}

// shutdown flushes pending logs and metrics and stops the exporters. It
// returns by ctx's deadline, or after 5 seconds when ctx has none. Later
// calls return nil. After it, Logger writes to the local output only.
func (c *client) shutdown(ctx context.Context) error {
	mu.Lock()
	defer mu.Unlock()
	if current.Load() != c {
		return nil
	}
	fallback.Store(&client{local: c.local, handler: c.local, exceptions: true})
	current.Store(nil)
	err := c.stop(ctx)
	if c.prevEH != nil && c.prevEH != otelErrors && otel.GetErrorHandler() == otelErrors {
		otel.SetErrorHandler(c.prevEH)
	}
	return err
}

func (c *client) stop(ctx context.Context) error {
	var fns []func(context.Context) error
	if c.lp != nil {
		fns = append(fns, c.lp.Shutdown)
	}
	if c.mp != nil {
		fns = append(fns, c.mp.Shutdown)
	}
	return runAll(ctx, fns...)
}

func (c *client) flush(ctx context.Context) {
	var fns []func(context.Context) error
	if c.lp != nil {
		fns = append(fns, c.lp.ForceFlush)
	}
	if c.mp != nil {
		fns = append(fns, c.mp.ForceFlush)
	}
	_ = runAll(ctx, fns...)
}

// runAll runs fns concurrently and returns their joined errors, or ctx's
// error once ctx is done, whichever comes first. Without a deadline on ctx,
// it gives up after shutdownTimeout.
func runAll(ctx context.Context, fns ...func(context.Context) error) error {
	if len(fns) == 0 {
		return nil
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, shutdownTimeout)
		defer cancel()
	}
	errs := make(chan error, len(fns))
	for _, fn := range fns {
		go func() { errs <- fn(ctx) }()
	}
	var all []error
	for range fns {
		select {
		case err := <-errs:
			all = append(all, err)
		case <-ctx.Done():
			return errors.Join(append(all, ctx.Err())...)
		}
	}
	return errors.Join(all...)
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

// isNilLeveler reports whether l is nil, including a typed nil such as a
// nil *slog.LevelVar, which would panic when its level is read.
func isNilLeveler(l slog.Leveler) bool {
	if l == nil {
		return true
	}
	v, ok := l.(*slog.LevelVar)
	return ok && v == nil
}
