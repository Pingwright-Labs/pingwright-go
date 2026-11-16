# pingwright-go

The official Go library for [Pingwright](https://pingwright.app). It sends your service's **logs**, **metrics** and **errors** to Pingwright with one call, using standard OpenTelemetry (OTLP/HTTP) under the hood.

- Logs: a `log/slog` logger that writes locally *and* to Pingwright, with trace and span ids attached.
- Errors: `CaptureError` and `Recover` turn errors and panics into issues, grouped by type and stack.
- Metrics: Go runtime metrics, HTTP server metrics from `Middleware`, and your own through `Meter`.

## Install

```sh
go get github.com/Pingwright-Labs/pingwright-go
```

Requires Go 1.26 or newer (the OpenTelemetry Go SDK it builds on requires it).

## Quickstart

```go
func main() {
	ctx := context.Background()
	shutdown, err := pingwright.Init(ctx, pingwright.Options{
		Key:         os.Getenv("PINGWRIGHT_KEY"), // pwi_..., from Settings → Telemetry
		Service:     "billing-api",
		Environment: "production",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer shutdown(context.Background()) // flushes on exit
	defer pingwright.Recover(ctx)        // reports a crash, then crashes as before

	slog.Info("started", "port", 8080) // stderr and Pingwright
}
```

`Init` makes `pingwright.Logger()` the `slog` default, so existing `slog.Info(...)` calls (and the standard `log` package) go to Pingwright without changes.

## net/http

```go
mux := http.NewServeMux()
mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
	slog.InfoContext(r.Context(), "loading order", "id", r.PathValue("id"))
	// ...
})
log.Fatal(http.ListenAndServe(":8080", pingwright.Middleware(mux)))
```

`Middleware` records OpenTelemetry HTTP server metrics (`http.server.request.duration` and friends, with `http.route` from the Go 1.22 mux pattern), reports a panic in a handler as an error and answers 500 instead of crashing, and picks up an incoming `traceparent` header so logs written with `r.Context()` carry the caller's trace id.

## Errors and panics

```go
if err := charge(ctx, order); err != nil {
	pingwright.CaptureError(ctx, err, slog.String("order.id", order.ID))
	return err
}
```

`CaptureError` sends a log record at `ERROR` with the OpenTelemetry exception attributes:

| Attribute | Value |
|---|---|
| `exception.type` | `%T` of the innermost error under plain wrappers (`fmt.Errorf("…: %w")`, `errors.Join`, `*url.Error`, `pkg/errors`), e.g. `*net.OpError` |
| `exception.message` | `err.Error()` |
| `exception.stacktrace` | the stack at the call site, in Go's `runtime/debug.Stack` format, your frame first |

Pingwright groups these into issues by type and the top frames of your own code.

`defer pingwright.Recover(ctx)` at the top of `main` and of each goroutine reports a panic at `FATAL`, with the stack from the line that panicked, flushes it, and panics again, so the process still crashes the way it would have.

## Configuration

| Option | Environment variable | Default |
|---|---|---|
| `Key` | `PINGWRIGHT_KEY` | none: local mode |
| `Service` (`service.name`) | `OTEL_SERVICE_NAME` | the executable's name |
| `Environment` (`deployment.environment.name`) | `OTEL_RESOURCE_ATTRIBUTES` | none |
| `Version` (`service.version`) | `OTEL_RESOURCE_ATTRIBUTES` | none |
| `Endpoint` | `PINGWRIGHT_ENDPOINT` | `https://ingest.pingwright.app` |
| `ResourceAttributes` | `OTEL_RESOURCE_ATTRIBUTES` | none |
| `LogLevel` | | `slog.LevelInfo` (local and sent) |
| `Output` / `TextOutput` | | JSON to `os.Stderr` |

An option set in code wins over its environment variable.

**Local mode.** Without a key, `Init` prints one warning and sends nothing. Logging, `CaptureError`, `Recover`, `Middleware` and `Meter` all still work locally, so development and tests need no setup.

## What is sent

- **Logs** to `POST /v1/logs` (OTLP/HTTP protobuf, gzip, `Authorization: Bearer pwi_…`), batched, at `LogLevel` and above. Record attributes keep their names; trace and span ids come from the context.
- **Errors** as log records at `ERROR` (`CaptureError`, `Middleware`) or `FATAL` (`Recover`) with the `exception.*` attributes above. They are sent even below `LogLevel`.
- **Metrics** to `POST /v1/metrics` every 30 seconds: Go runtime metrics (`go.memory.used`, `go.goroutine.count`, …), HTTP server metrics from `Middleware`, and anything you record with `pingwright.Meter` or the global `otel.Meter` (Init sets the global MeterProvider).
- **Resource attributes**: `service.name`, `deployment.environment.name`, `service.version`, `host.name`, the OpenTelemetry SDK version, and your own. For metrics, Pingwright keeps `service.name`, `service.namespace`, `deployment.environment.name`, `k8s.cluster.name`, `k8s.namespace.name`, `k8s.deployment.name` and `cloud.region` as labels and drops the rest (pod names and instance ids would multiply series on every deploy).
- **No traces.** Pingwright does not store traces yet. Use your own tracer and exporter if you have one; its ids still end up on log records.

Exports time out after 10 seconds and are retried with backoff. Export failures are reported through OpenTelemetry's error handler and never affect your program.

## Turning things off

```go
pingwright.Options{
	DisableLogs:           true, // no log records (errors are still sent)
	DisableErrors:         true, // CaptureError/Recover only log, without exception.* attributes
	DisableMetrics:        true, // no metrics at all
	DisableRuntimeMetrics: true, // keep HTTP and custom metrics, skip Go runtime metrics
	KeepDefaultLogger:     true, // don't replace slog.Default
	Output:                io.Discard, // nothing written locally
}
```

Leave `PINGWRIGHT_KEY` unset to turn off sending entirely.

## FAQ

**Can I use plain OpenTelemetry instead?**
Yes. Pingwright's ingest is standard OTLP/HTTP, and this library is a convenience on top of the OpenTelemetry Go SDK. With your own `otlploghttp` and `otlpmetrichttp` exporters (plus the `otelslog` bridge for `slog`), set:

```sh
OTEL_EXPORTER_OTLP_ENDPOINT=https://ingest.pingwright.app
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
OTEL_EXPORTER_OTLP_HEADERS=authorization=Bearer%20pwi_...
OTEL_SERVICE_NAME=billing-api
OTEL_RESOURCE_ATTRIBUTES=deployment.environment.name=production
OTEL_TRACES_EXPORTER=none
```

The same works from an OpenTelemetry Collector (`otlphttp` exporter with the `authorization` header). To get issues from plain OpenTelemetry, emit log records at `ERROR` or above with `exception.type`, `exception.message` and `exception.stacktrace` (Go panic or `debug.Stack()` format).

**Does it slow my handlers down?**
No network call happens on the request path. Records are batched and exported in the background.

**What happens on a crash?**
`Recover` flushes pending logs (up to 5 seconds) before re-panicking. For a normal exit, call the `shutdown` function from `Init`.

**Can I call `Init` twice?**
A second call returns `ErrAlreadyInitialized`. After `shutdown`, `Init` may be called again (useful in tests).

## License

MIT
