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
	defer func() { // flushes on exit, for at most 5 seconds
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdown(ctx)
	}()
	defer pingwright.Recover(ctx) // reports a crash, then crashes as before

	slog.Info("started", "port", 8080) // stderr and Pingwright
}
```

`Init` makes `pingwright.Logger()` the `slog` default, so existing `slog.Info(...)` calls (and the standard `log` package) go to Pingwright without changes.

`Init` returns an error only for a configuration that cannot work (a key that is blank or contains whitespace, an endpoint that is not an http(s) URL). Everything else degrades instead of failing: no key means local mode, and an unreachable or rejecting endpoint means export errors in your local output.

**It never gets in your way.** The library does not panic in your code (`Recover` re-panics on purpose, and `Middleware` aborts a half-written response, see below), and it never blocks your code on the network: records are batched and exported in the background, and `shutdown` returns by the context's deadline, or after 5 seconds when the context has none.

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

If the handler had already started the response when it panicked, a 500 is no longer possible. `Middleware` then reports the panic and panics with `http.ErrAbortHandler`, so `net/http` drops the connection and the client sees a broken response (as it would without `Middleware`) instead of a truncated body that looks complete.

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
| `exception.message` | `err.Error()` (the type, if `Error()` panics, as for a typed nil pointer) |
| `exception.stacktrace` | the stack at the call site, in Go's `runtime/debug.Stack` format, your frame first |

Pingwright groups these into issues by type and the top frames of your own code.

`defer pingwright.Recover(ctx)` at the top of `main` and of each goroutine reports a panic at `FATAL`, with the stack from the line that panicked, flushes it, and panics again, so the process still crashes the way it would have.

## Configuration

| Option | Environment variable | Default |
|---|---|---|
| `Key` (`pwi_…`) | `PINGWRIGHT_KEY` | none: local mode |
| `Service` (`service.name`) | `OTEL_SERVICE_NAME` | the executable's name |
| `Environment` (`deployment.environment.name`) | `OTEL_RESOURCE_ATTRIBUTES` | none |
| `Version` (`service.version`) | `OTEL_RESOURCE_ATTRIBUTES` | none |
| `Endpoint` | `PINGWRIGHT_ENDPOINT` | `https://ingest.pingwright.app` |
| `ResourceAttributes` | `OTEL_RESOURCE_ATTRIBUTES` | none |
| `LogLevel` (`slog.Leveler`: `slog.LevelDebug`, a `*slog.LevelVar`, …) | | `slog.LevelInfo` (local and sent) |
| `Output` / `TextOutput` | | JSON to `os.Stderr` |
| `Handler` (your own `slog.Handler` for local output; `Output` and `TextOutput` are then ignored) | | none |

An option set in code wins over its environment variable. Surrounding whitespace in the key is trimmed. A key that doesn't start with `pwi_` gets one local warning and is sent anyway; the server decides. A plain `http://` endpoint to a host other than localhost gets one local warning, since the key would travel unencrypted.

**Local mode.** Without a key, `Init` prints one warning and sends nothing. Logging, `CaptureError`, `Recover`, `Middleware` and `Meter` all still work locally, so development and tests need no setup.

## What is sent

- **Logs** to `POST /v1/logs` (OTLP/HTTP protobuf, gzip, `Authorization: Bearer pwi_…`), batched, at `LogLevel` and above. Record attributes keep their names; trace and span ids come from the context.
- **Errors** as log records at `ERROR` (`CaptureError`, `Middleware`) or `FATAL` (`Recover`) with the `exception.*` attributes above. They are sent even below `LogLevel`.
- **Metrics** to `POST /v1/metrics` every 30 seconds: Go runtime metrics (`go.memory.used`, `go.goroutine.count`, …), HTTP server metrics from `Middleware`, and anything you record with `pingwright.Meter` or the global `otel.Meter` (Init sets the global MeterProvider).
- **Resource attributes**: `service.name`, `deployment.environment.name`, `service.version`, `host.name`, the OpenTelemetry SDK version, and your own. For metrics, Pingwright keeps `service.name`, `service.namespace`, `deployment.environment.name`, `k8s.cluster.name`, `k8s.namespace.name`, `k8s.deployment.name` and `cloud.region` as labels and drops the rest (pod names and instance ids would multiply series on every deploy).
- **No traces.** Pingwright does not store traces yet. Use your own tracer and exporter if you have one; its ids still end up on log records.

Exports time out after 10 seconds and are retried with backoff. While `Init` is active, the OpenTelemetry error handler writes export failures to your local output only (never back to Pingwright), at most once every 30 seconds per distinct error, with a count of the repeats it skipped. `shutdown` restores the previous handler.

## Turning things off

```go
pingwright.Options{
	DisableLogs:           true, // no log records (errors are still sent)
	DisableErrors:         true, // CaptureError/Recover send a plain error log; local output keeps the stack
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

**What if Pingwright is unreachable at shutdown?**
`shutdown` gives up at the context's deadline (5 seconds without one) and returns the error. Logs and metrics are flushed in parallel within that time.

**Can I call `Init` twice?**
A second call returns `ErrAlreadyInitialized`. After `shutdown`, `Init` may be called again (useful in tests). Between the two, `Logger()` keeps writing to the local output you configured.

## License

MIT
