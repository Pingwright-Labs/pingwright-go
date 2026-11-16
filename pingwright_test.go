package pingwright_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/Pingwright-Labs/pingwright-go"
	"go.opentelemetry.io/otel/trace"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
)

var ctx = context.Background()

func TestLogsCarryAuthAndResource(t *testing.T) {
	c := newCollector(t)
	shutdown, out := start(t, c, pingwright.Options{
		Service:            "billing",
		Environment:        "staging",
		Version:            "1.2.3",
		ResourceAttributes: map[string]string{"k8s.cluster.name": "eu-1", "service.name": "overridden"},
		DisableMetrics:     true,
	})

	pingwright.Logger().Info("hello", "user", "u1")
	if err := shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	reqs := c.Requests()
	if len(reqs) == 0 {
		t.Fatal("no requests reached the collector")
	}
	for _, r := range reqs {
		if r.auth != "Bearer "+testKey {
			t.Errorf("Authorization = %q", r.auth)
		}
		if r.encoding != "gzip" {
			t.Errorf("Content-Encoding = %q, want gzip", r.encoding)
		}
		if r.contentType != "application/x-protobuf" {
			t.Errorf("Content-Type = %q", r.contentType)
		}
	}

	rec := c.Record(t, "hello")
	for k, want := range map[string]string{
		"service.name":                "billing",
		"deployment.environment.name": "staging",
		"service.version":             "1.2.3",
		"k8s.cluster.name":            "eu-1",
	} {
		if got := rec.resource[k]; got != want {
			t.Errorf("resource %s = %q, want %q", k, got, want)
		}
	}
	if v, _ := attr(rec.LogRecord, "user"); v != "u1" {
		t.Errorf("attribute user = %q", v)
	}
	if !strings.Contains(out.String(), `"msg":"hello"`) {
		t.Errorf("local output is missing the record: %s", out)
	}
}

func TestSeverityMapping(t *testing.T) {
	c := newCollector(t)
	shutdown, _ := start(t, c, pingwright.Options{DisableMetrics: true, LogLevel: slog.LevelDebug})

	l := pingwright.Logger()
	l.Debug("d")
	l.Info("i")
	l.Warn("w")
	l.Error("e")
	l.Log(ctx, pingwright.LevelFatal, "f")
	_ = shutdown(ctx)

	for body, want := range map[string]logspb.SeverityNumber{
		"d": logspb.SeverityNumber_SEVERITY_NUMBER_DEBUG,
		"i": logspb.SeverityNumber_SEVERITY_NUMBER_INFO,
		"w": logspb.SeverityNumber_SEVERITY_NUMBER_WARN,
		"e": logspb.SeverityNumber_SEVERITY_NUMBER_ERROR,
		"f": logspb.SeverityNumber_SEVERITY_NUMBER_FATAL,
	} {
		if got := c.Record(t, body).SeverityNumber; got != want {
			t.Errorf("%s: severity %v, want %v", body, got, want)
		}
	}
}

func TestLogLevelFiltersExport(t *testing.T) {
	c := newCollector(t)
	shutdown, out := start(t, c, pingwright.Options{DisableMetrics: true}) // default Info

	pingwright.Logger().Debug("hidden")
	pingwright.Logger().Info("shown")
	_ = shutdown(ctx)

	for _, r := range c.LogRecords() {
		if r.Body.GetStringValue() == "hidden" {
			t.Error("debug record exported at default level")
		}
	}
	c.Record(t, "shown")
	if strings.Contains(out.String(), "hidden") {
		t.Error("debug record written locally at default level")
	}
}

func TestLoggerBeforeInitAndTraceIDs(t *testing.T) {
	l := pingwright.Logger().With("component", "early") // obtained before Init

	c := newCollector(t)
	shutdown, out := start(t, c, pingwright.Options{DisableMetrics: true})

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:     trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	l.InfoContext(trace.ContextWithSpanContext(ctx, sc), "traced")
	slog.Info("via default") // Init made our logger slog.Default
	_ = shutdown(ctx)

	rec := c.Record(t, "traced")
	if v, _ := attr(rec.LogRecord, "component"); v != "early" {
		t.Errorf("component = %q", v)
	}
	if got := trace.TraceID(rec.TraceId); got != sc.TraceID() {
		t.Errorf("trace id = %s, want %s", got, sc.TraceID())
	}
	if got := trace.SpanID(rec.SpanId); got != sc.SpanID() {
		t.Errorf("span id = %s, want %s", got, sc.SpanID())
	}
	if _, ok := attr(rec.LogRecord, "trace_id"); ok {
		t.Error("trace_id should be a record field, not an attribute")
	}
	if !strings.Contains(out.String(), `"trace_id":"`+sc.TraceID().String()+`"`) {
		t.Errorf("local output lacks trace_id: %s", out)
	}
	c.Record(t, "via default")
}

//go:noinline
func captureHere(err error) {
	pingwright.CaptureError(ctx, err, slog.String("order", "o-1"))
}

func TestCaptureError(t *testing.T) {
	c := newCollector(t)
	shutdown, _ := start(t, c, pingwright.Options{DisableMetrics: true})

	opErr := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	err := fmt.Errorf("charge: %w", fmt.Errorf("fetch: %w", opErr))
	captureHere(err)
	pingwright.CaptureError(ctx, nil) // no-op
	_ = shutdown(ctx)

	rec := c.Record(t, err.Error())
	if rec.SeverityNumber != logspb.SeverityNumber_SEVERITY_NUMBER_ERROR {
		t.Errorf("severity = %v", rec.SeverityNumber)
	}
	if v, _ := attr(rec.LogRecord, "exception.type"); v != "*net.OpError" {
		t.Errorf("exception.type = %q, want *net.OpError", v)
	}
	if v, _ := attr(rec.LogRecord, "exception.message"); v != err.Error() {
		t.Errorf("exception.message = %q", v)
	}
	if v, _ := attr(rec.LogRecord, "order"); v != "o-1" {
		t.Errorf("order = %q", v)
	}
	stack, _ := attr(rec.LogRecord, "exception.stacktrace")
	assertTopFrame(t, stack, "pingwright-go_test.captureHere")
	if len(c.LogRecords()) != 1 {
		t.Errorf("got %d records, want 1 (nil error must not be captured)", len(c.LogRecords()))
	}
}

func TestExceptionTypeUnwrapping(t *testing.T) {
	c := newCollector(t)
	shutdown, _ := start(t, c, pingwright.Options{DisableMetrics: true})

	cases := map[string]error{
		"*errors.errorString": errors.New("plain"),
		"*net.OpError":        errors.Join(fmt.Errorf("a: %w", &net.OpError{Op: "read", Err: errors.New("x")}), errors.New("b")),
		"*net.DNSError":       fmt.Errorf("lookup: %w", &net.DNSError{Err: "no such host", Name: "x"}),
	}
	for _, err := range cases {
		pingwright.CaptureError(ctx, err)
	}
	_ = shutdown(ctx)

	for want, err := range cases {
		if got, _ := attr(c.Record(t, err.Error()).LogRecord, "exception.type"); got != want {
			t.Errorf("%q: exception.type = %q, want %q", err, got, want)
		}
	}
}

//go:noinline
func explode() {
	panic("kaboom")
}

func TestRecoverCapturesAndRepanics(t *testing.T) {
	c := newCollector(t)
	_, _ = start(t, c, pingwright.Options{DisableMetrics: true})

	var got any
	func() {
		defer func() { got = recover() }()
		func() {
			defer pingwright.Recover(ctx)
			explode()
		}()
	}()
	if got != "kaboom" {
		t.Fatalf("re-panicked with %v, want kaboom", got)
	}

	// Recover flushes before re-panicking: the record is there without shutdown.
	rec := c.Record(t, "panic: kaboom")
	if rec.SeverityNumber != logspb.SeverityNumber_SEVERITY_NUMBER_FATAL {
		t.Errorf("severity = %v, want FATAL", rec.SeverityNumber)
	}
	if v, _ := attr(rec.LogRecord, "exception.type"); v != "string" {
		t.Errorf("exception.type = %q", v)
	}
	if v, _ := attr(rec.LogRecord, "exception.message"); v != "kaboom" {
		t.Errorf("exception.message = %q", v)
	}
	stack, _ := attr(rec.LogRecord, "exception.stacktrace")
	assertTopFrame(t, stack, "pingwright-go_test.explode")
}

func TestRecoverRuntimeError(t *testing.T) {
	c := newCollector(t)
	_, _ = start(t, c, pingwright.Options{DisableMetrics: true})

	func() {
		defer func() { _ = recover() }()
		defer pingwright.Recover(ctx)
		items := strings.Fields("")
		_ = items[len(os.Args)] // index out of range
	}()
	var rec logRecord
	for _, r := range c.LogRecords() {
		if strings.HasPrefix(r.Body.GetStringValue(), "panic: ") {
			rec = r
		}
	}
	if rec.LogRecord == nil {
		t.Fatal("no panic record")
	}
	if v, _ := attr(rec.LogRecord, "exception.type"); !strings.HasPrefix(v, "runtime.") {
		t.Errorf("exception.type = %q, want a runtime error type", v)
	}
	stack, _ := attr(rec.LogRecord, "exception.stacktrace")
	assertTopFrame(t, stack, "pingwright-go_test.TestRecoverRuntimeError.func1")
}

func TestRecoverWithoutPanic(t *testing.T) {
	c := newCollector(t)
	shutdown, _ := start(t, c, pingwright.Options{DisableMetrics: true})
	func() { defer pingwright.Recover(ctx) }()
	_ = shutdown(ctx)
	if n := len(c.LogRecords()); n != 0 {
		t.Errorf("got %d records, want 0", n)
	}
}

func TestMiddlewarePanicReturns500(t *testing.T) {
	c := newCollector(t)
	shutdown, _ := start(t, c, pingwright.Options{DisableRuntimeMetrics: true})

	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		explode()
	})
	mux.HandleFunc("GET /ok", func(w http.ResponseWriter, r *http.Request) {
		pingwright.Logger().InfoContext(r.Context(), "in handler")
		w.WriteHeader(http.StatusNoContent)
	})
	h := pingwright.Middleware(mux)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/orders/42", nil))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rr.Code)
	}

	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	req := httptest.NewRequest(http.MethodGet, "/ok", nil)
	req.Header.Set("traceparent", "00-"+traceID+"-00f067aa0ba902b7-01")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", rr.Code)
	}
	_ = shutdown(ctx)

	rec := c.Record(t, "panic: kaboom")
	if rec.SeverityNumber != logspb.SeverityNumber_SEVERITY_NUMBER_ERROR {
		t.Errorf("severity = %v, want ERROR", rec.SeverityNumber)
	}
	for k, want := range map[string]string{
		"exception.type":      "string",
		"http.request.method": "GET",
		"url.path":            "/orders/42",
		"http.route":          "/orders/{id}",
	} {
		if v, _ := attr(rec.LogRecord, k); v != want {
			t.Errorf("%s = %q, want %q", k, v, want)
		}
	}
	stack, _ := attr(rec.LogRecord, "exception.stacktrace")
	assertTopFrame(t, stack, "pingwright-go_test.explode")

	if got := trace.TraceID(c.Record(t, "in handler").TraceId).String(); got != traceID {
		t.Errorf("handler log trace id = %s, want %s (from traceparent)", got, traceID)
	}

	var found bool
	for _, m := range c.Metrics() {
		if m.Name != "http.server.request.duration" {
			continue
		}
		for _, dp := range m.GetHistogram().GetDataPoints() {
			route, _ := kvString(dp.Attributes, "http.route")
			code := int64(0)
			for _, kv := range dp.Attributes {
				if kv.Key == "http.response.status_code" {
					code = kv.Value.GetIntValue()
				}
			}
			if route == "/orders/{id}" && code == 500 {
				found = true
			}
		}
	}
	if !found {
		t.Error("no http.server.request.duration point with http.route=/orders/{id} and status 500")
	}
}

func TestMiddlewarePanicAfterWrite(t *testing.T) {
	_, _ = start(t, newCollector(t), pingwright.Options{DisableMetrics: true})
	h := pingwright.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		panic("late")
	}))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusAccepted {
		t.Errorf("status = %d, want the already written 202", rr.Code)
	}
}

func TestMetricsArrive(t *testing.T) {
	c := newCollector(t)
	shutdown, _ := start(t, c, pingwright.Options{Service: "svc", Environment: "prod", DisableLogs: true, DisableErrors: true})

	counter, err := pingwright.Meter("test").Int64Counter("jobs.processed")
	if err != nil {
		t.Fatal(err)
	}
	counter.Add(ctx, 3)
	if err := shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	var sawCounter, sawRuntime bool
	for _, m := range c.Metrics() {
		if m.Name == "jobs.processed" {
			sawCounter = true
			if v := m.GetSum().GetDataPoints()[0].GetAsInt(); v != 3 {
				t.Errorf("jobs.processed = %d, want 3", v)
			}
			if m.resource["service.name"] != "svc" || m.resource["deployment.environment.name"] != "prod" {
				t.Errorf("metric resource = %v", m.resource)
			}
		}
		if strings.HasPrefix(m.Name, "go.") {
			sawRuntime = true
		}
	}
	if !sawCounter {
		t.Error("jobs.processed did not arrive")
	}
	if !sawRuntime {
		t.Error("no go.* runtime metrics arrived")
	}
	for _, r := range c.Requests() {
		if r.path == "/v1/logs" {
			t.Error("logs were sent with logs and errors disabled")
		}
	}
}

func TestDisableErrorsDropsExceptionAttributes(t *testing.T) {
	c := newCollector(t)
	shutdown, _ := start(t, c, pingwright.Options{DisableMetrics: true, DisableErrors: true})
	pingwright.CaptureError(ctx, errors.New("bad"))
	_ = shutdown(ctx)
	rec := c.Record(t, "bad")
	if _, ok := attr(rec.LogRecord, "exception.type"); ok {
		t.Error("exception.type sent with errors disabled")
	}
}

func TestErrorsWithLogsDisabled(t *testing.T) {
	c := newCollector(t)
	shutdown, _ := start(t, c, pingwright.Options{DisableMetrics: true, DisableLogs: true})
	pingwright.Logger().Info("not sent")
	pingwright.CaptureError(ctx, errors.New("sent"))
	_ = shutdown(ctx)
	recs := c.LogRecords()
	if len(recs) != 1 || recs[0].Body.GetStringValue() != "sent" {
		t.Fatalf("records = %v, want only the error", recs)
	}
}

func TestNoopModeSendsNothing(t *testing.T) {
	c := newCollector(t)
	t.Setenv("PINGWRIGHT_KEY", "")
	out := &syncBuffer{}
	shutdown, err := pingwright.Init(ctx, pingwright.Options{Endpoint: c.URL, Output: out})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	defer shutdown(ctx)

	pingwright.Logger().Info("local only")
	pingwright.CaptureError(ctx, errors.New("local error"))
	func() {
		defer func() { _ = recover() }()
		defer pingwright.Recover(ctx)
		panic("local panic")
	}()
	cnt, _ := pingwright.Meter("x").Int64Counter("n")
	cnt.Add(ctx, 1)
	rr := httptest.NewRecorder()
	pingwright.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("x") })).
		ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != 500 {
		t.Errorf("status = %d", rr.Code)
	}
	if err := shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	if n := len(c.Requests()); n != 0 {
		t.Errorf("collector got %d requests in no-op mode", n)
	}
	s := out.String()
	if strings.Count(s, "no key") != 1 {
		t.Errorf("want exactly one warning, got: %s", s)
	}
	for _, want := range []string{"local only", "local error", "local panic"} {
		if !strings.Contains(s, want) {
			t.Errorf("local output lacks %q", want)
		}
	}
}

func TestEnvFallbacks(t *testing.T) {
	c := newCollector(t)
	t.Setenv("PINGWRIGHT_KEY", testKey)
	t.Setenv("PINGWRIGHT_ENDPOINT", c.URL)
	t.Setenv("OTEL_SERVICE_NAME", "from-env")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")
	shutdown, err := pingwright.Init(ctx, pingwright.Options{Output: &syncBuffer{}, DisableMetrics: true})
	if err != nil {
		t.Fatal(err)
	}
	pingwright.Logger().Info("env")
	_ = shutdown(ctx)
	if got := c.Record(t, "env").resource["service.name"]; got != "from-env" {
		t.Errorf("service.name = %q", got)
	}
}

func TestInitTwiceAndShutdownIdempotent(t *testing.T) {
	shutdown, _ := start(t, newCollector(t), pingwright.Options{DisableMetrics: true})
	s2, err := pingwright.Init(ctx, pingwright.Options{Key: testKey})
	if !errors.Is(err, pingwright.ErrAlreadyInitialized) {
		t.Fatalf("second Init err = %v", err)
	}
	if s2 == nil || s2(ctx) != nil {
		t.Error("shutdown from a failed Init must be a non-nil no-op")
	}
	if err := shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := shutdown(ctx); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
	// Init works again after shutdown.
	start(t, newCollector(t), pingwright.Options{DisableMetrics: true})
}

func TestInvalidConfig(t *testing.T) {
	t.Setenv("PINGWRIGHT_KEY", "")
	t.Setenv("PINGWRIGHT_ENDPOINT", "")
	for name, opts := range map[string]pingwright.Options{
		"bad key":      {Key: "sk_live_nope"},
		"bad endpoint": {Key: testKey, Endpoint: "ftp://x"},
	} {
		s, err := pingwright.Init(ctx, opts)
		if err == nil {
			_ = s(ctx)
			t.Errorf("%s: want error", name)
		}
	}
}

func TestShutdownFlushes(t *testing.T) {
	c := newCollector(t)
	shutdown, _ := start(t, c, pingwright.Options{DisableMetrics: true})
	for i := range 50 {
		pingwright.Logger().Info(fmt.Sprintf("line %d", i))
	}
	if n := len(c.LogRecords()); n != 0 {
		t.Logf("%d records already exported before shutdown", n)
	}
	_ = shutdown(ctx)
	if n := len(c.LogRecords()); n != 50 {
		t.Errorf("got %d records after shutdown, want 50", n)
	}
}

// assertTopFrame checks that the first frame of a Go stack trace is fn
// (matched as a suffix), so no library or runtime frame sits above it.
func assertTopFrame(t *testing.T, stack, fn string) {
	t.Helper()
	lines := strings.Split(stack, "\n")
	if len(lines) < 3 || !strings.HasPrefix(lines[0], "goroutine ") || !strings.HasSuffix(lines[0], "[running]:") {
		t.Fatalf("not a Go stack trace:\n%s", stack)
	}
	top := strings.TrimSuffix(lines[1], "(...)")
	if !strings.HasSuffix(top, fn) {
		t.Errorf("top frame = %q, want %s\n%s", lines[1], fn, stack)
	}
	if !strings.HasPrefix(lines[2], "\t") || !strings.Contains(lines[2], ".go:") {
		t.Errorf("second line should be a file:line, got %q", lines[2])
	}
	if strings.Contains(stack, "runtime.gopanic") || strings.Contains(stack, "pingwright-go.capturePanic") {
		t.Errorf("stack contains library or runtime panic frames:\n%s", stack)
	}
}
