package pingwright_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Pingwright-Labs/pingwright-go"
	"go.opentelemetry.io/otel"
)

// recordingErrorHandler behaves like OpenTelemetry's default handler, which
// writes to the log package (and so, after Init, to Pingwright).
type recordingErrorHandler struct{ n atomic.Int64 }

func (h *recordingErrorHandler) Handle(err error) {
	h.n.Add(1)
	log.Print(err)
}

func TestExportFailuresStayLocal(t *testing.T) {
	prev := &recordingErrorHandler{}
	otel.SetErrorHandler(prev)

	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		requests.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	shutdown, out := start(t, nil, pingwright.Options{Endpoint: srv.URL, DisableMetrics: true})
	if otel.GetErrorHandler() == otel.ErrorHandler(prev) {
		t.Error("Init did not replace the OpenTelemetry error handler")
	}
	slog.Info("one record")
	// The batch processor exports every second; give a feedback loop
	// several rounds to show up.
	time.Sleep(3500 * time.Millisecond)
	sctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_ = shutdown(sctx)

	if n := requests.Load(); n != 1 {
		t.Errorf("export requests = %d, want 1 (export errors must not be exported)", n)
	}
	if n := strings.Count(out.String(), "pingwright: telemetry error"); n < 1 || n > 2 {
		t.Errorf("local export error lines = %d, want 1 or 2:\n%s", n, out)
	}
	if strings.Contains(out.String(), testKey) {
		t.Error("local output contains the key")
	}
	if otel.GetErrorHandler() != otel.ErrorHandler(prev) {
		t.Error("shutdown did not restore the previous OpenTelemetry error handler")
	}
	if prev.n.Load() != 0 {
		t.Errorf("previous handler got %d errors while Init was active", prev.n.Load())
	}
}

func TestShutdownWithoutDeadlineIsBounded(t *testing.T) {
	// An endpoint that accepts connections and never answers.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			c.Close()
		}
	})

	shutdown, _ := start(t, nil, pingwright.Options{Endpoint: "http://" + ln.Addr().String()})
	slog.Info("stuck")
	pingwright.CaptureError(ctx, errors.New("stuck too"))

	began := time.Now()
	_ = shutdown(context.Background())
	if d := time.Since(began); d > 6*time.Second {
		t.Errorf("shutdown took %v, want at most about 5s", d)
	}
}

func TestMiddlewarePanicAfterPartialWriteBreaksResponse(t *testing.T) {
	_, _ = start(t, newCollector(t), pingwright.Options{DisableMetrics: true})
	srv := httptest.NewServer(pingwright.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"items":[1,2,`))
		w.(http.Flusher).Flush()
		panic("boom")
	})))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		return // aborted before the headers: also not a clean response
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("read body = %q, err %v; want io.ErrUnexpectedEOF", body, err)
	}
}

type msgErr struct{ msg string }

func (e *msgErr) Error() string { return e.msg }

type panickyErr struct{}

func (panickyErr) Error() string { panic("Error() exploded") }

func TestCaptureErrorNeverPanics(t *testing.T) {
	c := newCollector(t)
	shutdown, _ := start(t, c, pingwright.Options{DisableMetrics: true})

	var typedNil *msgErr
	pingwright.CaptureError(ctx, typedNil)
	pingwright.CaptureError(ctx, panickyErr{})
	//lint:ignore SA1012 testing a nil ctx on purpose
	pingwright.CaptureError(nil, errors.New("nil ctx"))
	func() {
		defer func() { _ = recover() }()
		defer pingwright.Recover(ctx)
		panic(panickyErr{})
	}()
	_ = shutdown(ctx)

	for body, typ := range map[string]string{
		"*pingwright_test.msgErr":           "*pingwright_test.msgErr",
		"pingwright_test.panickyErr":        "pingwright_test.panickyErr",
		"panic: pingwright_test.panickyErr": "pingwright_test.panickyErr",
	} {
		if v, _ := attr(c.Record(t, body).LogRecord, "exception.type"); v != typ {
			t.Errorf("%s: exception.type = %q, want %q", body, v, typ)
		}
	}
	c.Record(t, "nil ctx")
}

func TestKeyShapes(t *testing.T) {
	c := newCollector(t)
	shutdown, out := start(t, c, pingwright.Options{Key: "  " + testKey + "\n", DisableMetrics: true})
	pingwright.Logger().Info("trimmed")
	_ = shutdown(ctx)
	for _, r := range c.Requests() {
		if r.auth != "Bearer "+testKey {
			t.Errorf("Authorization = %q, want the trimmed key", r.auth)
		}
	}
	if strings.Contains(out.String(), "does not look like") {
		t.Errorf("warned about a valid key: %s", out)
	}

	// An unexpected shape is sent anyway, with one local warning.
	c = newCollector(t)
	shutdown, out = start(t, c, pingwright.Options{Key: "sk_live_other", DisableMetrics: true})
	pingwright.Logger().Info("odd key")
	_ = shutdown(ctx)
	if n := strings.Count(out.String(), "does not look like an ingest key"); n != 1 {
		t.Errorf("want one warning, got %d: %s", n, out)
	}
	if strings.Contains(out.String(), "sk_live_other") {
		t.Error("warning echoes the key")
	}
	if reqs := c.Requests(); len(reqs) == 0 || reqs[0].auth != "Bearer sk_live_other" {
		t.Errorf("requests = %v, want the key sent as is", reqs)
	}
}

func TestEndpointWarningsAndRedaction(t *testing.T) {
	t.Setenv("PINGWRIGHT_KEY", "")
	t.Setenv("PINGWRIGHT_ENDPOINT", "")
	for _, ep := range []string{"ftp://user:secret@x", "http://user:secret@[::1"} {
		_, err := pingwright.Init(ctx, pingwright.Options{Key: testKey, Endpoint: ep})
		if err == nil || strings.Contains(err.Error(), "secret") {
			t.Errorf("%s: err = %v, want an error without the password", ep, err)
		}
	}

	shutdown, out := start(t, nil, pingwright.Options{Endpoint: "http://ingest.example.com", DisableMetrics: true})
	sctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	_ = shutdown(sctx)
	if !strings.Contains(out.String(), "plain http") {
		t.Errorf("no warning for plain http to a remote host: %s", out)
	}

	shutdown, out = start(t, newCollector(t), pingwright.Options{DisableMetrics: true}) // http://127.0.0.1
	_ = shutdown(ctx)
	if strings.Contains(out.String(), "plain http") {
		t.Errorf("warned for a loopback endpoint: %s", out)
	}
}

func TestLogLevelVarAndCustomHandler(t *testing.T) {
	c := newCollector(t)
	var local bytes.Buffer
	var mu sync.Mutex
	lv := new(slog.LevelVar)
	lv.Set(slog.LevelWarn)
	shutdown, _ := start(t, c, pingwright.Options{
		DisableMetrics: true,
		LogLevel:       lv,
		Handler:        slog.NewTextHandler(lockedWriter{&mu, &local}, &slog.HandlerOptions{Level: slog.LevelDebug}),
		Output:         io.Discard, // ignored: Handler wins
	})
	pingwright.Logger().Info("info while warn")
	lv.Set(slog.LevelDebug)
	pingwright.Logger().Debug("debug after lowering")
	_ = shutdown(ctx)

	for _, r := range c.LogRecords() {
		if r.Body.GetStringValue() == "info while warn" {
			t.Error("Info exported while LogLevel was Warn")
		}
	}
	c.Record(t, "debug after lowering")
	mu.Lock()
	s := local.String()
	mu.Unlock()
	if !strings.Contains(s, "msg=\"info while warn\"") || !strings.Contains(s, "msg=\"debug after lowering\"") {
		t.Errorf("custom handler output = %s", s)
	}
}

type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (l lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

func TestLoggerAfterShutdownUsesOutput(t *testing.T) {
	shutdown, out := start(t, newCollector(t), pingwright.Options{DisableMetrics: true, TextOutput: true})
	_ = shutdown(ctx)
	pingwright.Logger().Info("after shutdown")
	pingwright.CaptureError(ctx, errors.New("late error"))
	s := out.String()
	if !strings.Contains(s, "msg=\"after shutdown\"") || !strings.Contains(s, "late error") {
		t.Errorf("output after shutdown went elsewhere: %s", s)
	}
}

func TestDisableErrorsKeepsLocalStack(t *testing.T) {
	c := newCollector(t)
	shutdown, out := start(t, c, pingwright.Options{DisableMetrics: true, DisableErrors: true})
	pingwright.CaptureError(ctx, errors.New("bad"))
	_ = shutdown(ctx)
	if !strings.Contains(out.String(), "exception.stacktrace") {
		t.Errorf("local output lacks the stack: %s", out)
	}
	rec := c.Record(t, "bad")
	if _, ok := attr(rec.LogRecord, "exception.stacktrace"); ok {
		t.Error("stack sent with errors disabled")
	}
	if v, _ := attr(rec.LogRecord, "error.type"); v != "*errors.errorString" {
		t.Errorf("error.type = %q", v)
	}
}

func TestOTelErrorsAreRateLimited(t *testing.T) {
	shutdown, out := start(t, newCollector(t), pingwright.Options{DisableMetrics: true})
	msg := fmt.Sprintf("flaky export %d", time.Now().UnixNano()) // unique across -count runs
	for range 50 {
		otel.Handle(errors.New(msg))
	}
	otel.Handle(errors.New(msg + " other"))
	_ = shutdown(ctx)
	if n := strings.Count(out.String(), msg+`"`); n != 1 {
		t.Errorf("%d lines for 50 identical errors, want 1:\n%s", n, out)
	}
	if !strings.Contains(out.String(), msg+` other"`) {
		t.Errorf("a different error was suppressed:\n%s", out)
	}
}
