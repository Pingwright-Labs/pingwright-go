package pingwright

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/trace"
)

// fallback is used before Init (JSON to stderr at Info) and after shutdown
// (the last Init's local output).
var fallback atomic.Pointer[client]

func init() {
	h := newLocalHandler(os.Stderr, slog.LevelInfo, false)
	fallback.Store(&client{local: h, handler: h, exceptions: true})
}

func active() *client {
	if c := current.Load(); c != nil {
		return c
	}
	return fallback.Load()
}

var root = slog.New(&dynamicHandler{})

// Logger returns the slog logger that writes locally and to Pingwright.
//
// It is safe to call before Init, for example in a package-level variable:
// records go to stderr until Init runs and to the configured outputs after.
// Records get trace_id and span_id when their context carries a span, so
// use the Context variants (InfoContext, ErrorContext, ...) in request code.
//
// Unless Options.KeepDefaultLogger is set, Init also makes this logger
// slog.Default, so plain slog.Info calls behave the same.
func Logger() *slog.Logger { return root }

// dynamicHandler resolves the active client's handler at log time and
// replays WithAttrs/WithGroup on it, caching the result per client.
type dynamicHandler struct {
	parent *dynamicHandler
	attrs  []slog.Attr
	group  string
	cache  atomic.Pointer[resolvedHandler]
}

type resolvedHandler struct {
	c *client
	h slog.Handler
}

func (d *dynamicHandler) resolve() slog.Handler {
	c := active()
	if d.parent == nil {
		return c.handler
	}
	if r := d.cache.Load(); r != nil && r.c == c {
		return r.h
	}
	h := d.parent.resolve()
	if d.group != "" {
		h = h.WithGroup(d.group)
	} else {
		h = h.WithAttrs(d.attrs)
	}
	d.cache.Store(&resolvedHandler{c: c, h: h})
	return h
}

func (d *dynamicHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return d.resolve().Enabled(ctx, l)
}

func (d *dynamicHandler) Handle(ctx context.Context, r slog.Record) error {
	return d.resolve().Handle(ctx, r)
}

func (d *dynamicHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return d
	}
	return &dynamicHandler{parent: d, attrs: attrs}
}

func (d *dynamicHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return d
	}
	return &dynamicHandler{parent: d, group: name}
}

func localHandler(opts Options, level slog.Leveler) slog.Handler {
	if opts.Handler != nil {
		return opts.Handler
	}
	out := opts.Output
	if out == nil {
		out = os.Stderr
	}
	return newLocalHandler(out, level, opts.TextOutput)
}

func newLocalHandler(w io.Writer, level slog.Leveler, text bool) slog.Handler {
	o := &slog.HandlerOptions{Level: level, ReplaceAttr: fatalLevelName}
	if text {
		return traceHandler{next: slog.NewTextHandler(w, o)}
	}
	return traceHandler{next: slog.NewJSONHandler(w, o)}
}

func fatalLevelName(groups []string, a slog.Attr) slog.Attr {
	if len(groups) == 0 && a.Key == slog.LevelKey {
		if l, ok := a.Value.Any().(slog.Level); ok && l >= LevelFatal {
			a.Value = slog.StringValue("FATAL")
		}
	}
	return a
}

// traceHandler adds trace_id and span_id to local output. The OTLP side gets
// them from the context through the otelslog bridge, as record fields.
type traceHandler struct {
	next slog.Handler
}

func (h traceHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.next.Enabled(ctx, l)
}

func (h traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r = r.Clone()
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.next.Handle(ctx, r)
}

func (h traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return traceHandler{next: h.next.WithAttrs(attrs)}
}

func (h traceHandler) WithGroup(name string) slog.Handler {
	return traceHandler{next: h.next.WithGroup(name)}
}

type minLevel struct {
	level slog.Leveler
	slog.Handler
}

func (h minLevel) Enabled(ctx context.Context, l slog.Level) bool {
	return l >= h.level.Level() && h.Handler.Enabled(ctx, l)
}

func (h minLevel) WithAttrs(attrs []slog.Attr) slog.Handler {
	return minLevel{level: h.level, Handler: h.Handler.WithAttrs(attrs)}
}

func (h minLevel) WithGroup(name string) slog.Handler {
	return minLevel{level: h.level, Handler: h.Handler.WithGroup(name)}
}

const errorInterval = 30 * time.Second

// otelErrors is the OpenTelemetry error handler while Init is active (and,
// if it was the first handler ever set, after). It writes export failures
// to the local output only, never to the exporter, at most once per
// errorInterval per distinct message, with a count of the ones it skipped.
var otelErrors = &errorHandler{}

type errorHandler struct {
	mu   sync.Mutex
	seen map[string]*errorSeen
}

type errorSeen struct {
	last       time.Time
	suppressed int
}

func (h *errorHandler) Handle(err error) {
	if err == nil {
		return
	}
	defer func() { _ = recover() }()
	msg := errorText(err)
	now := time.Now()

	h.mu.Lock()
	s := h.seen[msg]
	if s != nil && now.Sub(s.last) < errorInterval {
		s.suppressed++
		h.mu.Unlock()
		return
	}
	suppressed := 0
	if s != nil {
		suppressed = s.suppressed
	}
	if h.seen == nil || len(h.seen) >= 100 {
		h.seen = map[string]*errorSeen{}
	}
	h.seen[msg] = &errorSeen{last: now}
	h.mu.Unlock()

	r := slog.NewRecord(now, slog.LevelError, "pingwright: telemetry error", 0)
	r.AddAttrs(slog.String("error", msg))
	if suppressed > 0 {
		r.AddAttrs(slog.Int("suppressed", suppressed))
	}
	_ = active().local.Handle(context.Background(), r)
}
