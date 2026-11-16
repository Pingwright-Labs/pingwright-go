package pingwright

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"

	"github.com/felixge/httpsnoop"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// Middleware instruments an HTTP server:
//
//   - OpenTelemetry HTTP server metrics (http.server.request.duration and
//     friends), with http.route taken from r.Pattern when next is an
//     http.ServeMux (Go 1.22 patterns) or sits under one.
//   - A panic in next is reported as an error event (ERROR, with the stack
//     from the panicking frame and the request's method, path and route),
//     answered with 500 if nothing was written yet, and not re-panicked, so
//     the server keeps running. http.ErrAbortHandler passes through.
//   - An incoming W3C traceparent header is extracted (unless you set a
//     global propagator yourself, which is then used), so logs written with
//     r.Context() carry the caller's trace and span ids.
//
// Wrap the whole mux so every route is covered:
//
//	http.ListenAndServe(":8080", pingwright.Middleware(mux))
func Middleware(next http.Handler) http.Handler {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		committed := false
		w = httpsnoop.Wrap(w, httpsnoop.Hooks{
			WriteHeader: func(f httpsnoop.WriteHeaderFunc) httpsnoop.WriteHeaderFunc {
				return func(code int) {
					if code >= 200 {
						committed = true
					}
					f(code)
				}
			},
			Write: func(f httpsnoop.WriteFunc) httpsnoop.WriteFunc {
				return func(b []byte) (int, error) { committed = true; return f(b) }
			},
			WriteString: func(f httpsnoop.WriteStringFunc) httpsnoop.WriteStringFunc {
				return func(s string) (int, error) { committed = true; return f(s) }
			},
			ReadFrom: func(f httpsnoop.ReadFromFunc) httpsnoop.ReadFromFunc {
				return func(src io.Reader) (int64, error) { committed = true; return f(src) }
			},
			Flush: func(f httpsnoop.FlushFunc) httpsnoop.FlushFunc {
				return func() { committed = true; f() }
			},
			FlushError: func(f httpsnoop.FlushErrorFunc) httpsnoop.FlushErrorFunc {
				return func() error { committed = true; return f() }
			},
			Hijack: func(f httpsnoop.HijackFunc) httpsnoop.HijackFunc {
				return func() (net.Conn, *bufio.ReadWriter, error) { committed = true; return f() }
			},
		})
		defer func() {
			v := recover()
			if v == nil {
				return
			}
			if v == http.ErrAbortHandler {
				panic(v)
			}
			attrs := []slog.Attr{
				slog.String("http.request.method", r.Method),
				slog.String("url.path", r.URL.Path),
			}
			if route := httpRoute(r.Pattern); route != "" {
				attrs = append(attrs, slog.String("http.route", route))
			}
			capturePanic(r.Context(), slog.LevelError, v, attrs...)
			if !committed {
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
	return otelhttp.NewHandler(inner, "http.server", otelhttp.WithPropagators(propagator{}))
}

// httpRoute turns a ServeMux pattern ("GET example.com/users/{id}") into an
// http.route ("/users/{id}").
func httpRoute(pattern string) string {
	if i := strings.IndexByte(pattern, ' '); i >= 0 {
		pattern = strings.TrimLeft(pattern[i+1:], " \t")
	}
	if i := strings.IndexByte(pattern, '/'); i > 0 {
		pattern = pattern[i:]
	}
	return pattern
}

var defaultPropagator = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})

// propagator uses the global propagator when the app configured one, and
// W3C TraceContext + Baggage otherwise.
type propagator struct{}

func (propagator) get() propagation.TextMapPropagator {
	if p := otel.GetTextMapPropagator(); len(p.Fields()) > 0 {
		return p
	}
	return defaultPropagator
}

func (p propagator) Inject(ctx context.Context, c propagation.TextMapCarrier) {
	p.get().Inject(ctx, c)
}

func (p propagator) Extract(ctx context.Context, c propagation.TextMapCarrier) context.Context {
	return p.get().Extract(ctx, c)
}

func (p propagator) Fields() []string { return p.get().Fields() }
