package pingwright_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/Pingwright-Labs/pingwright-go"
	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

const testKey = "pwi_0123456789abcdef0123456789abcdef"

type request struct {
	path, auth, encoding, contentType string
}

// collector is a fake ingest endpoint that decodes OTLP/HTTP protobuf.
type collector struct {
	*httptest.Server
	t        *testing.T
	mu       sync.Mutex
	requests []request
	logs     []*logspb.ResourceLogs
	metrics  []*metricspb.ResourceMetrics
}

func newCollector(t *testing.T) *collector {
	t.Helper()
	c := &collector{t: t}
	c.Server = httptest.NewServer(http.HandlerFunc(c.handle))
	t.Cleanup(c.Close)
	return c
}

func (c *collector) handle(w http.ResponseWriter, r *http.Request) {
	var body io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		body = zr
	}
	raw, err := io.ReadAll(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests = append(c.requests, request{
		path:        r.URL.Path,
		auth:        r.Header.Get("Authorization"),
		encoding:    r.Header.Get("Content-Encoding"),
		contentType: r.Header.Get("Content-Type"),
	})
	var resp proto.Message
	switch r.URL.Path {
	case "/v1/logs":
		var req collogs.ExportLogsServiceRequest
		if err := proto.Unmarshal(raw, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		c.logs = append(c.logs, req.ResourceLogs...)
		resp = &collogs.ExportLogsServiceResponse{}
	case "/v1/metrics":
		var req colmetrics.ExportMetricsServiceRequest
		if err := proto.Unmarshal(raw, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		c.metrics = append(c.metrics, req.ResourceMetrics...)
		resp = &colmetrics.ExportMetricsServiceResponse{}
	default:
		http.NotFound(w, r)
		return
	}
	out, _ := proto.Marshal(resp)
	w.Header().Set("Content-Type", "application/x-protobuf")
	_, _ = w.Write(out)
}

func (c *collector) Requests() []request {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]request(nil), c.requests...)
}

type logRecord struct {
	resource map[string]string
	*logspb.LogRecord
}

func (c *collector) LogRecords() []logRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []logRecord
	for _, rl := range c.logs {
		res := attrMap(rl.GetResource())
		for _, sl := range rl.ScopeLogs {
			for _, lr := range sl.LogRecords {
				out = append(out, logRecord{resource: res, LogRecord: lr})
			}
		}
	}
	return out
}

func (c *collector) Record(t *testing.T, body string) logRecord {
	t.Helper()
	for _, r := range c.LogRecords() {
		if r.Body.GetStringValue() == body {
			return r
		}
	}
	t.Fatalf("no log record with body %q; got %d records", body, len(c.LogRecords()))
	return logRecord{}
}

type metric struct {
	resource map[string]string
	*metricspb.Metric
}

func (c *collector) Metrics() []metric {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []metric
	for _, rm := range c.metrics {
		res := attrMap(rm.GetResource())
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				out = append(out, metric{resource: res, Metric: m})
			}
		}
	}
	return out
}

func attrMap(r *resourcepb.Resource) map[string]string {
	m := map[string]string{}
	for _, kv := range r.GetAttributes() {
		m[kv.Key] = kv.Value.GetStringValue()
	}
	return m
}

func attr(lr *logspb.LogRecord, key string) (string, bool) {
	return kvString(lr.Attributes, key)
}

func kvString(kvs []*commonpb.KeyValue, key string) (string, bool) {
	for _, kv := range kvs {
		if kv.Key == key {
			return kv.Value.GetStringValue(), true
		}
	}
	return "", false
}

// syncBuffer is a goroutine-safe local log output.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// start runs Init against c and registers shutdown. It returns the shutdown
// func so tests can flush before asserting.
func start(t *testing.T, c *collector, opts pingwright.Options) (func(context.Context) error, *syncBuffer) {
	t.Helper()
	t.Setenv("PINGWRIGHT_KEY", "")
	t.Setenv("PINGWRIGHT_ENDPOINT", "")
	t.Setenv("OTEL_SERVICE_NAME", "")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")
	out := &syncBuffer{}
	if opts.Key == "" {
		opts.Key = testKey
	}
	if opts.Endpoint == "" && c != nil {
		opts.Endpoint = c.URL
	}
	if opts.Output == nil {
		opts.Output = out
	}
	shutdown, err := pingwright.Init(context.Background(), opts)
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })
	return shutdown, out
}
