// Command basic is a small HTTP service that sends its logs, metrics and
// errors to Pingwright.
//
//	PINGWRIGHT_KEY=pwi_... go run ./examples/basic
//	curl localhost:8080/hello/world
//	curl localhost:8080/fail
//	curl localhost:8080/panic
//
// Without PINGWRIGHT_KEY it runs in local mode and only logs to stderr.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Pingwright-Labs/pingwright-go"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdown, err := pingwright.Init(ctx, pingwright.Options{
		Service:     "example-basic",
		Environment: "development",
		Version:     "0.1.0",
	})
	if err != nil {
		slog.Error("pingwright init", "error", err)
		os.Exit(1)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdown(ctx)
	}()
	defer pingwright.Recover(ctx)

	greetings, _ := pingwright.Meter("example-basic").Int64Counter("greetings")

	mux := http.NewServeMux()
	mux.HandleFunc("GET /hello/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		greetings.Add(r.Context(), 1)
		slog.InfoContext(r.Context(), "greeting", "name", name)
		fmt.Fprintf(w, "hello, %s\n", name)
	})
	mux.HandleFunc("GET /fail", func(w http.ResponseWriter, r *http.Request) {
		err := fmt.Errorf("loading invoice: %w", errors.New("database unavailable"))
		pingwright.CaptureError(r.Context(), err)
		http.Error(w, "try again later", http.StatusServiceUnavailable)
	})
	mux.HandleFunc("GET /panic", func(w http.ResponseWriter, r *http.Request) {
		items := r.URL.Query()["item"]
		fmt.Fprintln(w, items[0]) // index out of range without ?item=: Middleware reports it and answers 500
	})

	srv := &http.Server{Addr: ":8080", Handler: pingwright.Middleware(mux), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	slog.Info("listening", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server", "error", err)
	}
}
