package pingwright_test

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/Pingwright-Labs/pingwright-go"
)

func Example() {
	ctx := context.Background()
	shutdown, err := pingwright.Init(ctx, pingwright.Options{
		Key:         os.Getenv("PINGWRIGHT_KEY"),
		Service:     "billing-api",
		Environment: "production",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdown(ctx)
	}()
	defer pingwright.Recover(ctx)

	slog.Info("started", "port", 8080)
}

func ExampleMiddleware() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		slog.InfoContext(r.Context(), "loading order", "id", r.PathValue("id"))
	})
	log.Fatal(http.ListenAndServe(":8080", pingwright.Middleware(mux)))
}

func ExampleCaptureError() {
	ctx := context.Background()
	if err := charge(ctx); err != nil {
		pingwright.CaptureError(ctx, err, slog.String("order.id", "o-123"))
	}
}

func ExampleRecover() {
	ctx := context.Background()
	go func() {
		defer pingwright.Recover(ctx)
		processQueue(ctx)
	}()
}

func ExampleMeter() {
	ctx := context.Background()
	jobs, _ := pingwright.Meter("worker").Int64Counter("jobs.processed")
	jobs.Add(ctx, 1)
}

func charge(context.Context) error { return errors.New("card declined") }
func processQueue(context.Context) {}
