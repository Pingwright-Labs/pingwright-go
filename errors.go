package pingwright

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"runtime"
	"strings"
	"time"
)

// CaptureError reports err to Pingwright as an error event: a log record at
// ERROR with exception.type, exception.message and exception.stacktrace.
// The stack trace starts at the caller of CaptureError. Pingwright groups
// events into issues by type and the top frames of your own code.
//
// exception.type is the Go type (%T) of the innermost error under pure
// wrappers (fmt.Errorf %w, errors.Join, *url.Error, github.com/pkg/errors),
// so fmt.Errorf("fetch: %w", opErr) is reported as *net.OpError.
// exception.message is err.Error(). attrs are added to the record.
//
// The trace and span ids in ctx, if any, are attached. A nil err does
// nothing. CaptureError never panics on a nil ctx.
func CaptureError(ctx context.Context, err error, attrs ...slog.Attr) {
	if err == nil {
		return
	}
	frames := callers(0)
	emit(ctx, slog.LevelError, err.Error(), exceptionType(err), err.Error(), formatStack(frames), topPC(frames), attrs)
}

// Recover reports a panic as an error event at FATAL, flushes it, and
// panics again with the same value, so the process still crashes as it
// would have. Use it as the first deferred call of main and of goroutines:
//
//	go func() {
//		defer pingwright.Recover(ctx)
//		work()
//	}()
//
// The stack trace starts at the frame that panicked. Recover must be called
// directly by defer (not from inside another deferred function), as with
// the built-in recover. http.ErrAbortHandler is re-panicked without a report.
func Recover(ctx context.Context) {
	v := recover()
	if v == nil {
		return
	}
	if v != http.ErrAbortHandler {
		capturePanic(ctx, LevelFatal, v)
		fctx, cancel := context.WithTimeout(context.Background(), panicFlush)
		active().flush(fctx)
		cancel()
	}
	panic(v)
}

// capturePanic must be called from the deferred function that recovered.
func capturePanic(ctx context.Context, level slog.Level, v any, attrs ...slog.Attr) {
	typ, msg := fmt.Sprintf("%T", v), fmt.Sprint(v)
	if err, ok := v.(error); ok {
		typ, msg = exceptionType(err), err.Error()
	}
	frames := panicFrames(callers(0))
	emit(ctx, level, "panic: "+msg, typ, msg, formatStack(frames), topPC(frames), attrs)
}

func emit(ctx context.Context, level slog.Level, body, typ, msg, stack string, pc uintptr, attrs []slog.Attr) {
	if ctx == nil {
		ctx = context.Background()
	}
	c := active()
	h := c.errHandler
	if !h.Enabled(ctx, level) {
		return
	}
	r := slog.NewRecord(time.Now(), level, body, pc)
	if c.exceptions {
		r.AddAttrs(
			slog.String("exception.type", typ),
			slog.String("exception.message", msg),
			slog.String("exception.stacktrace", stack),
		)
	} else {
		r.AddAttrs(slog.String("error.type", typ))
	}
	r.AddAttrs(attrs...)
	_ = h.Handle(ctx, r)
}

// wrapperTypes add context to an error message but say nothing about what
// went wrong, so exception.type looks through them.
var wrapperTypes = map[string]bool{
	"*fmt.wrapError":      true,
	"*fmt.wrapErrors":     true,
	"*errors.joinError":   true,
	"*url.Error":          true,
	"*errors.withStack":   true, // github.com/pkg/errors
	"*errors.withMessage": true, // github.com/pkg/errors
	"*xerrors.wrapError":  true,
}

func exceptionType(err error) string {
	for wrapperTypes[fmt.Sprintf("%T", err)] {
		var next error
		switch u := err.(type) {
		case interface{ Unwrap() error }:
			next = u.Unwrap()
		case interface{ Unwrap() []error }:
			for _, e := range u.Unwrap() {
				if e != nil {
					next = e
					break
				}
			}
		}
		if next == nil {
			break
		}
		err = next
	}
	return fmt.Sprintf("%T", err)
}

// callers returns the frames starting at the caller of the function that
// called callers, skipping skip more frames.
func callers(skip int) []runtime.Frame {
	pcs := make([]uintptr, 64)
	n := runtime.Callers(skip+3, pcs)
	it := runtime.CallersFrames(pcs[:n])
	var out []runtime.Frame
	for {
		f, more := it.Next()
		if f.Function != "" || f.File != "" {
			out = append(out, f)
		}
		if !more {
			return out
		}
	}
}

// panicFrames drops everything up to and including runtime.gopanic and the
// runtime frames right after it (sigpanic, panicmem, goPanicIndex, ...), so
// the first frame is the code that panicked.
func panicFrames(frames []runtime.Frame) []runtime.Frame {
	cut := -1
	for i, f := range frames {
		if f.Function == "runtime.gopanic" || f.Function == "panic" {
			cut = i + 1
		}
	}
	if cut < 0 {
		return frames
	}
	for cut < len(frames) && strings.HasPrefix(frames[cut].Function, "runtime.") {
		cut++
	}
	return frames[cut:]
}

// formatStack renders frames in the format of runtime/debug.Stack and Go's
// crash output, which Pingwright parses:
//
//	goroutine 7 [running]:
//	main.handler(...)
//		/app/main.go:42 +0x1d
func formatStack(frames []runtime.Frame) string {
	var b strings.Builder
	b.WriteString(goroutineHeader())
	b.WriteByte('\n')
	for _, f := range frames {
		fn := f.Function
		if fn == "" {
			fn = "?"
		}
		fmt.Fprintf(&b, "%s(...)\n\t%s:%d", fn, f.File, f.Line)
		if f.Entry != 0 && f.PC >= f.Entry {
			fmt.Fprintf(&b, " +0x%x", f.PC-f.Entry)
		}
		b.WriteByte('\n')
	}
	return b.String()
}

func topPC(frames []runtime.Frame) uintptr {
	if len(frames) == 0 {
		return 0
	}
	return frames[0].PC
}

func goroutineHeader() string {
	buf := make([]byte, 64)
	buf = buf[:runtime.Stack(buf, false)]
	if i := bytes.IndexByte(buf, '\n'); i >= 0 {
		buf = buf[:i]
	}
	if !bytes.HasPrefix(buf, []byte("goroutine ")) {
		return "goroutine 0 [running]:"
	}
	return string(buf)
}
