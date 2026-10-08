package qumsg_test

import (
	"context"
	"testing"

	"github.com/romycode/qumsg"
)

type benchMessage struct{ ID int }

type benchPointer struct{ ID int }

func noop(context.Context, benchMessage) error { return nil }

func passthrough(next qumsg.HandleFunc) qumsg.HandleFunc {
	return func(ctx context.Context, msg any) error { return next(ctx, msg) }
}

// BenchmarkDirectCall is the baseline: calling the handler without the bus.
func BenchmarkDirectCall(b *testing.B) {
	ctx := b.Context()
	h := noop
	for b.Loop() {
		_ = h(ctx, benchMessage{ID: 1})
	}
}

func BenchmarkDispatch(b *testing.B) {
	for _, bc := range []struct {
		name        string
		handlers    int
		middlewares int
	}{
		{"1_handler", 1, 0},
		{"10_handlers", 10, 0},
		{"1_handler_3_middlewares", 1, 3},
	} {
		b.Run(bc.name, func(b *testing.B) {
			mws := make([]qumsg.Middleware, bc.middlewares)
			for i := range mws {
				mws[i] = passthrough
			}
			bus := qumsg.New(mws...)
			for range bc.handlers {
				_ = bus.Register(noop)
			}
			ctx := b.Context()

			b.ReportAllocs()
			for b.Loop() {
				_ = bus.Dispatch(ctx, benchMessage{ID: 1})
			}
		})
	}
}

// BenchmarkDispatchPointer avoids boxing the message into an interface.
func BenchmarkDispatchPointer(b *testing.B) {
	bus := qumsg.New()
	_ = bus.Register(func(context.Context, *benchPointer) error { return nil })
	msg := &benchPointer{ID: 1}
	ctx := b.Context()

	b.ReportAllocs()
	for b.Loop() {
		_ = bus.Dispatch(ctx, msg)
	}
}

func BenchmarkDispatchNoHandler(b *testing.B) {
	bus := qumsg.New()
	ctx := b.Context()

	b.ReportAllocs()
	for b.Loop() {
		_ = bus.Dispatch(ctx, benchMessage{ID: 1})
	}
}

func BenchmarkDispatchParallel(b *testing.B) {
	bus := qumsg.New()
	_ = bus.Register(noop)
	ctx := b.Context()

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = bus.Dispatch(ctx, benchMessage{ID: 1})
		}
	})
}
