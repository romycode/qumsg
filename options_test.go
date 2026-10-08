package qumsg_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/romycode/qumsg"
)

func TestAllowNoHandler(t *testing.T) {
	bus := qumsg.New(qumsg.AllowNoHandler())
	if err := bus.Dispatch(t.Context(), userCreated{}); err != nil {
		t.Fatalf("got %v, want nil", err)
	}
	if err := bus.Dispatch(t.Context(), nil); !errors.Is(err, qumsg.ErrNilMessage) {
		t.Fatalf("nil message: got %v, want ErrNilMessage", err)
	}
}

func TestContinueOnErrorRunsEveryHandlerAndJoinsErrors(t *testing.T) {
	bus := qumsg.New(qumsg.ContinueOnError())
	first, second := errors.New("first"), errors.New("second")
	var calls []string
	register := func(name string, err error) {
		t.Helper()
		mustRegister(t, bus, func(context.Context, userCreated) error {
			calls = append(calls, name)
			return err
		})
	}
	register("a", first)
	register("b", nil)
	register("c", second)

	err := bus.Dispatch(t.Context(), userCreated{})
	if !errors.Is(err, first) || !errors.Is(err, second) {
		t.Fatalf("got %v, want both errors", err)
	}
	if !slices.Equal(calls, []string{"a", "b", "c"}) {
		t.Fatalf("calls %v, want every handler in order", calls)
	}
}

func TestContinueOnErrorReturnsASingleErrorUnwrapped(t *testing.T) {
	bus := qumsg.New(qumsg.ContinueOnError())
	want := &customErr{msg: "boom"}
	mustRegister(t, bus, func(context.Context, userCreated) error { return nil })
	mustRegister(t, bus, func(context.Context, userCreated) error { return want })

	if err := bus.Dispatch(t.Context(), userCreated{}); err != want {
		t.Fatalf("got %#v, want the handler error itself", err)
	}
	if err := bus.Dispatch(t.Context(), productCreated{}); !errors.Is(err, qumsg.ErrNoHandler) {
		t.Fatalf("ContinueOnError alone keeps ErrNoHandler: got %v", err)
	}
}

func TestContinueOnErrorStopsWhenTheContextIsDone(t *testing.T) {
	bus := qumsg.New(qumsg.ContinueOnError())
	ctx, cancel := context.WithCancel(t.Context())
	boom := errors.New("boom")
	var calls int
	mustRegister(t, bus, func(context.Context, userCreated) error { calls++; cancel(); return boom })
	mustRegister(t, bus, func(context.Context, userCreated) error { calls++; return nil })

	err := bus.Dispatch(ctx, userCreated{})
	if !errors.Is(err, boom) || !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want the handler error and context.Canceled", err)
	}
	if calls != 1 {
		t.Fatalf("%d handlers ran, want 1", calls)
	}
}

func TestOptionsCombine(t *testing.T) {
	var seen []any
	bus := qumsg.New(
		qumsg.AllowNoHandler(),
		qumsg.ContinueOnError(),
		qumsg.WithMiddleware(func(next qumsg.HandleFunc) qumsg.HandleFunc {
			return func(ctx context.Context, msg any) error {
				seen = append(seen, msg)
				return next(ctx, msg)
			}
		}),
	)
	if err := bus.Dispatch(t.Context(), userCreated{ID: "1"}); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 1 {
		t.Fatalf("middleware saw %v", seen)
	}
}
