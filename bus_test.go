package qumsg_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/romycode/qumsg"
)

type userCreated struct{ ID string }

func (userCreated) Name() string { return "users.created" }

type productCreated struct{ ID string }

type sameName struct{}

func (sameName) Name() string { return "users.created" }

type emptyName struct{}

func (emptyName) Name() string { return "" }

type pointerNamed struct{}

func (*pointerNamed) Name() string { return "pointer.named" }

type samePointerName struct{}

func (samePointerName) Name() string { return "pointer.named" }

type fieldNamed struct{ meta *struct{ name string } }

// Name depends on a field, so it panics on the zero value used by Register.
func (m fieldNamed) Name() string { return m.meta.name }

type ctxKey struct{}

type customErr struct{ msg string }

func (e *customErr) Error() string { return e.msg }

func TestDispatchCallsHandlerForExactType(t *testing.T) {
	bus := qumsg.New()
	var got []string
	mustRegister(t, bus, func(_ context.Context, msg productCreated) error {
		got = append(got, "value:"+msg.ID)
		return nil
	})
	mustRegister(t, bus, func(_ context.Context, msg *productCreated) error {
		got = append(got, "pointer:"+msg.ID)
		return nil
	})

	if err := bus.Dispatch(t.Context(), productCreated{ID: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := bus.Dispatch(t.Context(), &productCreated{ID: "2"}); err != nil {
		t.Fatal(err)
	}

	want := []string{"value:1", "pointer:2"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestDispatchWithoutHandlerFails(t *testing.T) {
	bus := qumsg.New()
	mustRegister(t, bus, func(context.Context, productCreated) error { return nil })

	err := bus.Dispatch(t.Context(), &productCreated{})
	if !errors.Is(err, qumsg.ErrNoHandler) {
		t.Fatalf("got %v, want ErrNoHandler", err)
	}
}

func TestDispatchNilMessageFails(t *testing.T) {
	err := qumsg.New().Dispatch(t.Context(), nil)
	if !errors.Is(err, qumsg.ErrNilMessage) {
		t.Fatalf("got %v, want ErrNilMessage", err)
	}
}

func TestHandlersRunInRegistrationOrderAndStopOnFirstError(t *testing.T) {
	bus := qumsg.New()
	boom := errors.New("boom")
	var calls []int
	for i, err := range []error{nil, boom, nil} {
		mustRegister(t, bus, func(context.Context, productCreated) error {
			calls = append(calls, i)
			return err
		})
	}

	err := bus.Dispatch(t.Context(), productCreated{})
	if err != boom {
		t.Fatalf("got %v, want the handler error unchanged", err)
	}
	if want := []int{0, 1}; !slices.Equal(calls, want) {
		t.Fatalf("calls %v, want %v", calls, want)
	}
}

func TestHandlerErrorsKeepTheirIdentity(t *testing.T) {
	bus := qumsg.New(func(next qumsg.HandleFunc) qumsg.HandleFunc { return next })
	mustRegister(t, bus, func(context.Context, productCreated) error {
		return &customErr{msg: "custom"}
	})

	var target *customErr
	if err := bus.Dispatch(t.Context(), productCreated{}); !errors.As(err, &target) {
		t.Fatalf("got %v, want *customErr", err)
	}
}

func TestMiddlewareWrapsWholeDispatchInOrder(t *testing.T) {
	var events []string
	trace := func(name string) qumsg.Middleware {
		return func(next qumsg.HandleFunc) qumsg.HandleFunc {
			return func(ctx context.Context, msg any) error {
				events = append(events, name+":in")
				err := next(ctx, msg)
				events = append(events, name+":out")
				return err
			}
		}
	}
	bus := qumsg.New(trace("first"), trace("second"))
	for _, name := range []string{"h1", "h2"} {
		mustRegister(t, bus, func(context.Context, productCreated) error {
			events = append(events, name)
			return nil
		})
	}

	if err := bus.Dispatch(t.Context(), productCreated{}); err != nil {
		t.Fatal(err)
	}

	want := []string{"first:in", "second:in", "h1", "h2", "second:out", "first:out"}
	if !slices.Equal(events, want) {
		t.Fatalf("got %v, want %v", events, want)
	}
}

func TestMiddlewareCanShortCircuit(t *testing.T) {
	denied := errors.New("denied")
	bus := qumsg.New(func(qumsg.HandleFunc) qumsg.HandleFunc {
		return func(context.Context, any) error { return denied }
	})

	// No handler registered: the middleware stops the flow before resolution.
	if err := bus.Dispatch(t.Context(), productCreated{}); err != denied {
		t.Fatalf("got %v, want %v", err, denied)
	}
}

func TestNewPanicsOnNilMiddleware(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	qumsg.New(nil)
}

func TestContextPropagatesThroughMiddlewareToHandlers(t *testing.T) {
	bus := qumsg.New(func(next qumsg.HandleFunc) qumsg.HandleFunc {
		return func(ctx context.Context, msg any) error {
			return next(context.WithValue(ctx, ctxKey{}, "from-middleware"), msg)
		}
	})
	var got any
	mustRegister(t, bus, func(ctx context.Context, _ productCreated) error {
		got = ctx.Value(ctxKey{})
		return nil
	})

	if err := bus.Dispatch(t.Context(), productCreated{}); err != nil {
		t.Fatal(err)
	}
	if got != "from-middleware" {
		t.Fatalf("got %v", got)
	}
}

func TestCancellationIsCheckedBetweenHandlers(t *testing.T) {
	bus := qumsg.New()
	ctx, cancel := context.WithCancel(t.Context())
	secondCalled := false
	mustRegister(t, bus, func(context.Context, productCreated) error {
		cancel()
		return nil
	})
	mustRegister(t, bus, func(context.Context, productCreated) error {
		secondCalled = true
		return nil
	})

	err := bus.Dispatch(ctx, productCreated{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	if secondCalled {
		t.Fatal("second handler ran after cancellation")
	}
}

func TestCanceledContextRunsNoHandler(t *testing.T) {
	bus := qumsg.New()
	called := false
	mustRegister(t, bus, func(context.Context, productCreated) error {
		called = true
		return nil
	})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := bus.Dispatch(ctx, productCreated{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	if called {
		t.Fatal("handler ran with a canceled context")
	}
}

func TestRegisterValidation(t *testing.T) {
	t.Run("nil handler", func(t *testing.T) {
		err := qumsg.New().Register[productCreated](nil)
		if !errors.Is(err, qumsg.ErrNilHandler) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("interface type", func(t *testing.T) {
		err := qumsg.New().Register(func(context.Context, qumsg.NamedMessage) error { return nil })
		if !errors.Is(err, qumsg.ErrInterfaceType) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("empty name", func(t *testing.T) {
		err := qumsg.New().Register(func(context.Context, emptyName) error { return nil })
		if !errors.Is(err, qumsg.ErrEmptyName) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("name panics", func(t *testing.T) {
		bus := qumsg.New()
		err := bus.Register(func(context.Context, fieldNamed) error { return nil })
		if !errors.Is(err, qumsg.ErrNamePanic) {
			t.Fatalf("got %v, want ErrNamePanic", err)
		}
		if err := bus.Dispatch(t.Context(), fieldNamed{}); !errors.Is(err, qumsg.ErrNoHandler) {
			t.Fatalf("rejected handler was added: %v", err)
		}
	})

	t.Run("same type and name twice", func(t *testing.T) {
		bus := qumsg.New()
		mustRegister(t, bus, func(context.Context, userCreated) error { return nil })
		mustRegister(t, bus, func(context.Context, userCreated) error { return nil })
	})

	t.Run("name collision between types", func(t *testing.T) {
		bus := qumsg.New()
		mustRegister(t, bus, func(context.Context, userCreated) error { return nil })

		err := bus.Register(func(context.Context, sameName) error { return nil })
		if !errors.Is(err, qumsg.ErrDuplicateName) {
			t.Fatalf("got %v", err)
		}
		if err := bus.Dispatch(t.Context(), sameName{}); !errors.Is(err, qumsg.ErrNoHandler) {
			t.Fatalf("rejected handler was added: %v", err)
		}
	})

	t.Run("value and pointer share the alias", func(t *testing.T) {
		bus := qumsg.New()
		mustRegister(t, bus, func(context.Context, userCreated) error { return nil })

		// *userCreated inherits Name; reading it must not dereference nil.
		err := bus.Register(func(context.Context, *userCreated) error { return nil })
		if !errors.Is(err, qumsg.ErrDuplicateName) {
			t.Fatalf("got %v", err)
		}
	})
}

func TestPointerTypeRegistersItsAlias(t *testing.T) {
	bus := qumsg.New()
	mustRegister(t, bus, func(context.Context, *pointerNamed) error { return nil })

	err := bus.Register(func(context.Context, samePointerName) error { return nil })
	if !errors.Is(err, qumsg.ErrDuplicateName) {
		t.Fatalf("got %v, want ErrDuplicateName", err)
	}
}

func TestRegisterDuringDispatchesIsRaceFree(t *testing.T) {
	bus := qumsg.New()
	mustRegister(t, bus, func(context.Context, productCreated) error { return nil })

	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			if err := bus.Dispatch(t.Context(), productCreated{}); err != nil {
				t.Error(err)
			}
		})
		wg.Go(func() {
			if err := bus.Register(func(context.Context, productCreated) error { return nil }); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
}

func TestConcurrentDispatches(t *testing.T) {
	bus := qumsg.New()
	var count atomic.Int64
	mustRegister(t, bus, func(context.Context, productCreated) error {
		count.Add(1)
		return nil
	})
	mustRegister(t, bus, func(context.Context, userCreated) error {
		count.Add(1)
		return nil
	})

	const n = 100
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			var msg any = productCreated{}
			if i%2 == 0 {
				msg = userCreated{}
			}
			if err := bus.Dispatch(t.Context(), msg); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()

	if got := count.Load(); got != n {
		t.Fatalf("got %d handler calls, want %d", got, n)
	}
}

func mustRegister[T any](t *testing.T, bus *qumsg.Bus, handler func(context.Context, T) error) {
	t.Helper()
	if err := bus.Register(handler); err != nil {
		t.Fatal(err)
	}
}
