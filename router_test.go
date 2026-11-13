package qumsg_test

import (
	"context"
	"errors"
	"testing"

	"github.com/romycode/qumsg"
	"github.com/romycode/qumsg/memory"
)

// valueTransport is not comparable: a slice field and value receivers.
type valueTransport struct{ sent []qumsg.Envelope }

func (valueTransport) Send(context.Context, qumsg.Envelope) error { return nil }
func (valueTransport) Receive(context.Context) (qumsg.Delivery, error) {
	return nil, qumsg.ErrTransportClosed
}

func TestRouteRejectsUncomparableTransports(t *testing.T) {
	bus := qumsg.New()

	// Without the check, routing the type a second time compares two
	// valueTransport interfaces with == and panics.
	for range 2 {
		if err := bus.Route[job](valueTransport{}); !errors.Is(err, qumsg.ErrUncomparableTransport) {
			t.Fatalf("got %v, want ErrUncomparableTransport", err)
		}
	}
	if err := bus.RouteTypes(valueTransport{}, qumsg.TypeOf[userCreated]()); !errors.Is(err, qumsg.ErrUncomparableTransport) {
		t.Fatalf("RouteTypes: got %v, want ErrUncomparableTransport", err)
	}

	// A pointer to the same type is comparable.
	if err := bus.Route[job](&valueTransport{}); err != nil {
		t.Fatal(err)
	}
}

func TestRoutedMessagesAreOnlySent(t *testing.T) {
	tr := memory.New()
	var middleware int
	bus := qumsg.New(qumsg.WithMiddleware(func(next qumsg.HandleFunc) qumsg.HandleFunc {
		return func(ctx context.Context, msg any) error {
			middleware++
			return next(ctx, msg)
		}
	}))
	if err := bus.RouteTypes(tr, qumsg.TypeOf[userCreated](), qumsg.TypeOf[productCreated]()); err != nil {
		t.Fatal(err)
	}

	var handled []any
	record := func(_ context.Context, msg any) error {
		handled = append(handled, msg)
		return nil
	}
	if err := bus.RegisterFor(record, qumsg.TypeOf[userCreated](), qumsg.TypeOf[job]()); err != nil {
		t.Fatal(err)
	}

	// productCreated is routed without handlers: no ErrNoHandler.
	for _, msg := range []any{userCreated{ID: "u"}, productCreated{ID: "p"}, job{N: 1}} {
		if err := bus.Dispatch(t.Context(), msg); err != nil {
			t.Fatal(err)
		}
	}

	// userCreated has a handler, but it is routed: Dispatch only sends it.
	if len(handled) != 1 || handled[0] != (job{N: 1}) {
		t.Fatalf("got handled %v, want only job", handled)
	}
	if middleware != 3 {
		t.Fatalf("middleware ran %d times, want 3", middleware)
	}
	for _, want := range []any{userCreated{ID: "u"}, productCreated{ID: "p"}} {
		d, err := tr.Receive(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if d.Envelope().Message() != want {
			t.Fatalf("got %#v, want %#v", d.Envelope().Message(), want)
		}
	}
}

func TestDispatchReturnsTheSendErrorUnwrapped(t *testing.T) {
	tr := memory.New()
	tr.Close()
	bus := qumsg.New(qumsg.ContinueOnError())
	if err := bus.Route[job](tr); err != nil {
		t.Fatal(err)
	}
	if err := bus.Dispatch(t.Context(), job{}); err != qumsg.ErrTransportClosed {
		t.Fatalf("got %v, want ErrTransportClosed", err)
	}
}

// One bus sends and consumes: Dispatch sends the routed message and Consume
// runs its handler once.
func TestOneBusSendsAndConsumes(t *testing.T) {
	tr := memory.New()
	bus := qumsg.New()
	var got []job
	if err := bus.Register(func(_ context.Context, j job) error { got = append(got, j); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := bus.Route[job](tr); err != nil {
		t.Fatal(err)
	}
	if err := bus.Dispatch(t.Context(), job{N: 1}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatal("Dispatch ran the handler of a routed type")
	}
	tr.Close()
	if err := bus.Consume(t.Context(), tr); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != (job{N: 1}) || tr.Len() != 0 {
		t.Fatalf("got %v, %d queued", got, tr.Len())
	}
}

// The worker configures the same routes as the producer: what it consumes
// runs its handlers instead of being sent again.
func TestConsumeHandlesRoutedMessagesLocally(t *testing.T) {
	tr := memory.New()
	producer := qumsg.New()
	if err := producer.Route[job](tr); err != nil {
		t.Fatal(err)
	}
	if err := producer.Dispatch(t.Context(), job{N: 1}); err != nil {
		t.Fatal(err)
	}
	tr.Close()

	worker := qumsg.New()
	if err := worker.Route[job](tr); err != nil {
		t.Fatal(err)
	}
	var got []job
	if err := worker.Register(func(_ context.Context, j job) error { got = append(got, j); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := worker.Consume(t.Context(), tr); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != (job{N: 1}) || tr.Len() != 0 {
		t.Fatalf("got %v, %d queued", got, tr.Len())
	}
}

func TestRouteValidation(t *testing.T) {
	t.Run("nil transport", func(t *testing.T) {
		if err := qumsg.New().Route[job](nil); !errors.Is(err, qumsg.ErrNilTransport) {
			t.Fatalf("got %v, want ErrNilTransport", err)
		}
	})

	t.Run("same rules as Register", func(t *testing.T) {
		for name, tc := range map[string]struct {
			typ  qumsg.Type
			want error
		}{
			"empty name": {qumsg.TypeOf[emptyName](), qumsg.ErrEmptyName},
			"interface":  {qumsg.TypeOf[qumsg.NamedMessage](), qumsg.ErrInterfaceType},
			"zero Type":  {qumsg.Type{}, qumsg.ErrZeroType},
		} {
			if err := qumsg.New().RouteTypes(memory.New(), tc.typ); !errors.Is(err, tc.want) {
				t.Errorf("%s: got %v, want %v", name, err, tc.want)
			}
		}
	})

	t.Run("one transport per type", func(t *testing.T) {
		bus, a := qumsg.New(), memory.New()
		if err := bus.Route[job](a); err != nil {
			t.Fatal(err)
		}
		if err := bus.Route[job](a); err != nil {
			t.Fatalf("routing again to the same transport: %v", err)
		}
		if err := bus.Route[job](memory.New()); !errors.Is(err, qumsg.ErrAlreadyRouted) {
			t.Fatalf("got %v, want ErrAlreadyRouted", err)
		}
	})

	t.Run("all or nothing", func(t *testing.T) {
		bus, tr := qumsg.New(), memory.New()
		if err := bus.Route[userCreated](tr); err != nil {
			t.Fatal(err)
		}
		err := bus.RouteTypes(tr, qumsg.TypeOf[productCreated](), qumsg.TypeOf[sameName]())
		if !errors.Is(err, qumsg.ErrDuplicateName) {
			t.Fatalf("got %v, want ErrDuplicateName", err)
		}
		// productCreated was not routed: it is still handled locally.
		if err := bus.Dispatch(t.Context(), productCreated{}); !errors.Is(err, qumsg.ErrNoHandler) {
			t.Fatalf("got %v, want ErrNoHandler", err)
		}
	})

	t.Run("aliases are shared with Register", func(t *testing.T) {
		bus := qumsg.New()
		if err := bus.Register(func(context.Context, userCreated) error { return nil }); err != nil {
			t.Fatal(err)
		}
		if err := bus.Route[sameName](memory.New()); !errors.Is(err, qumsg.ErrDuplicateName) {
			t.Fatalf("Route: got %v, want ErrDuplicateName", err)
		}
		// Routing the registered type itself reuses its alias.
		if err := bus.Route[userCreated](memory.New()); err != nil {
			t.Fatal(err)
		}

		bus = qumsg.New()
		if err := bus.Route[userCreated](memory.New()); err != nil {
			t.Fatal(err)
		}
		if err := bus.Register(func(context.Context, sameName) error { return nil }); !errors.Is(err, qumsg.ErrDuplicateName) {
			t.Fatalf("Register: got %v, want ErrDuplicateName", err)
		}
	})
}

// decoderTransport records the types that Consume accepts.
type decoderTransport struct {
	*memory.Transport
	accepted  []qumsg.Type
	acceptErr error
}

func (d *decoderTransport) Accept(types ...qumsg.Type) error {
	d.accepted = append(d.accepted, types...)
	return d.acceptErr
}

func TestConsumeTellsADecoderTheRoutedTypes(t *testing.T) {
	tr := &decoderTransport{Transport: memory.New()}
	tr.Close()
	other := memory.New()
	bus := qumsg.New()
	if err := bus.RouteTypes(tr, qumsg.TypeOf[userCreated](), qumsg.TypeOf[job]()); err != nil {
		t.Fatal(err)
	}
	if err := bus.Route[productCreated](other); err != nil {
		t.Fatal(err)
	}

	if err := bus.Consume(t.Context(), tr); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, typ := range tr.accepted {
		got = append(got, typ.String())
	}
	if want := []string{"qumsg_test.job", "qumsg_test.userCreated"}; len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("accepted %v, want %v", got, want)
	}

	t.Run("Accept error", func(t *testing.T) {
		broken := errors.New("broken")
		tr.acceptErr = broken
		if err := bus.Consume(t.Context(), tr); !errors.Is(err, broken) {
			t.Fatalf("got %v, want the Accept error", err)
		}
	})

	t.Run("no routes", func(t *testing.T) {
		if err := qumsg.New().Consume(t.Context(), tr); !errors.Is(err, qumsg.ErrNoRoutes) {
			t.Fatalf("got %v, want ErrNoRoutes", err)
		}
	})
}
