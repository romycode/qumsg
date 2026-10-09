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
	router := qumsg.NewRouter()

	// Without the check, routing the type a second time compares two
	// valueTransport interfaces with == and panics.
	for range 2 {
		if err := router.Route[job](valueTransport{}); !errors.Is(err, qumsg.ErrUncomparableTransport) {
			t.Fatalf("got %v, want ErrUncomparableTransport", err)
		}
	}
	if err := router.RouteTypes(valueTransport{}, qumsg.TypeOf[userCreated]()); !errors.Is(err, qumsg.ErrUncomparableTransport) {
		t.Fatalf("RouteTypes: got %v, want ErrUncomparableTransport", err)
	}

	// A pointer to the same type is comparable.
	if err := router.Route[job](&valueTransport{}); err != nil {
		t.Fatal(err)
	}
}

func TestRoutedMessagesAreHandledAndSent(t *testing.T) {
	tr := memory.New()
	router := qumsg.NewRouter()
	if err := router.RouteTypes(tr, qumsg.TypeOf[userCreated](), qumsg.TypeOf[productCreated]()); err != nil {
		t.Fatal(err)
	}

	var handled []any
	var queued []int // messages in the transport when each handler ran
	var middleware int
	bus := qumsg.New(qumsg.WithRouter(router), qumsg.WithMiddleware(func(next qumsg.HandleFunc) qumsg.HandleFunc {
		return func(ctx context.Context, msg any) error {
			middleware++
			return next(ctx, msg)
		}
	}))
	record := func(_ context.Context, msg any) error {
		handled = append(handled, msg)
		queued = append(queued, tr.Len())
		return nil
	}
	if err := bus.RegisterFor(record, qumsg.TypeOf[userCreated](), qumsg.TypeOf[job]()); err != nil {
		t.Fatal(err)
	}

	// productCreated is routed without local handlers: no ErrNoHandler.
	for _, msg := range []any{userCreated{ID: "u"}, productCreated{ID: "p"}, job{N: 1}} {
		if err := bus.Dispatch(t.Context(), msg); err != nil {
			t.Fatal(err)
		}
	}

	if len(handled) != 2 || handled[0] != (userCreated{ID: "u"}) || handled[1] != (job{N: 1}) {
		t.Fatalf("got handled %v, want userCreated and job", handled)
	}
	if queued[0] != 0 {
		t.Fatal("userCreated was sent before its local handler ran")
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

func TestRoutedMessagesFollowTheErrorPolicy(t *testing.T) {
	boom := errors.New("boom")
	failing := func(context.Context, job) error { return boom }

	t.Run("a handler error prevents the send", func(t *testing.T) {
		tr := memory.New()
		router := qumsg.NewRouter()
		if err := router.Route[job](tr); err != nil {
			t.Fatal(err)
		}
		bus := qumsg.New(qumsg.WithRouter(router))
		if err := bus.Register(failing); err != nil {
			t.Fatal(err)
		}
		if err := bus.Dispatch(t.Context(), job{}); err != boom {
			t.Fatalf("got %v, want the handler error unwrapped", err)
		}
		if tr.Len() != 0 {
			t.Fatal("the message was sent")
		}
	})

	t.Run("ContinueOnError sends anyway and joins the errors", func(t *testing.T) {
		tr := memory.New()
		tr.Close() // Send fails with ErrTransportClosed
		router := qumsg.NewRouter()
		if err := router.Route[job](tr); err != nil {
			t.Fatal(err)
		}
		bus := qumsg.New(qumsg.WithRouter(router), qumsg.ContinueOnError())
		if err := bus.Register(failing); err != nil {
			t.Fatal(err)
		}
		err := bus.Dispatch(t.Context(), job{})
		if !errors.Is(err, boom) || !errors.Is(err, qumsg.ErrTransportClosed) {
			t.Fatalf("got %v, want both errors", err)
		}
	})

	t.Run("a send error alone is unwrapped", func(t *testing.T) {
		tr := memory.New()
		tr.Close()
		router := qumsg.NewRouter()
		if err := router.Route[job](tr); err != nil {
			t.Fatal(err)
		}
		if err := qumsg.New(qumsg.WithRouter(router)).Dispatch(t.Context(), job{}); err != qumsg.ErrTransportClosed {
			t.Fatalf("got %v, want ErrTransportClosed", err)
		}
	})
}

// The worker shares the router with the producer: what it consumes runs its
// handlers instead of being sent again.
func TestConsumeHandlesRoutedMessagesLocally(t *testing.T) {
	tr := memory.New()
	router := qumsg.NewRouter()
	if err := router.Route[job](tr); err != nil {
		t.Fatal(err)
	}
	if err := qumsg.New(qumsg.WithRouter(router)).Dispatch(t.Context(), job{N: 1}); err != nil {
		t.Fatal(err)
	}
	tr.Close()

	worker := qumsg.New(qumsg.WithRouter(router))
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

func TestRouterValidation(t *testing.T) {
	t.Run("nil transport", func(t *testing.T) {
		if err := qumsg.NewRouter().Route[job](nil); !errors.Is(err, qumsg.ErrNilTransport) {
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
			if err := qumsg.NewRouter().RouteTypes(memory.New(), tc.typ); !errors.Is(err, tc.want) {
				t.Errorf("%s: got %v, want %v", name, err, tc.want)
			}
		}
	})

	t.Run("one transport per type", func(t *testing.T) {
		router, a := qumsg.NewRouter(), memory.New()
		if err := router.Route[job](a); err != nil {
			t.Fatal(err)
		}
		if err := router.Route[job](a); err != nil {
			t.Fatalf("routing again to the same transport: %v", err)
		}
		if err := router.Route[job](memory.New()); !errors.Is(err, qumsg.ErrAlreadyRouted) {
			t.Fatalf("got %v, want ErrAlreadyRouted", err)
		}
	})

	t.Run("all or nothing", func(t *testing.T) {
		router, tr := qumsg.NewRouter(), memory.New()
		if err := router.Route[userCreated](tr); err != nil {
			t.Fatal(err)
		}
		err := router.RouteTypes(tr, qumsg.TypeOf[productCreated](), qumsg.TypeOf[sameName]())
		if !errors.Is(err, qumsg.ErrDuplicateName) {
			t.Fatalf("got %v, want ErrDuplicateName", err)
		}
		// productCreated was not routed: it is still handled locally.
		bus := qumsg.New(qumsg.WithRouter(router))
		if err := bus.Dispatch(t.Context(), productCreated{}); !errors.Is(err, qumsg.ErrNoHandler) {
			t.Fatalf("got %v, want ErrNoHandler", err)
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
	router := qumsg.NewRouter()
	if err := router.RouteTypes(tr, qumsg.TypeOf[userCreated](), qumsg.TypeOf[job]()); err != nil {
		t.Fatal(err)
	}
	if err := router.Route[productCreated](other); err != nil {
		t.Fatal(err)
	}

	if err := qumsg.New(qumsg.WithRouter(router)).Consume(t.Context(), tr); err != nil {
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
		if err := qumsg.New(qumsg.WithRouter(router)).Consume(t.Context(), tr); !errors.Is(err, broken) {
			t.Fatalf("got %v, want the Accept error", err)
		}
	})

	t.Run("no routes", func(t *testing.T) {
		if err := qumsg.New().Consume(t.Context(), tr); !errors.Is(err, qumsg.ErrNoRoutes) {
			t.Fatalf("got %v, want ErrNoRoutes", err)
		}
	})
}
