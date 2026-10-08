package qumsg_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/romycode/qumsg"
	"github.com/romycode/qumsg/memory"
)

// Acceptance tests: each example reproduces a README section and fails if the
// documented behaviour changes.

type UserCreated struct {
	ID    string
	Email string
}

func (UserCreated) Name() string {
	return "users.created"
}

func welcomeUser(ctx context.Context, msg UserCreated) error {
	fmt.Printf("Welcome, %s\n", msg.Email)
	return nil
}

// README: "Primer vistazo".
func Example() {
	run := func(ctx context.Context) error {
		bus := qumsg.New()

		if err := bus.Register(welcomeUser); err != nil {
			return err
		}

		return bus.Dispatch(ctx, UserCreated{
			ID:    "user-123",
			Email: "albert@example.com",
		})
	}

	if err := run(context.Background()); err != nil {
		fmt.Println("error:", err)
	}
	// Output:
	// Welcome, albert@example.com
}

// README: "Varios handlers". Handlers run in registration order and the first
// error stops the rest and reaches the caller unchanged.
func ExampleBus_Register_multipleHandlers() {
	errAudit := errors.New("audit unavailable")
	bus := qumsg.New()

	_ = bus.Register(welcomeUser)
	_ = bus.Register(func(ctx context.Context, msg UserCreated) error {
		fmt.Println("Auditing", msg.ID)
		return errAudit
	})
	_ = bus.Register(func(ctx context.Context, msg UserCreated) error {
		fmt.Println("Never reached")
		return nil
	})

	err := bus.Dispatch(context.Background(), UserCreated{ID: "user-123", Email: "albert@example.com"})
	fmt.Println("errors.Is:", errors.Is(err, errAudit))
	// Output:
	// Welcome, albert@example.com
	// Auditing user-123
	// errors.Is: true
}

// README: "Orden de ejecución". The first middleware is the outermost and the
// chain runs once around all handlers.
func ExampleMiddleware() {
	trace := func(name string) qumsg.Middleware {
		return func(next qumsg.HandleFunc) qumsg.HandleFunc {
			return func(ctx context.Context, msg any) error {
				fmt.Println(name, "in")
				err := next(ctx, msg)
				fmt.Println(name, "out")
				return err
			}
		}
	}

	bus := qumsg.New(qumsg.WithMiddleware(trace("first"), trace("second")))
	_ = bus.Register(func(context.Context, UserCreated) error {
		fmt.Println("handler 1")
		return nil
	})
	_ = bus.Register(func(context.Context, UserCreated) error {
		fmt.Println("handler 2")
		return nil
	})

	_ = bus.Dispatch(context.Background(), UserCreated{})
	// Output:
	// first in
	// second in
	// handler 1
	// handler 2
	// second out
	// first out
}

// Logging is the middleware from the README "Middleware" section.
func Logging(logger *slog.Logger) qumsg.Middleware {
	return func(next qumsg.HandleFunc) qumsg.HandleFunc {
		return func(ctx context.Context, msg any) error {
			started := time.Now()
			err := next(ctx, msg)

			attrs := []any{
				"message_type", fmt.Sprintf("%T", msg),
				"duration", time.Since(started),
			}

			if named, ok := msg.(qumsg.NamedMessage); ok {
				attrs = append(attrs, "message_name", named.Name())
			}

			if err != nil {
				attrs = append(attrs, "error", err)
				logger.ErrorContext(ctx, "Message dispatch failed", attrs...)
			} else {
				logger.InfoContext(ctx, "Message dispatched", attrs...)
			}

			return err
		}
	}
}

type OrderPlaced struct{}

// README: "Middleware" and "Nombre opcional". The logging middleware observes
// every message type and reports the optional alias.
func ExampleNew_logging() {
	// Drop time and duration so the output is deterministic.
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey || a.Key == "duration" {
				return slog.Attr{}
			}
			return a
		},
	}))

	bus := qumsg.New(qumsg.WithMiddleware(Logging(logger)))
	_ = bus.Register(func(context.Context, UserCreated) error { return nil })

	_ = bus.Dispatch(context.Background(), UserCreated{ID: "user-123"})
	_ = bus.Dispatch(context.Background(), OrderPlaced{}) // no handler
	// Output:
	// level=INFO msg="Message dispatched" message_type=qumsg_test.UserCreated message_name=users.created
	// level=ERROR msg="Message dispatch failed" message_type=qumsg_test.OrderPlaced error="qumsg: no handler registered for message type: qumsg_test.OrderPlaced"
}

type AccountCreated struct{}

func (AccountCreated) Name() string { return "users.created" }

// README: "Nombre opcional". Another type cannot reuse an alias, and T and *T
// are different types that share it.
func ExampleNamedMessage() {
	bus := qumsg.New()
	_ = bus.Register(welcomeUser)

	err := bus.Register(func(context.Context, AccountCreated) error { return nil })
	fmt.Println(errors.Is(err, qumsg.ErrDuplicateName))

	err = bus.Register(func(context.Context, *UserCreated) error { return nil })
	fmt.Println(errors.Is(err, qumsg.ErrDuplicateName))
	// Output:
	// true
	// true
}

type SendWelcomeEmail struct {
	Email string
}

// README: "Ejecución asíncrona". Both processes share the router: the
// producer sends the routed message and the worker consumes it with the real
// handler.
func ExampleBus_Consume() {
	ctx := context.Background()
	transport := memory.New()

	router := qumsg.NewRouter()
	_ = router.Route[SendWelcomeEmail](transport)

	producer := qumsg.New(qumsg.WithRouter(router))

	worker := qumsg.New(qumsg.WithRouter(router))
	_ = worker.Register(func(ctx context.Context, msg SendWelcomeEmail) error {
		fmt.Println("Sending welcome email to", msg.Email)
		return nil
	})

	_ = producer.Dispatch(ctx, SendWelcomeEmail{Email: "albert@example.com"})
	_ = producer.Dispatch(ctx, SendWelcomeEmail{Email: "ana@example.com"})
	fmt.Println("Queued:", transport.Len())

	transport.Close() // Consume returns once the queue is drained
	if err := worker.Consume(ctx, transport); err != nil {
		fmt.Println("error:", err)
	}
	// Output:
	// Queued: 2
	// Sending welcome email to albert@example.com
	// Sending welcome email to ana@example.com
}

// README: "Reintentos y fallos". A failed message is delivered again until
// its last attempt, which goes to the failure handler.
func ExampleWithFailureHandler() {
	ctx := context.Background()
	transport := memory.New()
	_ = transport.Send(ctx, qumsg.NewEnvelope(SendWelcomeEmail{Email: "albert@example.com"}))

	worker := qumsg.New()
	_ = worker.Register(func(ctx context.Context, msg SendWelcomeEmail) error {
		fmt.Println("Attempt to send to", msg.Email)
		return errors.New("smtp unavailable")
	})

	_ = worker.Consume(ctx, transport,
		qumsg.WithMaxAttempts(3),
		qumsg.WithFailureHandler(func(ctx context.Context, msg any, err error) {
			fmt.Printf("Giving up on %+v: %v\n", msg, err)
			transport.Close()
		}),
	)
	// Output:
	// Attempt to send to albert@example.com
	// Attempt to send to albert@example.com
	// Attempt to send to albert@example.com
	// Giving up on {Email:albert@example.com}: smtp unavailable
}

type OrderShipped struct{ ID string }

// README "Opciones del bus": the same handlers with the default options and
// with AllowNoHandler and ContinueOnError.
func ExampleNew_options() {
	ctx := context.Background()
	register := func(bus *qumsg.Bus) {
		_ = bus.Register(func(ctx context.Context, e OrderShipped) error {
			fmt.Println("email for", e.ID)
			return errors.New("smtp down")
		})
		_ = bus.Register(func(ctx context.Context, e OrderShipped) error {
			fmt.Println("stock for", e.ID)
			return nil
		})
	}

	strict := qumsg.New()
	register(strict)
	fmt.Println(strict.Dispatch(ctx, OrderShipped{ID: "o-1"}))
	err := strict.Dispatch(ctx, UserCreated{ID: "u-1"})
	fmt.Println(errors.Is(err, qumsg.ErrNoHandler))

	lenient := qumsg.New(qumsg.AllowNoHandler(), qumsg.ContinueOnError())
	register(lenient)
	fmt.Println(lenient.Dispatch(ctx, OrderShipped{ID: "o-2"}))
	fmt.Println(lenient.Dispatch(ctx, UserCreated{ID: "u-2"}))
	// Output:
	// email for o-1
	// smtp down
	// true
	// email for o-2
	// stock for o-2
	// smtp down
	// <nil>
}

type InvoicePaid struct{ ID string }
type InvoiceVoided struct{ ID string }

// README "Familias de mensajes": one list of types for a family of messages.
func ExampleTypeOf() {
	ctx := context.Background()
	invoiceEvents := []qumsg.Type{
		qumsg.TypeOf[InvoicePaid](),
		qumsg.TypeOf[InvoiceVoided](),
	}

	bus := qumsg.New()
	_ = bus.RegisterFor(func(ctx context.Context, msg any) error {
		fmt.Printf("audit %T\n", msg)
		return nil
	}, invoiceEvents...)

	transport := memory.New()
	router := qumsg.NewRouter()
	_ = router.RouteTypes(transport, invoiceEvents...)
	producer := qumsg.New(qumsg.WithRouter(router))

	_ = bus.Dispatch(ctx, InvoicePaid{ID: "i-1"})
	_ = bus.Dispatch(ctx, InvoiceVoided{ID: "i-1"})
	_ = producer.Dispatch(ctx, InvoicePaid{ID: "i-2"})
	fmt.Println("queued:", transport.Len())
	// Output:
	// audit qumsg_test.InvoicePaid
	// audit qumsg_test.InvoiceVoided
	// queued: 1
}

type requestIDKey struct{}

// README: "Metadatos". The request ID stored in the context of the producer
// travels in the envelope and is back in the context of the worker handler.
func ExampleWithPropagators() {
	ctx := context.Background()
	transport := memory.New()
	requestID := qumsg.ContextValue("request_id", requestIDKey{})
	router := qumsg.NewRouter()
	_ = router.Route[SendWelcomeEmail](transport)

	producer := qumsg.New(qumsg.WithRouter(router), qumsg.WithPropagators(requestID))

	worker := qumsg.New(qumsg.WithPropagators(requestID))
	_ = worker.Register(func(ctx context.Context, msg SendWelcomeEmail) error {
		fmt.Println("Request", ctx.Value(requestIDKey{}), "sends email to", msg.Email)
		return nil
	})

	ctx = context.WithValue(ctx, requestIDKey{}, "req-42")
	_ = producer.Dispatch(ctx, SendWelcomeEmail{Email: "albert@example.com"})

	transport.Close()
	_ = worker.Consume(context.Background(), transport)
	// Output:
	// Request req-42 sends email to albert@example.com
}
