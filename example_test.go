package qumsg_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/romycode/qumsg"
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

	bus := qumsg.New(trace("first"), trace("second"))
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

	bus := qumsg.New(Logging(logger))
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
