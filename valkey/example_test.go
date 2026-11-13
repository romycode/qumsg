//go:build integration

package valkey_test

import (
	"context"
	"fmt"
	"os"

	"github.com/romycode/qumsg"
	"github.com/romycode/qumsg/valkey"
	vk "github.com/valkey-io/valkey-go"
)

type SendWelcomeEmail struct {
	Email string
}

func (SendWelcomeEmail) Name() string { return "emails.welcome" }

// README of the valkey module: a producer routes the message to a stream and a
// worker, usually in another process, consumes it.
func Example() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	addr := os.Getenv("VALKEY_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	client, err := vk.NewClient(vk.ClientOption{InitAddress: []string{addr}})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer client.Close()

	const stream = "qumsg-example:emails"
	defer client.Do(context.Background(), client.B().Del().Key(stream).Build())

	// Both processes build the same bus: SendWelcomeEmail is routed to
	// Valkey, so Dispatch sends it and Consume runs its handler.
	newBus := func(t qumsg.Transport) *qumsg.Bus {
		bus := qumsg.New()
		_ = bus.Register(func(ctx context.Context, msg SendWelcomeEmail) error {
			fmt.Println("Sending welcome email to", msg.Email)
			cancel() // stop after one message to end the example
			return nil
		})
		_ = bus.Route[SendWelcomeEmail](t)
		return bus
	}

	// Producer process.
	_ = newBus(valkey.New(client, stream)).Dispatch(ctx, SendWelcomeEmail{Email: "albert@example.com"})

	// Worker process: Consume tells the transport which types it receives.
	transport := valkey.New(client, stream)
	_ = newBus(transport).Consume(ctx, transport)
	// Output:
	// Sending welcome email to albert@example.com
}
