package qumsg

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrTransportClosed is returned by Transport.Receive when the transport
	// is closed and has no more messages. Consume treats it as a normal stop.
	ErrTransportClosed = errors.New("qumsg: transport closed")
	// ErrNilTransport is returned by Route when the transport is nil.
	ErrNilTransport = errors.New("qumsg: nil transport")
)

// Transport moves messages between the bus that sends them and the bus that
// consumes them, possibly in another goroutine or process.
type Transport interface {
	// Send enqueues the envelope, keeping its metadata. It may block, for
	// example when the transport is full, and must stop waiting when ctx is
	// done.
	Send(ctx context.Context, e Envelope) error
	// Receive blocks until a message is available, ctx is done or the
	// transport is closed (ErrTransportClosed).
	Receive(ctx context.Context) (Delivery, error)
}

// Delivery is a received message that must be settled exactly once, with Ack
// or Nack.
type Delivery interface {
	// Envelope returns the message with the metadata it was sent with.
	Envelope() Envelope
	// Attempt is the delivery number of the message, starting at 1.
	Attempt() int
	// Ack confirms the message was processed; the transport forgets it.
	Ack(ctx context.Context) error
	// Nack reports the message failed; the transport delivers it again.
	Nack(ctx context.Context) error
}

// Route registers a handler that sends messages of the exact type T to t
// instead of processing them, wrapped in an envelope filled by the
// propagators of the bus. It follows the same rules as Register: it runs in
// registration order with the other handlers of T and validates the optional
// alias.
//
//	bus.Route[UserCreated](transport)
func (b *Bus) Route[T any](t Transport) error {
	if t == nil {
		return ErrNilTransport
	}
	return b.Register(func(ctx context.Context, msg T) error {
		return t.Send(ctx, b.envelope(ctx, msg))
	})
}

// FailureHandler is called by Consume when a message fails its last attempt,
// before acknowledging it. err is the error of the last dispatch.
type FailureHandler func(ctx context.Context, message any, err error)

// ConsumeOption configures Consume.
type ConsumeOption func(*consumeConfig)

type consumeConfig struct {
	workers     int
	maxAttempts int
	onFailure   FailureHandler
}

// WithWorkers sets how many messages are processed concurrently. Default 1.
func WithWorkers(n int) ConsumeOption {
	if n < 1 {
		panic("qumsg: WithWorkers requires n >= 1")
	}
	return func(c *consumeConfig) { c.workers = n }
}

// WithMaxAttempts sets how many times a message is dispatched before it is
// given up. Default 3.
func WithMaxAttempts(n int) ConsumeOption {
	if n < 1 {
		panic("qumsg: WithMaxAttempts requires n >= 1")
	}
	return func(c *consumeConfig) { c.maxAttempts = n }
}

// WithFailureHandler sets the function called with messages that fail their
// last attempt, for example to log them or send them to a dead-letter
// transport. Without it, those messages are dropped.
func WithFailureHandler(f FailureHandler) ConsumeOption {
	return func(c *consumeConfig) { c.onFailure = f }
}

// Consume receives messages from t and dispatches them on b until ctx is done
// or t is closed, which return nil. A successful dispatch acknowledges the
// message; a failed one is negatively acknowledged so the transport delivers
// it again, until the last attempt, which goes to the failure handler and is
// acknowledged.
//
// When ctx is done Consume stops receiving and waits for the messages in
// flight: their dispatch, Ack and Nack use a context that keeps the values of
// ctx but is not canceled with it. Any other error from the transport stops
// Consume and is returned.
func (b *Bus) Consume(ctx context.Context, t Transport, opts ...ConsumeOption) error {
	cfg := consumeConfig{workers: 1, maxAttempts: 3}
	for _, opt := range opts {
		opt(&cfg)
	}

	ctx, stop := context.WithCancelCause(ctx)
	defer stop(nil)

	var (
		wg      sync.WaitGroup
		errOnce sync.Once
		failed  error
	)
	fail := func(err error) {
		errOnce.Do(func() {
			failed = err
			stop(err)
		})
	}

	for range cfg.workers {
		wg.Go(func() {
			for {
				d, err := t.Receive(ctx)
				if err != nil {
					if ctx.Err() == nil && !errors.Is(err, ErrTransportClosed) {
						fail(err)
					}
					return
				}

				// A received message is always settled, even if ctx was
				// canceled meanwhile.

				if err := b.settle(context.WithoutCancel(ctx), d, &cfg); err != nil {
					fail(err)
					return
				}
			}
		})
	}
	wg.Wait()

	return failed
}

// settle dispatches a delivery and acknowledges it according to the result.
// The propagators of the bus restore the metadata of the envelope into ctx
// before the dispatch.
func (b *Bus) settle(ctx context.Context, d Delivery, cfg *consumeConfig) error {
	e := d.Envelope()
	ctx = b.extract(ctx, e)
	err := b.Dispatch(ctx, e.Message())
	switch {
	case err == nil:
		return d.Ack(ctx)
	case d.Attempt() < cfg.maxAttempts:
		return d.Nack(ctx)
	default:
		if cfg.onFailure != nil {
			cfg.onFailure(ctx, e.Message(), err)
		}
		return d.Ack(ctx)
	}
}
