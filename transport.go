package qumsg

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	// ErrDeliveryLost means this delivery no longer owns its message.
	// Consume settles it without stopping unrelated workers.
	ErrDeliveryLost = errors.New("qumsg: delivery lost")
	// ErrTransportClosed is returned by Transport.Receive when the transport
	// is closed and has no more messages. Consume treats it as a normal stop.
	ErrTransportClosed = errors.New("qumsg: transport closed")
	// ErrNilTransport is returned by Bus.Route and Bus.RouteTypes when
	// the transport is nil.
	ErrNilTransport = errors.New("qumsg: nil transport")
	// ErrUncomparableTransport is returned by Bus.Route and
	// Bus.RouteTypes when the dynamic type of the transport is not
	// comparable, such as a struct value with a slice or map field. The
	// bus compares transports with ==, which would panic; use a pointer.
	ErrUncomparableTransport = errors.New("qumsg: transport type is not comparable")
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

// DeliveryContext optionally binds handling to a delivery's ownership.
// The returned cleanup function must be called after settlement.
type DeliveryContext interface {
	Context(parent context.Context) (context.Context, context.CancelFunc)
}

// ReleasableDelivery can return interrupted work without counting an attempt.
type ReleasableDelivery interface {
	Release(context.Context) error
}

type attemptKey struct{}

// Attempt returns the current delivery attempt, or zero outside Consume.
func Attempt(ctx context.Context) int {
	n, _ := ctx.Value(attemptKey{}).(int)
	return n
}

// Decoder is implemented by transports that must know the message types to
// decode what they receive, such as one that stores messages as bytes.
// Consume calls Accept, before receiving, with the types that the bus routes
// to the transport. Accept may be called more than once and must keep the
// types it already accepted.
type Decoder interface {
	Accept(types ...Type) error
}

// FailureHandler is called by Consume when a message fails its last attempt,
// before acknowledging it. err is the error of the last dispatch.
type FailureHandler func(ctx context.Context, message any, err error)

// ConsumeOption configures Consume.
type ConsumeOption func(*consumeConfig)

type consumeConfig struct {
	workers          int
	maxAttempts      int
	onFailure        FailureHandler
	cancelOnShutdown bool
}

// WithUnlimitedAttempts retries failures until they succeed or Consume stops.
func WithUnlimitedAttempts() ConsumeOption {
	return func(c *consumeConfig) { c.maxAttempts = 0 }
}

// WithCancelOnShutdown cancels in-flight handlers when Consume is canceled.
// Interrupted deliveries supporting Release return without counting an attempt;
// other transports use Nack. Handlers must observe cancellation and return.
// Settlement uses a detached context bounded to five seconds.
func WithCancelOnShutdown() ConsumeOption {
	return func(c *consumeConfig) { c.cancelOnShutdown = true }
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
// or t is closed, which return nil. Received messages run the middleware and
// the handlers of b, but are never routed again, so the producer and the
// worker can configure the same routes, and even be the same bus. If t is a
// Decoder, Consume first passes it the types that b routes to t, and fails
// with ErrNoRoutes if there are none. A successful dispatch acknowledges the
// message; a failed one is negatively acknowledged so the transport delivers
// it again, until the last attempt, which goes to the failure handler and is
// acknowledged.
//
// When ctx is done Consume stops receiving and waits for the messages in
// flight: their dispatch, Ack and Nack use a context that keeps the values of
// ctx but is not canceled with it. WithCancelOnShutdown opts into cancellation
// and release instead. DeliveryContext can cancel handling on ownership loss
// in either mode. Settlements are bounded to five seconds. Transport errors
// other than ErrDeliveryLost stop Consume and are returned.
func (b *Bus) Consume(ctx context.Context, t Transport, opts ...ConsumeOption) error {
	cfg := consumeConfig{workers: 1, maxAttempts: 3}
	for _, opt := range opts {
		opt(&cfg)
	}

	if d, ok := t.(Decoder); ok {
		types := b.typesFor(t)
		if len(types) == 0 {
			return ErrNoRoutes
		}
		if err := d.Accept(types...); err != nil {
			return err
		}
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

				if err := b.settle(ctx, d, &cfg); err != nil && !errors.Is(err, ErrDeliveryLost) {
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
	if !cfg.cancelOnShutdown {
		ctx = context.WithoutCancel(ctx)
	}
	if owned, ok := d.(DeliveryContext); ok {
		var cleanup context.CancelFunc
		ctx, cleanup = owned.Context(ctx)
		defer cleanup()
	}
	e := d.Envelope()
	ctx = b.extract(ctx, e)
	ctx = context.WithValue(ctx, attemptKey{}, d.Attempt())
	err := b.consume(ctx, e.Message())
	settlement, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	switch {
	case errors.Is(context.Cause(ctx), ErrDeliveryLost), errors.Is(err, ErrDeliveryLost):
		return d.Nack(settlement)
	case err == nil:
		return d.Ack(settlement)
	case cfg.cancelOnShutdown && ctx.Err() != nil:
		if released, ok := d.(ReleasableDelivery); ok {
			return released.Release(settlement)
		}
		return d.Nack(settlement)
	case cfg.maxAttempts == 0 || d.Attempt() < cfg.maxAttempts:
		return d.Nack(settlement)
	default:
		if cfg.onFailure != nil {
			cfg.onFailure(settlement, e.Message(), err)
		}
		return d.Ack(settlement)
	}
}
