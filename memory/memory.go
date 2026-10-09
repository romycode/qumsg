// Package memory implements an in-process qumsg.Transport backed by a bounded
// queue. Messages live only in memory: they are lost when the process stops,
// so use it for development, tests or work that can be lost.
package memory

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/romycode/qumsg"
)

// Option configures a Transport.
type Option func(*Transport)

// WithCapacity sets how many messages the queue holds before Send blocks.
// Default 100.
func WithCapacity(n int) Option {
	if n < 1 {
		panic("memory: WithCapacity requires n >= 1")
	}
	return func(t *Transport) { t.queue = make(chan *delivery, n) }
}

// WithRetryDelay sets how long a negatively acknowledged message waits before
// it is delivered again. Default 0.
func WithRetryDelay(d time.Duration) Option {
	if d < 0 {
		panic("memory: WithRetryDelay requires d >= 0")
	}
	return func(t *Transport) { t.retryDelay = d }
}

// Transport is an in-memory qumsg.Transport, safe for concurrent use.
type Transport struct {
	queue      chan *delivery
	retryDelay time.Duration

	// mu orders Close against Nack, so no retry starts once Close waits.
	mu      sync.Mutex
	closed  chan struct{}
	retries sync.WaitGroup
}

var _ qumsg.Transport = (*Transport)(nil)

// New creates an empty transport.
func New(opts ...Option) *Transport {
	t := &Transport{
		queue:  make(chan *delivery, 100),
		closed: make(chan struct{}),
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// Send enqueues the envelope. When the queue is full it blocks until there is
// room, ctx is done or the transport is closed (qumsg.ErrTransportClosed).
func (t *Transport) Send(ctx context.Context, e qumsg.Envelope) error {
	return t.enqueue(ctx, &delivery{transport: t, envelope: e, attempt: 1})
}

func (t *Transport) enqueue(ctx context.Context, d *delivery) error {
	// Checked first so a closed transport never accepts messages, even when
	// the queue has room.
	select {
	case <-t.closed:
		return qumsg.ErrTransportClosed
	default:
	}
	// Likewise a canceled context never enqueues: with room in the queue, the
	// select below would pick between both cases at random.
	if err := ctx.Err(); err != nil {
		return err
	}

	select {
	case t.queue <- d:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-t.closed:
		return qumsg.ErrTransportClosed
	}
}

// Receive returns the next message. After Close it keeps returning the queued
// messages and then qumsg.ErrTransportClosed.
func (t *Transport) Receive(ctx context.Context) (qumsg.Delivery, error) {
	select {
	case d := <-t.queue:
		return d, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.closed:
		select {
		case d := <-t.queue:
			return d, nil
		default:
			return nil, qumsg.ErrTransportClosed
		}
	}
}

// Close stops accepting messages. Queued messages can still be received;
// messages waiting for a retry delay, or negatively acknowledged after Close,
// are discarded. Close waits for pending retries to stop and is safe to call
// more than once.
func (t *Transport) Close() error {
	t.mu.Lock()
	select {
	case <-t.closed:
	default:
		close(t.closed)
	}
	t.mu.Unlock()

	t.retries.Wait()
	return nil
}

// Len returns the number of queued messages, excluding those waiting for a
// retry delay.
func (t *Transport) Len() int {
	return len(t.queue)
}

type delivery struct {
	transport *Transport
	envelope  qumsg.Envelope
	attempt   int
	settled   atomic.Bool
}

func (d *delivery) Envelope() qumsg.Envelope { return d.envelope }

func (d *delivery) Attempt() int { return d.attempt }

func (d *delivery) Ack(context.Context) error {
	d.settled.CompareAndSwap(false, true)
	return nil
}

// Nack enqueues the message again, after the retry delay, with the next
// attempt number. It does not block the caller.
func (d *delivery) Nack(context.Context) error {
	if !d.settled.CompareAndSwap(false, true) {
		return nil
	}

	t := d.transport
	next := &delivery{transport: t, envelope: d.envelope, attempt: d.attempt + 1}

	t.mu.Lock()
	defer t.mu.Unlock()
	select {
	case <-t.closed:
		return nil
	default:
	}

	t.retries.Go(func() {
		if t.retryDelay > 0 {
			timer := time.NewTimer(t.retryDelay)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-t.closed:
				return
			}
		}
		_ = t.enqueue(context.Background(), next)
	})

	return nil
}
