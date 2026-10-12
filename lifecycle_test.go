package qumsg_test

import (
	"context"
	"errors"
	"testing"

	"github.com/romycode/qumsg"
	"github.com/romycode/qumsg/memory"
)

func TestUnlimitedAttemptsExposeAttempt(t *testing.T) {
	tr := memory.New()
	_ = tr.Send(t.Context(), qumsg.NewEnvelope(job{}))
	bus := qumsg.New()
	calls := 0
	mustRegister(t, bus, func(ctx context.Context, _ job) error {
		calls++
		if got := qumsg.Attempt(ctx); got != calls {
			t.Fatalf("attempt = %d, want %d", got, calls)
		}
		if calls == 5 {
			tr.Close()
			return nil
		}
		return errors.New("retry")
	})
	if err := bus.Consume(t.Context(), tr, qumsg.WithUnlimitedAttempts()); err != nil {
		t.Fatal(err)
	}
	if calls != 5 {
		t.Fatalf("calls = %d", calls)
	}
}

type lifecycleDelivery struct {
	released, acked bool
	lost            context.Context
}

func (d *lifecycleDelivery) Envelope() qumsg.Envelope   { return qumsg.NewEnvelope(job{}) }
func (d *lifecycleDelivery) Attempt() int               { return 1 }
func (d *lifecycleDelivery) Ack(context.Context) error  { d.acked = true; return nil }
func (d *lifecycleDelivery) Nack(context.Context) error { return qumsg.ErrDeliveryLost }
func (d *lifecycleDelivery) Release(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	d.released = true
	return nil
}
func (d *lifecycleDelivery) Context(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancelCause(parent)
	stop := context.AfterFunc(d.lost, func() { cancel(qumsg.ErrDeliveryLost) })
	return ctx, func() { stop(); cancel(nil) }
}

type lifecycleTransport struct{ d, next *lifecycleDelivery }

func (*lifecycleTransport) Send(context.Context, qumsg.Envelope) error { return nil }
func (tr *lifecycleTransport) Receive(context.Context) (qumsg.Delivery, error) {
	if tr.d == nil {
		return nil, qumsg.ErrTransportClosed
	}
	d := tr.d
	tr.d, tr.next = tr.next, nil
	return d, nil
}

func TestCancelOnShutdownReleasesDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	d := &lifecycleDelivery{lost: t.Context()}
	bus := qumsg.New()
	mustRegister(t, bus, func(ctx context.Context, _ job) error { cancel(); <-ctx.Done(); return ctx.Err() })
	if err := bus.Consume(ctx, &lifecycleTransport{d: d}, qumsg.WithCancelOnShutdown()); err != nil {
		t.Fatal(err)
	}
	if !d.released || d.acked {
		t.Fatalf("released=%v acked=%v", d.released, d.acked)
	}
}

func TestLeaseLossCancelsHandlerWithoutStoppingConsumer(t *testing.T) {
	lost, cancel := context.WithCancel(t.Context())
	d := &lifecycleDelivery{lost: lost}
	next := &lifecycleDelivery{lost: t.Context()}
	bus := qumsg.New()
	calls := 0
	mustRegister(t, bus, func(ctx context.Context, _ job) error {
		calls++
		if calls == 2 {
			return nil
		}
		cancel()
		<-ctx.Done()
		return context.Cause(ctx)
	})
	if err := bus.Consume(t.Context(), &lifecycleTransport{d: d, next: next}); err != nil {
		t.Fatal(err)
	}
	if d.acked {
		t.Fatal("lost delivery acknowledged")
	}
	if calls != 2 || !next.acked {
		t.Fatal("consumer did not process the next delivery")
	}
}
