package qumsg_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/romycode/qumsg"
	"github.com/romycode/qumsg/memory"
)

type job struct{ N int }

// sendAll enqueues the jobs and closes the transport, so Consume returns once
// they are processed.
func sendAll(t *testing.T, tr *memory.Transport, jobs ...int) {
	t.Helper()
	for _, n := range jobs {
		if err := tr.Send(t.Context(), qumsg.NewEnvelope(job{N: n})); err != nil {
			t.Fatal(err)
		}
	}
	tr.Close()
}

func TestConsumeProcessesMessagesUntilTransportCloses(t *testing.T) {
	tr := memory.New()
	sendAll(t, tr, 1, 2, 3)

	worker := qumsg.New()
	var got []int
	mustRegister(t, worker, func(_ context.Context, j job) error {
		got = append(got, j.N)
		return nil
	})

	if err := worker.Consume(t.Context(), tr); err != nil {
		t.Fatal(err)
	}
	if want := []int{1, 2, 3}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestConsumeRetriesUntilSuccess(t *testing.T) {
	tr := memory.New()
	if err := tr.Send(t.Context(), qumsg.NewEnvelope(job{})); err != nil {
		t.Fatal(err)
	}

	worker := qumsg.New()
	calls := 0
	mustRegister(t, worker, func(context.Context, job) error {
		calls++
		if calls < 3 {
			return errors.New("temporary")
		}
		tr.Close()
		return nil
	})

	failed := false
	err := worker.Consume(t.Context(), tr, qumsg.WithFailureHandler(func(context.Context, any, error) {
		failed = true
	}))
	if err != nil {
		t.Fatal(err)
	}
	if calls != 3 || failed {
		t.Fatalf("calls %d, failed %v; want 3 calls and no failure", calls, failed)
	}
}

func TestConsumeHandsOverMessagesThatExhaustTheirAttempts(t *testing.T) {
	tr := memory.New()
	if err := tr.Send(t.Context(), qumsg.NewEnvelope(job{N: 1})); err != nil {
		t.Fatal(err)
	}

	boom := errors.New("boom")
	worker := qumsg.New()
	calls := 0
	mustRegister(t, worker, func(context.Context, job) error {
		calls++
		return boom
	})

	var gotMsg any
	var gotErr error
	err := worker.Consume(t.Context(), tr,
		qumsg.WithMaxAttempts(2),
		qumsg.WithFailureHandler(func(_ context.Context, msg any, err error) {
			gotMsg, gotErr = msg, err
			tr.Close()
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("got %d attempts, want 2", calls)
	}
	if gotMsg != (job{N: 1}) || gotErr != boom {
		t.Fatalf("failure handler got (%v, %v)", gotMsg, gotErr)
	}
	if tr.Len() != 0 {
		t.Fatal("failed message was not acknowledged")
	}
}

func TestConsumeDropsFailedMessagesWithoutFailureHandler(t *testing.T) {
	tr := memory.New()
	sendAll(t, tr, 1, 2)

	worker := qumsg.New()
	var got []int
	mustRegister(t, worker, func(_ context.Context, j job) error {
		got = append(got, j.N)
		if j.N == 1 {
			return errors.New("always fails")
		}
		return nil
	})

	if err := worker.Consume(t.Context(), tr, qumsg.WithMaxAttempts(1)); err != nil {
		t.Fatal(err)
	}
	if want := []int{1, 2}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestConsumeFinishesInFlightMessagesOnCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := memory.New()
		if err := tr.Send(t.Context(), qumsg.NewEnvelope(job{})); err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(context.WithValue(t.Context(), ctxKey{}, "kept"))
		started, release := make(chan struct{}), make(chan struct{})
		var handlerCtxErr error
		var value any

		worker := qumsg.New()
		mustRegister(t, worker, func(ctx context.Context, _ job) error {
			close(started)
			<-release
			handlerCtxErr, value = ctx.Err(), ctx.Value(ctxKey{})
			return nil
		})

		done := make(chan error)
		go func() { done <- worker.Consume(ctx, tr) }()

		<-started
		cancel()
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("Consume returned before the in-flight message finished")
		default:
		}

		close(release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if handlerCtxErr != nil || value != "kept" {
			t.Fatalf("handler ctx: err %v, value %v; want not canceled with values", handlerCtxErr, value)
		}
		if tr.Len() != 0 {
			t.Fatal("in-flight message was not acknowledged")
		}
	})
}

func TestConsumeRunsWorkersConcurrently(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const workers = 4
		tr := memory.New()
		sendAll(t, tr, 1, 2, 3, 4)

		// Every handler waits until all workers are inside a handler.
		var wg sync.WaitGroup
		wg.Add(workers)
		worker := qumsg.New()
		mustRegister(t, worker, func(context.Context, job) error {
			wg.Done()
			wg.Wait()
			return nil
		})

		if err := worker.Consume(t.Context(), tr, qumsg.WithWorkers(workers)); err != nil {
			t.Fatal(err)
		}
	})
}

func TestConsumeStopsOnTransportErrors(t *testing.T) {
	broken := errors.New("connection lost")

	t.Run("receive", func(t *testing.T) {
		tr := &fakeTransport{receiveErr: broken}
		if err := qumsg.New().Consume(t.Context(), tr); err != broken {
			t.Fatalf("got %v, want %v", err, broken)
		}
	})

	t.Run("ack", func(t *testing.T) {
		tr := &fakeTransport{delivery: &fakeDelivery{ackErr: broken}}
		worker := qumsg.New()
		mustRegister(t, worker, func(context.Context, job) error { return nil })

		if err := worker.Consume(t.Context(), tr); err != broken {
			t.Fatalf("got %v, want %v", err, broken)
		}
	})

	t.Run("nack", func(t *testing.T) {
		tr := &fakeTransport{delivery: &fakeDelivery{nackErr: broken}}
		worker := qumsg.New()
		mustRegister(t, worker, func(context.Context, job) error { return errors.New("fails") })

		if err := worker.Consume(t.Context(), tr); err != broken {
			t.Fatalf("got %v, want %v", err, broken)
		}
	})
}

func TestConsumeOptionsValidateArguments(t *testing.T) {
	for name, build := range map[string]func(){
		"workers":      func() { qumsg.WithWorkers(0) },
		"max attempts": func() { qumsg.WithMaxAttempts(0) },
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			build()
		})
	}
}

// fakeTransport returns receiveErr, or delivery once and then
// ErrTransportClosed.
type fakeTransport struct {
	receiveErr error
	delivery   *fakeDelivery
}

func (f *fakeTransport) Send(context.Context, qumsg.Envelope) error { return nil }

func (f *fakeTransport) Receive(context.Context) (qumsg.Delivery, error) {
	if f.receiveErr != nil {
		return nil, f.receiveErr
	}
	if f.delivery == nil {
		return nil, qumsg.ErrTransportClosed
	}
	d := f.delivery
	f.delivery = nil
	return d, nil
}

type fakeDelivery struct{ ackErr, nackErr error }

func (*fakeDelivery) Envelope() qumsg.Envelope     { return qumsg.NewEnvelope(job{}) }
func (*fakeDelivery) Attempt() int                 { return 1 }
func (d *fakeDelivery) Ack(context.Context) error  { return d.ackErr }
func (d *fakeDelivery) Nack(context.Context) error { return d.nackErr }
