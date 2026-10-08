package memory_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/romycode/qumsg"
	"github.com/romycode/qumsg/memory"
)

func mustSend(t *testing.T, tr *memory.Transport, msg any) {
	t.Helper()
	if err := tr.Send(t.Context(), msg); err != nil {
		t.Fatal(err)
	}
}

func mustReceive(t *testing.T, tr *memory.Transport) qumsg.Delivery {
	t.Helper()
	d, err := tr.Receive(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestReceiveReturnsMessagesInOrder(t *testing.T) {
	tr := memory.New()
	mustSend(t, tr, "a")
	mustSend(t, tr, "b")

	for _, want := range []string{"a", "b"} {
		d := mustReceive(t, tr)
		if d.Message() != want || d.Attempt() != 1 {
			t.Fatalf("got (%v, attempt %d), want (%v, attempt 1)", d.Message(), d.Attempt(), want)
		}
	}
}

func TestSendBlocksWhenFull(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := memory.New(memory.WithCapacity(1))
		mustSend(t, tr, "a")

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := tr.Send(ctx, "b"); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got %v, want DeadlineExceeded", err)
		}
	})
}

func TestSendUnblocksWhenThereIsRoom(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := memory.New(memory.WithCapacity(1))
		mustSend(t, tr, "a")

		sent := make(chan error)
		go func() { sent <- tr.Send(t.Context(), "b") }()
		synctest.Wait()

		mustReceive(t, tr)
		if err := <-sent; err != nil {
			t.Fatal(err)
		}
		if got := mustReceive(t, tr).Message(); got != "b" {
			t.Fatalf("got %v", got)
		}
	})
}

func TestReceiveBlocksUntilContextIsDone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()

		if _, err := memory.New().Receive(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got %v, want DeadlineExceeded", err)
		}
	})
}

func TestCloseDrainsQueueAndRejectsSends(t *testing.T) {
	// Several messages so Receive goes through both of its select branches
	// for a closed transport, which Go picks at random.
	const queued = 20
	tr := memory.New()
	for i := range queued {
		mustSend(t, tr, i)
	}
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	if err := tr.Send(t.Context(), "late"); !errors.Is(err, qumsg.ErrTransportClosed) {
		t.Fatalf("Send after Close: got %v", err)
	}
	for i := range queued {
		if got := mustReceive(t, tr).Message(); got != i {
			t.Fatalf("got %v, want %d", got, i)
		}
	}
	if _, err := tr.Receive(t.Context()); !errors.Is(err, qumsg.ErrTransportClosed) {
		t.Fatalf("Receive on drained transport: got %v", err)
	}
}

func TestCloseUnblocksPendingCalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := memory.New(memory.WithCapacity(1))
		mustSend(t, tr, "a")

		sent := make(chan error)
		go func() { sent <- tr.Send(t.Context(), "b") }()
		synctest.Wait()

		tr.Close()
		if err := <-sent; !errors.Is(err, qumsg.ErrTransportClosed) {
			t.Fatalf("got %v", err)
		}
	})
}

func TestNackRedeliversWithNextAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := memory.New(memory.WithRetryDelay(time.Minute))
		mustSend(t, tr, "a")

		if err := mustReceive(t, tr).Nack(t.Context()); err != nil {
			t.Fatal(err)
		}

		time.Sleep(time.Minute - time.Nanosecond)
		synctest.Wait()
		if tr.Len() != 0 {
			t.Fatal("redelivered before the retry delay")
		}

		time.Sleep(time.Nanosecond)
		synctest.Wait()
		d := mustReceive(t, tr)
		if d.Message() != "a" || d.Attempt() != 2 {
			t.Fatalf("got (%v, attempt %d), want (a, attempt 2)", d.Message(), d.Attempt())
		}
	})
}

func TestDeliveryIsSettledOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := memory.New()
		mustSend(t, tr, "a")

		d := mustReceive(t, tr)
		d.Ack(t.Context())
		d.Nack(t.Context())
		d.Nack(t.Context())
		synctest.Wait()

		if tr.Len() != 0 {
			t.Fatal("an acknowledged delivery was redelivered")
		}
	})
}

func TestCloseDiscardsPendingRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := memory.New(memory.WithRetryDelay(time.Hour))
		mustSend(t, tr, "a")
		mustSend(t, tr, "b")
		first, second := mustReceive(t, tr), mustReceive(t, tr)

		first.Nack(t.Context()) // waiting for its retry delay
		tr.Close()              // returns without waiting for the delay
		second.Nack(t.Context())

		if _, err := tr.Receive(t.Context()); !errors.Is(err, qumsg.ErrTransportClosed) {
			t.Fatalf("got %v, want ErrTransportClosed", err)
		}
	})
}

func TestConcurrentUse(t *testing.T) {
	tr := memory.New(memory.WithCapacity(8))
	const n = 200

	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() { mustSend(t, tr, i) })
	}

	seen := make(map[int]bool)
	for range n {
		d := mustReceive(t, tr)
		seen[d.Message().(int)] = true
		d.Ack(t.Context())
	}
	wg.Wait()

	if len(seen) != n {
		t.Fatalf("got %d distinct messages, want %d", len(seen), n)
	}
}

func TestOptionsValidateArguments(t *testing.T) {
	for name, build := range map[string]func(){
		"capacity":    func() { memory.WithCapacity(0) },
		"retry delay": func() { memory.WithRetryDelay(-time.Second) },
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
