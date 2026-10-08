//go:build integration

package valkey_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/romycode/qumsg"
	"github.com/romycode/qumsg/valkey"
	vk "github.com/valkey-io/valkey-go"
)

// Integration tests need a Valkey server: docker compose up -d --wait valkey.
// VALKEY_ADDR overrides the default localhost:6379.

func newClient(t testing.TB) vk.Client {
	t.Helper()
	addr := os.Getenv("VALKEY_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	client, err := vk.NewClient(vk.ClientOption{InitAddress: []string{addr}})
	if err != nil {
		t.Fatalf("connect to Valkey at %s (docker compose up -d --wait valkey): %v", addr, err)
	}
	t.Cleanup(client.Close)
	return client
}

// newStream returns a unique stream key, deleted when the test ends.
func newStream(t testing.TB, client vk.Client) string {
	t.Helper()
	suffix := make([]byte, 6)
	_, _ = rand.Read(suffix)
	stream := "qumsg-test:" + t.Name() + ":" + hex.EncodeToString(suffix)
	t.Cleanup(func() {
		client.Do(context.Background(), client.B().Del().Key(stream).Build())
	})
	return stream
}

func newTransport(t testing.TB, opts ...valkey.Option) (*valkey.Transport, vk.Client, string) {
	t.Helper()
	client := newClient(t)
	stream := newStream(t, client)
	opts = append([]valkey.Option{valkey.WithBlock(50 * time.Millisecond)}, opts...)
	tr := valkey.New(client, stream, opts...)
	if err := tr.Register[userCreated](); err != nil {
		t.Fatal(err)
	}
	return tr, client, stream
}

func mustSend(t *testing.T, tr *valkey.Transport, msg any) {
	t.Helper()
	if err := tr.Send(t.Context(), msg); err != nil {
		t.Fatal(err)
	}
}

func mustReceive(t *testing.T, tr *valkey.Transport) qumsg.Delivery {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	d, err := tr.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func pendingCount(t *testing.T, client vk.Client, stream string) int64 {
	t.Helper()
	res, err := client.Do(t.Context(), client.B().Xpending().Key(stream).Group("qumsg").Build()).ToArray()
	if err != nil {
		t.Fatal(err)
	}
	n, err := res[0].AsInt64()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestSendAndReceive(t *testing.T) {
	tr, client, stream := newTransport(t)
	want := userCreated{ID: "user-123", Email: "albert@example.com"}

	// Sent before any consumer exists: the group reads from the beginning.
	mustSend(t, tr, want)

	d := mustReceive(t, tr)
	if d.Message() != want || d.Attempt() != 1 {
		t.Fatalf("got (%#v, attempt %d), want (%#v, attempt 1)", d.Message(), d.Attempt(), want)
	}
	if n := pendingCount(t, client, stream); n != 1 {
		t.Fatalf("pending before Ack: %d, want 1", n)
	}

	if err := d.Ack(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n := pendingCount(t, client, stream); n != 0 {
		t.Fatalf("pending after Ack: %d, want 0", n)
	}
}

func TestPointerMessages(t *testing.T) {
	tr, _, _ := newTransport(t)
	if err := tr.Register[*renamed](); err != nil {
		t.Fatal(err)
	}

	mustSend(t, tr, &renamed{ID: "1"})

	got, ok := mustReceive(t, tr).Message().(*renamed)
	if !ok || got.ID != "1" {
		t.Fatalf("got %#v", got)
	}
}

type renamed struct{ ID string }

func (*renamed) Name() string { return "renamed" }

func TestNackRedeliversWithNextAttempt(t *testing.T) {
	tr, client, stream := newTransport(t)
	mustSend(t, tr, userCreated{ID: "1"})

	if err := mustReceive(t, tr).Nack(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n := pendingCount(t, client, stream); n != 0 {
		t.Fatalf("pending after Nack: %d, want 0 (original acknowledged)", n)
	}

	d := mustReceive(t, tr)
	if d.Message() != (userCreated{ID: "1"}) || d.Attempt() != 2 {
		t.Fatalf("got (%#v, attempt %d), want attempt 2", d.Message(), d.Attempt())
	}
}

func TestClaimsMessagesOfAStoppedConsumer(t *testing.T) {
	client := newClient(t)
	stream := newStream(t, client)
	newConsumer := func(name string) *valkey.Transport {
		tr := valkey.New(client, stream,
			valkey.WithConsumer(name),
			valkey.WithBlock(20*time.Millisecond),
			valkey.WithClaimIdle(100*time.Millisecond),
		)
		if err := tr.Register[userCreated](); err != nil {
			t.Fatal(err)
		}
		return tr
	}
	stopped, alive := newConsumer("stopped"), newConsumer("alive")

	mustSend(t, stopped, userCreated{ID: "1"})
	mustReceive(t, stopped) // never settled, as if the process died

	d := mustReceive(t, alive)
	if d.Message() != (userCreated{ID: "1"}) || d.Attempt() != 2 {
		t.Fatalf("got (%#v, attempt %d), want the claimed message at attempt 2", d.Message(), d.Attempt())
	}
}

func TestConsumersShareTheGroup(t *testing.T) {
	client := newClient(t)
	stream := newStream(t, client)
	a := valkey.New(client, stream, valkey.WithConsumer("a"), valkey.WithBlock(20*time.Millisecond))
	b := valkey.New(client, stream, valkey.WithConsumer("b"), valkey.WithBlock(20*time.Millisecond))
	for _, tr := range []*valkey.Transport{a, b} {
		if err := tr.Register[userCreated](); err != nil {
			t.Fatal(err)
		}
	}

	mustSend(t, a, userCreated{ID: "1"})
	mustSend(t, a, userCreated{ID: "2"})

	first, second := mustReceive(t, a).Message(), mustReceive(t, b).Message()
	if first == second {
		t.Fatalf("both consumers got %#v", first)
	}
}

func TestUndecodableMessages(t *testing.T) {
	tr, client, stream := newTransport(t)

	t.Run("unknown name", func(t *testing.T) {
		mustSend(t, tr, unregistered{})

		msg, ok := mustReceive(t, tr).Message().(valkey.UndecodableMessage)
		if !ok || msg.Name != "unregistered" || msg.Err == nil {
			t.Fatalf("got %#v", msg)
		}
	})

	t.Run("invalid payload", func(t *testing.T) {
		cmd := client.B().Xadd().Key(stream).Id("*").FieldValue().
			FieldValue("name", "users.created").
			FieldValue("payload", "{not json").
			FieldValue("attempt", "1").
			Build()
		if err := client.Do(t.Context(), cmd).Error(); err != nil {
			t.Fatal(err)
		}

		msg, ok := mustReceive(t, tr).Message().(valkey.UndecodableMessage)
		if !ok || string(msg.Payload) != "{not json" || msg.Err == nil {
			t.Fatalf("got %#v", msg)
		}
	})
}

type unregistered struct{}

func (unregistered) Name() string { return "unregistered" }

func TestReceiveStopsWithContextAndClose(t *testing.T) {
	tr, _, _ := newTransport(t)

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	if _, err := tr.Receive(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want DeadlineExceeded", err)
	}

	done := make(chan error)
	go func() {
		_, err := tr.Receive(t.Context())
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	tr.Close()

	select {
	case err := <-done:
		if !errors.Is(err, qumsg.ErrTransportClosed) {
			t.Fatalf("got %v, want ErrTransportClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Receive did not return after Close")
	}
}

func TestMaxLenTrimsTheStream(t *testing.T) {
	tr, client, stream := newTransport(t, valkey.WithMaxLen(10))
	for i := range 1000 {
		mustSend(t, tr, userCreated{ID: string(rune('a' + i%26))})
	}

	n, err := client.Do(t.Context(), client.B().Xlen().Key(stream).Build()).AsInt64()
	if err != nil {
		t.Fatal(err)
	}
	// Approximate trimming removes whole internal nodes, so allow some slack.
	if n >= 1000 || n < 10 {
		t.Fatalf("stream length %d, want about 10", n)
	}
}

func TestConsumeEndToEnd(t *testing.T) {
	tr, _, _ := newTransport(t)

	producer := qumsg.New()
	if err := producer.Route[userCreated](tr); err != nil {
		t.Fatal(err)
	}

	const total = 50
	var mu sync.Mutex
	seen := make(map[string]int)
	allDone := make(chan struct{})

	worker := qumsg.New()
	if err := worker.Register(func(_ context.Context, msg userCreated) error {
		mu.Lock()
		defer mu.Unlock()
		seen[msg.ID]++
		if len(seen) == total {
			close(allDone)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	consumed := make(chan error)
	go func() { consumed <- worker.Consume(ctx, tr, qumsg.WithWorkers(4)) }()

	for i := range total {
		if err := producer.Dispatch(t.Context(), userCreated{ID: string(rune('A' + i))}); err != nil {
			t.Fatal(err)
		}
	}

	select {
	case <-allDone:
	case <-time.After(10 * time.Second):
		t.Fatal("messages were not consumed")
	}
	cancel()
	if err := <-consumed; err != nil {
		t.Fatal(err)
	}

	for id, n := range seen {
		if n != 1 {
			t.Fatalf("message %s processed %d times", id, n)
		}
	}
}

func TestConsumeHandsOverFailedMessages(t *testing.T) {
	tr, _, _ := newTransport(t)
	mustSend(t, tr, userCreated{ID: "1"})
	mustSend(t, tr, unregistered{})

	boom := errors.New("boom")
	worker := qumsg.New()
	attempts := 0
	if err := worker.Register(func(context.Context, userCreated) error {
		attempts++
		return boom
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	failed := make(map[string]error)
	err := worker.Consume(ctx, tr,
		qumsg.WithMaxAttempts(3),
		qumsg.WithFailureHandler(func(_ context.Context, msg any, err error) {
			switch m := msg.(type) {
			case userCreated:
				failed["users.created"] = err
			case valkey.UndecodableMessage:
				failed[m.Name] = err
			}
			if len(failed) == 2 {
				cancel()
			}
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	if attempts != 3 || failed["users.created"] != boom {
		t.Fatalf("attempts %d, failure %v; want 3 attempts ending in boom", attempts, failed["users.created"])
	}
	if !errors.Is(failed["unregistered"], qumsg.ErrNoHandler) {
		t.Fatalf("undecodable message failed with %v, want ErrNoHandler", failed["unregistered"])
	}
}

// countingCodec wraps JSON and fails to encode when fail is set.
type countingCodec struct {
	mu                 sync.Mutex
	marshal, unmarshal int
	fail               bool
}

func (c *countingCodec) Marshal(v any) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.marshal++
	if c.fail {
		return nil, errors.New("cannot encode")
	}
	return json.Marshal(v)
}

func (c *countingCodec) Unmarshal(data []byte, v any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.unmarshal++
	return json.Unmarshal(data, v)
}

func TestCodecAndGroupOptions(t *testing.T) {
	codec := &countingCodec{}
	tr, client, stream := newTransport(t, valkey.WithCodec(codec), valkey.WithGroup("mailers"))

	mustSend(t, tr, userCreated{ID: "1"})
	if got := mustReceive(t, tr).Message(); got != (userCreated{ID: "1"}) {
		t.Fatalf("got %#v", got)
	}
	if codec.marshal != 1 || codec.unmarshal != 1 {
		t.Fatalf("codec used %d/%d times, want 1/1", codec.marshal, codec.unmarshal)
	}

	groups, err := client.Do(t.Context(), client.B().XinfoGroups().Key(stream).Build()).ToArray()
	if err != nil {
		t.Fatal(err)
	}
	info, _ := groups[0].AsStrMap()
	if info["name"] != "mailers" {
		t.Fatalf("group %q, want mailers", info["name"])
	}

	codec.fail = true
	if err := tr.Send(t.Context(), userCreated{}); err == nil {
		t.Fatal("Send did not return the encoding error")
	}
}

// breakStream replaces the stream with a string, so stream commands fail
// with WRONGTYPE.
func breakStream(t *testing.T, client vk.Client, stream string) {
	t.Helper()
	if err := client.Do(t.Context(), client.B().Set().Key(stream).Value("not a stream").Build()).Error(); err != nil {
		t.Fatal(err)
	}
}

func TestValkeyErrorsAreReturned(t *testing.T) {
	t.Run("send and create group", func(t *testing.T) {
		tr, client, stream := newTransport(t)
		breakStream(t, client, stream)

		if err := tr.Send(t.Context(), userCreated{}); err == nil {
			t.Fatal("Send: want WRONGTYPE error")
		}
		if _, err := tr.Receive(t.Context()); err == nil {
			t.Fatal("Receive: want error creating the group")
		}
	})

	t.Run("claim", func(t *testing.T) {
		tr, client, stream := newTransport(t)
		mustSend(t, tr, userCreated{})
		mustReceive(t, tr) // creates the group
		breakStream(t, client, stream)

		if _, err := tr.Receive(t.Context()); err == nil {
			t.Fatal("Receive: want WRONGTYPE error")
		}
	})

	t.Run("ack and nack", func(t *testing.T) {
		tr, client, stream := newTransport(t)
		mustSend(t, tr, userCreated{})
		mustSend(t, tr, userCreated{})
		first, second := mustReceive(t, tr), mustReceive(t, tr)
		breakStream(t, client, stream)

		if err := first.Ack(t.Context()); err == nil {
			t.Fatal("Ack: want WRONGTYPE error")
		}
		if err := second.Nack(t.Context()); err == nil {
			t.Fatal("Nack: want WRONGTYPE error")
		}
	})
}
