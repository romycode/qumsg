package qumsg_test

import (
	"context"
	"maps"
	"testing"

	"github.com/romycode/qumsg"
	"github.com/romycode/qumsg/memory"
)

func TestEnvelopeWithDoesNotChangeTheOriginal(t *testing.T) {
	base := qumsg.NewEnvelope(job{N: 1}).With("a", "1")
	changed := base.With("a", "2").With("b", "3")

	if got := maps.Collect(base.Metadata()); !maps.Equal(got, map[string]string{"a": "1"}) {
		t.Fatalf("original changed: %v", got)
	}
	if got := maps.Collect(changed.Metadata()); !maps.Equal(got, map[string]string{"a": "2", "b": "3"}) {
		t.Fatalf("got %v", got)
	}
	if changed.Message() != (job{N: 1}) {
		t.Fatalf("got message %v", changed.Message())
	}
	if _, ok := qumsg.NewEnvelope(job{}).Get("a"); ok {
		t.Fatal("empty envelope has metadata")
	}
}

type metaKey string

func TestPropagatorsCarryContextValuesThroughTheTransport(t *testing.T) {
	tr := memory.New()
	propagators := qumsg.WithPropagators(
		qumsg.ContextValue("request_id", metaKey("request")),
		qumsg.ContextValue("actor", metaKey("actor")),
	)

	router := qumsg.NewRouter()
	if err := router.Route[job](tr); err != nil {
		t.Fatal(err)
	}
	producer := qumsg.New(propagators, qumsg.WithRouter(router))
	ctx := context.WithValue(t.Context(), metaKey("request"), "req-1") // no actor
	if err := producer.Dispatch(ctx, job{N: 1}); err != nil {
		t.Fatal(err)
	}

	d, err := tr.Receive(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got := maps.Collect(d.Envelope().Metadata()); !maps.Equal(got, map[string]string{"request_id": "req-1"}) {
		t.Fatalf("got metadata %v", got)
	}
	if err := tr.Send(t.Context(), d.Envelope()); err != nil {
		t.Fatal(err)
	}
	tr.Close()

	worker := qumsg.New(propagators)
	var request, actor any
	if err := worker.Register(func(ctx context.Context, _ job) error {
		request, actor = ctx.Value(metaKey("request")), ctx.Value(metaKey("actor"))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := worker.Consume(t.Context(), tr); err != nil {
		t.Fatal(err)
	}
	if request != "req-1" || actor != nil {
		t.Fatalf("got request %v, actor %v", request, actor)
	}
}
