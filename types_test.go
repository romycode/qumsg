package qumsg_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/romycode/qumsg"
	"github.com/romycode/qumsg/memory"
)

func TestTypeOf(t *testing.T) {
	typ := qumsg.TypeOf[userCreated]()
	if typ.Reflect() != reflect.TypeFor[userCreated]() || typ.String() != "qumsg_test.userCreated" {
		t.Fatalf("unexpected type %v", typ)
	}
	if name, named := typ.Name(); name != "users.created" || !named {
		t.Fatalf("Name() = %q, %v", name, named)
	}
	if err := typ.Err(); err != nil {
		t.Fatal(err)
	}
	if name, named := qumsg.TypeOf[productCreated]().Name(); name != "" || named {
		t.Fatalf("unnamed type: Name() = %q, %v", name, named)
	}
	if name, _ := qumsg.TypeOf[*pointerNamed]().Name(); name != "pointer.named" {
		t.Fatalf("pointer type: Name() = %q", name)
	}

	var zero qumsg.Type
	if zero.Reflect() != nil || zero.String() != "<zero qumsg.Type>" || !errors.Is(zero.Err(), qumsg.ErrZeroType) {
		t.Fatalf("unexpected zero Type %v, %v", zero, zero.Err())
	}
}

func TestTypeOfInvalidTypes(t *testing.T) {
	for name, tc := range map[string]struct {
		typ  qumsg.Type
		want error
	}{
		"interface":   {qumsg.TypeOf[qumsg.NamedMessage](), qumsg.ErrInterfaceType},
		"empty name":  {qumsg.TypeOf[emptyName](), qumsg.ErrEmptyName},
		"name panics": {qumsg.TypeOf[fieldNamed](), qumsg.ErrNamePanic},
		"zero":        {qumsg.Type{}, qumsg.ErrZeroType},
	} {
		t.Run(name, func(t *testing.T) {
			if err := tc.typ.Err(); !errors.Is(err, tc.want) {
				t.Fatalf("Err: got %v, want %v", err, tc.want)
			}
			bus := qumsg.New()
			ok := qumsg.TypeOf[productCreated]()
			if err := bus.RegisterFor(func(context.Context, any) error { return nil }, ok, tc.typ); !errors.Is(err, tc.want) {
				t.Fatalf("RegisterFor: got %v, want %v", err, tc.want)
			}
			if err := bus.Dispatch(t.Context(), productCreated{}); !errors.Is(err, qumsg.ErrNoHandler) {
				t.Fatalf("an invalid type must prevent registering the others: %v", err)
			}
		})
	}
}

func TestTypeDecode(t *testing.T) {
	msg, err := qumsg.TypeOf[userCreated]().Decode(func(target any) error {
		return json.Unmarshal([]byte(`{"ID":"42"}`), target)
	})
	if err != nil || msg != (userCreated{ID: "42"}) {
		t.Fatalf("got %#v, %v", msg, err)
	}

	ptr, err := qumsg.TypeOf[*userCreated]().Decode(func(target any) error {
		return json.Unmarshal([]byte(`{"ID":"7"}`), target)
	})
	if p, ok := ptr.(*userCreated); err != nil || !ok || p.ID != "7" {
		t.Fatalf("pointer type: got %#v, %v", ptr, err)
	}

	boom := errors.New("boom")
	if _, err := qumsg.TypeOf[userCreated]().Decode(func(any) error { return boom }); err != boom {
		t.Fatalf("unmarshal error: got %v", err)
	}
	if _, err := (qumsg.Type{}).Decode(func(any) error { return nil }); !errors.Is(err, qumsg.ErrZeroType) {
		t.Fatalf("zero Type: got %v", err)
	}
}

func TestRegisterFor(t *testing.T) {
	bus := qumsg.New()
	var got []any
	handler := func(_ context.Context, msg any) error {
		got = append(got, msg)
		return nil
	}
	types := []qumsg.Type{qumsg.TypeOf[userCreated](), qumsg.TypeOf[productCreated]()}
	if err := bus.RegisterFor(handler, types...); err != nil {
		t.Fatal(err)
	}
	// It is a normal handler: it runs with the typed ones, in order.
	_ = bus.Register(func(_ context.Context, msg userCreated) error {
		got = append(got, "typed")
		return nil
	})

	for _, msg := range []any{userCreated{ID: "u"}, productCreated{ID: "p"}} {
		if err := bus.Dispatch(t.Context(), msg); err != nil {
			t.Fatal(err)
		}
	}
	want := []any{userCreated{ID: "u"}, "typed", productCreated{ID: "p"}}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	if err := bus.RegisterFor(nil, types...); !errors.Is(err, qumsg.ErrNilHandler) {
		t.Fatalf("nil handler: got %v", err)
	}
	if err := bus.RegisterFor(handler, qumsg.TypeOf[sameName]()); !errors.Is(err, qumsg.ErrDuplicateName) {
		t.Fatalf("aliases are still unique: got %v", err)
	}
}

func TestRouteTypes(t *testing.T) {
	transport := memory.New()
	bus := qumsg.New()
	if err := bus.RouteTypes(transport, qumsg.TypeOf[userCreated](), qumsg.TypeOf[productCreated]()); err != nil {
		t.Fatal(err)
	}
	for _, msg := range []any{userCreated{ID: "u"}, productCreated{ID: "p"}} {
		if err := bus.Dispatch(t.Context(), msg); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []any{userCreated{ID: "u"}, productCreated{ID: "p"}} {
		d, err := transport.Receive(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if d.Envelope().Message() != want {
			t.Fatalf("got %#v, want %#v", d.Envelope().Message(), want)
		}
		_ = d.Ack(t.Context())
	}
	if err := bus.RouteTypes(nil, qumsg.TypeOf[userCreated]()); !errors.Is(err, qumsg.ErrNilTransport) {
		t.Fatalf("nil transport: got %v", err)
	}
}
