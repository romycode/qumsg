package valkey_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/romycode/qumsg"
	"github.com/romycode/qumsg/valkey"
)

type userCreated struct {
	ID    string
	Email string
}

func (userCreated) Name() string { return "users.created" }

type sameName struct{}

func (sameName) Name() string { return "users.created" }

type unnamed struct{}

type emptyName struct{}

func (emptyName) Name() string { return "" }

type fieldNamed struct{ meta *struct{ name string } }

func (m fieldNamed) Name() string { return m.meta.name }

// The validations below run before any command, so no Valkey server is needed.
func newOffline(opts ...valkey.Option) *valkey.Transport {
	return valkey.New(nil, "offline", opts...)
}

func TestAcceptValidation(t *testing.T) {
	cases := map[string]struct {
		accept func(*valkey.Transport) error
		want   error
	}{
		"interface type": {func(tr *valkey.Transport) error { return tr.Accept(qumsg.TypeOf[qumsg.NamedMessage]()) }, qumsg.ErrInterfaceType},
		"unnamed":        {func(tr *valkey.Transport) error { return tr.Accept(qumsg.TypeOf[unnamed]()) }, valkey.ErrUnnamedMessage},
		"empty name":     {func(tr *valkey.Transport) error { return tr.Accept(qumsg.TypeOf[emptyName]()) }, qumsg.ErrEmptyName},
		"name panics":    {func(tr *valkey.Transport) error { return tr.Accept(qumsg.TypeOf[fieldNamed]()) }, qumsg.ErrNamePanic},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if err := tc.accept(newOffline()); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}

	t.Run("same type twice", func(t *testing.T) {
		tr := newOffline()
		if err := tr.Accept(qumsg.TypeOf[userCreated]()); err != nil {
			t.Fatal(err)
		}
		if err := tr.Accept(qumsg.TypeOf[userCreated]()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("name collision", func(t *testing.T) {
		tr := newOffline()
		if err := tr.Accept(qumsg.TypeOf[userCreated]()); err != nil {
			t.Fatal(err)
		}
		if err := tr.Accept(qumsg.TypeOf[sameName]()); !errors.Is(err, qumsg.ErrDuplicateName) {
			t.Fatalf("got %v, want ErrDuplicateName", err)
		}
	})

	t.Run("pointer type", func(t *testing.T) {
		if err := newOffline().Accept(qumsg.TypeOf[*userCreated]()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestSendValidation(t *testing.T) {
	ctx := context.Background()
	cases := map[string]struct {
		message any
		want    error
	}{
		"nil":         {nil, qumsg.ErrNilMessage},
		"unnamed":     {unnamed{}, valkey.ErrUnnamedMessage},
		"empty name":  {emptyName{}, qumsg.ErrEmptyName},
		"name panics": {fieldNamed{}, qumsg.ErrNamePanic},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if err := newOffline().Send(ctx, qumsg.NewEnvelope(tc.message)); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestClosedTransport(t *testing.T) {
	tr := newOffline()
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tr.Send(t.Context(), qumsg.NewEnvelope(userCreated{})); !errors.Is(err, qumsg.ErrTransportClosed) {
		t.Fatalf("Send: got %v", err)
	}
	if _, err := tr.Receive(t.Context()); !errors.Is(err, qumsg.ErrTransportClosed) {
		t.Fatalf("Receive: got %v", err)
	}
}

func TestOptionsValidateArguments(t *testing.T) {
	for name, build := range map[string]func(){
		"block":      func() { valkey.WithBlock(0) },
		"claim idle": func() { valkey.WithClaimIdle(time.Microsecond) },
		"max len":    func() { valkey.WithMaxLen(0) },
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

type productCreated struct{ ID string }

func (productCreated) Name() string { return "products.created" }

func TestAccept(t *testing.T) {
	t.Run("valid list", func(t *testing.T) {
		tr := newOffline()
		if err := tr.Accept(qumsg.TypeOf[userCreated](), qumsg.TypeOf[productCreated]()); err != nil {
			t.Fatal(err)
		}
		// Accepting again, alone or in a list, is fine.
		if err := tr.Accept(qumsg.TypeOf[userCreated]()); err != nil {
			t.Fatal(err)
		}
	})

	cases := map[string]struct {
		types []qumsg.Type
		want  error
	}{
		"zero type":         {[]qumsg.Type{qumsg.TypeOf[productCreated](), {}}, qumsg.ErrZeroType},
		"unnamed":           {[]qumsg.Type{qumsg.TypeOf[productCreated](), qumsg.TypeOf[unnamed]()}, valkey.ErrUnnamedMessage},
		"collision in list": {[]qumsg.Type{qumsg.TypeOf[productCreated](), qumsg.TypeOf[userCreated](), qumsg.TypeOf[sameName]()}, qumsg.ErrDuplicateName},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tr := newOffline()
			if err := tr.Accept(tc.types...); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			// Nothing was accepted: productCreated can still take its alias
			// with another type.
			if err := tr.Accept(qumsg.TypeOf[*productCreated]()); err != nil {
				t.Fatalf("a failed Accept accepted some types: %v", err)
			}
		})
	}

	t.Run("collision with an accepted type", func(t *testing.T) {
		tr := newOffline()
		if err := tr.Accept(qumsg.TypeOf[userCreated]()); err != nil {
			t.Fatal(err)
		}
		if err := tr.Accept(qumsg.TypeOf[productCreated](), qumsg.TypeOf[sameName]()); !errors.Is(err, qumsg.ErrDuplicateName) {
			t.Fatalf("got %v, want ErrDuplicateName", err)
		}
		if err := tr.Accept(qumsg.TypeOf[*productCreated]()); err != nil {
			t.Fatalf("productCreated was accepted by a failed call: %v", err)
		}
	})
}
