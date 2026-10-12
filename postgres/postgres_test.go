package postgres_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/romycode/qumsg"
	"github.com/romycode/qumsg/postgres"
)

type userCreated struct {
	ID    string
	Email string
}

func (userCreated) Name() string { return "users.created" }

type sameName struct{}

func (sameName) Name() string { return "users.created" }

type productCreated struct{ ID string }

func (productCreated) Name() string { return "products.created" }

type unnamed struct{}

type emptyName struct{}

func (emptyName) Name() string { return "" }

type fieldNamed struct{ meta *struct{ name string } }

func (m fieldNamed) Name() string { return m.meta.name }

type verifyResource struct{ ResourceID string }

func (verifyResource) Name() string { return "resources.verify" }

func (m verifyResource) DedupKey() string { return "verify:" + m.ResourceID }

type unencodable struct{ C chan int }

func (unencodable) Name() string { return "unencodable" }

// fakeDB records the statements Send runs. inserted is the row count the
// INSERT reports.
type fakeDB struct {
	inserted int
	err      error
	calls    []call
}

type call struct {
	sql  string
	args []any
}

func (db *fakeDB) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	db.calls = append(db.calls, call{sql: sql, args: args})
	if db.err != nil {
		return pgconn.CommandTag{}, db.err
	}
	switch statement := strings.TrimSpace(sql); {
	case strings.HasPrefix(statement, "INSERT"):
		return pgconn.NewCommandTag("INSERT 0 " + string(rune('0'+db.inserted))), nil
	case strings.HasPrefix(statement, "DELETE"):
		return pgconn.NewCommandTag("DELETE 0"), nil
	}
	return pgconn.NewCommandTag("SELECT 1"), nil
}

func (db *fakeDB) QueryRow(context.Context, string, ...any) pgx.Row {
	panic("Send must not query rows")
}

// The validations below run before any statement, so no PostgreSQL server is
// needed: the transport is built without a pool.
func newOffline(t *testing.T, opts ...postgres.Option) *postgres.Transport {
	t.Helper()
	tr, err := postgres.New(nil, "messages", opts...)
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func TestNewValidatesTable(t *testing.T) {
	for _, table := range []string{"", "Messages", "1messages", "messages;drop", "public.messages", "messages-x", strings.Repeat("a", 57)} {
		t.Run(table, func(t *testing.T) {
			if _, err := postgres.New(nil, table); !errors.Is(err, postgres.ErrInvalidTable) {
				t.Fatalf("got %v, want ErrInvalidTable", err)
			}
		})
	}
	for _, table := range []string{"messages", "resource_jobs", "q2", strings.Repeat("a", 56)} {
		if _, err := postgres.New(nil, table); err != nil {
			t.Fatalf("%s: %v", table, err)
		}
	}
}

func TestSchema(t *testing.T) {
	ddl, err := postgres.Schema("resource_jobs")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`CREATE TABLE "resource_jobs"`, "dedup_key", "available_at", "locked_by", `CREATE INDEX "resource_jobs_ready"`} {
		if !strings.Contains(ddl, want) {
			t.Errorf("schema lacks %q:\n%s", want, ddl)
		}
	}
	if _, err := postgres.Schema("bad name"); !errors.Is(err, postgres.ErrInvalidTable) {
		t.Fatalf("got %v, want ErrInvalidTable", err)
	}
}

func TestOptionsValidateArguments(t *testing.T) {
	for name, build := range map[string]func(){
		"lease":         func() { postgres.WithLease(0) },
		"short lease":   func() { postgres.WithLease(time.Millisecond) },
		"poll interval": func() { postgres.WithPollInterval(0) },
		"backoff":       func() { postgres.WithBackoff(nil) },
		"db":            func() { postgres.WithDB(nil) },
		"owner":         func() { postgres.WithOwner("") },
		"codec":         func() { postgres.WithCodec(nil) },
		"delay":         func() { postgres.ContextWithDelay(context.Background(), -time.Second) },
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

func TestDefaultBackoff(t *testing.T) {
	want := map[int]time.Duration{
		1:  time.Second,
		2:  2 * time.Second,
		3:  4 * time.Second,
		9:  256 * time.Second,
		10: 5 * time.Minute,
		50: 5 * time.Minute,
	}
	for attempt, d := range want {
		if got := postgres.DefaultBackoff(attempt); got != d {
			t.Errorf("DefaultBackoff(%d) = %v, want %v", attempt, got, d)
		}
	}
}

func TestAcceptValidation(t *testing.T) {
	cases := map[string]struct {
		types []qumsg.Type
		want  error
	}{
		"zero type":         {[]qumsg.Type{qumsg.TypeOf[productCreated](), {}}, qumsg.ErrZeroType},
		"interface type":    {[]qumsg.Type{qumsg.TypeOf[qumsg.NamedMessage]()}, qumsg.ErrInterfaceType},
		"unnamed":           {[]qumsg.Type{qumsg.TypeOf[productCreated](), qumsg.TypeOf[unnamed]()}, postgres.ErrUnnamedMessage},
		"empty name":        {[]qumsg.Type{qumsg.TypeOf[emptyName]()}, qumsg.ErrEmptyName},
		"name panics":       {[]qumsg.Type{qumsg.TypeOf[fieldNamed]()}, qumsg.ErrNamePanic},
		"collision in list": {[]qumsg.Type{qumsg.TypeOf[productCreated](), qumsg.TypeOf[userCreated](), qumsg.TypeOf[sameName]()}, qumsg.ErrDuplicateName},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tr := newOffline(t)
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

	t.Run("same type twice", func(t *testing.T) {
		tr := newOffline(t)
		for range 2 {
			if err := tr.Accept(qumsg.TypeOf[userCreated](), qumsg.TypeOf[productCreated]()); err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("collision with an accepted type", func(t *testing.T) {
		tr := newOffline(t)
		if err := tr.Accept(qumsg.TypeOf[userCreated]()); err != nil {
			t.Fatal(err)
		}
		if err := tr.Accept(qumsg.TypeOf[sameName]()); !errors.Is(err, qumsg.ErrDuplicateName) {
			t.Fatalf("got %v, want ErrDuplicateName", err)
		}
	})
}

func TestSendValidation(t *testing.T) {
	cases := map[string]struct {
		message any
		want    error
	}{
		"nil":         {nil, qumsg.ErrNilMessage},
		"unnamed":     {unnamed{}, postgres.ErrUnnamedMessage},
		"empty name":  {emptyName{}, qumsg.ErrEmptyName},
		"name panics": {fieldNamed{}, qumsg.ErrNamePanic},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			db := &fakeDB{inserted: 1}
			err := newOffline(t, postgres.WithDB(db)).Send(t.Context(), qumsg.NewEnvelope(tc.message))
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			if len(db.calls) != 0 {
				t.Fatalf("ran %d statements for an invalid message", len(db.calls))
			}
		})
	}

	t.Run("payload codec error", func(t *testing.T) {
		db := &fakeDB{inserted: 1}
		err := newOffline(t, postgres.WithDB(db)).Send(t.Context(), qumsg.NewEnvelope(unencodable{}))
		if err == nil || !strings.Contains(err.Error(), "encode unencodable") {
			t.Fatalf("got %v, want an encode error", err)
		}
	})
}

func TestSendInsertsAndNotifies(t *testing.T) {
	db := &fakeDB{inserted: 1}
	tr := newOffline(t, postgres.WithDB(db))

	e := qumsg.NewEnvelope(userCreated{ID: "u1", Email: "a@example.com"}).With("request_id", "r-1")
	if err := tr.Send(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	if len(db.calls) != 2 {
		t.Fatalf("got %d statements, want INSERT and NOTIFY", len(db.calls))
	}

	insert := db.calls[0]
	if !strings.Contains(insert.sql, `INSERT INTO "messages"`) {
		t.Fatalf("first statement: %s", insert.sql)
	}
	if insert.args[0] != "users.created" {
		t.Errorf("name: got %v", insert.args[0])
	}
	if got := string(insert.args[1].([]byte)); got != `{"ID":"u1","Email":"a@example.com"}` {
		t.Errorf("payload: got %s", got)
	}
	if insert.args[2] != `{"request_id":"r-1"}` {
		t.Errorf("metadata: got %v", insert.args[2])
	}
	if key := insert.args[3].(*string); key != nil {
		t.Errorf("dedup key: got %q, want none", *key)
	}
	if insert.args[4] != int64(0) {
		t.Errorf("delay: got %v, want 0", insert.args[4])
	}

	notify := db.calls[1]
	if !strings.Contains(notify.sql, "pg_notify") || notify.args[0] != "qumsg_messages" {
		t.Fatalf("second statement: %s %v", notify.sql, notify.args)
	}
}

func TestSendWithoutMetadata(t *testing.T) {
	db := &fakeDB{inserted: 1}
	if err := newOffline(t, postgres.WithDB(db)).Send(t.Context(), qumsg.NewEnvelope(userCreated{})); err != nil {
		t.Fatal(err)
	}
	if db.calls[0].args[2] != `{}` {
		t.Fatalf("metadata: got %v, want {}", db.calls[0].args[2])
	}
}

func TestSendDedupAndDelay(t *testing.T) {
	db := &fakeDB{inserted: 0}
	tr := newOffline(t, postgres.WithDB(db))

	ctx := postgres.ContextWithDelay(t.Context(), 90*time.Second)
	if err := tr.Send(ctx, qumsg.NewEnvelope(verifyResource{ResourceID: "r1"})); err != nil {
		t.Fatal(err)
	}
	if len(db.calls) != 1 {
		t.Fatalf("an absorbed message must not notify: %d statements", len(db.calls))
	}
	args := db.calls[0].args
	if key := args[3].(*string); key == nil || *key != "verify:r1" {
		t.Fatalf("dedup key: got %v", key)
	}
	if args[4] != (90 * time.Second).Microseconds() {
		t.Fatalf("delay: got %v µs", args[4])
	}
}

func TestSendWrapsDatabaseErrors(t *testing.T) {
	boom := errors.New("boom")
	err := newOffline(t, postgres.WithDB(&fakeDB{err: boom})).Send(t.Context(), qumsg.NewEnvelope(userCreated{}))
	if !errors.Is(err, boom) {
		t.Fatalf("got %v, want it to wrap the database error", err)
	}
}

func TestClosedTransport(t *testing.T) {
	db := &fakeDB{inserted: 1}
	tr := newOffline(t, postgres.WithDB(db))
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := tr.Send(t.Context(), qumsg.NewEnvelope(userCreated{})); !errors.Is(err, qumsg.ErrTransportClosed) {
		t.Fatalf("Send: got %v", err)
	}
	if _, err := tr.Receive(t.Context()); !errors.Is(err, qumsg.ErrTransportClosed) {
		t.Fatalf("Receive: got %v", err)
	}
	if len(db.calls) != 0 {
		t.Fatalf("a closed transport ran %d statements", len(db.calls))
	}
}

func TestReceiveCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := newOffline(t).Receive(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

func TestCompleteWithoutLease(t *testing.T) {
	db := &fakeDB{}
	err := newOffline(t).Complete(t.Context(), db)
	if !errors.Is(err, postgres.ErrNoLease) {
		t.Fatalf("got %v, want ErrNoLease", err)
	}
	if len(db.calls) != 0 {
		t.Fatal("Complete without a lease ran a statement")
	}
}

func TestPropagator(t *testing.T) {
	tr := newOffline(t)
	p := tr.Propagator()

	t.Run("inject adds nothing", func(t *testing.T) {
		e := p.Inject(t.Context(), qumsg.NewEnvelope(userCreated{}))
		for key := range e.Metadata() {
			t.Fatalf("Inject added %q", key)
		}
	})

	t.Run("extract ignores foreign or malformed metadata", func(t *testing.T) {
		for _, value := range []string{"", "not json", `{"table":"other","id":1,"attempt":1,"owner":"w"}`} {
			e := qumsg.NewEnvelope(userCreated{}).With("qumsg.postgres.lease", value)
			ctx := p.Extract(t.Context(), e)
			if err := tr.Complete(ctx, &fakeDB{}); !errors.Is(err, postgres.ErrNoLease) {
				t.Fatalf("%q: got %v, want ErrNoLease", value, err)
			}
		}
	})

	t.Run("lease of the transport", func(t *testing.T) {
		e := qumsg.NewEnvelope(userCreated{}).With("qumsg.postgres.lease", `{"table":"messages","id":7,"attempt":2,"owner":"w"}`)
		db := &fakeDB{inserted: 1}
		err := tr.Complete(p.Extract(t.Context(), e), db)
		// The fake reports no deleted row: another owner holds the message.
		if !errors.Is(err, postgres.ErrLeaseLost) {
			t.Fatalf("got %v, want ErrLeaseLost", err)
		}
		if len(db.calls) != 1 || !strings.Contains(db.calls[0].sql, `DELETE FROM "messages"`) {
			t.Fatalf("statements: %v", db.calls)
		}
		if got := db.calls[0].args; got[0] != int64(7) || got[1] != "w" || got[2] != 2 {
			t.Fatalf("fence args: %v", got)
		}
	})
}
