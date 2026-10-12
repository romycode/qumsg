// Package postgres implements a qumsg.Transport on a PostgreSQL table.
//
// Each message is a row. Send inserts it, optionally inside the caller's own
// transaction (see WithDB), so the message exists exactly when the write that
// needs it commits. Receive claims one ready row with FOR UPDATE SKIP LOCKED
// and records a lease on it (owner, time and attempt), so several workers and
// processes share the work and each message is processed by one of them. The
// lease is renewed while the delivery is outstanding; if its worker stops,
// another one reclaims the row once the lease expires. Ack deletes the row,
// Nack makes it available again after a backoff. Every settlement is fenced
// by owner and attempt: a worker whose lease was taken over gets ErrLeaseLost
// and changes nothing. Delivery is at least once.
//
// Receivers wait with LISTEN/NOTIFY: Send announces each new message with
// NOTIFY, which PostgreSQL delivers when the transaction commits. A polling
// interval covers missed notifications, scheduled retries and expired leases.
//
// Messages are identified by their qumsg.NamedMessage alias, not by their Go
// type, and the consumer side learns the types it can decode from the router
// of the bus that consumes it (see Accept). The metadata of the
// qumsg.Envelope are stored as a JSON object, independent of the payload
// codec.
//
// The application creates the table in its own migration; Schema returns the
// statements.
package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/romycode/qumsg"
)

var (
	// ErrInvalidTable is returned by New and Schema when the table name is
	// not a plain lowercase identifier of at most 56 characters.
	ErrInvalidTable = errors.New("postgres: invalid table name")
	// ErrUnnamedMessage is returned by Send and Accept when the message type
	// does not implement qumsg.NamedMessage, which is required to identify it
	// in the table.
	ErrUnnamedMessage = errors.New("postgres: message type must implement qumsg.NamedMessage")
	// ErrLeaseLost is returned by Ack, Nack and Complete when the message is
	// no longer leased by this delivery: its lease expired and another
	// worker reclaimed it. Nothing was changed.
	ErrLeaseLost = errors.New("postgres: lease lost")
	// ErrNoLease is returned by Complete when ctx does not carry a delivery
	// of the transport: the bus that consumes it lacks Transport.Propagator,
	// or Complete was called outside a handler.
	ErrNoLease = errors.New("postgres: context carries no lease of the transport")
)

// DB is what Send and Complete run their statements on: a *pgxpool.Pool, a
// pgx.Tx or an executor that picks the transaction carried by the context.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Deduplicated is implemented by messages that must have at most one pending
// copy. While a message with the same non-empty key is in the table (waiting,
// in progress or waiting for a retry), Send of another one does nothing and
// returns nil.
type Deduplicated interface {
	DedupKey() string
}

// Codec encodes message payloads. The default is JSON.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// UndecodableMessage is delivered instead of a message whose name is not
// accepted or whose payload or metadata cannot be decoded. No handler is
// usually registered for it, so it fails with qumsg.ErrNoHandler, exhausts its
// attempts and reaches the FailureHandler of Consume.
type UndecodableMessage struct {
	ID      int64
	Name    string
	Payload []byte
	Err     error
}

// DefaultBackoff waits 1s before the second attempt and doubles the wait for
// each attempt after it, up to 5 minutes.
func DefaultBackoff(attempt int) time.Duration {
	const limit = 5 * time.Minute
	delay := time.Second
	for n := 1; n < attempt && delay < limit; n++ {
		delay *= 2
	}
	return min(delay, limit)
}

// Option configures a Transport.
type Option func(*Transport)

// WithDB sets what Send inserts with. Pass the executor of the application,
// one that runs on the transaction carried by ctx when there is one, and a
// message sent inside that transaction commits or rolls back with it.
// Default: the pool. Receive and the settlement of deliveries always use the
// pool, in short transactions of their own.
func WithDB(db DB) Option {
	if db == nil {
		panic("postgres: WithDB requires a DB")
	}
	return func(t *Transport) { t.db = db }
}

// WithOwner sets the name recorded in the leases of this transport. It must
// be unique per process. Default: hostname plus a random suffix.
func WithOwner(name string) Option {
	if name == "" {
		panic("postgres: WithOwner requires a name")
	}
	return func(t *Transport) { t.owner = name }
}

// WithLease sets how long a delivery keeps its message without renewing the
// lease. The transport renews it every third of the lease until Ack or Nack,
// so a long handler keeps its message; another worker reclaims the message
// only when its worker stopped renewing, such as after a crash, for longer
// than the lease. Default 1m.
func WithLease(d time.Duration) Option {
	if d < 30*time.Millisecond {
		panic("postgres: WithLease requires d >= 30ms")
	}
	return func(t *Transport) { t.lease = d }
}

// WithBackoff sets how long a message waits after Nack before it is delivered
// again, given the attempt that failed. Default DefaultBackoff.
func WithBackoff(f func(attempt int) time.Duration) Option {
	if f == nil {
		panic("postgres: WithBackoff requires a function")
	}
	return func(t *Transport) { t.backoff = f }
}

// WithPollInterval sets the longest an idle Receive waits before checking the
// table again without a notification: a safety net for missed notifications
// or while LISTEN is not available. Receive also wakes up when the next
// retry is due or the oldest lease expires. Default 5s.
func WithPollInterval(d time.Duration) Option {
	if d < time.Millisecond {
		panic("postgres: WithPollInterval requires d >= 1ms")
	}
	return func(t *Transport) { t.poll = d }
}

// WithCodec sets the payload codec. Default JSON.
func WithCodec(c Codec) Option {
	if c == nil {
		panic("postgres: WithCodec requires a codec")
	}
	return func(t *Transport) { t.codec = c }
}

type delayKey struct{}

// ContextWithDelay returns a context that makes Send store messages to be
// delivered d after now, by the clock of the database. Use it on the context
// of Dispatch: every message the bus sends to a PostgreSQL transport with it
// is delayed. It panics if d is negative.
func ContextWithDelay(ctx context.Context, d time.Duration) context.Context {
	if d < 0 {
		panic("postgres: ContextWithDelay requires d >= 0")
	}
	return context.WithValue(ctx, delayKey{}, d)
}

// tableName restricts table names to plain lowercase identifiers, spliced
// into the statements: identifiers cannot be bound as parameters. 56
// characters leave room for the index names and the notification channel
// within the 63 that PostgreSQL keeps.
var tableName = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,55}$`)

func validTable(table string) error {
	if !tableName.MatchString(table) {
		return fmt.Errorf("%w: %q", ErrInvalidTable, table)
	}
	return nil
}

// Schema returns the statements that create the table and its indexes. Put
// them in a migration of the application. The table has these columns:
//
//	id           bigint, identity primary key
//	name         text, the qumsg.NamedMessage alias
//	payload      bytea, the message encoded by the codec
//	metadata     jsonb, the metadata of the envelope ({} if none)
//	dedup_key    text, unique, from Deduplicated (NULL if none)
//	attempt      integer, how many times the message was delivered
//	available_at timestamptz, when it can be delivered (next)
//	locked_by    text, owner of the lease (NULL if not leased)
//	locked_at    timestamptz, last renewal of the lease
//	created_at   timestamptz
func Schema(table string) (string, error) {
	if err := validTable(table); err != nil {
		return "", err
	}
	r := strings.NewReplacer("{t}", pgx.Identifier{table}.Sanitize(),
		"{ready}", pgx.Identifier{table + "_ready"}.Sanitize(),
		"{locked}", pgx.Identifier{table + "_locked"}.Sanitize())
	return r.Replace(`CREATE TABLE {t}
(
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name         text        NOT NULL,
    payload      bytea       NOT NULL,
    metadata     jsonb       NOT NULL DEFAULT '{}',
    dedup_key    text UNIQUE,
    attempt      integer     NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    available_at timestamptz NOT NULL DEFAULT now(),
    locked_by    text,
    locked_at    timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX {ready} ON {t} (available_at, id) WHERE locked_at IS NULL;
CREATE INDEX {locked} ON {t} (locked_at) WHERE locked_at IS NOT NULL;
`), nil
}

// Transport is a qumsg.Transport on a PostgreSQL table, safe for concurrent
// use. It does not own the pool: close the pool after the transport.
type Transport struct {
	pool    *pgxpool.Pool
	db      DB
	table   string
	channel string
	owner   string
	lease   time.Duration
	backoff func(attempt int) time.Duration
	poll    time.Duration
	codec   Codec

	sql statements

	mu       sync.RWMutex
	decoders map[string]decoder
	types    map[string]reflect.Type

	// wake is closed, and replaced, on every notification: a Receive that
	// takes it before claiming sees any notification that arrives after.
	wakeMu sync.Mutex
	wake   chan struct{}

	listenOnce sync.Once
	listening  chan struct{} // closed after the first LISTEN attempt
	stopListen context.CancelFunc
	listenDone sync.WaitGroup

	closed    atomic.Bool
	closedCh  chan struct{}
	closeOnce sync.Once
}

type decoder func(codec Codec, payload []byte) (any, error)

var (
	_ qumsg.Transport = (*Transport)(nil)
	_ qumsg.Decoder   = (*Transport)(nil)
)

// New creates a transport on table, which must have the columns and indexes
// of Schema.
func New(pool *pgxpool.Pool, table string, opts ...Option) (*Transport, error) {
	if err := validTable(table); err != nil {
		return nil, err
	}
	t := &Transport{
		pool:      pool,
		table:     table,
		channel:   "qumsg_" + table,
		owner:     defaultOwner(),
		lease:     time.Minute,
		backoff:   DefaultBackoff,
		poll:      5 * time.Second,
		codec:     jsonCodec{},
		sql:       newStatements(table),
		decoders:  make(map[string]decoder),
		types:     make(map[string]reflect.Type),
		wake:      make(chan struct{}),
		listening: make(chan struct{}),
		closedCh:  make(chan struct{}),
	}
	if pool != nil {
		t.db = pool
	}
	for _, opt := range opts {
		opt(t)
	}
	return t, nil
}

func defaultOwner() string {
	host, err := os.Hostname()
	if err != nil {
		host = "qumsg"
	}
	suffix := make([]byte, 4)
	_, _ = rand.Read(suffix)
	return host + "-" + hex.EncodeToString(suffix)
}

// Accept declares the message types that can be received, all of them or,
// if any is invalid or its alias is taken, none. qumsg.Bus.Consume calls it
// with the types that the router of the bus routes to this transport. Each
// type must implement qumsg.NamedMessage with the same rules as
// qumsg.Bus.Register: a non-empty alias, unique per transport. Accepting a
// type again is allowed.
func (t *Transport) Accept(types ...qumsg.Type) error {
	names := make([]string, len(types))
	for i, typ := range types {
		if err := typ.Err(); err != nil {
			return err
		}
		name, named := typ.Name()
		if !named {
			return fmt.Errorf("%w: %s", ErrUnnamedMessage, typ)
		}
		names[i] = name
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	pending := make(map[string]reflect.Type, len(types))
	for i, typ := range types {
		other, ok := t.types[names[i]]
		if !ok {
			other, ok = pending[names[i]]
		}
		if ok && other != typ.Reflect() {
			return fmt.Errorf("%w: %q is used by %s, cannot register %s", qumsg.ErrDuplicateName, names[i], other, typ)
		}
		pending[names[i]] = typ.Reflect()
	}
	for i, typ := range types {
		t.types[names[i]] = typ.Reflect()
		t.decoders[names[i]] = func(codec Codec, payload []byte) (any, error) {
			return typ.Decode(func(target any) error { return codec.Unmarshal(payload, target) })
		}
	}
	return nil
}

// nameOf calls Name, turning a panic into qumsg.ErrNamePanic.
func nameOf(named qumsg.NamedMessage) (name string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %T: %v", qumsg.ErrNamePanic, named, r)
		}
	}()

	name = named.Name()
	if name == "" {
		return "", fmt.Errorf("%w: %T", qumsg.ErrEmptyName, named)
	}
	return name, nil
}

// Send inserts the message of the envelope and its metadata with the DB of
// WithDB, and announces it with NOTIFY on the same DB: inside a transaction,
// both take effect when it commits. The message type must implement
// qumsg.NamedMessage; it does not need to be accepted. A Deduplicated message
// whose key is already in the table is dropped without error. A delay set
// with ContextWithDelay postpones the first delivery.
func (t *Transport) Send(ctx context.Context, e qumsg.Envelope) error {
	if t.closed.Load() {
		return qumsg.ErrTransportClosed
	}
	message := e.Message()
	if message == nil {
		return qumsg.ErrNilMessage
	}

	named, ok := message.(qumsg.NamedMessage)
	if !ok {
		return fmt.Errorf("%w: %T", ErrUnnamedMessage, message)
	}
	name, err := nameOf(named)
	if err != nil {
		return err
	}

	payload, err := t.codec.Marshal(message)
	if err != nil {
		return fmt.Errorf("postgres: encode %s: %w", name, err)
	}
	metadata, err := encodeMetadata(e)
	if err != nil {
		return fmt.Errorf("postgres: encode metadata of %s: %w", name, err)
	}

	var dedup *string
	if d, ok := message.(Deduplicated); ok {
		if key := d.DedupKey(); key != "" {
			dedup = &key
		}
	}
	delay, _ := ctx.Value(delayKey{}).(time.Duration)

	tag, err := t.db.Exec(ctx, t.sql.insert, name, payload, metadata, dedup, delay.Microseconds())
	if err != nil {
		return fmt.Errorf("postgres: send %s: %w", name, err)
	}
	if tag.RowsAffected() == 0 {
		// Absorbed by a pending message with the same dedup key.
		return nil
	}
	if _, err := t.db.Exec(ctx, notifySQL, t.channel); err != nil {
		return fmt.Errorf("postgres: notify %s: %w", name, err)
	}
	return nil
}

// encodeMetadata returns the metadata of e as a JSON object.
func encodeMetadata(e qumsg.Envelope) (string, error) {
	metadata := maps.Collect(e.Metadata())
	if len(metadata) == 0 {
		return "{}", nil
	}
	data, err := json.Marshal(metadata)
	return string(data), err
}

// decodeMetadata adds the metadata encoded by encodeMetadata to e.
func decodeMetadata(e qumsg.Envelope, data string) (qumsg.Envelope, error) {
	var metadata map[string]string
	if err := json.Unmarshal([]byte(data), &metadata); err != nil {
		return e, err
	}
	for key, value := range metadata {
		e = e.With(key, value)
	}
	return e, nil
}

// Receive returns the next message: one whose lease expired, left by a worker
// that stopped, or else the oldest available one. With nothing to claim it
// waits for a notification, for the next retry to be due, for the oldest
// lease to expire or for the poll interval, whichever comes first. It returns
// when ctx is done or after Close (qumsg.ErrTransportClosed); queued messages
// stay in the table.
//
// The first Receive opens a connection of its own, outside the pool and with
// its settings, to LISTEN until Close. It must reach PostgreSQL directly or
// through a session-mode pooler. If it cannot listen, Receive polls.
func (t *Transport) Receive(ctx context.Context) (qumsg.Delivery, error) {
	if t.closed.Load() {
		return nil, qumsg.ErrTransportClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.startListening()
	select {
	case <-t.listening:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.closedCh:
		return nil, qumsg.ErrTransportClosed
	}

	for {
		if t.closed.Load() {
			return nil, qumsg.ErrTransportClosed
		}
		// Taken before the claim, so a message announced after the claim
		// came back empty wakes this Receive.
		wake := t.wakeChan()
		d, err := t.claim(ctx)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			return nil, err
		}
		if d != nil {
			return d, nil
		}

		wait, err := t.nextWait(ctx)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			return nil, err
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-t.closedCh:
			timer.Stop()
			return nil, qumsg.ErrTransportClosed
		case <-wake:
		case <-timer.C:
		}
		timer.Stop()
	}
}

// busyWait is the wait when a message is due but the claim did not get it:
// a transaction holds its row, and claiming again at once would spin.
const busyWait = 100 * time.Millisecond

// nextWait is how long an idle Receive can wait without a notification.
func (t *Transport) nextWait(ctx context.Context) (time.Duration, error) {
	var due, expiry *float64
	if err := t.pool.QueryRow(ctx, t.sql.nextDue, t.lease.Microseconds()).Scan(&due, &expiry); err != nil {
		return 0, fmt.Errorf("postgres: next due: %w", err)
	}
	wait := t.poll
	for _, seconds := range []*float64{due, expiry} {
		if seconds == nil {
			continue
		}
		d := time.Duration(*seconds * float64(time.Second))
		if d <= 0 {
			d = busyWait
		}
		wait = min(wait, d)
	}
	return wait, nil
}

// claim leases one message, if any is claimable.
func (t *Transport) claim(ctx context.Context) (*delivery, error) {
	var (
		id       int64
		name     string
		payload  []byte
		metadata string
		attempt  int
	)
	err := t.pool.QueryRow(ctx, t.sql.claim, t.owner, t.lease.Microseconds()).Scan(&id, &name, &payload, &metadata, &attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: claim: %w", err)
	}

	d := &delivery{transport: t, id: id, attempt: attempt}
	d.envelope = t.decode(id, name, payload, metadata, attempt)
	d.startHeartbeat()
	return d, nil
}

// decode builds the envelope of a claimed row, or an UndecodableMessage.
// Decoded messages carry their lease for Propagator.
func (t *Transport) decode(id int64, name string, payload []byte, metadata string, attempt int) qumsg.Envelope {
	t.mu.RLock()
	decode, ok := t.decoders[name]
	t.mu.RUnlock()

	undecodable := func(err error) qumsg.Envelope {
		return qumsg.NewEnvelope(UndecodableMessage{ID: id, Name: name, Payload: payload, Err: err})
	}
	if !ok {
		return undecodable(fmt.Errorf("postgres: no type registered for %q", name))
	}
	msg, err := decode(t.codec, payload)
	if err != nil {
		return undecodable(fmt.Errorf("postgres: decode %s: %w", name, err))
	}
	e, err := decodeMetadata(qumsg.NewEnvelope(msg), metadata)
	if err != nil {
		return undecodable(fmt.Errorf("postgres: decode metadata of %s: %w", name, err))
	}
	value, _ := json.Marshal(leaseInfo{Table: t.table, ID: id, Attempt: attempt, Owner: t.owner})
	return e.With(leaseMetadata, string(value))
}

func (t *Transport) wakeChan() chan struct{} {
	t.wakeMu.Lock()
	defer t.wakeMu.Unlock()
	return t.wake
}

// broadcast wakes every waiting Receive.
func (t *Transport) broadcast() {
	t.wakeMu.Lock()
	defer t.wakeMu.Unlock()
	close(t.wake)
	t.wake = make(chan struct{})
}

// startListening starts, once, the goroutine that LISTENs until Close.
func (t *Transport) startListening() {
	t.listenOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		t.stopListen = cancel
		t.listenDone.Go(func() { t.listen(ctx) })
	})
}

// listen keeps a LISTEN connection open and broadcasts its notifications.
// After a failure it wakes the waiting receivers, so they check the table,
// and tries again after a pause.
func (t *Transport) listen(ctx context.Context) {
	var ready sync.Once
	markReady := func() { ready.Do(func() { close(t.listening) }) }
	defer markReady()

	for ctx.Err() == nil {
		conn, err := pgx.ConnectConfig(ctx, t.pool.Config().ConnConfig.Copy())
		if err == nil {
			// The channel derives from a validated table name: a plain
			// identifier, safe to splice (LISTEN takes no parameters).
			if _, err = conn.Exec(ctx, "LISTEN "+t.channel); err == nil {
				markReady()
				// Anything sent while not listening is found by this claim.
				t.broadcast()
				for {
					if _, err = conn.WaitForNotification(ctx); err != nil {
						break
					}
					t.broadcast()
				}
			}
			closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = conn.Close(closeCtx)
			cancel()
		}
		markReady()
		if ctx.Err() != nil {
			return
		}
		t.broadcast()
		pause := time.NewTimer(min(t.poll, time.Second))
		select {
		case <-ctx.Done():
			pause.Stop()
			return
		case <-pause.C:
		}
	}
}

// Close makes Receive and Send return qumsg.ErrTransportClosed and closes the
// LISTEN connection. Messages in the table are kept, and deliveries already
// received can still be settled. It does not close the pool.
func (t *Transport) Close() error {
	t.closeOnce.Do(func() {
		t.closed.Store(true)
		close(t.closedCh)
		t.listenOnce.Do(func() {}) // a later Receive must not start listening
		if t.stopListen != nil {
			t.stopListen()
		}
	})
	t.listenDone.Wait()
	return nil
}

// leaseMetadata is the metadata entry that carries the lease of a delivery
// to the context of its handler (see Propagator).
const leaseMetadata = "qumsg.postgres.lease"

type leaseInfo struct {
	Table   string `json:"table"`
	ID      int64  `json:"id"`
	Attempt int    `json:"attempt"`
	Owner   string `json:"owner"`
}

type leaseKey struct{ t *Transport }

// Propagator returns the qumsg.Propagator that puts the lease of each
// delivery of t into the context of its handler, so that the handler can end
// the message in its own transaction with Complete. Add it to the bus that
// consumes t with qumsg.WithPropagators. Its Inject does nothing: the lease
// never travels to other messages.
func (t *Transport) Propagator() qumsg.Propagator { return leasePropagator{t: t} }

type leasePropagator struct{ t *Transport }

func (p leasePropagator) Inject(_ context.Context, e qumsg.Envelope) qumsg.Envelope { return e }

func (p leasePropagator) Extract(ctx context.Context, e qumsg.Envelope) context.Context {
	value, ok := e.Get(leaseMetadata)
	if !ok {
		return ctx
	}
	var lease leaseInfo
	if err := json.Unmarshal([]byte(value), &lease); err != nil || lease.Table != p.t.table {
		return ctx
	}
	return context.WithValue(ctx, leaseKey{p.t}, lease)
}

// Complete deletes the message being handled, fenced by its lease, with db:
// the transaction of the handler, so the message ends atomically with the
// handler's writes. ctx must come from a handler of a bus that consumes t
// with t.Propagator (ErrNoLease otherwise). If the lease was lost, it
// returns ErrLeaseLost and deletes nothing: roll the transaction back. After
// it commits, the Ack that follows the handler finds nothing left to do. A
// nil db uses the DB of WithDB.
func (t *Transport) Complete(ctx context.Context, db DB) error {
	lease, ok := ctx.Value(leaseKey{t}).(leaseInfo)
	if !ok {
		return ErrNoLease
	}
	if db == nil {
		db = t.db
	}
	tag, err := db.Exec(ctx, t.sql.complete, lease.ID, lease.Owner, lease.Attempt)
	if err != nil {
		return fmt.Errorf("postgres: complete %d: %w", lease.ID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: message %d", ErrLeaseLost, lease.ID)
	}
	return nil
}

type delivery struct {
	transport *Transport
	id        int64
	attempt   int
	envelope  qumsg.Envelope
	settled   atomic.Bool

	stopHeartbeat context.CancelFunc
	heartbeatDone chan struct{}
}

func (d *delivery) Envelope() qumsg.Envelope { return d.envelope }

func (d *delivery) Attempt() int { return d.attempt }

// lockNotAvailable is what FOR UPDATE NOWAIT raises on a row that another
// transaction holds.
const lockNotAvailable = "55P03"

// startHeartbeat renews the lease every third of it until the delivery is
// settled or the lease is lost.
func (d *delivery) startHeartbeat() {
	t := d.transport
	ctx, cancel := context.WithCancel(context.Background())
	d.stopHeartbeat = cancel
	d.heartbeatDone = make(chan struct{})
	every := t.lease / 3

	go func() {
		defer close(d.heartbeatDone)
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			rctx, rcancel := context.WithTimeout(ctx, every)
			tag, err := t.pool.Exec(rctx, t.sql.renew, d.id, t.owner, d.attempt)
			rcancel()
			if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == lockNotAvailable {
				// A transaction holds the row, normally the handler's own
				// Complete: nobody can reclaim it meanwhile, and the next
				// renewal tells whether the message is still ours.
				continue
			}
			if err == nil && tag.RowsAffected() == 0 {
				// Lost or ended: settling tells which.
				return
			}
			// On any other error, retry on the next tick: the lease still
			// lasts two more.
		}
	}()
}

// settle stops the heartbeat, so no renewal races the settlement. It reports
// false if the delivery was already settled.
func (d *delivery) settle() bool {
	if !d.settled.CompareAndSwap(false, true) {
		return false
	}
	d.stopHeartbeat()
	<-d.heartbeatDone
	return true
}

// Ack deletes the message. If the handler already ended it with Complete, or
// a worker that reclaimed it ended it, there is nothing to delete and Ack
// returns nil. If another worker holds it now, Ack returns ErrLeaseLost.
func (d *delivery) Ack(ctx context.Context) error {
	if !d.settle() {
		return nil
	}
	t := d.transport
	return t.fenced(ctx, "ack", t.sql.ack, d)
}

// Nack makes the message available again after the backoff of its attempt,
// releases the lease and wakes the waiting receivers. The attempt is already
// counted: the next delivery has the next number. If another worker holds
// the message now, Nack returns ErrLeaseLost.
func (d *delivery) Nack(ctx context.Context) error {
	if !d.settle() {
		return nil
	}
	t := d.transport
	if err := t.fenced(ctx, "nack", t.sql.nack, d, t.backoff(d.attempt).Microseconds()); err != nil {
		return err
	}
	if _, err := t.pool.Exec(ctx, notifySQL, t.channel); err != nil {
		return fmt.Errorf("postgres: notify retry of %d: %w", d.id, err)
	}
	return nil
}

// fenced runs a settlement whose statement reports whether it changed the
// row and whether the row still exists.
func (t *Transport) fenced(ctx context.Context, op, sql string, d *delivery, extra ...any) error {
	var changed, exists bool
	args := append([]any{d.id, t.owner, d.attempt}, extra...)
	if err := t.pool.QueryRow(ctx, sql, args...).Scan(&changed, &exists); err != nil {
		return fmt.Errorf("postgres: %s %d: %w", op, d.id, err)
	}
	if !changed && exists {
		return fmt.Errorf("%w: message %d", ErrLeaseLost, d.id)
	}
	return nil
}

// notifySQL announces a new or retried message. The payload is empty: a
// wake-up only means "claim again".
const notifySQL = `SELECT pg_notify($1, '')`

type statements struct {
	insert   string
	claim    string
	nextDue  string
	renew    string
	ack      string
	nack     string
	complete string
}

// fence matches the row only while this delivery holds its lease: same
// owner and same attempt, which grows on every claim.
const fence = `id = $1 AND locked_by = $2 AND attempt = $3`

func newStatements(table string) statements {
	r := strings.NewReplacer("{t}", pgx.Identifier{table}.Sanitize(), "{fence}", fence)
	micros := func(param string) string { return param + `::bigint * interval '1 microsecond'` }
	return statements{
		insert: r.Replace(`INSERT INTO {t} (name, payload, metadata, dedup_key, available_at)
			VALUES ($1, $2, $3::jsonb, $4, now() + ` + micros("$5") + `)
			ON CONFLICT (dedup_key) DO NOTHING`),
		// A message whose lease expired goes first, then the oldest
		// available one. Each scalar subquery skips the rows that other
		// claims hold, and COALESCE only runs the second one when the first
		// finds nothing.
		claim: r.Replace(`UPDATE {t} SET locked_by = $1, locked_at = now(), attempt = attempt + 1
			WHERE id = COALESCE(
				(SELECT id FROM {t} WHERE locked_at < now() - ` + micros("$2") + `
					ORDER BY locked_at, id LIMIT 1 FOR UPDATE SKIP LOCKED),
				(SELECT id FROM {t} WHERE locked_at IS NULL AND available_at <= now()
					ORDER BY available_at, id LIMIT 1 FOR UPDATE SKIP LOCKED))
			RETURNING id, name, payload, metadata::text, attempt`),
		// Seconds until the next available message and until the oldest
		// lease expires (plus a millisecond, since the claim wants it
		// strictly older), by the clock of the database. Both use the
		// partial indexes.
		nextDue: r.Replace(`SELECT
				EXTRACT(EPOCH FROM (SELECT min(available_at) FROM {t} WHERE locked_at IS NULL) - now())::float8,
				EXTRACT(EPOCH FROM (SELECT min(locked_at) FROM {t} WHERE locked_at IS NOT NULL)
					+ ` + micros("$1") + ` - now())::float8 + 0.001`),
		// NOWAIT: a renewal never queues behind a transaction holding the
		// row (see startHeartbeat).
		renew: r.Replace(`UPDATE {t} SET locked_at = now()
			WHERE id = (SELECT id FROM {t} WHERE {fence} FOR UPDATE NOWAIT)`),
		// The outer SELECT sees the table as it was before the statement,
		// so exists tells a lost lease (the row is still there) from an
		// ended message.
		ack: r.Replace(`WITH done AS (DELETE FROM {t} WHERE {fence} RETURNING 1)
			SELECT EXISTS (SELECT 1 FROM done), EXISTS (SELECT 1 FROM {t} WHERE id = $1)`),
		nack: r.Replace(`WITH done AS (
				UPDATE {t} SET locked_by = NULL, locked_at = NULL, available_at = now() + ` + micros("$4") + `
				WHERE {fence} RETURNING 1)
			SELECT EXISTS (SELECT 1 FROM done), EXISTS (SELECT 1 FROM {t} WHERE id = $1)`),
		complete: r.Replace(`DELETE FROM {t} WHERE {fence}`),
	}
}
