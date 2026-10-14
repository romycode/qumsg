//go:build integration

package postgres_test

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/romycode/qumsg"
	"github.com/romycode/qumsg/postgres"
)

type integrationMessage struct{ ID string }

func (integrationMessage) Name() string       { return "integration.message" }
func (m integrationMessage) DedupKey() string { return m.ID }

func integrationPool(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	url := os.Getenv("POSTGRES_URL")
	if url == "" {
		url = "postgres://qumsg:qumsg@localhost:5432/qumsg?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	table := fmt.Sprintf("qumsg_test_%x", rand.Text()[:12])
	schema, err := postgres.Schema(table)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := pool.Exec(ctx, "DROP TABLE "+table); err != nil {
			t.Error(err)
		}
	})
	return pool, table
}

func integrationTransport(t *testing.T, pool *pgxpool.Pool, table string, opts ...postgres.Option) *postgres.Transport {
	t.Helper()
	tr, err := postgres.New(pool, table, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := tr.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := tr.Accept(qumsg.TypeOf[integrationMessage]()); err != nil {
		t.Fatal(err)
	}
	return tr
}

func receiveIntegration(t *testing.T, tr *postgres.Transport) qumsg.Delivery {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	d, err := tr.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = d.Ack(ctx)
	})
	return d
}

func sendIntegration(t *testing.T, tr *postgres.Transport, id string) {
	t.Helper()
	if err := tr.Send(t.Context(), qumsg.NewEnvelope(integrationMessage{ID: id})); err != nil {
		t.Fatal(err)
	}
}

func countIntegration(t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM "+table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestPostgresRoundTripAndDeduplication(t *testing.T) {
	pool, table := integrationPool(t)
	tr := integrationTransport(t, pool, table)
	e := qumsg.NewEnvelope(integrationMessage{ID: "one"}).With("trace", "abc")
	if err := tr.Send(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	sendIntegration(t, tr, "one")
	if n := countIntegration(t, pool, table); n != 1 {
		t.Fatalf("rows = %d, want 1", n)
	}
	d := receiveIntegration(t, tr)
	if d.Envelope().Message() != e.Message() || d.Attempt() != 1 {
		t.Fatalf("unexpected delivery: %#v, attempt %d", d.Envelope().Message(), d.Attempt())
	}
	if trace, _ := d.Envelope().Get("trace"); trace != "abc" {
		t.Fatalf("trace = %q", trace)
	}
	sendIntegration(t, tr, "one")
	if n := countIntegration(t, pool, table); n != 1 {
		t.Fatalf("leased message did not deduplicate: %d", n)
	}
	if err := d.Ack(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n := countIntegration(t, pool, table); n != 0 {
		t.Fatalf("rows after Ack = %d", n)
	}
	sendIntegration(t, tr, "one")
	if n := countIntegration(t, pool, table); n != 1 {
		t.Fatalf("key not released after Ack: %d", n)
	}
}

func TestPostgresReservedTableName(t *testing.T) {
	base, namespace := integrationPool(t)
	if _, err := base.Exec(t.Context(), "CREATE SCHEMA "+namespace); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := base.Exec(context.Background(), "DROP SCHEMA "+namespace+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	cfg := base.Config()
	cfg.ConnConfig.RuntimeParams["search_path"] = namespace
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	ddl, err := postgres.Schema("select")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), ddl); err != nil {
		t.Fatal(err)
	}
	tr := integrationTransport(t, pool, "select", postgres.WithBackoff(func(int) time.Duration { return 0 }))
	sendIntegration(t, tr, "reserved")
	d := receiveIntegration(t, tr)
	if err := d.Nack(t.Context()); err != nil {
		t.Fatal(err)
	}
	d = receiveIntegration(t, tr)
	if d.Attempt() != 2 {
		t.Fatalf("attempt = %d", d.Attempt())
	}
	if err := tr.Complete(tr.Propagator().Extract(t.Context(), d.Envelope()), pool); err != nil {
		t.Fatal(err)
	}
	if err := d.Ack(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresSendTransaction(t *testing.T) {
	for _, commit := range []bool{false, true} {
		t.Run(fmt.Sprint(commit), func(t *testing.T) {
			pool, table := integrationPool(t)
			tx, err := pool.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(context.Background())
			tr := integrationTransport(t, pool, table, postgres.WithDB(tx))
			sendIntegration(t, tr, "transaction")
			if n := countIntegration(t, pool, table); n != 0 {
				t.Fatalf("uncommitted rows visible: %d", n)
			}
			if commit {
				err = tx.Commit(t.Context())
			} else {
				err = tx.Rollback(t.Context())
			}
			if err != nil {
				t.Fatal(err)
			}
			want := 0
			if commit {
				want = 1
			}
			if n := countIntegration(t, pool, table); n != want {
				t.Fatalf("rows = %d, want %d", n, want)
			}
		})
	}
}

func TestPostgresRetryAndDelay(t *testing.T) {
	pool, table := integrationPool(t)
	tr := integrationTransport(t, pool, table, postgres.WithBackoff(func(int) time.Duration { return 150 * time.Millisecond }))
	ctx := postgres.ContextWithDelay(t.Context(), 150*time.Millisecond)
	start := time.Now()
	if err := tr.Send(ctx, qumsg.NewEnvelope(integrationMessage{ID: "delayed"})); err != nil {
		t.Fatal(err)
	}
	d := receiveIntegration(t, tr)
	if time.Since(start) < 100*time.Millisecond {
		t.Fatal("delivered before delay")
	}
	start = time.Now()
	if err := d.Nack(t.Context()); err != nil {
		t.Fatal(err)
	}
	retry := receiveIntegration(t, tr)
	if retry.Attempt() != 2 {
		t.Fatalf("attempt = %d", retry.Attempt())
	}
	if time.Since(start) < 100*time.Millisecond {
		t.Fatal("retry before backoff")
	}
}

func TestPostgresCompleteTransaction(t *testing.T) {
	pool, table := integrationPool(t)
	tr := integrationTransport(t, pool, table)
	sendIntegration(t, tr, "complete")
	d := receiveIntegration(t, tr)
	ctx := tr.Propagator().Extract(t.Context(), d.Envelope())
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if err := tr.Complete(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if n := countIntegration(t, pool, table); n != 1 {
		t.Fatal("delete visible before commit")
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if n := countIntegration(t, pool, table); n != 1 {
		t.Fatal("rollback lost message")
	}
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if err := tr.Complete(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := d.Ack(ctx); err != nil {
		t.Fatal(err)
	}
	if n := countIntegration(t, pool, table); n != 0 {
		t.Fatal("Complete left message")
	}
}

func TestPostgresExpiredLeaseAndFencing(t *testing.T) {
	for _, operation := range []string{"ack", "nack", "complete", "own", "release"} {
		t.Run(operation, func(t *testing.T) {
			pool, table := integrationPool(t)
			tr := integrationTransport(t, pool, table, postgres.WithLease(time.Hour))
			sendIntegration(t, tr, "expired")
			stale := receiveIntegration(t, tr)
			if _, err := pool.Exec(t.Context(), "UPDATE "+table+" SET locked_at = now() - interval '2 hours'"); err != nil {
				t.Fatal(err)
			}
			other := integrationTransport(t, pool, table, postgres.WithLease(time.Hour))
			current := receiveIntegration(t, other)
			if current.Attempt() != 2 {
				t.Fatalf("attempt = %d", current.Attempt())
			}
			var err error
			switch operation {
			case "ack":
				err = stale.Ack(t.Context())
			case "nack":
				err = stale.Nack(t.Context())
			case "complete":
				err = tr.Complete(tr.Propagator().Extract(t.Context(), stale.Envelope()), pool)
			case "own":
				err = tr.Own(tr.Propagator().Extract(t.Context(), stale.Envelope()), pool)
			case "release":
				err = stale.(qumsg.ReleasableDelivery).Release(t.Context())
			}
			if !errors.Is(err, postgres.ErrLeaseLost) {
				t.Fatalf("got %v, want ErrLeaseLost", err)
			}
			if n := countIntegration(t, pool, table); n != 1 {
				t.Fatal("stale owner changed message")
			}
		})
	}
}

func TestPostgresReleasePreservesAttemptAndFencesOldContext(t *testing.T) {
	pool, table := integrationPool(t)
	tr := integrationTransport(t, pool, table)
	sendIntegration(t, tr, "release")
	first := receiveIntegration(t, tr)
	stale := tr.Propagator().Extract(t.Context(), first.Envelope())
	if err := first.(qumsg.ReleasableDelivery).Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	next := receiveIntegration(t, tr)
	if next.Attempt() != first.Attempt() {
		t.Fatalf("attempt changed: %d -> %d", first.Attempt(), next.Attempt())
	}
	if err := tr.Own(stale, pool); !errors.Is(err, qumsg.ErrDeliveryLost) {
		t.Fatalf("stale Own = %v", err)
	}
	if err := tr.Complete(stale, pool); !errors.Is(err, qumsg.ErrDeliveryLost) {
		t.Fatalf("stale Complete = %v", err)
	}
	if err := tr.Own(tr.Propagator().Extract(t.Context(), next.Envelope()), pool); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresLeaseLossCancelsDelivery(t *testing.T) {
	pool, table := integrationPool(t)
	tr := integrationTransport(t, pool, table, postgres.WithLease(300*time.Millisecond))
	sendIntegration(t, tr, "lost")
	d := receiveIntegration(t, tr)
	ctx, cancel := d.(qumsg.DeliveryContext).Context(t.Context())
	defer cancel()
	if _, err := pool.Exec(t.Context(), "UPDATE "+table+" SET locked_by = 'other'"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
		if !errors.Is(context.Cause(ctx), qumsg.ErrDeliveryLost) {
			t.Fatal(context.Cause(ctx))
		}
	case <-time.After(time.Second):
		t.Fatal("handler not canceled after losing ownership")
	}
}

func TestPostgresUncertainRenewalCancelsBeforeExpiry(t *testing.T) {
	pool, table := integrationPool(t)
	// Ticks every 500ms; safeUntil is 1s after the claim, before the lease ends.
	tr := integrationTransport(t, pool, table, postgres.WithLease(1500*time.Millisecond))
	sendIntegration(t, tr, "uncertain")
	d := receiveIntegration(t, tr)
	ctx, cancel := d.(qumsg.DeliveryContext).Context(t.Context())
	defer cancel()
	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	// The row stays locked past safeUntil (1s) until the test ends.
	if err := tr.Own(tr.Propagator().Extract(t.Context(), d.Envelope()), tx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
		if !errors.Is(context.Cause(ctx), qumsg.ErrDeliveryLost) {
			t.Fatal(context.Cause(ctx))
		}
	case <-time.After(1300 * time.Millisecond):
		t.Fatal("uncertain lease was not canceled before expiry")
	}
}

func TestPostgresLockedRowDoesNotCancelWhileSafe(t *testing.T) {
	pool, table := integrationPool(t)
	// Ticks every 300ms; the lease is safe until about 600ms after the claim.
	tr := integrationTransport(t, pool, table, postgres.WithLease(900*time.Millisecond))
	sendIntegration(t, tr, "locked-safe")
	d := receiveIntegration(t, tr)
	ctx, cancel := d.(qumsg.DeliveryContext).Context(t.Context())
	defer cancel()
	var before time.Time
	if err := pool.QueryRow(t.Context(), "SELECT locked_at FROM "+table).Scan(&before); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if err := tr.Own(tr.Propagator().Extract(t.Context(), d.Envelope()), tx); err != nil {
		t.Fatal(err)
	}
	// Hold the row across the first tick (300ms): the renewal is blocked.
	select {
	case <-ctx.Done():
		t.Fatalf("delivery canceled while still safe: %v", context.Cause(ctx))
	case <-time.After(400 * time.Millisecond):
	}
	// Seen from another connection, nothing was renewed while the row was
	// locked: the 55P03 path ran and did not count as a renewal.
	var locked time.Time
	if err := pool.QueryRow(t.Context(), "SELECT locked_at FROM "+table).Scan(&locked); err != nil {
		t.Fatal(err)
	}
	if !locked.Equal(before) {
		t.Fatalf("locked_at moved while the row was locked: before %v, now %v", before, locked)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Past the original safeUntil (600ms) only a real renewal keeps it alive.
	select {
	case <-ctx.Done():
		t.Fatalf("delivery canceled after the lock was released: %v", context.Cause(ctx))
	case <-time.After(800 * time.Millisecond):
	}
	var after time.Time
	if err := pool.QueryRow(t.Context(), "SELECT locked_at FROM "+table).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if !after.After(before) {
		t.Fatalf("locked_at did not advance: before %v, after %v", before, after)
	}
}

// A claim that is slow to come back (here, blocked by a table lock) leaves
// little of the safe limit, which counts from before the claim request. The
// first renewal must still come one heartbeat interval after the claim
// started, like every later one after its renewal started, and not one
// interval after the claim returned, which is already past the safe limit.
func TestPostgresSlowClaimRenewsBeforeSafeLimit(t *testing.T) {
	pool, table := integrationPool(t)
	// Ticks every 200ms; the safe limit is 400ms after the claim starts.
	const lease = 600 * time.Millisecond
	tr := integrationTransport(t, pool, table, postgres.WithLease(lease))
	sendIntegration(t, tr, "slow-claim")

	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	// Conflicts with the ROW EXCLUSIVE lock of the claim's UPDATE.
	if _, err := tx.Exec(t.Context(), "LOCK TABLE "+table+" IN SHARE ROW EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	received := make(chan qumsg.Delivery, 1)
	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		d, err := tr.Receive(ctx)
		if err != nil {
			t.Error(err)
		}
		received <- d
	}()
	// Wait until the claim is blocked on the table lock, then hold it for
	// half the lease: the claim returns 300ms after it started.
	deadline := time.Now().Add(2 * time.Second)
	for {
		var waiting bool
		err := pool.QueryRow(t.Context(), `SELECT EXISTS (SELECT 1 FROM pg_locks
			WHERE relation = $1::regclass AND NOT granted)`, table).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("claim never blocked on the table lock")
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(lease / 2)
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	d := <-received
	if d == nil {
		t.FailNow()
	}
	defer func() { _ = d.Ack(context.Background()) }()
	ctx, cancel := d.(qumsg.DeliveryContext).Context(t.Context())
	defer cancel()

	// Nothing holds the row: the heartbeat can renew whenever it tries.
	select {
	case <-ctx.Done():
		var left float64
		_ = pool.QueryRow(context.Background(), `SELECT
				EXTRACT(EPOCH FROM locked_at + $1::bigint * interval '1 microsecond' - clock_timestamp())::float8
			FROM `+table, lease.Microseconds()).Scan(&left)
		t.Fatalf("healthy delivery canceled (%v) with %.0fms of lease left in the database",
			context.Cause(ctx), left*1000)
	case <-time.After(2 * lease):
	}
	if err := d.Ack(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// TestPostgresSettleWaitingOnOwnCompleteIsNotLost settles a delivery while
// its handler's own Complete still holds the row: the settlement waits on the
// row lock and, once Complete commits, must see an ended message (nil), not
// a lost lease, although the row was there when its statement began.
func TestPostgresSettleWaitingOnOwnCompleteIsNotLost(t *testing.T) {
	settlements := map[string]func(qumsg.Delivery, context.Context) error{
		"ack":     qumsg.Delivery.Ack,
		"nack":    qumsg.Delivery.Nack,
		"release": func(d qumsg.Delivery, ctx context.Context) error { return d.(qumsg.ReleasableDelivery).Release(ctx) },
	}
	for name, settle := range settlements {
		t.Run(name, func(t *testing.T) {
			pool, table := integrationPool(t)
			tr := integrationTransport(t, pool, table)
			sendIntegration(t, tr, name)
			d := receiveIntegration(t, tr)
			ctx := tr.Propagator().Extract(t.Context(), d.Envelope())
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(context.Background())
			if err := tr.Complete(ctx, tx); err != nil {
				t.Fatal(err)
			}

			settled := make(chan error, 1)
			go func() { settled <- settle(d, t.Context()) }()
			// Wait until the settlement queues behind the Complete's row lock.
			deadline := time.Now().Add(2 * time.Second)
			for {
				var waiting bool
				err := pool.QueryRow(t.Context(), `SELECT EXISTS (SELECT 1 FROM pg_stat_activity
					WHERE wait_event_type = 'Lock' AND position($1 IN query) > 0)`, table).Scan(&waiting)
				if err != nil {
					t.Fatal(err)
				}
				if waiting {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("settlement never waited on the row lock")
				}
				time.Sleep(5 * time.Millisecond)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if err := <-settled; err != nil {
				t.Fatalf("%s after our own Complete = %v, want nil", name, err)
			}
			if n := countIntegration(t, pool, table); n != 0 {
				t.Fatalf("%d messages left, want 0", n)
			}
		})
	}
}

func TestPostgresConcurrentReceivers(t *testing.T) {
	pool, table := integrationPool(t)
	tr := integrationTransport(t, pool, table)
	const total = 20
	for i := range total {
		sendIntegration(t, tr, fmt.Sprint(i))
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	results := make(chan qumsg.Delivery, total)
	errs := make(chan error, total)
	var wg sync.WaitGroup
	for range total {
		wg.Go(func() {
			d, err := tr.Receive(ctx)
			if err != nil {
				errs <- err
				return
			}
			results <- d
		})
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	seen := make(map[string]bool)
	for d := range results {
		msg, ok := d.Envelope().Message().(integrationMessage)
		if !ok {
			t.Errorf("unexpected message: %#v", d.Envelope().Message())
		}
		if seen[msg.ID] {
			t.Errorf("duplicate delivery: %s", msg.ID)
		}
		seen[msg.ID] = true
		if err := d.Ack(t.Context()); err != nil {
			t.Error(err)
		}
	}
	if len(seen) != total {
		t.Fatalf("received %d, want %d", len(seen), total)
	}
}

func TestPostgresHeartbeatPreventsReclaim(t *testing.T) {
	pool, table := integrationPool(t)
	tr := integrationTransport(t, pool, table, postgres.WithLease(300*time.Millisecond))
	sendIntegration(t, tr, "heartbeat")
	d := receiveIntegration(t, tr)
	other := integrationTransport(t, pool, table, postgres.WithLease(300*time.Millisecond))
	ctx, cancel := context.WithTimeout(t.Context(), 900*time.Millisecond)
	defer cancel()
	duplicate, err := other.Receive(ctx)
	if duplicate != nil {
		_ = duplicate.Ack(t.Context())
		t.Fatal("live delivery was reclaimed")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Receive: %v", err)
	}
	if err := d.Ack(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresCompleteAcrossHeartbeat(t *testing.T) {
	pool, table := integrationPool(t)
	tr := integrationTransport(t, pool, table, postgres.WithLease(300*time.Millisecond))
	sendIntegration(t, tr, "complete-lock")
	d := receiveIntegration(t, tr)
	ctx := tr.Propagator().Extract(t.Context(), d.Envelope())
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if err := tr.Complete(ctx, tx); err != nil {
		t.Fatal(err)
	}
	other := integrationTransport(t, pool, table, postgres.WithLease(300*time.Millisecond))
	waitCtx, cancel := context.WithTimeout(t.Context(), 600*time.Millisecond)
	defer cancel()
	duplicate, err := other.Receive(waitCtx)
	if duplicate != nil {
		_ = duplicate.Ack(t.Context())
		t.Fatal("claimed transaction-locked message")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := d.Ack(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresSkipsLockedRows(t *testing.T) {
	pool, table := integrationPool(t)
	tr := integrationTransport(t, pool, table)
	sendIntegration(t, tr, "locked")
	sendIntegration(t, tr, "free")
	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(t.Context(), "SELECT id FROM "+table+" WHERE dedup_key = 'locked' FOR UPDATE"); err != nil {
		t.Fatal(err)
	}
	d := receiveIntegration(t, tr)
	if d.Envelope().Message() != (integrationMessage{ID: "free"}) {
		t.Fatalf("received %#v", d.Envelope().Message())
	}
	if err := d.Ack(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	d = receiveIntegration(t, tr)
	if d.Envelope().Message() != (integrationMessage{ID: "locked"}) {
		t.Fatalf("received %#v", d.Envelope().Message())
	}
}

func TestPostgresNotifyAndClose(t *testing.T) {
	pool, table := integrationPool(t)
	tr := integrationTransport(t, pool, table, postgres.WithPollInterval(time.Hour))
	// An empty Receive initializes LISTEN before the producer sends anything.
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	_, err := tr.Receive(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("empty Receive: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		d, err := tr.Receive(ctx)
		if err == nil {
			err = d.Ack(ctx)
		}
		done <- err
	}()
	sendIntegration(t, tr, "notify")
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	go func() { _, err := tr.Receive(t.Context()); done <- err }()
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, qumsg.ErrTransportClosed) {
			t.Fatalf("closed Receive: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not unblock Receive")
	}
}

func TestPostgresPoisonMessagesReachFailureHandler(t *testing.T) {
	for _, poison := range []struct{ name, payload, metadata string }{
		{"unknown", `{}`, `{}`},
		{"integration.message", `{invalid`, `{}`},
		{"integration.message", `{}`, `{"trace":42}`},
	} {
		t.Run(poison.name+poison.payload+poison.metadata, func(t *testing.T) {
			pool, table := integrationPool(t)
			tr := integrationTransport(t, pool, table, postgres.WithBackoff(func(int) time.Duration { return 0 }))
			if _, err := pool.Exec(t.Context(), "INSERT INTO "+table+" (name,payload,metadata) VALUES ($1,$2,$3::jsonb)", poison.name, []byte(poison.payload), poison.metadata); err != nil {
				t.Fatal(err)
			}
			router := qumsg.NewRouter()
			if err := router.Route[integrationMessage](tr); err != nil {
				t.Fatal(err)
			}
			bus := qumsg.New(qumsg.WithRouter(router))
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			failures := 0
			err := bus.Consume(ctx, tr, qumsg.WithMaxAttempts(2), qumsg.WithFailureHandler(func(_ context.Context, msg any, err error) {
				failures++
				bad, ok := msg.(postgres.UndecodableMessage)
				if !ok || bad.Err == nil || !errors.Is(err, qumsg.ErrNoHandler) {
					t.Errorf("unexpected failure: %#v, %v", msg, err)
				}
				var attempt int
				if err := pool.QueryRow(t.Context(), "SELECT attempt FROM "+table).Scan(&attempt); err != nil || attempt != 2 {
					t.Errorf("attempt = %d, err = %v", attempt, err)
				}
				cancel()
			}))
			if err != nil {
				t.Fatal(err)
			}
			if failures != 1 || countIntegration(t, pool, table) != 0 {
				t.Fatalf("failures = %d; poison not settled", failures)
			}
		})
	}
}
