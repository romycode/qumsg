//go:build integration

package postgres_test

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/romycode/qumsg"
	"github.com/romycode/qumsg/postgres"
)

// stressSeedEnv overrides the seed of TestPostgresHeartbeatStress, to replay
// a plan: QUMSG_STRESS_SEED=42 go test -tags integration -run Stress ./postgres/...
const stressSeedEnv = "QUMSG_STRESS_SEED"

const defaultStressSeed = 20261014

type stressMessage struct{ Group, Seq int }

func (stressMessage) Name() string       { return "stress.message" }
func (m stressMessage) DedupKey() string { return stressKey(m.Group, m.Seq) }

func stressKey(group, seq int) string { return fmt.Sprintf("g%d-m%d", group, seq) }

// stressKind is what a handler does with its delivery.
type stressKind int

const (
	// No transaction: work across several heartbeats, then record the effect.
	stressPlain stressKind = iota
	// Own/Complete right after the claim, holding the row lock across the
	// first tick (lease/3) but releasing it well before safeUntil (2*lease/3).
	stressOwnAligned
	stressCompleteAligned
	// Own/Complete at a random point of the lease, holding the lock for less
	// than lease/3: safe whatever the phase of the heartbeat.
	stressOwnShort
	stressCompleteShort
	// Own/Complete holding the lock far past safeUntil: must be canceled with
	// ErrLeaseLost while the lease is still valid in the database.
	stressOwnLong
	stressCompleteLong
)

var stressKindNames = [...]string{"plain", "own-aligned", "complete-aligned", "own-short", "complete-short", "own-long", "complete-long"}

func (k stressKind) String() string { return stressKindNames[k] }

func (k stressKind) long() bool { return k == stressOwnLong || k == stressCompleteLong }
func (k stressKind) complete() bool {
	return k == stressCompleteAligned || k == stressCompleteShort || k == stressCompleteLong
}
func (k stressKind) locks() bool { return k != stressPlain }

// stressStep is the behavior of one attempt: wait pre, then hold (the row
// lock, or plain work), then keep working for post after an Own commit.
type stressStep struct {
	kind            stressKind
	pre, hold, post time.Duration
}

// stressPlan is the behavior of a message: first for attempt 1 and retry for
// later attempts, which only happen after a long hold.
type stressPlan struct{ first, retry stressStep }

func newStressStep(rng *mathrand.Rand, lease time.Duration, kind stressKind) stressStep {
	frac := func(lo, hi float64) time.Duration {
		return time.Duration((lo + rng.Float64()*(hi-lo)) * float64(lease))
	}
	s := stressStep{kind: kind}
	switch kind {
	case stressPlain:
		s.hold = frac(0, 1.5)
	case stressOwnAligned, stressCompleteAligned:
		// Spans the first tick (lease/3) and, capped by the handler, ends
		// by 0.52*lease after Receive began: well before the original
		// safeUntil (2*lease/3 after the claim started).
		s.hold = frac(0.35, 0.42)
		s.post = frac(0, 0.5)
	case stressOwnShort, stressCompleteShort:
		// Anywhere in the lease, but shorter than lease/3 minus a margin, the
		// least time until safeUntil that a lock taken at any phase gets.
		s.pre = frac(0, 1)
		s.hold = frac(0.05, 0.2)
		s.post = frac(0, 0.3)
	case stressOwnLong, stressCompleteLong:
		s.pre = frac(0, 0.5)
		s.hold = frac(1.2, 1.5)
	}
	if kind.complete() {
		s.post = 0 // the message no longer exists after a Complete commit
	}
	return s
}

func newStressPlans(seed uint64, group, n int, lease time.Duration) []stressPlan {
	rng := mathrand.New(mathrand.NewPCG(seed, uint64(group)))
	firsts := []stressKind{
		stressPlain, stressPlain,
		stressOwnAligned, stressOwnAligned, stressCompleteAligned, stressCompleteAligned,
		stressOwnShort, stressCompleteShort,
		stressOwnLong, stressCompleteLong,
	}
	retries := []stressKind{stressPlain, stressOwnAligned, stressCompleteAligned, stressOwnShort, stressCompleteShort}
	plans := make([]stressPlan, n)
	for i := range plans {
		plans[i].first = newStressStep(rng, lease, firsts[rng.IntN(len(firsts))])
		plans[i].retry = newStressStep(rng, lease, retries[rng.IntN(len(retries))])
	}
	return plans
}

// stressRecord is what was observed for one message.
type stressRecord struct {
	plan        stressPlan
	attempts    []int             // attempt numbers dispatched, in order
	received    map[int]time.Time // by attempt: when its Receive began
	completions int               // attempts whose effect was committed
	acks, nacks int
	settleErrs  []error
	// The attempt canceled while holding the row lock, if any.
	canceled         bool
	cancelAfterLock  time.Duration // from acquiring the lock to the cancellation
	remainingAtCheck time.Duration // lease left in the database when checked
}

type stressState struct {
	mu      sync.Mutex
	records map[string]*stressRecord
}

func (s *stressState) update(key string, f func(r *stressRecord)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f(s.records[key])
}

func (s *stressState) complete(key string) {
	s.update(key, func(r *stressRecord) { r.completions++ })
}

// pgDelivery is the method set of a postgres delivery that Consume uses.
type pgDelivery interface {
	qumsg.Delivery
	qumsg.DeliveryContext
	qumsg.ReleasableDelivery
}

// countingTransport records how Consume settles each delivery.
type countingTransport struct {
	*postgres.Transport
	state *stressState
	t     *testing.T
}

func (c *countingTransport) Receive(ctx context.Context) (qumsg.Delivery, error) {
	// No later than the claim started, which is what safeUntil counts from.
	start := time.Now()
	d, err := c.Transport.Receive(ctx)
	if err != nil {
		return nil, err
	}
	pd, ok := d.(pgDelivery)
	if !ok {
		c.t.Errorf("delivery %T lacks Context or Release", d)
		return d, nil
	}
	cd := &countingDelivery{pgDelivery: pd, c: c}
	if key := cd.key(); key != "" {
		c.state.update(key, func(r *stressRecord) { r.received[pd.Attempt()] = start })
	}
	return cd, nil
}

type countingDelivery struct {
	pgDelivery
	c *countingTransport
}

func (d *countingDelivery) key() string {
	m, ok := d.Envelope().Message().(stressMessage)
	if !ok {
		d.c.t.Errorf("unexpected message %#v", d.Envelope().Message())
		return ""
	}
	return stressKey(m.Group, m.Seq)
}

func (d *countingDelivery) settled(op string, err error) {
	key := d.key()
	if key == "" {
		return
	}
	d.c.state.update(key, func(r *stressRecord) {
		switch {
		case err != nil:
			r.settleErrs = append(r.settleErrs, fmt.Errorf("%s attempt %d: %w", op, d.Attempt(), err))
		case op == "ack":
			r.acks++
		default:
			r.nacks++
		}
	})
}

func (d *countingDelivery) Ack(ctx context.Context) error {
	err := d.pgDelivery.Ack(ctx)
	d.settled("ack", err)
	return err
}

func (d *countingDelivery) Nack(ctx context.Context) error {
	err := d.pgDelivery.Nack(ctx)
	d.settled("nack", err)
	return err
}

func (d *countingDelivery) Release(ctx context.Context) error {
	err := d.pgDelivery.Release(ctx)
	d.settled("release", err)
	return err
}

// countGoroutines counts the goroutines running a function whose name
// contains fn (not merely created by it).
func countGoroutines(fn string) int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	count := 0
	for g := range strings.SplitSeq(string(buf), "\n\n") {
		for line := range strings.SplitSeq(g, "\n") {
			if !strings.HasPrefix(line, "created by ") && strings.Contains(line, fn) {
				count++
				break
			}
		}
	}
	return count
}

const (
	heartbeatFunc  = "qumsg/postgres.(*delivery).startHeartbeat"
	transportFuncs = "qumsg/postgres."
)

func stressSleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// TestPostgresHeartbeatStress runs many concurrent deliveries with short
// leases, whose handlers hold their row lock with Own/Complete transactions
// shorter or longer than the safe limit of the lease, and checks the
// invariants of the heartbeat:
//
//  1. a lock released before safeUntil never cancels the delivery;
//  2. a lock held past safeUntil cancels it with ErrLeaseLost while the row
//     is still leased to that attempt and its lease has not expired in the
//     database;
//  3. every message is completed exactly once and none is lost;
//  4. no heartbeat outlives its delivery, nor any goroutine its transport.
func TestPostgresHeartbeatStress(t *testing.T) {
	seed := uint64(defaultStressSeed)
	if v := os.Getenv(stressSeedEnv); v != "" {
		parsed, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			t.Fatalf("%s=%q: %v", stressSeedEnv, v, err)
		}
		seed = parsed
	}
	t.Logf("seed %d (override with %s)", seed, stressSeedEnv)

	type group struct {
		lease      time.Duration
		transports int
		workers    int
		messages   int
	}
	groups := []group{
		{lease: 300 * time.Millisecond, transports: 2, workers: 3, messages: 40},
		{lease: 600 * time.Millisecond, transports: 2, workers: 3, messages: 40},
		{lease: 900 * time.Millisecond, transports: 2, workers: 3, messages: 40},
	}
	totalWorkers := 0
	for _, g := range groups {
		totalWorkers += g.transports * g.workers
	}

	url := os.Getenv("POSTGRES_URL")
	if url == "" {
		url = "postgres://qumsg:qumsg@localhost:5432/qumsg?sslmode=disable"
	}
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	// Per worker: the handler transaction, the expiry check and a renewal at
	// once, plus the receivers. Starving the renewals of connections would
	// cancel deliveries for a reason the test does not study.
	config.MaxConns = int32(3*totalWorkers + 10)
	setup, cancelSetup := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelSetup()
	// Open every connection up front, so no claim or renewal waits for one.
	config.MinConns = config.MaxConns
	pool, err := pgxpool.NewWithConfig(setup, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	conns := make([]*pgxpool.Conn, config.MaxConns)
	for i := range conns {
		if conns[i], err = pool.Acquire(setup); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range conns {
		c.Release()
	}

	suffix := strings.ToLower(rand.Text()[:10])
	effects := "qumsg_stress_effects_" + suffix
	if _, err := pool.Exec(setup, "CREATE TABLE "+effects+" (key text NOT NULL, attempt int NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	tables := []string{effects}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cleanup, err := pgxpool.New(ctx, url)
		if err != nil {
			t.Error(err)
			return
		}
		defer cleanup.Close()
		for _, table := range tables {
			if _, err := cleanup.Exec(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
				t.Error(err)
			}
		}
	})

	state := &stressState{records: map[string]*stressRecord{}}
	type consumer struct {
		bus     *qumsg.Bus
		tr      *countingTransport
		workers int
	}
	var consumers []consumer
	groupTables := make([]string, len(groups))

	for gi, g := range groups {
		table := fmt.Sprintf("qumsg_stress_%s_%d", suffix, gi)
		schema, err := postgres.Schema(table)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(setup, schema); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, table)
		groupTables[gi] = table

		plans := newStressPlans(seed, gi, g.messages, g.lease)
		for seq, plan := range plans {
			state.records[stressKey(gi, seq)] = &stressRecord{plan: plan, received: map[int]time.Time{}}
		}

		for ti := range g.transports {
			tr, err := postgres.New(pool, table,
				postgres.WithLease(g.lease),
				postgres.WithOwner(fmt.Sprintf("g%d-t%d", gi, ti)),
				postgres.WithBackoff(func(int) time.Duration { return 0 }),
				postgres.WithPollInterval(100*time.Millisecond),
			)
			if err != nil {
				t.Fatal(err)
			}
			ct := &countingTransport{Transport: tr, state: state, t: t}
			bus := qumsg.New(qumsg.WithPropagators(tr.Propagator()))
			if err := bus.Route[stressMessage](ct); err != nil {
				t.Fatal(err)
			}
			handler := stressHandler(t, pool, tr, table, effects, g.lease, plans, state)
			if err := bus.Register(handler); err != nil {
				t.Fatal(err)
			}
			consumers = append(consumers, consumer{bus: bus, tr: ct, workers: g.workers})
		}

		for seq := range plans {
			if err := consumers[len(consumers)-1].tr.Send(setup, qumsg.NewEnvelope(stressMessage{Group: gi, Seq: seq})); err != nil {
				t.Fatal(err)
			}
		}
	}
	defer func() {
		for _, c := range consumers {
			_ = c.tr.Close()
		}
	}()

	// Do not collect all goroutine stacks while leases are active:
	// runtime.Stack with all=true stops the world, but PostgreSQL's lease
	// clock keeps running. Check for leaked goroutines after draining instead.

	start := time.Now()
	consumeCtx, stopConsume := context.WithCancel(t.Context())
	defer stopConsume()
	var (
		consumeWG   sync.WaitGroup
		consumeErrs = make([]error, len(consumers))
	)
	for i, c := range consumers {
		consumeWG.Go(func() {
			consumeErrs[i] = c.bus.Consume(consumeCtx, c.tr, qumsg.WithWorkers(c.workers), qumsg.WithUnlimitedAttempts())
		})
	}

	// Every message was sent before consuming: the run ends when the queues
	// are empty, whatever the handlers saw.
	drained := func() bool {
		for _, table := range groupTables {
			var left int
			if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM "+table).Scan(&left); err != nil {
				t.Error(err)
				return true
			}
			if left > 0 {
				return false
			}
		}
		return true
	}
	for deadline := time.Now().Add(25 * time.Second); !drained(); time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Error("queues not drained within 25s")
			break
		}
	}
	stopConsume()
	consumeWG.Wait()
	elapsed := time.Since(start)

	for i, err := range consumeErrs {
		if err != nil {
			t.Errorf("Consume %d: %v", i, err)
		}
	}

	// Invariant 4: every delivery was settled, and settling stops its heartbeat.
	if n := countGoroutines(heartbeatFunc); n != 0 {
		t.Errorf("%d heartbeat goroutines alive after every delivery was settled", n)
	}
	// Close waits for its LISTEN goroutine: nothing of the transport is left.
	for _, c := range consumers {
		if err := c.tr.Close(); err != nil {
			t.Error(err)
		}
	}
	if n := countGoroutines(transportFuncs); n != 0 {
		t.Errorf("%d goroutines of the transport alive after Close", n)
	}

	// Invariant 3: exactly one committed effect per message, and no rows left.
	check, cancelCheck := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCheck()
	effectCount := map[string]int{}
	rows, err := pool.Query(check, "SELECT key, count(*) FROM "+effects+" GROUP BY key")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var key string
		var n int
		if err := rows.Scan(&key, &n); err != nil {
			t.Fatal(err)
		}
		effectCount[key] = n
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for gi, table := range groupTables {
		var left int
		if err := pool.QueryRow(check, "SELECT count(*) FROM "+table).Scan(&left); err != nil {
			t.Fatal(err)
		}
		if left != 0 {
			t.Errorf("group %d: %d messages left in %s", gi, left, table)
		}
	}

	var (
		byKind         = map[stressKind]int{}
		cancellations  int
		minRemaining   = time.Duration(1<<63 - 1)
		minAfterLock   = time.Duration(1<<63 - 1)
		completeNacked int
	)
	state.mu.Lock()
	defer state.mu.Unlock()
	for key, r := range state.records {
		first := r.plan.first.kind
		byKind[first]++
		wantAttempts := 1
		if first.long() {
			wantAttempts = 2
		}
		final := r.plan.first.kind
		if wantAttempts == 2 {
			final = r.plan.retry.kind
		}
		if len(r.attempts) != wantAttempts {
			t.Errorf("%s (%v then %v): attempts %v, want %d", key, first, r.plan.retry.kind, r.attempts, wantAttempts)
		}
		if r.completions != 1 || effectCount[key] != 1 {
			t.Errorf("%s (%v): completed %d times, %d effects committed, want 1", key, first, r.completions, effectCount[key])
		}
		for _, err := range r.settleErrs {
			t.Errorf("%s: %v", key, err)
		}
		// A Complete commit deletes the row: a renewal in flight then finds
		// none and cancels, so Consume may Nack the already deleted message.
		settlements := r.acks + r.nacks
		if settlements != len(r.attempts) {
			t.Errorf("%s: %d acks and %d nacks for %d attempts", key, r.acks, r.nacks, len(r.attempts))
		}
		if !final.complete() && r.acks != 1 {
			t.Errorf("%s (%v): acked %d times, want 1", key, final, r.acks)
		}
		if final.complete() && r.acks == 0 {
			completeNacked++
		}
		if r.canceled {
			cancellations++
			minRemaining = min(minRemaining, r.remainingAtCheck)
			minAfterLock = min(minAfterLock, r.cancelAfterLock)
		}
	}
	t.Logf("%d messages in %v (%d workers)",
		len(state.records), elapsed.Round(time.Millisecond), totalWorkers)
	for k := range stressKindNames {
		t.Logf("  first attempt %-16s %d", stressKind(k), byKind[stressKind(k)])
	}
	if cancellations > 0 {
		t.Logf("%d long locks canceled: earliest %v after locking, least lease left in the database %v", cancellations, minAfterLock.Round(time.Millisecond), minRemaining.Round(time.Millisecond))
	}
	t.Logf("%d completed messages settled by Nack after their Complete commit", completeNacked)
}

// stressHandler runs the plan of each message and checks invariants 1 and 2
// from inside the handler.
func stressHandler(t *testing.T, pool *pgxpool.Pool, tr *postgres.Transport, table, effects string, lease time.Duration, plans []stressPlan, state *stressState) func(context.Context, stressMessage) error {
	recordEffect := func(ctx context.Context, db postgres.DB, key string, attempt int) error {
		_, err := db.Exec(ctx, "INSERT INTO "+effects+" (key, attempt) VALUES ($1, $2)", key, attempt)
		return err
	}
	return func(ctx context.Context, msg stressMessage) error {
		key := stressKey(msg.Group, msg.Seq)
		attempt := qumsg.Attempt(ctx)
		step := plans[msg.Seq].first
		if attempt > 1 {
			step = plans[msg.Seq].retry
		}
		state.update(key, func(r *stressRecord) { r.attempts = append(r.attempts, attempt) })
		unexpected := func(where string) error {
			t.Errorf("%s attempt %d (%v): canceled %s: %v", key, attempt, step.kind, where, context.Cause(ctx))
			return context.Cause(ctx)
		}

		if !stressSleep(ctx, step.pre) {
			return unexpected("before locking")
		}
		if !step.kind.locks() {
			if !stressSleep(ctx, step.hold) {
				return unexpected("during plain work")
			}
			if err := recordEffect(ctx, pool, key, attempt); err != nil {
				t.Errorf("%s: record effect: %v", key, err)
				return err
			}
			state.complete(key)
			return nil
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Errorf("%s: begin: %v", key, err)
			return err
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		if step.kind.complete() {
			err = tr.Complete(ctx, tx)
		} else {
			err = tr.Own(ctx, tx)
		}
		if err != nil {
			t.Errorf("%s attempt %d (%v): lock: %v", key, attempt, step.kind, err)
			return err
		}
		locked := time.Now()
		if err := recordEffect(ctx, tx, key, attempt); err != nil {
			t.Errorf("%s: record effect: %v", key, err)
			return err
		}

		hold := step.hold
		if step.kind == stressOwnAligned || step.kind == stressCompleteAligned {
			var received time.Time
			state.update(key, func(r *stressRecord) { received = r.received[attempt] })
			hold = min(hold, time.Until(received.Add(lease*52/100)))
		}
		if stressSleep(ctx, hold) {
			if step.kind.long() {
				t.Errorf("%s attempt %d: lock held %v past safeUntil (lease %v) without cancellation", key, attempt, step.hold, lease)
				return errors.New("not canceled")
			}
			if ctx.Err() != nil {
				return unexpected("before commit")
			}
			if err := tx.Commit(ctx); err != nil {
				t.Errorf("%s attempt %d (%v): commit: %v", key, attempt, step.kind, err)
				return err
			}
			state.complete(key)
			if !stressSleep(ctx, step.post) {
				return unexpected("after the Own commit")
			}
			return nil
		}

		// Canceled while holding the lock.
		canceledAt := time.Now()
		cause := context.Cause(ctx)
		if !step.kind.long() {
			return unexpected("while holding a short lock")
		}
		if !errors.Is(cause, postgres.ErrLeaseLost) {
			t.Errorf("%s attempt %d: cancel cause %v, want ErrLeaseLost", key, attempt, cause)
		}
		// Seen from another connection the row is still ours (same attempt)
		// and its lease has not expired, even after the time this check takes.
		check, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var remaining float64
		var rowAttempt int
		err = pool.QueryRow(check, `SELECT
				EXTRACT(EPOCH FROM locked_at + $2::bigint * interval '1 microsecond' - clock_timestamp())::float8,
				attempt
			FROM `+table+` WHERE dedup_key = $1`, key, lease.Microseconds()).Scan(&remaining, &rowAttempt)
		if err != nil {
			t.Errorf("%s: check lease: %v", key, err)
			return cause
		}
		left := time.Duration(remaining * float64(time.Second))
		afterLock := canceledAt.Sub(locked)
		if left <= 0 {
			t.Errorf("%s attempt %d: canceled %v after its lease expired in the database", key, attempt, -left)
		}
		if rowAttempt != attempt {
			t.Errorf("%s: row at attempt %d while canceling attempt %d", key, rowAttempt, attempt)
		}
		// Whatever the phase of the heartbeat when the lock was taken, the
		// last renewal started at most lease/3 before it, so safeUntil is at
		// least lease/3 after it, also on the first one after a slow claim
		// (TestPostgresSlowClaimRenewsBeforeSafeLimit). A quarter of that is
		// left as margin for scheduling and for locked being read late.
		if minimum := lease / 3 * 3 / 4; afterLock < minimum {
			t.Errorf("%s attempt %d: canceled %v after locking, want at least %v (lease %v)", key, attempt, afterLock, minimum, lease)
		}
		state.update(key, func(r *stressRecord) {
			r.canceled = true
			r.cancelAfterLock = afterLock
			r.remainingAtCheck = left
		})
		return cause
	}
}
