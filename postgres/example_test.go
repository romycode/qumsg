//go:build integration

package postgres_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/romycode/qumsg"
	"github.com/romycode/qumsg/postgres"
)

type SendWelcomeEmail struct{ Email string }

func (SendWelcomeEmail) Name() string       { return "emails.welcome" }
func (m SendWelcomeEmail) DedupKey() string { return m.Email }

// Each runnable example owns its table and has a bounded execution time.
func exampleDatabase() (context.Context, *pgxpool.Pool, string, func()) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	url := os.Getenv("POSTGRES_URL")
	if url == "" {
		url = "postgres://qumsg:qumsg@localhost:5432/qumsg?sslmode=disable"
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		cancel()
		panic(err)
	}
	table := fmt.Sprintf("qumsg_example_%x", rand.Text()[:12])
	ddl, err := postgres.Schema(table)
	mustExample(err)
	_, err = pool.Exec(ctx, ddl)
	mustExample(err)
	return ctx, pool, table, func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		defer cancel()
		defer pool.Close()
		_, err := pool.Exec(cleanupCtx, "DROP TABLE "+table)
		mustExample(err)
	}
}
func mustExample(err error) {
	if err != nil {
		panic(err)
	}
}

// Producer and worker share their routing configuration.
func Example() {
	ctx, pool, table, cleanup := exampleDatabase()
	defer cleanup()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	transport, err := postgres.New(pool, table)
	mustExample(err)
	defer transport.Close()
	bus := qumsg.New()
	mustExample(bus.Register(func(ctx context.Context, msg SendWelcomeEmail) error {
		fmt.Println("Sending welcome email to", msg.Email)
		cancel()
		return nil
	}))
	mustExample(bus.Route[SendWelcomeEmail](transport))
	mustExample(bus.Dispatch(ctx, SendWelcomeEmail{Email: "albert@example.com"})) // only sends
	mustExample(bus.Consume(ctx, transport))
	// Output: Sending welcome email to albert@example.com
}

// A rollback removes the message along with the producer's other writes.
func ExampleWithDB() {
	ctx, pool, table, cleanup := exampleDatabase()
	defer cleanup()
	tx, err := pool.Begin(ctx)
	mustExample(err)
	defer tx.Rollback(context.Background())
	transport, err := postgres.New(pool, table, postgres.WithDB(tx))
	mustExample(err)
	defer transport.Close()
	mustExample(transport.Send(ctx, qumsg.NewEnvelope(SendWelcomeEmail{Email: "albert@example.com"})))
	mustExample(tx.Rollback(ctx))
	var pending int
	mustExample(pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&pending))
	fmt.Println("Pending after rollback:", pending)
	// Output: Pending after rollback: 0
}

// Complete commits the handler's work and the acknowledgement together.
func ExampleTransport_Complete() {
	ctx, pool, table, cleanup := exampleDatabase()
	defer cleanup()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	transport, err := postgres.New(pool, table)
	mustExample(err)
	defer transport.Close()
	bus := qumsg.New(qumsg.WithPropagators(transport.Propagator()))
	mustExample(bus.Route[SendWelcomeEmail](transport))
	mustExample(bus.Register(func(ctx context.Context, msg SendWelcomeEmail) error {
		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(context.Background())
		if err := transport.Complete(ctx, tx); err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		cancel()
		return nil
	}))
	mustExample(transport.Send(ctx, qumsg.NewEnvelope(SendWelcomeEmail{Email: "albert@example.com"})))
	mustExample(bus.Consume(ctx, transport))
	var pending int
	mustExample(pool.QueryRow(context.WithoutCancel(ctx), "SELECT count(*) FROM "+table).Scan(&pending))
	fmt.Println("Pending after completion:", pending)
	// Output: Pending after completion: 0
}

func ExampleDeduplicated() {
	ctx, pool, table, cleanup := exampleDatabase()
	defer cleanup()
	transport, err := postgres.New(pool, table)
	mustExample(err)
	defer transport.Close()
	message := qumsg.NewEnvelope(SendWelcomeEmail{Email: "albert@example.com"})
	mustExample(transport.Send(ctx, message))
	mustExample(transport.Send(ctx, message))
	var pending int
	mustExample(pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&pending))
	fmt.Println("Pending after two sends:", pending)
	// Output: Pending after two sends: 1
}

func ExampleContextWithDelay() {
	ctx, pool, table, cleanup := exampleDatabase()
	defer cleanup()
	transport, err := postgres.New(pool, table)
	mustExample(err)
	defer transport.Close()
	mustExample(transport.Send(postgres.ContextWithDelay(ctx, time.Hour), qumsg.NewEnvelope(SendWelcomeEmail{Email: "albert@example.com"})))
	var ready int
	mustExample(pool.QueryRow(ctx, "SELECT count(*) FROM "+table+" WHERE available_at <= now()").Scan(&ready))
	fmt.Println("Ready immediately:", ready)
	// Output: Ready immediately: 0
}
