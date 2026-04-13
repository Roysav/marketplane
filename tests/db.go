package tests

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/roysav/marketplane/pkg/record"
	"github.com/roysav/marketplane/pkg/service"
	"github.com/roysav/marketplane/pkg/storage/postgres"
)

const DefaultDSN = "postgresql://marketplane:marketplane@localhost:5432/marketplane"

// Pool creates a temporary database for the test, runs migrations, and
// returns a connection pool to it. The database is dropped on test cleanup.
// Fails if the temporary database already exists — this indicates a prior
// test did not clean up properly.
func Pool(ctx context.Context, t *testing.T) *pgxpool.Pool {
	t.Helper()

	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		dsn = DefaultDSN
	}

	// Connect to the default database to create the temp one
	adminPool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("postgres not available: %v", err)
	}
	if err := adminPool.Ping(ctx); err != nil {
		adminPool.Close()
		t.Skipf("postgres not available: %v", err)
	}

	// Create a unique database name from the test name
	dbName := "test_" + sanitize(t.Name())
	createSQL := fmt.Sprintf("CREATE DATABASE \"%s\"", dbName)
	if _, err := adminPool.Exec(ctx, createSQL); err != nil {
		adminPool.Close()
		t.Fatalf("create test database %s: %v", dbName, err)
	}
	adminPool.Close()

	// Connect to the new database
	testDSN := replaceDSNDatabase(dsn, dbName)
	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil {
		t.Fatalf("connect to test database: %v", err)
	}

	// Run migrations
	if err := postgres.Migrate(ctx, pool); err != nil {
		pool.Close()
		t.Fatalf("migrate: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		// Reconnect to admin database to drop the temp one
		cleanupPool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Errorf("cleanup: connect to admin db: %v", err)
			return
		}
		dropSQL := fmt.Sprintf("DROP DATABASE \"%s\"", dbName)
		if _, err := cleanupPool.Exec(ctx, dropSQL); err != nil {
			t.Errorf("cleanup: drop test database %s: %v", dbName, err)
		}
		cleanupPool.Close()
	})

	return pool
}

// SVC creates a Service backed by a fresh temporary database.
// Skips the test if postgres is not available.
func SVC(ctx context.Context, t *testing.T) *service.Service {
	t.Helper()

	pool := Pool(ctx, t)
	rows := postgres.New(pool)

	validator, err := record.NewValidator()
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	return service.New(service.Config{
		Rows:      rows,
		Validator: *validator,
		Logger:    logger,
	})
}

// sanitize turns a test name into a safe database name.
func sanitize(name string) string {
	r := strings.NewReplacer("/", "_", " ", "_", "-", "_")
	s := strings.ToLower(r.Replace(name))
	if len(s) > 50 {
		s = s[:50]
	}
	return s
}

// replaceDSNDatabase swaps the database name in a PostgreSQL DSN.
func replaceDSNDatabase(dsn, dbName string) string {
	lastSlash := strings.LastIndex(dsn, "/")
	if lastSlash == -1 {
		return dsn + "/" + dbName
	}
	base := dsn[:lastSlash+1]
	rest := dsn[lastSlash+1:]
	if idx := strings.Index(rest, "?"); idx != -1 {
		return base + dbName + rest[idx:]
	}
	return base + dbName
}