// Package postgres provides shared PostgreSQL connection pooling and migration helpers for WPO services.
// Thomas Wire <tom@thomaswire.com>
package postgres

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/thomaswire/wpo/pkg/shared/config"
)

// NewPool opens a connection pool to the configured PostgreSQL database.
func NewPool(ctx context.Context, cfg *config.Config) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(cfg.DSN(cfg.ServiceName))
	if err != nil {
		return nil, fmt.Errorf("parse postgres config: %w", err)
	}

	config.MaxConns = int32(cfg.PostgresMaxConns)
	config.MinConns = int32(cfg.PostgresMinConns)
	config.ConnConfig.ConnectTimeout = cfg.PostgresConnectTimeout

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}

	// Verify connectivity with a simple health check
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return pool, nil
}

// NewPoolForDB creates a pool for a specific database name (for migrations).
func NewPoolForDB(dsn string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres config: %w", err)
	}
	return pgxpool.NewWithConfig(context.Background(), config)
}

// Migrate runs all SQL migration files in the given directory against the pool.
// Files must be named with 3-digit numeric prefix (e.g., 001_create_orders.sql).
func Migrate(ctx context.Context, logger *zap.Logger, pool *pgxpool.Pool, migrationsDir string) error {
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return fmt.Errorf("read migrations dir %s: %w", migrationsDir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}

		sqlPath := filepath.Join(migrationsDir, entry.Name())
		logger.Info("running migration", zap.String("file", entry.Name()))

		data, err := os.ReadFile(sqlPath)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", entry.Name(), err)
		}

		_, err = pool.Exec(ctx, string(data))
		if err != nil {
			return fmt.Errorf("apply migration %s: %w", entry.Name(), err)
		}

		logger.Info("migration applied", zap.String("file", entry.Name()))
	}

	return nil
}

// MigrateForDB runs migrations against a database specified by DSN (not via pool).
func MigrateForDB(logger *zap.Logger, dsn string, migrationsDir string) error {
	pool, err := NewPoolForDB(dsn)
	if err != nil {
		return err
	}
	defer pool.Close()

	return Migrate(context.Background(), logger, pool, migrationsDir)
}
