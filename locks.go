package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// acquirelock attempts to acquire a database-backed lock with the given key and value.
// retries with 100ms sleep until timeout is reached.
// returns true if lock was acquired, false if timeout occurred.
func AcquireLock(ctx context.Context, db *sql.DB, key, value string, timeout time.Duration) (bool, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		res, err := db.ExecContext(ctx, `
			INSERT INTO locks (k, v) VALUES (?, ?)
			ON CONFLICT(k) DO NOTHING
		`, key, value)
		if err != nil {
			// actual sql error
			return false, err
		}
		if n, _ := res.RowsAffected(); n == 1 {
			// row truly inserted ⇒ lock acquired
			return true, nil
		}
		// lock is currently held by another party
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return false, fmt.Errorf("failed to acquire lock %s within %s", key, timeout)
}

// releaselock releases a database-backed lock.
// only releases if the stored value matches (prevents releasing someone else's lock).
func ReleaseLock(ctx context.Context, db *sql.DB, key, value string) error {
	_, err := db.ExecContext(ctx, `
		DELETE FROM locks WHERE k = ? AND v = ?
	`, key, value)
	return err
}
