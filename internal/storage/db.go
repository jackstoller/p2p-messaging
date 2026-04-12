package storage

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/jackstoller/p2p-messaging/internal/logging"
	_ "modernc.org/sqlite"
)

const sqliteBusyTimeoutMillis = 5000
const sqliteMaxOpenConns = 1
const sqliteMaxIdleConns = 1

// Store is the local SQLite-backed persistence layer.
type Store struct {
	db *sql.DB
}

// Open opens (or creates) the SQLite database at path and runs migrations.
// Use ":memory:" for an ephemeral in-process database.
func Open(path string) (*Store, error) {

	db, err := sql.Open("sqlite", path)
	if err != nil {
		logging.Error("Could not open local storage with db path=%v, error=%v.", path, err)
		return nil, fmt.Errorf("storage: open %q: %w", path, err)
	}

	// SQLite allows only one writer at a time; keep a single pooled connection
	// to avoid cross-connection writer lock contention.
	db.SetMaxOpenConns(sqliteMaxOpenConns)
	db.SetMaxIdleConns(sqliteMaxIdleConns)

	if err := configureSQLite(db); err != nil {
		return nil, err
	}

	store := &Store{db: db}
	if err := store.migrate(); err != nil {
		logging.Error("Database migration failed with error=%v.", err)
		return nil, err
	}

	return store, nil
}

// Close closes the underlying database connection.
func (s *Store) Close() error { return s.db.Close() }

func configureSQLite(db *sql.DB) error {

	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		logging.Error("Failed to set SQLite WAL mode with error=%v.", err)
		return fmt.Errorf("storage: enable WAL: %w", err)
	}
	if _, err := db.Exec(`PRAGMA synchronous=NORMAL`); err != nil {
		logging.Error("Failed to set SQLite synchronous mode with error=%v.", err)
		return fmt.Errorf("storage: set synchronous: %w", err)
	}
	if _, err := db.Exec(fmt.Sprintf("PRAGMA busy_timeout=%d", sqliteBusyTimeoutMillis)); err != nil {
		logging.Error("Failed to set SQLite busy timeout with busy timeout ms=%v, error=%v.", sqliteBusyTimeoutMillis, err)
		return fmt.Errorf("storage: set busy_timeout: %w", err)
	}

	return nil
}

func (s *Store) migrate() error {

	_, err := s.db.Exec(`
	-- Key/value records owned by this node (as primary or replica).
	CREATE TABLE IF NOT EXISTS records (
		key       TEXT    NOT NULL PRIMARY KEY,
		value     BLOB    NOT NULL,
		vnode_id  TEXT    NOT NULL,
		timestamp INTEGER NOT NULL  -- unix millis; last-write-wins on conflict
	);
	CREATE INDEX IF NOT EXISTS idx_records_vnode ON records(vnode_id);

	-- Virtual nodes this node is responsible for.
	CREATE TABLE IF NOT EXISTS owned_vnodes (
		id       TEXT NOT NULL PRIMARY KEY,  -- "nodeId:index"
		position TEXT NOT NULL,
		state    TEXT NOT NULL DEFAULT 'inactive'
			CHECK(state IN ('inactive', 'active'))
	);
	`)
	if err != nil {
		logging.Error("Database migration failed with error=%v.", err)
		return fmt.Errorf("storage: migrate: %w", err)
	}

	return nil
}

func withSQLiteBusyRetry(maxAttempts int, retryStep time.Duration, onBusy func(attempt int, err error), operation func(attempt int) error) error {
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := operation(attempt)
		if err == nil {
			return nil
		}
		if !isSQLiteBusy(err) {
			return err
		}

		lastErr = err
		onBusy(attempt, err)
		if attempt == maxAttempts {
			break
		}
		time.Sleep(time.Duration(attempt) * retryStep)
	}
	return lastErr
}

func isSQLiteBusy(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "SQLITE_BUSY") || strings.Contains(msg, "database is locked")
}

// NowMillis returns the current Unix time in milliseconds.
func NowMillis() int64 { return time.Now().UnixMilli() }
