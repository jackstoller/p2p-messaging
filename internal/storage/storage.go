package storage

import (
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// Store is the local SQLite-backed data store for this node.
type Store struct {
	db *sql.DB
}

// TODO: Make this generic
// UserRecord mirrors the proto type but lives in the storage layer
// to avoid a circular dependency on the generated proto package.
type UserRecord struct {
	Username  string
	NodeID    string // node the user is connected to
	RingPos   uint64 // hash(username) — stored for range queries
	Version   int64  // monotonic, used for replica conflict resolution
	UpdatedAt int64  // unix millis
}

// Open opens (or creates) the SQLite database at path and runs migrations.
// Use path = ":memory:" for an in-memory database (lost on process exit).
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("storage: open %s: %w", path, err)
	}

	// WAL mode — concurrent reads with a single writer, no reader blocking.
	if _, err = db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		return nil, fmt.Errorf("storage: enable WAL: %w", err)
	}
	// Slightly relaxed durability; we can reconstruct from replicas on crash.
	if _, err = db.Exec(`PRAGMA synchronous=NORMAL`); err != nil {
		return nil, fmt.Errorf("storage: set synchronous: %w", err)
	}

	s := &Store{db: db}
	if err = s.migrate(); err != nil {
		return nil, err
	}
	return s, nil
}

// Close closes the underlying database connection.
func (s *Store) Close() error {
	return s.db.Close()
}

// ─── schema ─────────────────────────────────────────────────────────────────

func (s *Store) migrate() error {
	_, err := s.db.Exec(`
	CREATE TABLE IF NOT EXISTS user_records (
		username    TEXT    PRIMARY KEY,
		node_id     TEXT    NOT NULL,
		ring_pos    INTEGER NOT NULL,   -- hash(username) for range queries
		version     INTEGER NOT NULL DEFAULT 0,
		updated_at  INTEGER NOT NULL    -- unix millis
	);

	-- Range scans during transfer (GetRange) use ring_pos heavily.
	CREATE INDEX IF NOT EXISTS idx_user_records_ring_pos
		ON user_records(ring_pos);

	-- Metadata about ranges this node is currently authoritative for.
	-- Used to quickly answer "do I own this key?" without querying the ring.
	CREATE TABLE IF NOT EXISTS owned_ranges (
		vnode_id    TEXT    PRIMARY KEY,
		range_start INTEGER NOT NULL,   -- exclusive
		range_end   INTEGER NOT NULL,   -- inclusive
		role        TEXT    NOT NULL    -- 'primary' | 'replica' | 'transferring'
	);
	`)
	if err != nil {
		return fmt.Errorf("storage: migrate: %w", err)
	}
	return nil
}

// ─── user records ────────────────────────────────────────────────────────────

// Upsert writes a user record, but only if the incoming version is newer.
// Returns (true, nil) if the write was applied, (false, nil) if skipped.
func (s *Store) Upsert(r UserRecord) (applied bool, err error) {
	res, err := s.db.Exec(`
		INSERT INTO user_records (username, node_id, ring_pos, version, updated_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(username) DO UPDATE SET
			node_id    = excluded.node_id,
			ring_pos   = excluded.ring_pos,
			version    = excluded.version,
			updated_at = excluded.updated_at
		WHERE excluded.version > user_records.version
	`, r.Username, r.NodeID, r.RingPos, r.Version, r.UpdatedAt)
	if err != nil {
		return false, fmt.Errorf("storage: upsert %s: %w", r.Username, err)
	}
	rows, _ := res.RowsAffected()
	return rows > 0, nil
}

// Get returns a user record by username.
func (s *Store) Get(username string) (UserRecord, bool, error) {
	var r UserRecord
	err := s.db.QueryRow(`
		SELECT username, node_id, ring_pos, version, updated_at
		FROM user_records WHERE username = ?
	`, username).Scan(&r.Username, &r.NodeID, &r.RingPos, &r.Version, &r.UpdatedAt)
	if err == sql.ErrNoRows {
		return UserRecord{}, false, nil
	}
	if err != nil {
		return UserRecord{}, false, fmt.Errorf("storage: get %s: %w", username, err)
	}
	return r, true, nil
}

// Delete removes a user record. No-op if it doesn't exist.
func (s *Store) Delete(username string) error {
	_, err := s.db.Exec(`DELETE FROM user_records WHERE username = ?`, username)
	return err
}

// GetRange returns all records whose ring_pos falls in (start, end].
// Handles ring wrap-around (start > end).
func (s *Store) GetRange(start, end uint64) ([]UserRecord, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if start < end {
		rows, err = s.db.Query(`
			SELECT username, node_id, ring_pos, version, updated_at
			FROM user_records
			WHERE ring_pos > ? AND ring_pos <= ?
		`, start, end)
	} else {
		// Wrap-around: (start, maxUint64] ∪ [0, end]
		rows, err = s.db.Query(`
			SELECT username, node_id, ring_pos, version, updated_at
			FROM user_records
			WHERE ring_pos > ? OR ring_pos <= ?
		`, start, end)
	}
	if err != nil {
		return nil, fmt.Errorf("storage: getRange (%d,%d]: %w", start, end, err)
	}
	defer rows.Close()

	var records []UserRecord
	for rows.Next() {
		var r UserRecord
		if err := rows.Scan(&r.Username, &r.NodeID, &r.RingPos, &r.Version, &r.UpdatedAt); err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

// DeleteRange removes all records in (start, end], used after DropReplica.
func (s *Store) DeleteRange(start, end uint64) error {
	var err error
	if start < end {
		_, err = s.db.Exec(`
			DELETE FROM user_records WHERE ring_pos > ? AND ring_pos <= ?
		`, start, end)
	} else {
		_, err = s.db.Exec(`
			DELETE FROM user_records WHERE ring_pos > ? OR ring_pos <= ?
		`, start, end)
	}
	return err
}

// ─── owned ranges ────────────────────────────────────────────────────────────

type RangeRole string

const (
	RolePrimary      RangeRole = "primary"
	RoleReplica      RangeRole = "replica"
	RoleTransferring RangeRole = "transferring"
)

// SetRange records that this node has a given role for a vnode range.
func (s *Store) SetRange(vnodeID string, start, end uint64, role RangeRole) error {
	_, err := s.db.Exec(`
		INSERT INTO owned_ranges (vnode_id, range_start, range_end, role)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(vnode_id) DO UPDATE SET
			range_start = excluded.range_start,
			range_end   = excluded.range_end,
			role        = excluded.role
	`, vnodeID, start, end, string(role))
	return err
}

// RemoveRange deletes a range entry (used after DropReplica / graceful release).
func (s *Store) RemoveRange(vnodeID string) error {
	_, err := s.db.Exec(`DELETE FROM owned_ranges WHERE vnode_id = ?`, vnodeID)
	return err
}

// GetOwnedRanges returns all ranges this node tracks locally.
func (s *Store) GetOwnedRanges() ([]OwnedRangeRow, error) {
	rows, err := s.db.Query(`
		SELECT vnode_id, range_start, range_end, role FROM owned_ranges
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []OwnedRangeRow
	for rows.Next() {
		var o OwnedRangeRow
		var role string
		if err := rows.Scan(&o.VnodeID, &o.Start, &o.End, &role); err != nil {
			return nil, err
		}
		o.Role = RangeRole(role)
		result = append(result, o)
	}
	return result, rows.Err()
}

type OwnedRangeRow struct {
	VnodeID string
	Start   uint64
	End     uint64
	Role    RangeRole
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func NowMillis() int64 {
	return time.Now().UnixMilli()
}