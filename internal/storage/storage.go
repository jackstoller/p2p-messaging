package storage

import (
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// Store is the local SQLite-backed persistence layer.
type Store struct {
	db *sql.DB
}

// Open opens (or creates) the SQLite database at path and runs migrations.
// Use ":memory:" for an ephemeral in-process database.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("storage: open %q: %w", path, err)
	}
	// WAL: concurrent reads with a single writer, no reader blocking.
	if _, err = db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		return nil, fmt.Errorf("storage: enable WAL: %w", err)
	}
	// Relaxed durability — replicas can reconstruct on crash.
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
func (s *Store) Close() error { return s.db.Close() }

// ─── Schema ──────────────────────────────────────────────────────────────────

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
		id       TEXT NOT NULL PRIMARY KEY,  -- "nodeID:index"
		position INTEGER NOT NULL,
		state    TEXT NOT NULL DEFAULT 'inactive'
			CHECK(state IN ('inactive', 'active'))
	);
	`)
	if err != nil {
		return fmt.Errorf("storage: migrate: %w", err)
	}
	return nil
}

// ─── Records ─────────────────────────────────────────────────────────────────

// Record is the in-memory representation of a stored key/value.
type Record struct {
	Key       string
	Value     []byte
	VnodeID   string
	Timestamp int64 // unix millis
}

// UpsertRecord writes r only if the incoming timestamp is newer than the stored
// one. Returns true if the write was applied.
func (s *Store) UpsertRecord(r Record) (applied bool, err error) {
	res, err := s.db.Exec(`
		INSERT INTO records (key, value, vnode_id, timestamp)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET
			value     = excluded.value,
			vnode_id  = excluded.vnode_id,
			timestamp = excluded.timestamp
		WHERE excluded.timestamp > records.timestamp
	`, r.Key, r.Value, r.VnodeID, r.Timestamp)
	if err != nil {
		return false, fmt.Errorf("storage: upsert %q: %w", r.Key, err)
	}
	rows, _ := res.RowsAffected()
	return rows > 0, nil
}

// GetRecord looks up a key. Returns (record, true, nil) if found.
func (s *Store) GetRecord(key string) (Record, bool, error) {
	var r Record
	err := s.db.QueryRow(
		`SELECT key, value, vnode_id, timestamp FROM records WHERE key = ?`, key,
	).Scan(&r.Key, &r.Value, &r.VnodeID, &r.Timestamp)
	if err == sql.ErrNoRows {
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, fmt.Errorf("storage: get %q: %w", key, err)
	}
	return r, true, nil
}

// GetRecordsByVnode returns all records belonging to a vnode.
func (s *Store) GetRecordsByVnode(vnodeID string) ([]Record, error) {
	rows, err := s.db.Query(
		`SELECT key, value, vnode_id, timestamp FROM records WHERE vnode_id = ?`, vnodeID,
	)
	if err != nil {
		return nil, fmt.Errorf("storage: get vnode %q: %w", vnodeID, err)
	}
	defer rows.Close()

	var records []Record
	for rows.Next() {
		var r Record
		if err := rows.Scan(&r.Key, &r.Value, &r.VnodeID, &r.Timestamp); err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

// DeleteRecordsByVnode removes all records belonging to a vnode.
// Called after handing off primary authority.
func (s *Store) DeleteRecordsByVnode(vnodeID string) error {
	_, err := s.db.Exec(`DELETE FROM records WHERE vnode_id = ?`, vnodeID)
	return err
}

// ─── Owned vnodes ─────────────────────────────────────────────────────────────

// OwnedVnode is the persisted state of a vnode this node is responsible for.
type OwnedVnode struct {
	ID       string // "nodeID:index"
	Position uint64
	State    string // "inactive" | "active"
}

// SetVnodeState persists a vnode's ownership state.
func (s *Store) SetVnodeState(id string, position uint64, state string) error {
	_, err := s.db.Exec(`
		INSERT INTO owned_vnodes (id, position, state) VALUES (?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET position = excluded.position, state = excluded.state
	`, id, position, state)
	return err
}

// GetOwnedVnodes returns all vnodes this node has persisted ownership of.
func (s *Store) GetOwnedVnodes() ([]OwnedVnode, error) {
	rows, err := s.db.Query(`SELECT id, position, state FROM owned_vnodes`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []OwnedVnode
	for rows.Next() {
		var v OwnedVnode
		if err := rows.Scan(&v.ID, &v.Position, &v.State); err != nil {
			return nil, err
		}
		result = append(result, v)
	}
	return result, rows.Err()
}

// RemoveVnode removes a vnode from owned_vnodes (called after a full handoff).
func (s *Store) RemoveVnode(id string) error {
	_, err := s.db.Exec(`DELETE FROM owned_vnodes WHERE id = ?`, id)
	return err
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

// NowMillis returns the current Unix time in milliseconds.
func NowMillis() int64 { return time.Now().UnixMilli() }
