package storage

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackstoller/p2p-messaging/internal/ring"
	_ "modernc.org/sqlite"
)

// Store is the local SQLite-backed persistence layer.
type Store struct {
	db *sql.DB
}

const sqliteBusyTimeoutMillis = 1500
const setVnodeStateMaxAttempts = 6
const setVnodeStateRetryStep = 40 * time.Millisecond

// Open opens (or creates) the SQLite database at path and runs migrations.
// Use ":memory:" for an ephemeral in-process database.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("storage: open %q: %w", path, err)
	}
	// WAL allows concurrent reads with a single writer.
	if _, err = db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		return nil, fmt.Errorf("storage: enable WAL: %w", err)
	}
	// Relaxed durability is fine because replicas can recover after crashes.
	if _, err = db.Exec(`PRAGMA synchronous=NORMAL`); err != nil {
		return nil, fmt.Errorf("storage: set synchronous: %w", err)
	}
	// Wait briefly when a concurrent writer holds the lock instead of failing
	// immediate startup-state updates with SQLITE_BUSY.
	if _, err = db.Exec(fmt.Sprintf("PRAGMA busy_timeout=%d", sqliteBusyTimeoutMillis)); err != nil {
		return nil, fmt.Errorf("storage: set busy_timeout: %w", err)
	}

	s := &Store{db: db}
	if err = s.migrate(); err != nil {
		return nil, err
	}
	return s, nil
}

// Close closes the underlying database connection.
func (s *Store) Close() error { return s.db.Close() }

// Schema

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
		return fmt.Errorf("storage: migrate: %w", err)
	}
	return nil
}

// Records

// Record is the in-memory representation of a stored key/value.
type Record struct {
	Key       string
	Value     []byte
	VnodeId   string
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
	`, r.Key, r.Value, r.VnodeId, r.Timestamp) // TODO: Store position instead of vnode_id??
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
	).Scan(&r.Key, &r.Value, &r.VnodeId, &r.Timestamp)
	if err == sql.ErrNoRows {
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, fmt.Errorf("storage: get %q: %w", key, err)
	}
	return r, true, nil
}

// GetRecordsByVnode returns all records belonging to a vnode.
func (s *Store) GetRecordsByVnode(vnodeId string) ([]Record, error) {
	rows, err := s.db.Query(
		`SELECT key, value, vnode_id, timestamp FROM records WHERE vnode_id = ?`, vnodeId,
	)
	if err != nil {
		return nil, fmt.Errorf("storage: get vnode %q: %w", vnodeId, err)
	}
	defer rows.Close()

	var records []Record
	for rows.Next() {
		var r Record
		if err := rows.Scan(&r.Key, &r.Value, &r.VnodeId, &r.Timestamp); err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

// DeleteRecordsByVnode removes all records belonging to a vnode.
// Called after handing off primary authority.
func (s *Store) DeleteRecordsByVnode(vnodeId string) error {
	_, err := s.db.Exec(`DELETE FROM records WHERE vnode_id = ?`, vnodeId)
	return err
}

// DeleteRecordsInVnodeRange removes records owned by vnodeId whose key hashes
// fall in (start, end]. This is used after partial range transfer completes.
func (s *Store) DeleteRecordsInVnodeRange(vnodeId string, start, end uint64) error {
	records, err := s.GetRecordsByVnode(vnodeId)
	if err != nil {
		return err
	}

	rng := ring.OwnedRange{Start: start, End: end}
	if len(records) == 0 {
		return nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`DELETE FROM records WHERE key = ?`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, r := range records {
		if !rng.InRange(ring.KeyPosition(r.Key)) {
			continue
		}
		if _, err := stmt.Exec(r.Key); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// Owned vnodes

// OwnedVnode is the persisted state of a vnode this node is responsible for.
type OwnedVnode struct {
	Id       string // "nodeId:index"
	Position uint64
	State    string // "inactive" | "active"
}

const (
	OwnedVnodeStateInactive = "inactive"
	OwnedVnodeStateActive   = "active"
)

// SetVnodeState persists a vnode's ownership state.
func (s *Store) SetVnodeState(id string, position uint64, state string) error {
	encodedPos := encodeVnodePosition(position)
	var lastErr error
	for attempt := 1; attempt <= setVnodeStateMaxAttempts; attempt++ {
		_, err := s.db.Exec(`
			INSERT INTO owned_vnodes (id, position, state) VALUES (?, ?, ?)
			ON CONFLICT(id) DO UPDATE SET position = excluded.position, state = excluded.state
		`, id, encodedPos, state)
		if err == nil {
			return nil
		}
		if !isSQLiteBusy(err) {
			return err
		}

		lastErr = err
		if attempt == setVnodeStateMaxAttempts {
			break
		}
		time.Sleep(time.Duration(attempt) * setVnodeStateRetryStep)
	}

	return fmt.Errorf("storage: set vnode state %s: %w", id, lastErr)
}

func isSQLiteBusy(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "SQLITE_BUSY") || strings.Contains(msg, "database is locked")
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
		var encodedPos string
		if err := rows.Scan(&v.Id, &encodedPos, &v.State); err != nil {
			return nil, err
		}
		decoded, err := decodeVnodePosition(encodedPos)
		if err != nil {
			return nil, err
		}
		v.Position = decoded
		result = append(result, v)
	}
	return result, rows.Err()
}

func encodeVnodePosition(pos uint64) string {
	return "u:" + strconv.FormatUint(pos, 10)
}

func decodeVnodePosition(encoded string) (uint64, error) {
	trimmed := strings.TrimPrefix(encoded, "u:")
	if trimmed == "" {
		return 0, fmt.Errorf("storage: empty vnode position %q", encoded)
	}
	pos, err := strconv.ParseUint(trimmed, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("storage: parse vnode position %q: %w", encoded, err)
	}
	return pos, nil
}

// RemoveVnode removes a vnode from owned_vnodes (called after a full handoff).
func (s *Store) RemoveVnode(id string) error {
	_, err := s.db.Exec(`DELETE FROM owned_vnodes WHERE id = ?`, id)
	return err
}

// Helpers

// NowMillis returns the current Unix time in milliseconds.
func NowMillis() int64 { return time.Now().UnixMilli() }
