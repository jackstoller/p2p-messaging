package storage

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/jackstoller/p2p-messaging/internal/logging"
	"github.com/jackstoller/p2p-messaging/internal/ring"
)

const upsertRecordMaxAttempts = 10
const upsertRecordRetryStep = 75 * time.Millisecond

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

	err = withSQLiteBusyRetry(
		upsertRecordMaxAttempts,
		upsertRecordRetryStep,
		func(attempt int, err error) {
		},
		func(attempt int) error {
			res, err := s.db.Exec(`
				INSERT INTO records (key, value, vnode_id, timestamp)
				VALUES (?, ?, ?, ?)
				ON CONFLICT(key) DO UPDATE SET
					value     = excluded.value,
					vnode_id  = excluded.vnode_id,
					timestamp = excluded.timestamp
				WHERE excluded.timestamp > records.timestamp
			`, r.Key, r.Value, r.VnodeId, r.Timestamp)
			if err != nil {
				return err
			}

			rows, _ := res.RowsAffected()
			applied = rows > 0
			return nil
		},
	)
	if err != nil {
		logging.Error("Record write failed with key=%v, vnode=%v, error=%v.", r.Key, r.VnodeId, err)
		return false, fmt.Errorf("storage: upsert %q: %w", r.Key, err)
	}
	return applied, nil
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
		logging.Error("Record read failed with key=%v, error=%v.", key, err)
		return Record{}, false, fmt.Errorf("storage: get %q: %w", key, err)
	}
	return r, true, nil
}

// GetRecordsByVnode returns all records belonging to a vnode.
func (s *Store) GetRecordsByVnode(vnodeId string) ([]Record, error) {
	rows, err := s.db.Query(`SELECT key, value, vnode_id, timestamp FROM records WHERE vnode_id = ?`, vnodeId)
	if err != nil {
		logging.Error("Listing records for vnode failed with vnode=%v, error=%v.", vnodeId, err)
		return nil, fmt.Errorf("storage: get vnode %q: %w", vnodeId, err)
	}
	defer rows.Close()

	var records []Record
	for rows.Next() {
		var r Record
		if err := rows.Scan(&r.Key, &r.Value, &r.VnodeId, &r.Timestamp); err != nil {
			logging.Error("Reading records for vnode failed with vnode=%v, error=%v.", vnodeId, err)
			return nil, err
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		logging.Error("Listing records for vnode failed with vnode=%v, error=%v.", vnodeId, err)
		return nil, err
	}
	return records, nil
}

// GetAllRecords returns all locally stored records, including primary and
// replica copies.
func (s *Store) GetAllRecords() ([]Record, error) {
	rows, err := s.db.Query(`SELECT key, value, vnode_id, timestamp FROM records`)
	if err != nil {
		logging.Error("Listing all records failed with error=%v.", err)
		return nil, fmt.Errorf("storage: list all records: %w", err)
	}
	defer rows.Close()

	var records []Record
	for rows.Next() {
		var r Record
		if err := rows.Scan(&r.Key, &r.Value, &r.VnodeId, &r.Timestamp); err != nil {
			logging.Error("Reading all records failed with error=%v.", err)
			return nil, err
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		logging.Error("Listing all records failed with error=%v.", err)
		return nil, err
	}

	return records, nil
}

// DeleteRecordsByVnode removes all records belonging to a vnode.
// Called after handing off primary authority.
func (s *Store) DeleteRecordsByVnode(vnodeId string) error {
	if _, err := s.GetRecordsByVnode(vnodeId); err != nil {
		logging.Error("Deleting vnode records failed with vnode=%v, error=%v.", vnodeId, err)
		return err
	}

	_, err := s.db.Exec(`DELETE FROM records WHERE vnode_id = ?`, vnodeId)
	if err != nil {
		logging.Error("Deleting vnode records failed with vnode=%v, error=%v.", vnodeId, err)
		return err
	}

	return nil
}

// DeleteRecordsInVnodeRange removes records owned by vnodeId whose key hashes
// fall in (start, end]. This is used after partial range transfer completes.
func (s *Store) DeleteRecordsInVnodeRange(vnodeId string, start, end uint64) error {

	records, err := s.GetRecordsByVnode(vnodeId)
	if err != nil {
		logging.Error("Range record delete failed with vnode=%v, range start=%v, range end=%v, error=%v.", vnodeId, start, end, err)
		return err
	}

	toDelete := recordsInRange(records, start, end)
	if len(toDelete) == 0 {
		return nil
	}

	if err := s.deleteRecordsByKey(toDelete); err != nil {
		logging.Error("Range record delete failed with vnode=%v, range start=%v, range end=%v, error=%v.", vnodeId, start, end, err)
		return err
	}

	return nil
}

func recordsInRange(records []Record, start, end uint64) []Record {
	rng := ring.OwnedRange{Start: start, End: end}
	filtered := make([]Record, 0, len(records))
	for _, record := range records {
		if rng.InRange(ring.KeyPosition(record.Key)) {
			filtered = append(filtered, record)
		}
	}
	return filtered
}

func (s *Store) deleteRecordsByKey(records []Record) error {
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

	for _, record := range records {
		if _, err := stmt.Exec(record.Key); err != nil {
			return err
		}
	}

	return tx.Commit()
}
