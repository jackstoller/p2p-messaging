package storage

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/jackstoller/p2p-messaging/internal/logging"
	"github.com/jackstoller/p2p-messaging/internal/ring"
)

const upsertRecordMaxAttempts = 6
const upsertRecordRetryStep = 40 * time.Millisecond

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
	log := logging.Component("storage")

	err = withSQLiteBusyRetry(
		upsertRecordMaxAttempts,
		upsertRecordRetryStep,
		func(attempt int, err error) {
			log.Warn("storage.record.upsert.retry", logging.Outcome(logging.OutcomeFailed), logging.AttrKey, r.Key, logging.AttrVnodeId, r.VnodeId, "timestamp", r.Timestamp, "attempt", attempt, logging.Err(err))
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
			log.Debug("storage.record.upsert", logging.Outcome(logging.OutcomeSucceeded), logging.AttrKey, r.Key, logging.AttrVnodeId, r.VnodeId, "timestamp", r.Timestamp, "attempt", attempt, "applied", applied)
			return nil
		},
	)
	if err != nil {
		log.Error("storage.record.upsert", logging.Outcome(logging.OutcomeFailed), logging.AttrKey, r.Key, logging.AttrVnodeId, r.VnodeId, "timestamp", r.Timestamp, logging.Err(err))
		return false, fmt.Errorf("storage: upsert %q: %w", r.Key, err)
	}
	return applied, nil
}

// GetRecord looks up a key. Returns (record, true, nil) if found.
func (s *Store) GetRecord(key string) (Record, bool, error) {
	log := logging.Component("storage")
	var r Record
	err := s.db.QueryRow(
		`SELECT key, value, vnode_id, timestamp FROM records WHERE key = ?`, key,
	).Scan(&r.Key, &r.Value, &r.VnodeId, &r.Timestamp)
	if err == sql.ErrNoRows {
		log.Debug("storage.record.get", logging.Outcome(logging.OutcomeSkipped), logging.AttrKey, key, "found", false)
		return Record{}, false, nil
	}
	if err != nil {
		log.Error("storage.record.get", logging.Outcome(logging.OutcomeFailed), logging.AttrKey, key, logging.Err(err))
		return Record{}, false, fmt.Errorf("storage: get %q: %w", key, err)
	}
	log.Debug("storage.record.get", logging.Outcome(logging.OutcomeSucceeded), logging.AttrKey, key, logging.AttrVnodeId, r.VnodeId, "found", true, "timestamp", r.Timestamp)
	return r, true, nil
}

// GetRecordsByVnode returns all records belonging to a vnode.
func (s *Store) GetRecordsByVnode(vnodeId string) ([]Record, error) {
	log := logging.Component("storage")
	rows, err := s.db.Query(`SELECT key, value, vnode_id, timestamp FROM records WHERE vnode_id = ?`, vnodeId)
	if err != nil {
		log.Error("storage.record.list_by_vnode", logging.Outcome(logging.OutcomeFailed), logging.AttrVnodeId, vnodeId, logging.Err(err))
		return nil, fmt.Errorf("storage: get vnode %q: %w", vnodeId, err)
	}
	defer rows.Close()

	var records []Record
	for rows.Next() {
		var r Record
		if err := rows.Scan(&r.Key, &r.Value, &r.VnodeId, &r.Timestamp); err != nil {
			log.Error("storage.record.list_by_vnode.scan", logging.Outcome(logging.OutcomeFailed), logging.AttrVnodeId, vnodeId, logging.Err(err))
			return nil, err
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		log.Error("storage.record.list_by_vnode", logging.Outcome(logging.OutcomeFailed), logging.AttrVnodeId, vnodeId, logging.Err(err))
		return nil, err
	}
	log.Debug("storage.record.list_by_vnode", logging.Outcome(logging.OutcomeSucceeded), logging.AttrVnodeId, vnodeId, "records", len(records))
	return records, nil
}

// GetAllRecords returns all locally stored records, including primary and
// replica copies.
func (s *Store) GetAllRecords() ([]Record, error) {
	log := logging.Component("storage")
	rows, err := s.db.Query(`SELECT key, value, vnode_id, timestamp FROM records`)
	if err != nil {
		log.Error("storage.record.list_all", logging.Outcome(logging.OutcomeFailed), logging.Err(err))
		return nil, fmt.Errorf("storage: list all records: %w", err)
	}
	defer rows.Close()

	var records []Record
	for rows.Next() {
		var r Record
		if err := rows.Scan(&r.Key, &r.Value, &r.VnodeId, &r.Timestamp); err != nil {
			log.Error("storage.record.list_all.scan", logging.Outcome(logging.OutcomeFailed), logging.Err(err))
			return nil, err
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		log.Error("storage.record.list_all", logging.Outcome(logging.OutcomeFailed), logging.Err(err))
		return nil, err
	}

	log.Debug("storage.record.list_all", logging.Outcome(logging.OutcomeSucceeded), "records", len(records))
	return records, nil
}

// DeleteRecordsByVnode removes all records belonging to a vnode.
// Called after handing off primary authority.
func (s *Store) DeleteRecordsByVnode(vnodeId string) error {
	log := logging.Component("storage")
	records, err := s.GetRecordsByVnode(vnodeId)
	if err != nil {
		log.Error("storage.record.delete_by_vnode", logging.Outcome(logging.OutcomeFailed), logging.AttrVnodeId, vnodeId, logging.Err(err))
		return err
	}

	res, err := s.db.Exec(`DELETE FROM records WHERE vnode_id = ?`, vnodeId)
	if err != nil {
		log.Error("storage.record.delete_by_vnode", logging.Outcome(logging.OutcomeFailed), logging.AttrVnodeId, vnodeId, logging.Err(err))
		return err
	}

	rows, _ := res.RowsAffected()
	for _, record := range records {
		log.Info("storage.record.delete_by_vnode.item", logging.Outcome(logging.OutcomeSucceeded), logging.AttrVnodeId, vnodeId, logging.AttrKey, record.Key, "timestamp", record.Timestamp)
	}
	log.Info("storage.record.delete_by_vnode", logging.Outcome(logging.OutcomeSucceeded), logging.AttrVnodeId, vnodeId, "deleted", rows)
	return nil
}

// DeleteRecordsInVnodeRange removes records owned by vnodeId whose key hashes
// fall in (start, end]. This is used after partial range transfer completes.
func (s *Store) DeleteRecordsInVnodeRange(vnodeId string, start, end uint64) error {
	log := logging.Component("storage")
	log.Info("storage.record.delete_range", logging.Outcome(logging.OutcomeStarted), logging.AttrVnodeId, vnodeId, "range_start", start, "range_end", end)

	records, err := s.GetRecordsByVnode(vnodeId)
	if err != nil {
		log.Error("storage.record.delete_range", logging.Outcome(logging.OutcomeFailed), logging.AttrVnodeId, vnodeId, "range_start", start, "range_end", end, logging.Err(err))
		return err
	}

	toDelete := recordsInRange(records, start, end)
	if len(toDelete) == 0 {
		log.Info("storage.record.delete_range", logging.Outcome(logging.OutcomeSkipped), logging.AttrVnodeId, vnodeId, "range_start", start, "range_end", end, "deleted", 0)
		return nil
	}

	if err := s.deleteRecordsByKey(toDelete); err != nil {
		log.Error("storage.record.delete_range", logging.Outcome(logging.OutcomeFailed), logging.AttrVnodeId, vnodeId, "range_start", start, "range_end", end, logging.Err(err))
		return err
	}

	for _, record := range toDelete {
		log.Info("storage.record.delete_range.item", logging.Outcome(logging.OutcomeSucceeded), logging.AttrVnodeId, vnodeId, logging.AttrKey, record.Key, "timestamp", record.Timestamp, "range_start", start, "range_end", end)
	}
	log.Info("storage.record.delete_range", logging.Outcome(logging.OutcomeSucceeded), logging.AttrVnodeId, vnodeId, "range_start", start, "range_end", end, "deleted", len(toDelete))
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
