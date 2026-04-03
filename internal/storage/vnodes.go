package storage

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackstoller/p2p-messaging/internal/logging"
)

const setVnodeStateMaxAttempts = 6
const setVnodeStateRetryStep = 40 * time.Millisecond

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
	log := logging.Component("storage")
	log.Info("storage.vnode.state.set", logging.Outcome(logging.OutcomeStarted), logging.AttrVnodeId, id, "position", position, "state", state)

	err := withSQLiteBusyRetry(
		setVnodeStateMaxAttempts,
		setVnodeStateRetryStep,
		func(attempt int, err error) {
			log.Warn("storage.vnode.state.set.retry", logging.Outcome(logging.OutcomeFailed), logging.AttrVnodeId, id, "position", position, "state", state, "attempt", attempt, logging.Err(err))
		},
		func(attempt int) error {
			_, err := s.db.Exec(`
				INSERT INTO owned_vnodes (id, position, state) VALUES (?, ?, ?)
				ON CONFLICT(id) DO UPDATE SET position = excluded.position, state = excluded.state
			`, id, encodeVnodePosition(position), state)
			if err != nil {
				return err
			}

			log.Info("storage.vnode.state.set", logging.Outcome(logging.OutcomeSucceeded), logging.AttrVnodeId, id, "position", position, "state", state, "attempt", attempt)
			return nil
		},
	)
	if err != nil {
		log.Error("storage.vnode.state.set", logging.Outcome(logging.OutcomeFailed), logging.AttrVnodeId, id, "position", position, "state", state, logging.Err(err))
		if isSQLiteBusy(err) {
			return fmt.Errorf("storage: set vnode state %s: %w", id, err)
		}
		return err
	}
	return nil
}

// GetOwnedVnodes returns all vnodes this node has persisted ownership of.
func (s *Store) GetOwnedVnodes() ([]OwnedVnode, error) {
	log := logging.Component("storage")
	rows, err := s.db.Query(`SELECT id, position, state FROM owned_vnodes`)
	if err != nil {
		log.Error("storage.vnode.list", logging.Outcome(logging.OutcomeFailed), logging.Err(err))
		return nil, err
	}
	defer rows.Close()

	var result []OwnedVnode
	for rows.Next() {
		vnode, err := scanOwnedVnode(rows)
		if err != nil {
			log.Error("storage.vnode.list.scan", logging.Outcome(logging.OutcomeFailed), logging.Err(err))
			return nil, err
		}
		result = append(result, vnode)
	}
	if err := rows.Err(); err != nil {
		log.Error("storage.vnode.list", logging.Outcome(logging.OutcomeFailed), logging.Err(err))
		return nil, err
	}

	log.Debug("storage.vnode.list", logging.Outcome(logging.OutcomeSucceeded), "vnodes", len(result))
	return result, nil
}

func scanOwnedVnode(scanner interface{ Scan(dest ...any) error }) (OwnedVnode, error) {
	var vnode OwnedVnode
	var encodedPos string
	if err := scanner.Scan(&vnode.Id, &encodedPos, &vnode.State); err != nil {
		return OwnedVnode{}, err
	}

	position, err := decodeVnodePosition(encodedPos)
	if err != nil {
		return OwnedVnode{}, fmt.Errorf("storage: decode vnode %s position: %w", vnode.Id, err)
	}
	vnode.Position = position
	return vnode, nil
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
	log := logging.Component("storage")
	res, err := s.db.Exec(`DELETE FROM owned_vnodes WHERE id = ?`, id)
	if err != nil {
		log.Error("storage.vnode.remove", logging.Outcome(logging.OutcomeFailed), logging.AttrVnodeId, id, logging.Err(err))
		return err
	}
	rows, _ := res.RowsAffected()
	log.Info("storage.vnode.remove", logging.Outcome(logging.OutcomeSucceeded), logging.AttrVnodeId, id, "deleted", rows)
	return nil
}
