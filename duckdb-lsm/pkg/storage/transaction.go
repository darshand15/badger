package storage

import (
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4/duckdb-lsm/pkg/types"
)

var ErrKeyNotFound = errors.New("key not found")

type Transaction struct {
	storage   *DuckDBStorage
	timestamp types.CustomTs
	update    bool
	writes    map[string]*writeEntry
}

type writeEntry struct {
	key   []byte
	value []byte
}

// NewTransactionAt creates transaction at given timestamp
func (s *DuckDBStorage) NewTransactionAt(ts types.CustomTs, update bool) *Transaction {
	return &Transaction{
		storage:   s,
		timestamp: ts,
		update:    update,
		writes:    make(map[string]*writeEntry),
	}
}

// Set buffers a write (doesn't write to DB yet)
func (t *Transaction) Set(key, value []byte) error {
	if !t.update {
		return errors.New("transaction is read-only")
	}

	t.writes[string(key)] = &writeEntry{
		key:   key,
		value: value,
	}
	return nil
}

// Get reads value at transaction's timestamp
func (t *Transaction) Get(key []byte) (*types.Item, error) {
	// Check buffered writes first
	if entry, ok := t.writes[string(key)]; ok {
		return types.NewItem(entry.key, entry.value, t.timestamp), nil
	}

	// Query database - fetch latest version (rows sorted DESC)
	partition := t.storage.getPartition(key)
	tableName := fmt.Sprintf("partition_%d", partition)

	query := fmt.Sprintf(`
        SELECT epoch_id, broker_id, assigned_ts, value
        FROM %s
        WHERE key = ?
        ORDER BY epoch_id DESC, broker_id DESC, assigned_ts DESC
        LIMIT 1
    `, tableName)

	row := t.storage.db.QueryRow(query, key)

	var epochID, brokerID, assignedTs int64
	var value []byte
	if err := row.Scan(&epochID, &brokerID, &assignedTs, &value); err != nil {
		// No rows -> key not found
		return nil, ErrKeyNotFound
	}

	versionTs := types.CustomTs{
		EpochID:    epochID,
		BrokerID:   brokerID,
		AssignedTs: assignedTs,
	}

	return types.NewItem(key, value, versionTs), nil
}

// Commit writes all buffered writes to database
func (t *Transaction) Commit() error {
	if !t.update {
		return nil
	}

	// Begin SQL transaction
	tx, err := t.storage.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Insert all writes
	for _, entry := range t.writes {
		partition := t.storage.getPartition(entry.key)
		tableName := fmt.Sprintf("partition_%d", partition)

		// INSERT new version (never UPDATE - LSM tree principle!)
		insertSQL := fmt.Sprintf(`
            INSERT INTO %s (key, epoch_id, broker_id, assigned_ts, value)
            VALUES (?, ?, ?, ?, ?)
        `, tableName)

		_, err := tx.Exec(insertSQL,
			entry.key,
			t.timestamp.EpochID,
			t.timestamp.BrokerID,
			t.timestamp.AssignedTs,
			entry.value,
		)

		if err != nil {
			return fmt.Errorf("insert failed: %w", err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return err
	}

	t.writes = make(map[string]*writeEntry)
	return nil
}

func (t *Transaction) Discard() {
	t.writes = make(map[string]*writeEntry)
}
