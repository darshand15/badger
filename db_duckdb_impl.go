//go:build duckdb

/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"fmt"

	"github.com/dgraph-io/badger/v4/duckdb"
	"github.com/dgraph-io/badger/v4/y"
)

// duckDBStorageWrapper wraps *duckdb.DuckDBStorage to implement duckDBIface.
type duckDBStorageWrapper struct {
	s *duckdb.DuckDBStorage
}

// newDuckDBBackend creates a DuckDB-backed storage implementation.
func newDuckDBBackend(path string, parts int) (duckDBIface, error) {
	s, err := duckdb.NewDuckDBStorage(path, parts)
	if err != nil {
		return nil, err
	}
	return &duckDBStorageWrapper{s: s}, nil
}

func (w *duckDBStorageWrapper) Read(key []byte, epochID int64) ([]byte, uint64, error) {
	entry, err := w.s.Read(key, duckdb.CustomTs{EpochID: epochID})
	if err != nil || entry == nil {
		return nil, 0, err
	}
	return entry.Value, uint64(entry.Timestamp.EpochID), nil
}

func (w *duckDBStorageWrapper) FlushEntries(entries []duckEntry) error {
	darshanEntries := make([]*duckdb.DarshanEntry, len(entries))
	for i, e := range entries {
		darshanEntries[i] = &duckdb.DarshanEntry{
			Key:     e.Key,
			Value:   e.Value,
			Version: e.Version,
			Timestamp: duckdb.CustomTs{
				EpochID: int64(e.Version),
			},
		}
	}
	return w.s.FlushDarshanEntries(darshanEntries)
}

func (w *duckDBStorageWrapper) CompactPartitions() error {
	return w.s.CompactPartitions()
}

func (w *duckDBStorageWrapper) Close() error {
	return w.s.Close()
}

// handleMemTableFlushPartitioned flushes a memtable to DuckDB (full implementation).
func (db *DB) handleMemTableFlushPartitioned(mt *memTable, dropPrefixes [][]byte) error {
	if db.duckDBStorage == nil {
		return db.handleMemTableFlushClassic(mt, dropPrefixes)
	}

	itr := mt.sl.NewUniIterator(false)
	defer itr.Close()

	var entries []duckEntry
	for itr.Rewind(); itr.Valid(); itr.Next() {
		rawKey := itr.Key()
		vs := itr.Value()
		logicalKey := append([]byte(nil), y.ParseKey(rawKey)...)
		version := y.ParseTs(rawKey)
		val := append([]byte(nil), vs.Value...)
		entries = append(entries, duckEntry{Key: logicalKey, Value: val, Version: version})
	}

	if len(entries) == 0 {
		return nil
	}

	// Flush synchronously so reads after this call see the written data.
	// The async goroutine caused read-after-write inconsistency: the function
	// returned nil before DuckDB had persisted any entries.
	if err := db.duckDBStorage.FlushEntries(entries); err != nil {
		return fmt.Errorf("DuckDB flush failed: %w", err)
	}

	return nil
}
