/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

// duckDBIface is the interface for the optional DuckDB storage backend.
// It deliberately uses only built-in types so that files importing it never
// pull in CGO (which would degrade escape analysis for the whole package).
type duckDBIface interface {
	// Read looks up the latest value for key with epoch <= epochID.
	// Returns (nil, 0, nil) when the key is not found.
	Read(key []byte, epochID int64) (value []byte, version uint64, err error)

	// FlushEntries writes a batch of entries to DuckDB.
	FlushEntries(entries []duckEntry) error

	// CompactPartitions removes superseded key versions to reclaim space.
	CompactPartitions() error

	// Close releases all DuckDB resources.
	Close() error
}

// duckEntry is a single key/value entry used when flushing to DuckDB.
type duckEntry struct {
	Key     []byte
	Value   []byte
	Version uint64
}
