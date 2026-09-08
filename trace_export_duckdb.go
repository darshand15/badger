//go:build duckdb

package badger

import "github.com/dgraph-io/badger/v4/types"

// ScanPrefixAt exports visible DuckDB rows. The normal Badger iterator does
// not see rows that have already been routed to the DuckDB backend.
func (db *DB) ScanPrefixAt(prefix []byte, readTs types.CustomTs) ([]VisibleEntry, error) {
	if db.duckDBStorage == nil {
		return nil, ErrDBClosed
	}
	rows, err := db.duckDBStorage.ScanPrefix(prefix, readTs)
	if err != nil {
		return nil, err
	}
	result := make([]VisibleEntry, 0, len(rows))
	for _, row := range rows {
		if !row.Found {
			continue
		}
		result = append(result, VisibleEntry{
			Key:     append([]byte(nil), row.Key...),
			Value:   append([]byte(nil), row.Value...),
			Version: row.Version,
		})
	}
	return result, nil
}
