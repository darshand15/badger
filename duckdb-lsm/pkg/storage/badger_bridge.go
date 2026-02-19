package storage

import (
	"github.com/dgraph-io/badger/v4/duckdb-lsm/pkg/types"
)

// ConvertDarshanEntry converts a Badger entry to storage Entry type
func ConvertDarshanEntry(key []byte, value []byte, version uint64) Entry {
	return Entry{
		Key:   key,
		Value: value,
		Timestamp: types.CustomTs{
			EpochID:    int64(version),
			BrokerID:   0,
			AssignedTs: 0,
		},
	}
}