package types

import "fmt"

type CustomTs struct {
	EpochID    int64
	BrokerID   int64
	AssignedTs int64
}

func (t CustomTs) Compare(other CustomTs) int {
	if t.EpochID != other.EpochID {
		if t.EpochID < other.EpochID {
			return -1
		}
		return 1
	}
	if t.BrokerID != other.BrokerID {
		if t.BrokerID < other.BrokerID {
			return -1
		}
		return 1
	}
	if t.AssignedTs != other.AssignedTs {
		if t.AssignedTs < other.AssignedTs {
			return -1
		}
		return 1
	}
	return 0
}

func (t CustomTs) LessOrEqual(other CustomTs) bool {
	return t.Compare(other) <= 0
}

func (t CustomTs) String() string {
	return fmt.Sprintf("(%d,%d,%d)", t.EpochID, t.BrokerID, t.AssignedTs)
}

type BadgerEntry struct {
	Key       []byte
	Value     []byte
	Timestamp CustomTs
	Meta      byte
	UserMeta  byte
	ExpiresAt uint64
}

type Item struct {
	key       []byte
	value     []byte
	timestamp CustomTs
}

func NewItem(key, value []byte, ts CustomTs) *Item {
	return &Item{key: key, value: value, timestamp: ts}
}

func (i *Item) Key() []byte         { return i.key }
func (i *Item) Value() []byte       { return i.value }
func (i *Item) Timestamp() CustomTs { return i.timestamp }
