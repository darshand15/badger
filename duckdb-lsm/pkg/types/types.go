package types

// CustomTs represents a custom timestamp with three components
type CustomTs struct {
	EpochID    int64
	BrokerID   int64
	AssignedTs int64
}

// Item represents a key-value pair with timestamp
type Item struct {
	Key       []byte
	Val       []byte
	Timestamp CustomTs
}

// Compare returns:
//  -1 if t < other
//   0 if t == other
//  +1 if t > other
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

// LessOrEqual returns true if t <= other
func (t CustomTs) LessOrEqual(other CustomTs) bool {
	return t.Compare(other) <= 0
}

// Less returns true if t < other
func (t CustomTs) Less(other CustomTs) bool {
	return t.Compare(other) < 0
}