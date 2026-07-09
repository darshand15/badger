package types

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
)

const TsSize = 12

type CustomTs struct {
	EpochID    uint32
	BrokerID   uint32
	AssignedTs uint32
}

// MaxTs acts as the "Infinity" timestamp for iterators
var MaxTs = CustomTs{
	EpochID:    math.MaxUint32,
	BrokerID:   math.MaxUint32,
	AssignedTs: math.MaxUint32,
}

// ToBytes encodes the struct into a 12-byte slice with bitwise inversion
func (t CustomTs) ToBytes() []byte {
	buf := make([]byte, TsSize)

	// Encode fields in significance order (Epoch > Broker > AssignedTS)
	binary.BigEndian.PutUint32(buf[0:4], t.EpochID)
	binary.BigEndian.PutUint32(buf[4:8], t.BrokerID)
	binary.BigEndian.PutUint32(buf[8:12], t.AssignedTs)

	// Invert bits for descending sort order in LSM tree
	for i := range buf {
		buf[i] = ^buf[i]
	}
	return buf
}

// ParseTsFromBytes decodes the 12 bytes back into the struct
func ParseTsFromBytes(buf []byte) CustomTs {
	raw := make([]byte, TsSize)
	// Invert bits back
	for i, b := range buf {
		raw[i] = ^b
	}

	return CustomTs{
		EpochID:    binary.BigEndian.Uint32(raw[0:4]),
		BrokerID:   binary.BigEndian.Uint32(raw[4:8]),
		AssignedTs: binary.BigEndian.Uint32(raw[8:12]),
	}
}

func (t CustomTs) Less(o CustomTs) bool {
	if t.EpochID != o.EpochID {
		return t.EpochID < o.EpochID
	}
	if t.BrokerID != o.BrokerID {
		return t.BrokerID < o.BrokerID
	}
	return t.AssignedTs < o.AssignedTs
}

func (t CustomTs) Greater(o CustomTs) bool {
	return o.Less(t)
}

func (t CustomTs) Equal(o CustomTs) bool {
	return t.EpochID == o.EpochID && t.BrokerID == o.BrokerID && t.AssignedTs == o.AssignedTs
}

func (t CustomTs) IsZero() bool {
	return t.EpochID == 0 && t.BrokerID == 0 && t.AssignedTs == 0
}

// Incr (equivalent to ++)
// It increments AssignedTs. If that overflows, it increments BrokerID, then EpochID.
func (t CustomTs) Incr() CustomTs {
	t.AssignedTs++
	if t.AssignedTs == 0 { // Overflow wrapped to 0
		t.BrokerID++
		if t.BrokerID == 0 { // Overflow wrapped to 0
			t.EpochID++
		}
	}
	return t
}

// Decr (equivalent to -1)
// It decrements AssignedTs. If that underflows, it borrows from BrokerID, then EpochID.
func (t CustomTs) Decr() CustomTs {
	if t.AssignedTs == 0 {
		t.AssignedTs = math.MaxUint32 // Wrap to max
		if t.BrokerID == 0 {
			t.BrokerID = math.MaxUint32 // Wrap to max
			t.EpochID--                 // Underflow on EpochID is allowed (wraps to MaxUint32) just like uint64
		} else {
			t.BrokerID--
		}
	} else {
		t.AssignedTs--
	}
	return t
}

// Avg calculates the midpoint between t and other: (t + other) / 2
func (t CustomTs) Avg(other CustomTs) CustomTs {
	// Sum lowest parts (AssignedTs)
	// Cast to uint64 to prevent overflow during addition
	sumTS := uint64(t.AssignedTs) + uint64(other.AssignedTs)
	carryTS := sumTS >> 32 // Get the carry (0 or 1)

	// Sum middle parts (BrokerID) including carry
	sumBroker := uint64(t.BrokerID) + uint64(other.BrokerID) + carryTS
	carryBroker := sumBroker >> 32 // Get the carry

	// Sum highest parts (EpochID) including carry
	sumEpoch := uint64(t.EpochID) + uint64(other.EpochID) + carryBroker

	// Divide total sum by 2 (Bitwise Right Shift 1)
	// We propagate the remainder (lowest bit) of each upper field
	// to the highest bit of the next lower field.

	avgEpoch := uint32(sumEpoch >> 1)
	remEpoch := uint32(sumEpoch & 1) // Remainder from Epoch division

	// Shift Broker, OR in the bit from Epoch
	avgBroker := (uint32(sumBroker) >> 1) | (remEpoch << 31)
	remBroker := uint32(sumBroker & 1) // Remainder from Broker division

	// Shift AssignedTs, OR in the bit from Broker
	avgTS := (uint32(sumTS) >> 1) | (remBroker << 31)

	return CustomTs{
		EpochID:    avgEpoch,
		BrokerID:   avgBroker,
		AssignedTs: avgTS,
	}
}

// String returns a readable representation of the timestamp (e.g., "1-5-100")
func (t CustomTs) String() string {
	return fmt.Sprintf("%d-%d-%d", t.EpochID, t.BrokerID, t.AssignedTs)
}

// ParseCustomTsString parses a string in "Epoch-Broker-TS" format.
func ParseCustomTsString(s string) (CustomTs, error) {
	parts := strings.Split(s, "-")
	if len(parts) != 3 {
		return CustomTs{}, fmt.Errorf("invalid custom timestamp format: %s", s)
	}

	e, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return CustomTs{}, err
	}

	b, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return CustomTs{}, err
	}

	ts, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil {
		return CustomTs{}, err
	}

	return CustomTs{
		EpochID:    uint32(e),
		BrokerID:   uint32(b),
		AssignedTs: uint32(ts),
	}, nil
}

// ToUint64 packs CustomTs into a single uint64 for protobuf varint serialization.
// Encoding: EpochID[63:48] | BrokerID[47:32] | AssignedTs[31:0].
// Supports EpochID/BrokerID 0-65535 and AssignedTs 0-4294967295.
func (t CustomTs) ToUint64() uint64 {
	return uint64(t.EpochID)<<48 | uint64(t.BrokerID)<<32 | uint64(t.AssignedTs)
}

// CustomTsFromUint64 is the inverse of ToUint64.
func CustomTsFromUint64(v uint64) CustomTs {
	return CustomTs{
		EpochID:    uint32(v >> 48),
		BrokerID:   uint32((v >> 32) & 0xFFFF),
		AssignedTs: uint32(v & 0xFFFFFFFF),
	}
}
