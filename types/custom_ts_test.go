/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package types

import (
	"math"
	"testing"
)

// TestToUint64Roundtrip pins down the boundary of CustomTs.ToUint64's 16-bit
// EpochID/BrokerID encoding. Values at or below math.MaxUint16 must survive a
// ToUint64 -> CustomTsFromUint64 roundtrip exactly and must report
// CanRoundtripUint64() == true. Values above the boundary must be flagged by
// CanRoundtripUint64() == false so callers can reject them instead of
// silently losing the high bits.
func TestToUint64Roundtrip(t *testing.T) {
	cases := []struct {
		name        string
		ts          CustomTs
		canRoundtrip bool
	}{
		{"zero", CustomTs{0, 0, 0}, true},
		{"small values", CustomTs{1, 2, 3}, true},
		{"epoch at boundary", CustomTs{math.MaxUint16, 0, 0}, true},
		{"broker at boundary", CustomTs{0, math.MaxUint16, 0}, true},
		{"assigned ts full 32 bits", CustomTs{0, 0, math.MaxUint32}, true},
		{"epoch one above boundary", CustomTs{math.MaxUint16 + 1, 0, 0}, false},
		{"broker one above boundary", CustomTs{0, math.MaxUint16 + 1, 0}, false},
		{"both epoch and broker too large", CustomTs{math.MaxUint32, math.MaxUint32, 0}, false},
		{"max ts overall", MaxTs, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.ts.CanRoundtripUint64()
			if got != tc.canRoundtrip {
				t.Fatalf("CanRoundtripUint64() = %v, want %v for %+v", got, tc.canRoundtrip, tc.ts)
			}
			if !tc.canRoundtrip {
				// Not asserting a specific corrupted value here -- the whole point
				// is that ToUint64() is unsafe in this regime. We only assert that
				// callers relying on CanRoundtripUint64() would correctly detect it.
				return
			}
			packed := tc.ts.ToUint64()
			roundtripped := CustomTsFromUint64(packed)
			if roundtripped != tc.ts {
				t.Fatalf("roundtrip mismatch: got %+v, want %+v (packed=%d)",
					roundtripped, tc.ts, packed)
			}
		})
	}
}

// TestToUint64SilentTruncation documents (rather than "fixes" at the encoding
// level) that ToUint64() itself still silently drops high bits when called
// directly on out-of-range values -- CanRoundtripUint64() is a guard callers
// must check themselves before calling ToUint64(), not a change to
// ToUint64()'s own behavior (which several call sites depend on staying a
// pure, non-error-returning uint64 conversion for fixed test values).
func TestToUint64SilentTruncation(t *testing.T) {
	ts := CustomTs{EpochID: math.MaxUint16 + 1, BrokerID: 0, AssignedTs: 42}
	if ts.CanRoundtripUint64() {
		t.Fatalf("expected CanRoundtripUint64() == false for EpochID > MaxUint16")
	}
	packed := ts.ToUint64()
	roundtripped := CustomTsFromUint64(packed)
	if roundtripped == ts {
		t.Fatalf("expected roundtrip to NOT match for out-of-range EpochID " +
			"(if this now passes, ToUint64's encoding width changed and this " +
			"test plus the CanRoundtripUint64 boundary should be updated together)")
	}
}
