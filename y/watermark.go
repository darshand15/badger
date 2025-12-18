/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package y

import (
	"container/heap"
	"context"
	"sync/atomic"

	"github.com/dgraph-io/ristretto/v2/z"
)

// type uint64Heap []uint64
type tsHeap []CustomTs

func (u tsHeap) Len() int            { return len(u) }
func (u tsHeap) Less(i, j int) bool  { return u[i].Less(u[j]) }
func (u tsHeap) Swap(i, j int)       { u[i], u[j] = u[j], u[i] }
func (u *tsHeap) Push(x interface{}) { *u = append(*u, x.(CustomTs)) }
func (u *tsHeap) Pop() interface{} {
	old := *u
	n := len(old)
	x := old[n-1]
	*u = old[0 : n-1]
	return x
}

// mark contains one of more indices, along with a done boolean to indicate the
// status of the index: begin or done. It also contains waiters, who could be
// waiting for the watermark to reach >= a certain index.
type mark struct {
	// Either this is an (index, waiter) pair or (index, done) or (indices, done).
	index   CustomTs
	waiter  chan struct{}
	indices []CustomTs
	done    bool // Set to true if the index is done.
}

// WaterMark is used to keep track of the minimum un-finished index.  Typically, an index k becomes
// finished or "done" according to a WaterMark once Done(k) has been called
//  1. as many times as Begin(k) has, AND
//  2. a positive number of times.
//
// An index may also become "done" by calling SetDoneUntil at a time such that it is not
// inter-mingled with Begin/Done calls.
//
// Since doneUntil and lastIndex addresses are passed to sync/atomic packages, we ensure that they
// are 64-bit aligned by putting them at the beginning of the structure.
type WaterMark struct {
	doneUntil atomic.Pointer[CustomTs]
	lastIndex atomic.Pointer[CustomTs]
	Name      string
	markCh    chan mark
}

// Init initializes a WaterMark struct. MUST be called before using it.
func (w *WaterMark) Init(closer *z.Closer) {
	w.markCh = make(chan mark, 100)

	// Initialize pointers to Zero values so Load() doesn't return nil
	zero := CustomTs{}
	w.doneUntil.Store(&zero)
	w.lastIndex.Store(&zero)

	go w.process(closer)
}

// Begin sets the last index to the given value.
func (w *WaterMark) Begin(index CustomTs) {
	// Store a copy of index on the heap for the atomic pointer
	val := index
	w.lastIndex.Store(&val)
	w.markCh <- mark{index: index, done: false}
}

// BeginMany works like Begin but accepts multiple indices.
func (w *WaterMark) BeginMany(indices []CustomTs) {
	if len(indices) == 0 {
		return
	}
	// Store the last one
	val := indices[len(indices)-1]
	w.lastIndex.Store(&val)
	w.markCh <- mark{index: CustomTs{}, indices: indices, done: false}
}

// Done sets a single index as done.
func (w *WaterMark) Done(index CustomTs) {
	w.markCh <- mark{index: index, done: true}
}

// DoneMany works like Done but accepts multiple indices.
func (w *WaterMark) DoneMany(indices []CustomTs) {
	w.markCh <- mark{index: CustomTs{}, indices: indices, done: true}
}

// DoneUntil returns the maximum index that has the property that all indices
// less than or equal to it are done.
func (w *WaterMark) DoneUntil() CustomTs {
	val := w.doneUntil.Load()
	if val == nil {
		return CustomTs{}
	}
	return *val
}

// SetDoneUntil sets the maximum index that has the property that all indices
// less than or equal to it are done.
func (w *WaterMark) SetDoneUntil(val CustomTs) {
	w.doneUntil.Store(&val)
}

// LastIndex returns the last index for which Begin has been called.
func (w *WaterMark) LastIndex() CustomTs {
	val := w.lastIndex.Load()
	if val == nil {
		return CustomTs{}
	}
	return *val
}

// WaitForMark waits until the given index is marked as done.
func (w *WaterMark) WaitForMark(ctx context.Context, index CustomTs) error {
	// if w.DoneUntil() >= index
	if !index.Greater(w.DoneUntil()) {
		return nil
	}
	waitCh := make(chan struct{})
	w.markCh <- mark{index: index, waiter: waitCh}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitCh:
		return nil
	}
}

// process is used to process the Mark channel. This is not thread-safe,
// so only run one goroutine for process. One is sufficient, because
// all goroutine ops use purely memory and cpu.
// Each index has to emit atleast one begin watermark in serial order otherwise waiters
// can get blocked idefinitely. Example: We had an watermark at 100 and a waiter at 101,
// if no watermark is emitted at index 101 then waiter would get stuck indefinitely as it
// can't decide whether the task at 101 has decided not to emit watermark or it didn't get
// scheduled yet.
func (w *WaterMark) process(closer *z.Closer) {
	defer closer.Done()

	var indices tsHeap
	// pending maps raft proposal index to the number of pending mutations for this proposal.
	pending := make(map[CustomTs]int)
	waiters := make(map[CustomTs][]chan struct{})

	heap.Init(&indices)

	processOne := func(index CustomTs, done bool) {
		// If not already done, then set. Otherwise, don't undo a done entry.
		prev, present := pending[index]
		if !present {
			heap.Push(&indices, index)
		}

		delta := 1
		if done {
			delta = -1
		}
		pending[index] = prev + delta

		// Update mark by going through all indices in order; and checking if they have
		// been done. Stop at the first index, which isn't done.

		// Load the current pointer
		doneUntilPtr := w.doneUntil.Load()
		doneUntil := *doneUntilPtr

		if doneUntil.Greater(index) {
			AssertTruef(false, "Name: %s doneUntil: %v. Index: %v", w.Name, doneUntil, index)
		}

		until := doneUntil
		loops := 0

		for len(indices) > 0 {
			min := indices[0]
			if done := pending[min]; done > 0 {
				break // len(indices) will be > 0.
			}
			// Even if done is called multiple times causing it to become
			// negative, we should still pop the index.
			heap.Pop(&indices)
			delete(pending, min)
			until = min
			loops++
		}

		if until != doneUntil {
			// CompareAndSwap expects pointers. We allocate a new one for the new value.
			newPtr := &until
			AssertTrue(w.doneUntil.CompareAndSwap(doneUntilPtr, newPtr))
		}

		notifyAndRemove := func(idx CustomTs, toNotify []chan struct{}) {
			for _, ch := range toNotify {
				close(ch)
			}
			delete(waiters, idx) // Release the memory back.
		}

		// if until-doneUntil <= uint64(len(waiters)) {
		// 	// Issue #908 showed that if doneUntil is close to 2^60, while until is zero, this loop
		// 	// can hog up CPU just iterating over integers creating a busy-wait loop. So, only do
		// 	// this path if until - doneUntil is less than the number of waiters.
		// 	for idx := doneUntil + 1; idx <= until; idx++ {
		// 		if toNotify, ok := waiters[idx]; ok {
		// 			notifyAndRemove(idx, toNotify)
		// 		}
		// 	}
		// } else {
		// 	for idx, toNotify := range waiters {
		// 		if idx <= until {
		// 			notifyAndRemove(idx, toNotify)
		// 		}
		// 	}
		// } // end of notifying waiters.

		// CHANGED: The original code had an optimization loop for dense integers:
		// "if until-doneUntil <= len(waiters) { iterate i++ }"
		// With composite CustomTs, "until - doneUntil" is not a simple scalar,
		// and iterating every tick between them is impossible/inefficient.
		// We fallback strictly to iterating the waiters map.

		for idx, toNotify := range waiters {
			// if idx <= until
			if !idx.Greater(until) {
				notifyAndRemove(idx, toNotify)
			}
		}
	}

	for {
		select {
		case <-closer.HasBeenClosed():
			return
		case mark := <-w.markCh:
			if mark.waiter != nil {
				doneUntil := w.DoneUntil()
				// if doneUntil >= mark.index
				if !mark.index.Greater(doneUntil) {
					close(mark.waiter)
				} else {
					ws, ok := waiters[mark.index]
					if !ok {
						waiters[mark.index] = []chan struct{}{mark.waiter}
					} else {
						waiters[mark.index] = append(ws, mark.waiter)
					}
				}
			} else {
				// it is possible that mark.index is zero. We need to handle that case as well.
				if !mark.index.IsZero() || (mark.index.IsZero() && len(mark.indices) == 0) {
					processOne(mark.index, mark.done)
				}
				for _, index := range mark.indices {
					processOne(index, mark.done)
				}
			}
		}
	}
}
