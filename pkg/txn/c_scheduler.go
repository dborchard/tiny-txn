package txn

import (
	"context"
	"sync"
)

type Oracle struct {
	sync.Mutex
	nextTs uint64

	// `readTsMarker` marks the visibility(ts) of read operations in a `newTransaction` to other transactions.
	// Here we don't need WaterMark as such, as we are not using the `WaitFor` API. However,
	// we are using the `DoneTill` API to get the last completed readTs to remove the old readyToCommitTxns.
	readTsMarker *WaterMark
	// `commitVisibilityWaiter` blocks `newTransaction` to ensure previous commits are visible to new reads.
	commitVisibilityWaiter *WaterMark

	readyToCommitTxns []ReadyToCommitTxn
}

func NewScheduler() *Oracle {
	scheduler := &Oracle{
		nextTs:                 1,
		readTsMarker:           NewTsWaiter(),
		commitVisibilityWaiter: NewTsWaiter(),
	}

	scheduler.readTsMarker.Done(scheduler.nextTs - 1)
	scheduler.commitVisibilityWaiter.Done(scheduler.nextTs - 1)
	return scheduler
}

func (o *Oracle) Stop() {
	o.readTsMarker.Stop()
	o.commitVisibilityWaiter.Stop()
}

func (o *Oracle) NewReadTs() uint64 {
	o.Lock()
	defer o.Unlock()

	beginTimestamp := o.nextTs - 1
	o.readTsMarker.Begin(beginTimestamp)

	// Wait for all txns which have no conflicts, have been assigned a commit
	// timestamp and are going through the write to value log and LSM tree
	// process. Not waiting here could mean that some txns which have been
	// committed would not be read.
	err := o.commitVisibilityWaiter.WaitFor(context.Background(), beginTimestamp)
	if err != nil {
		panic(err)
	}
	return beginTimestamp
}

func (o *Oracle) NewCommitTs(transaction *Txn) (uint64, error) {
	o.Lock()
	defer o.Unlock()

	if o.hasConflictFor(transaction) {
		return 0, TxnConflictErr
	}

	o.DoneRead(transaction)
	o.gcOldReadyToCommitTxns()

	{
		// Start Checkpoint
		// Wal start checkpoint entry
		// Wal end checkpoint entry
		// End Checkpoint
	}

	commitTs := o.nextTs
	o.nextTs = o.nextTs + 1

	o.addReadyToCommitTxn(transaction, commitTs)
	o.commitVisibilityWaiter.Begin(commitTs)
	return commitTs, nil
}

func (o *Oracle) DoneRead(transaction *Txn) {
	o.readTsMarker.Done(transaction.snapshot.ts)
}

func (o *Oracle) DoneCommit(commitTs uint64) {
	o.commitVisibilityWaiter.Done(commitTs)
}

func (o *Oracle) hasConflictFor(txn *Txn) bool {
	currTxnBeginTs := txn.snapshot.ts

	for _, readyToCommitTxn := range o.readyToCommitTxns {
		if readyToCommitTxn.commitTs <= currTxnBeginTs {
			continue
		}

		for _, key := range txn.readSet {
			if readyToCommitTxn.txn.writeSet.Contains(key) {
				return true
			}
		}
	}
	return false
}

func (o *Oracle) gcOldReadyToCommitTxns() {
	updatedReadyToCommitTxns := o.readyToCommitTxns[:0]
	lastActiveReadTs := o.readTsMarker.DoneTill()

	for _, readyToCommitTxn := range o.readyToCommitTxns {
		if readyToCommitTxn.commitTs <= lastActiveReadTs {
			continue
		}
		updatedReadyToCommitTxns = append(updatedReadyToCommitTxns, readyToCommitTxn)
	}
	o.readyToCommitTxns = updatedReadyToCommitTxns
}

type ReadyToCommitTxn struct {
	commitTs uint64
	txn      *Txn
}

func (o *Oracle) addReadyToCommitTxn(txn *Txn, commitTs uint64) {
	o.readyToCommitTxns = append(o.readyToCommitTxns, ReadyToCommitTxn{
		commitTs: commitTs,
		txn:      txn,
	})
}
