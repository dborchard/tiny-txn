package txn

import (
	"context"
	"sync"
)

type Scheduler struct {
	sync.Mutex
	nextTs uint64

	// `readTsMarker` marks the visibility(ts) of read operations in a `newTransaction` to other transactions.
	// Here we don't need TsWaiter as such, as we are not using the `WaitFor` API. However,
	// we are using the `DoneTill` API to get the last completed readTs to remove the old readyToCommitTxns.
	readTsMarker *TsWaiter
	// `commitVisibilityWaiter` blocks `newTransaction` to ensure previous commits are visible to new reads.
	commitVisibilityWaiter *TsWaiter

	readyToCommitTxns []ReadyToCommitTxn
}

func NewScheduler() *Scheduler {
	scheduler := &Scheduler{
		nextTs:                 1,
		readTsMarker:           NewTsWaiter(),
		commitVisibilityWaiter: NewTsWaiter(),
	}

	scheduler.readTsMarker.Done(scheduler.nextTs - 1)
	scheduler.commitVisibilityWaiter.Done(scheduler.nextTs - 1)
	return scheduler
}

func (o *Scheduler) Stop() {
	o.readTsMarker.Stop()
	o.commitVisibilityWaiter.Stop()
}

func (o *Scheduler) NewReadTs() uint64 {
	o.Lock()
	defer o.Unlock()

	beginTimestamp := o.nextTs - 1
	o.readTsMarker.Begin(beginTimestamp)

	err := o.commitVisibilityWaiter.WaitFor(context.Background(), beginTimestamp)
	if err != nil {
		panic(err)
	}
	return beginTimestamp
}

func (o *Scheduler) NewCommitTs(transaction *Txn) (uint64, error) {
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

func (o *Scheduler) DoneRead(transaction *Txn) {
	o.readTsMarker.Done(transaction.snapshot.ts)
}

func (o *Scheduler) DoneCommit(commitTs uint64) {
	o.commitVisibilityWaiter.Done(commitTs)
}

func (o *Scheduler) hasConflictFor(txn *Txn) bool {
	for _, readyToCommitTxn := range o.readyToCommitTxns {
		txnBeginTs := txn.snapshot.ts
		if readyToCommitTxn.commitTs <= txnBeginTs {
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

func (o *Scheduler) gcOldReadyToCommitTxns() {
	updatedReadyToCommitTxns := o.readyToCommitTxns[:0]
	lastCommittedTxnTs := o.readTsMarker.DoneTill()

	for _, readyToCommitTxn := range o.readyToCommitTxns {
		if readyToCommitTxn.commitTs <= lastCommittedTxnTs {
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

func (o *Scheduler) addReadyToCommitTxn(txn *Txn, commitTs uint64) {
	o.readyToCommitTxns = append(o.readyToCommitTxns, ReadyToCommitTxn{
		commitTs: commitTs,
		txn:      txn,
	})
}
