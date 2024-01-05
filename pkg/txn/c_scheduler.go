package txn

import (
	"context"
	"sync"
)

type Scheduler struct {
	sync.Mutex
	nextTs uint64

	readWaiter   *Waiter
	commitWaiter *Waiter

	readyToCommitTxns []ReadyToCommitTxn
}

func NewScheduler() *Scheduler {
	scheduler := &Scheduler{
		nextTs:       1,
		readWaiter:   NewWaiter(),
		commitWaiter: NewWaiter(),
	}

	scheduler.readWaiter.Done(scheduler.nextTs - 1)
	scheduler.commitWaiter.Done(scheduler.nextTs - 1)
	return scheduler
}

func (o *Scheduler) Stop() {
	o.readWaiter.Stop()
	o.commitWaiter.Stop()
}

func (o *Scheduler) NewReadTs() uint64 {
	o.Lock()
	defer o.Unlock()

	beginTimestamp := o.nextTs - 1
	o.readWaiter.Begin(beginTimestamp)

	err := o.commitWaiter.WaitFor(context.Background(), beginTimestamp)
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
	o.commitWaiter.Begin(commitTs)
	return commitTs, nil
}

func (o *Scheduler) DoneRead(transaction *Txn) {
	o.readWaiter.Done(transaction.snapshot.ts)
}

func (o *Scheduler) DoneCommit(commitTs uint64) {
	o.commitWaiter.Done(commitTs)
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
	lastCommittedTxnTs := o.readWaiter.DoneTill()

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
