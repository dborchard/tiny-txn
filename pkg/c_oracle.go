package pkg

import (
	"context"
	"sync"
)

type Oracle struct {
	sync.Mutex
	nextTs uint64

	readMark   *CommitWaiter
	commitMark *CommitWaiter

	readyToCommitTxns []ReadyToCommitTxn
}

func NewOracle() *Oracle {
	oracle := &Oracle{
		nextTs:     1,
		readMark:   NewCommitWaiter(),
		commitMark: NewCommitWaiter(),
	}

	oracle.readMark.Done(oracle.nextTs - 1)
	oracle.commitMark.Done(oracle.nextTs - 1)
	return oracle
}

func (o *Oracle) Stop() {
	o.readMark.Stop()
	o.commitMark.Stop()

}

func (o *Oracle) NewReadTs() uint64 {
	o.Lock()
	defer o.Unlock()

	beginTimestamp := o.nextTs - 1
	o.readMark.Begin(beginTimestamp)

	err := o.commitMark.WaitFor(context.Background(), beginTimestamp)
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

	commitTs := o.nextTs
	o.nextTs = o.nextTs + 1

	o.addReadyToCommitTxn(transaction, commitTs)
	o.commitMark.Begin(commitTs)
	return commitTs, nil
}

func (o *Oracle) DoneRead(transaction *Txn) {
	o.readMark.Done(transaction.snapshot.ts)
}

func (o *Oracle) DoneCommit(commitTs uint64) {
	o.commitMark.Done(commitTs)
}

func (o *Oracle) hasConflictFor(txn *Txn) bool {
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

func (o *Oracle) gcOldReadyToCommitTxns() {
	updatedReadyToCommitTxns := o.readyToCommitTxns[:0]
	lastCommittedTxnTs := o.readMark.DoneTill()

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

func (o *Oracle) addReadyToCommitTxn(txn *Txn, commitTs uint64) {
	o.readyToCommitTxns = append(o.readyToCommitTxns, ReadyToCommitTxn{
		commitTs: commitTs,
		txn:      txn,
	})
}
