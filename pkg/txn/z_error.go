package txn

import "errors"

var DbAlreadyStoppedErr = errors.New("db is stopped, can not perform the operation")
var ReadOnlyTxnErr = errors.New("txn is read-only, can not perform the operation")
var EmptyTxnErr = errors.New("txn is empty, can not perform the operation")
var BatchDuplicateKeyErr = errors.New("batch already contains the key")
var TxnConflictErr = errors.New("txn has conflict, can not commit")
