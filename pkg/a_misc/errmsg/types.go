package errmsg

import "errors"

var (
	NotExist   = errors.New("not exist")
	KeyIsEmpty = errors.New("key is empty")

	TransactionConflict = errors.New("transaction conflict")
	ReadOnlyTransaction = errors.New("read-only transaction")
)
