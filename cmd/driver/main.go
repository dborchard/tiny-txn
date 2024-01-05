package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
	"tiny_txn/pkg/db"
	"tiny_txn/pkg/txn"
)

func main() {
	database := db.New()

	// Test 1:  Normal Read and Write
	err := database.Update(func(txn *txn.Txn) error {
		_ = txn.Set([]byte("HDD"), []byte("Hard disk"))
		return nil
	})
	if err != nil {
		panic(err)
	}

	err = database.Update(func(txn *txn.Txn) error {
		_ = txn.Set([]byte("HDD"), []byte("Hard disk drive"))
		return nil
	})
	if err != nil {
		panic(err)
	}

	_ = database.View(func(txn *txn.Txn) error {
		value, exists := txn.Get([]byte("HDD"))

		fmt.Println(exists)
		fmt.Println(string(value.Slice()))

		return nil
	})

	// Test 2: Conflict
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		err := database.Update(func(txn *txn.Txn) error {
			delayCommit := func() {
				time.Sleep(25 * time.Millisecond)
			}
			_, _ = txn.Get([]byte("HDD"))
			_ = txn.Set([]byte("SSD"), []byte("Solid state drive"))
			delayCommit()
			return nil
		})
		if err == nil {
			panic("error should not be nil")
		}
		if err != nil && !errors.Is(err, txn.TxnConflictErr) {
			panic(err)
		}
	}()

	go func() {
		defer wg.Done()
		err := database.Update(func(transaction *txn.Txn) error {
			delayCommit := func() {
				time.Sleep(10 * time.Millisecond)
			}
			_ = transaction.Set([]byte("HDD"), []byte("Hard disk"))
			delayCommit()
			return nil
		})
		if err != nil {
			panic(err)
		}
	}()
	wg.Wait()

	_ = database.View(func(txn *txn.Txn) error {
		value, exists := txn.Get([]byte("HDD"))

		fmt.Println(exists)
		fmt.Println(string(value.Slice()))

		return nil
	})
}
