package pkg

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestGetsTheValueOfANonExistingKey(t *testing.T) {
	db := New()
	_ = db.View(func(txn *Txn) error {
		_, exists := txn.Get([]byte("non-existing"))
		assert.Equal(t, false, exists)
		return nil
	})
}

func TestGetsTheValueOfAnExistingKey(t *testing.T) {
	db := New()
	err := db.Update(func(txn *Txn) error {
		_ = txn.Set([]byte("HDD"), []byte("Hard disk"))
		return nil
	})
	assert.Nil(t, err)

	err = db.Update(func(txn *Txn) error {
		_ = txn.Set([]byte("HDD"), []byte("Hard disk drive"))
		return nil
	})
	assert.Nil(t, err)

	_ = db.View(func(txn *Txn) error {
		value, exists := txn.Get([]byte("HDD"))
		assert.Equal(t, true, exists)
		fmt.Println(string(value.Slice()))
		assert.Equal(t, []byte("Hard disk drive"), value.Slice())
		return nil
	})
}

func TestPutsMultipleKeyValuesInATransaction(t *testing.T) {
	db := New()
	err := db.Update(func(txn *Txn) error {
		for count := 1; count <= 100; count++ {
			_ = txn.Set([]byte("Key:"+strconv.Itoa(count)), []byte("Value:"+strconv.Itoa(count)))
		}
		return nil
	})
	assert.Nil(t, err)

	err = db.Update(func(txn *Txn) error {
		for count := 1; count <= 100; count++ {
			_ = txn.Set([]byte("Key:"+strconv.Itoa(count)), []byte("Value#"+strconv.Itoa(count)))
		}
		return nil
	})
	assert.Nil(t, err)

	_ = db.View(func(txn *Txn) error {
		for count := 1; count <= 100; count++ {
			value, exists := txn.Get([]byte("Key:" + strconv.Itoa(count)))
			assert.Equal(t, true, exists)
			assert.Equal(t, []byte("Value#"+strconv.Itoa(count)), value.Slice())
		}
		return nil
	})
}

func TestInvolvesConflictingTransactions(t *testing.T) {
	db := New()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		err := db.Update(func(txn *Txn) error {
			delayCommit := func() {
				time.Sleep(25 * time.Millisecond)
			}
			_, _ = txn.Get([]byte("HDD"))
			_ = txn.Set([]byte("SSD"), []byte("Solid state drive"))
			delayCommit()
			return nil
		})
		assert.Error(t, err)
		assert.Equal(t, TxnConflictErr, err)
	}()

	go func() {
		defer wg.Done()
		err := db.Update(func(transaction *Txn) error {
			delayCommit := func() {
				time.Sleep(10 * time.Millisecond)
			}
			_ = transaction.Set([]byte("HDD"), []byte("Hard disk"))
			delayCommit()
			return nil
		})
		assert.Nil(t, err)
	}()
	wg.Wait()
}
