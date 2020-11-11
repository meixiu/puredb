package store

import (
	"sync"

	"github.com/meixiu/puredb/driver"
	"github.com/meixiu/puredb/purext/logger"
)

type Batch struct {
	batch  driver.Batch
	locker sync.Locker
}

func newBatch(b driver.Batch) *Batch {
	return &Batch{b, &sync.Mutex{}}
}

func (b *Batch) Put(key, value []byte) {
	b.batch.Put(key, value)
}

func (b *Batch) Delete(keys ...[]byte) {
	for _, key := range keys {
		b.batch.Delete(key)
	}
}

func (b *Batch) Begin() *Batch {
	b.locker.Lock()
	return b
}

func (b *Batch) Release() error {
	defer b.locker.Unlock()
	err := b.batch.Rollback()
	return err
}

func (b *Batch) Commit() error {
	if err := b.batch.Commit(); err != nil {
		logger.Error("commit:", err)
		return err
	}
	return nil
}

func (b *Batch) Data() []byte {
	return b.batch.Data()
}
