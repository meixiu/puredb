package leveldb

import "github.com/syndtr/goleveldb/leveldb"

type Batch struct {
	db    *DB
	batch *leveldb.Batch
}

func (w *Batch) Put(key, value []byte) {
	w.batch.Put(key, value)
}

func (w *Batch) Delete(key []byte) {
	w.batch.Delete(key)
}

func (w *Batch) Commit() error {
	return w.db.rdb.Write(w.batch, nil)
}

func (w *Batch) Rollback() error {
	w.batch.Reset()
	return nil
}

func (w *Batch) Close() {
	w.batch.Reset()
}

func (w *Batch) Data() []byte {
	return w.batch.Dump()
}
