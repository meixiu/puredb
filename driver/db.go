package driver

type DB interface {
	Close()
	Clear() error
	NewBatch() Batch
	NewIterator() Iterator
	NewRangeIterator(start, end []byte) Iterator
	NewSnapshot() Snapshot
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Has(key []byte) (bool, error)
	Delete(key []byte) error
	Compact() error
	Stats() string
}
