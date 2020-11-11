package driver

type Batch interface {
	Put(key, value []byte)
	Delete(key []byte)
	Commit() error
	Rollback() error
	Close()
	Data() []byte
}
