package driver

type Snapshot interface {
	Close()
	NewIterator() Iterator
	Get(key []byte) ([]byte, error)
}
