package driver

type Iterator interface {
	Close()
	SeekTo(key []byte)
	SeekToFirst()
	SeekToLast()
	Valid() bool
	Next()
	Prev()
	Key() []byte
	Value() []byte
	Error() error
}
