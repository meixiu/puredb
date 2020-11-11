package config

type LevelDBConfig struct {
	BlockSize       int `json:"block_size" yaml:"block_size"`
	CacheSize       int `json:"cache_size" yaml:"cache_size"`
	WriteBufferSize int `json:"write_buffer_size" yaml:"write_buffer_size"`
	BloomFilterSize int `json:"bloom_filter_size" yaml:"bloom_filter_size"`
	MaxOpenFiles    int `json:"max_open_files" yaml:"max_open_files"`
}
