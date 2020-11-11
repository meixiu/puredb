package store

import "fmt"

// Exists check data exists or not.
func (s *Store) Exists(key []byte) (bool, error) {
	meta, err := s.getMeta(key)
	if err != nil {
		return false, err
	}
	return meta.Exists(), nil
}

func (s *Store) Del(keys ...[]byte) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	b := s.batch.Begin()
	defer b.Release()
	var n int64
	for _, key := range keys {
		meta, _ := s.getMeta(key)
		fmt.Println(meta)
		if !meta.Exists() {
			continue
		}
		fmt.Println("**", meta)
		var err error
		switch meta.Code {
		case StringCode:
			err = s.kvClear(b, key)
		case HashCode:
			err = s.hClear(b, key)
		case ListCode:
			err = s.lClear(b, key)
		case ZSetCode:
			err = s.zClear(b, key)
		default:
			err = ErrMetaType
		}
		if err != nil {
			fmt.Println(err)
			return 0, err
		}
		// delete meta
		s.delMeta(b, key)
		n++
	}
	err := b.Commit()
	return n, err
}
