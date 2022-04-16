package storage

import (
	"fmt"
	"path"

	"github.com/cockroachdb/pebble"
	"github.com/prabhatsharma/zinc/pkg/zutils"
)

type pebbleStorage struct {
	db *pebble.DB
}

func NewPebble(indexName string) (Storager, error) {
	db, err := openPebbleDB(indexName)
	if err != nil {
		return nil, fmt.Errorf("open pebble db err %v", err.Error())
	}
	return &pebbleStorage{db: db}, nil
}

func openPebbleDB(indexName string) (*pebble.DB, error) {
	dataPath := zutils.GetEnv("ZINC_DATA_PATH", "./data")
	opt := &pebble.Options{}
	return pebble.Open(path.Join(dataPath, "storage", indexName), opt)
}

func (t *pebbleStorage) Set(key string, value []byte) error {
	if err := t.db.Set([]byte(key), value, pebble.NoSync); err != nil {
		return fmt.Errorf("storage.pebble.Set: key[%s] err %v", key, err.Error())
	}
	return nil
}

func (t *pebbleStorage) Get(key string) ([]byte, error) {
	item, closer, err := t.db.Get([]byte(key))
	if err != nil {
		return nil, fmt.Errorf("storage.pebble.Get: key[%s] err %v", key, err.Error())
	}
	valCopy := make([]byte, len(item))
	copy(valCopy, item)
	closer.Close()
	return valCopy, nil
}

func (t *pebbleStorage) Gets(keys []string) (map[string][]byte, error) {
	result := make(map[string][]byte, len(keys))
	for _, key := range keys {
		if val, err := t.Get(key); err != nil {
			return nil, err
		} else {
			result[key] = val
		}
	}
	return result, nil
}

func (t *pebbleStorage) Delete(key string) error {
	if err := t.db.Delete([]byte(key), pebble.Sync); err != nil {
		return fmt.Errorf("storage.pebble.Delete: key[%s] err %v", key, err.Error())
	}
	return nil
}

func (t *pebbleStorage) Bulk(update bool) StorageBulker {
	return nil
}

func (t *pebbleStorage) Close() {
	t.db.Close()
}
