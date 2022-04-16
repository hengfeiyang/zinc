package storage

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/dgraph-io/badger/v3"
)

const (
	DBEngineBadger = "badger"
	DBEnginePebble = "pebble"
)

type Storager interface {
	Set(key string, value []byte) error
	Get(key string) ([]byte, error)
	Gets(keys []string) (map[string][]byte, error)
	Delete(key string) error
	Bulk(update bool) StorageBulker
	Close()
}

type StorageBulker interface {
	Set(key string, value []byte) error
	Delete(key string) error
	Commit() error
}

type Storage struct {
	idGenerator *badger.Sequence
	db          map[string]Storager
	lock        sync.RWMutex
}

var Cli *Storage

func init() {
	Cli = new(Storage)
	Cli.db = make(map[string]Storager, 32)

	// idb, _ := openBadgerDB("_id")
	// Cli.idGenerator, _ = idb.GetSequence([]byte("id"), 1000)
}

func (t *Storage) GetIndex(indexName string, dbEngine string) (Storager, error) {
	t.lock.Lock()
	index, ok := t.db[indexName]
	t.lock.Unlock()
	if ok {
		return index, nil
	}

	var err error
	switch dbEngine {
	case DBEngineBadger:
		index, err = NewBadger(indexName)
	case DBEnginePebble:
		index, err = NewPebble(indexName)
	default:
		index, err = NewBadger(indexName)
	}
	if err != nil {
		return nil, fmt.Errorf("storage.GetIndex: create index err %v", err)
	}

	t.lock.Lock()
	t.db[indexName] = index
	t.lock.Unlock()
	return index, nil
}

func (t *Storage) DeleteIndex(indexName string) {
	t.lock.Lock()
	index, ok := t.db[indexName]
	delete(t.db, indexName)
	t.lock.Unlock()
	if ok {
		index.Close()
	}
}

func (t *Storage) GenerateID() (string, error) {
	id, err := t.idGenerator.Next()
	if err != nil {
		return "", fmt.Errorf("storage.GenerateID: err %v", err.Error())
	}
	return strconv.FormatUint(id, 10), nil
}

func (t *Storage) Close() {
	_ = t.idGenerator.Release()
	for _, index := range t.db {
		index.Close()
	}
}
