package storage

import (
	"fmt"
	"path"
	"runtime"
	"strconv"
	"sync"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"github.com/prabhatsharma/zinc/pkg/zutils"
)

type Storage struct {
	db   map[string]*StorageIndex
	lock sync.RWMutex
}

type StorageIndex struct {
	name string
	db   *badger.DB
	seq  *badger.Sequence
}

type StorageIndexBulk struct {
	index *StorageIndex
	txn   *badger.Txn
}

var Cli *Storage

func init() {
	Cli = new(Storage)
	Cli.db = make(map[string]*StorageIndex, 32)
}

func (t *Storage) Close() {
	for _, index := range t.db {
		index.Close()
	}
}

func (t *Storage) GetIndex(indexName string) (*StorageIndex, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	index, ok := t.db[indexName]
	if ok {
		return index, nil
	}

	dataPath := zutils.GetEnv("ZINC_DATA_PATH", "./data")
	opt := badger.DefaultOptions(path.Join(dataPath, "storage", indexName))
	opt.NumGoroutines = runtime.NumGoroutine()
	opt.Compression = options.ZSTD
	opt.BlockSize = 1024 * 128
	opt.MetricsEnabled = false
	opt.Logger = Logger
	db, err := badger.Open(opt)
	if err != nil {
		return nil, fmt.Errorf("storage.Storage.GetIndex: OpenDB err %v", err.Error())
	}
	index = &StorageIndex{name: indexName, db: db}
	index.seq, err = db.GetSequence([]byte("id"), 1000)
	if err != nil {
		return nil, fmt.Errorf("storage.Storage.GetIndex: GetSequence err %v", err.Error())
	}
	t.db[indexName] = index
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

func (t *StorageIndex) Close() {
	t.seq.Release()
	t.db.Close()
}

func (t *StorageIndex) GenerateID() (string, error) {
	id, err := t.seq.Next()
	if err != nil {
		return "", fmt.Errorf("storage.StorageIndex.GenerateID: err %v", err.Error())
	}
	return strconv.FormatUint(id, 10), nil
}

func (t *StorageIndex) Set(key string, value []byte) error {
	err := t.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), value)
	})
	if err != nil {
		return fmt.Errorf("storage.StorageIndex.Append: key[%s] err %v", key, err.Error())
	}
	return nil
}

func (t *StorageIndex) Get(key string) ([]byte, error) {
	var valCopy []byte
	err := t.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		valCopy, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("storage.StorageIndex.Read: key[%s] err %v", key, err.Error())
	}
	return valCopy, nil
}

func (t *StorageIndex) Gets(keys []string) (map[string][]byte, error) {
	result := make(map[string][]byte, len(keys))
	err := t.db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			item, err := txn.Get([]byte(key))
			if err != nil {
				return err
			}
			valCopy, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			result[key] = valCopy
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("storage.StorageIndex.Reads: err %v", err.Error())
	}
	return result, nil
}

func (t *StorageIndex) Delete(key string) error {
	err := t.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
	if err != nil {
		return fmt.Errorf("storage.StorageIndex.Delete: key[%s] err %v", key, err.Error())
	}
	return nil
}

func (t *StorageIndex) Bulk(update bool) *StorageIndexBulk {
	return &StorageIndexBulk{index: t, txn: t.db.NewTransaction(update)}
}

func (t *StorageIndexBulk) Set(key string, value []byte) error {
	err := t.txn.Set([]byte(key), value)
	if err == nil {
		return nil
	}
	if err == badger.ErrTxnTooBig {
		if err = t.txn.Commit(); err != nil {
			return fmt.Errorf("storage.StorageIndexBulk.Append: transaction.Commit err %v", err.Error())
		}
		t.txn = t.index.db.NewTransaction(true)
		if err := t.txn.Set([]byte(key), value); err != nil {
			return fmt.Errorf("storage.StorageIndexBulk.Append: key[%s] err %v", key, err.Error())
		}
	}
	return fmt.Errorf("storage.StorageIndexBulk.Append: key[%s] err %v", key, err.Error())
}

func (t *StorageIndexBulk) Commit() error {
	if err := t.txn.Commit(); err != nil {
		return fmt.Errorf("storage.StorageIndexBulk.Commit: err %v", err.Error())
	}
	return nil
}
