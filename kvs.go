package kvs

import (
	"bytes"
	"encoding/gob"
	"errors"
	"time"

	"github.com/boltdb/bolt"
)

type KVStore struct {
	db *bolt.DB
}

var (
	ErrNotFound = errors.New("kvs: key not found")
	ErrBadValue = errors.New("kvs: bad value")
	bucketName  = []byte("kvs")
)

// Open a Key-Value Store. Create it if it doesn't exist.
// Path = full path, with all leading directories already existing.
// Can only be used by one process at a time.
func Open(path string) (*KVStore, error) {
	opts := &bolt.Options{
		Timeout: 50 * time.Millisecond,
	}
	if db, err := bolt.Open(path, 0640, opts); err != nil {
		return nil, err
	} else {
		err := db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists(bucketName)
			return err
		})
		if err != nil {
			return nil, err
		} else {
			return &KVStore{db: db}, nil
		}
	}
}

// Puts an entry into the Key-Value Store. It is gob-encoded.
// Nil values are not allowed (empty strings valid)
func (kvs *KVStore) Put(key string, value interface{}) error {
	if value == nil {
		return ErrBadValue
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return err
	}
	return kvs.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Put([]byte(key), buf.Bytes())
	})
}

// Return an entry from the Key-Value Store
// Value must be pointer-typed.
// No matching values returns ErrNotFound
func (kvs *KVStore) Get(key string, value interface{}) error {
	return kvs.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(bucketName).Cursor()
		if k, v := cursor.Seek([]byte(key)); k == nil || string(k) != key {
			return ErrNotFound
		} else if value == nil {
			return nil
		} else {
			decoder := gob.NewDecoder(bytes.NewReader(v))
			return decoder.Decode(value)
		}
	})
}

// Delete a key from the Key-Value Store.
// Returns ErrNotFound like Get.
func (kvs *KVStore) Delete(key string) error {
	return kvs.db.Update(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(bucketName).Cursor()
		if k, _ := cursor.Seek([]byte(key)); k == nil || string(k) != key {
			return ErrNotFound
		} else {
			return cursor.Delete()
		}
	})
}

func (kvs *KVStore) Close() error {
	return kvs.db.Close()
}
