package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/tecbot/gorocksdb"
)

var vsize = flag.Int64("vsize", 1<<10, "value size")
var totalSize = flag.Int64("totalSize", 16*(1<<30), "total value size")

type Storage interface {
	Set(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	NewIterator() Iterator
	Close()

	// 删除
	DeleteRange(start []byte, end []byte) error
}

type Iterator interface {
	Seek(prefix []byte)
	Next() bool

	Key() []byte
	Value() []byte
	Close()
}

type Badger struct {
	db *badger.DB
}

func NewBadger(name string) (*Badger, error) {
	opts := badger.DefaultOptions
	opts.Dir = name
	opts.ValueDir = name

	// opts.SyncWrites = true
	// opts.ValueLogLoadingMode = options.FileIO

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	badger := &Badger{
		db: db,
	}

	return badger, err
}

func (b *Badger) Close() {
	b.db.Close()
}

func (b *Badger) Set(key []byte, value []byte) error {
	txn := b.db.NewTransaction(true)
	err := txn.Set(key, value)
	if err != nil {
		return err
	}
	err = txn.Commit(nil)

	return err
}

func (b *Badger) Get(key []byte) ([]byte, error) {
	txn := b.db.NewTransaction(false)
	item, err := txn.Get(key)
	if err != nil {
		return nil, err
	}

	// copy
	value, err := item.ValueCopy(nil)
	return value, err
}

func (b *Badger) Delete(key []byte) error {
	txn := b.db.NewTransaction(true)
	err := txn.Delete(key)
	if err != nil {
		return err
	}
	err = txn.Commit(nil)

	return err

}

type BadgerIterator struct {
	txn  *badger.Txn
	iter *badger.Iterator
}

func (iter *BadgerIterator) Seek(prefix []byte) {
	iter.iter.Seek(prefix)
}

func (iter *BadgerIterator) Next() bool {
	iter.iter.Next()
	return iter.iter.Valid()
}

func (iter *BadgerIterator) Key() []byte {
	return iter.iter.Item().Key()
}

func (iter *BadgerIterator) Value() []byte {
	value, err := iter.iter.Item().Value()
	if err != nil {
		panic(err)
	}
	return value
}

func (iter *BadgerIterator) Close() {
	iter.iter.Close()
	iter.txn.Discard()
}

func (r *Badger) NewIterator() Iterator {
	txn := r.db.NewTransaction(false)
	opts := badger.DefaultIteratorOptions
	iter := txn.NewIterator(opts)

	return &BadgerIterator{
		txn:  txn,
		iter: iter,
	}
}

func (r *Badger) DeleteRange(start []byte, end []byte) error {
	panic("no")
}

type AppendDB struct {
	db *gorocksdb.DB
	wo *gorocksdb.WriteOptions
	ro *gorocksdb.ReadOptions

	appendLock     sync.Mutex
	append         *os.File
	appendFileName string
	size           int64
}

func NewAppendDB(name string) (*AppendDB, error) {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)

	opts.SetCompression(gorocksdb.NoCompression)

	db, err := gorocksdb.OpenDb(opts, name)
	if err != nil {
		return nil, err
	}

	wo := gorocksdb.NewDefaultWriteOptions()
	wo.SetSync(false)

	appendFileName := path.Join(name, "append.mylog")
	append, err := os.OpenFile(appendFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	adb := &AppendDB{
		db:             db,
		wo:             wo,
		ro:             gorocksdb.NewDefaultReadOptions(),
		append:         append,
		appendFileName: appendFileName,
	}

	stat, err := append.Stat()
	if err != nil {
		return nil, err
	}
	adb.size = stat.Size()

	return adb, err
}

func (a *AppendDB) Set(key []byte, value []byte) (err error) {
	a.appendLock.Lock()
	offset := a.size
	_, err = a.append.Write(value)
	if err != nil {
		a.appendLock.Unlock()
		return
	}
	a.size += int64(len(value))
	a.appendLock.Unlock()

	meta := make([]byte, 16)
	var metalen int
	n := binary.PutVarint(meta, offset)
	metalen += n
	n = binary.PutVarint(meta[n:], int64(len(value)))
	metalen += n

	err = a.db.Put(a.wo, key, meta[:metalen])

	return
}

func (a *AppendDB) Get(key []byte) (value []byte, err error) {
	panic("todo")
}

type AppendIterator struct {
	iter   *gorocksdb.Iterator
	append *os.File
}

func (iter *AppendIterator) Seek(prefix []byte) {
	iter.iter.Seek(prefix)
}

func (iter *AppendIterator) Next() bool {
	if iter.iter.Valid() {
		iter.iter.Key().Free()
		iter.iter.Value().Free()
	}
	iter.iter.Next()
	return iter.iter.Valid()
}

func (iter *AppendIterator) Key() []byte {
	return iter.iter.Key().Data()
}

func (iter *AppendIterator) Value() []byte {
	meta := iter.iter.Value().Data()
	offset, n := binary.Varint(meta)
	size, n := binary.Varint(meta[n:])

	value := make([]byte, size)
	n, err := iter.append.ReadAt(value, offset)
	if err != nil {
		panic(err)
	}

	return value
}

func (iter *AppendIterator) Close() {
	iter.iter.Close()
	iter.append.Close()
}

func (a *AppendDB) NewIterator() Iterator {
	iter := a.db.NewIterator(a.ro)
	append, err := os.Open(a.appendFileName)
	if err != nil {
		panic(err)
	}
	return &AppendIterator{
		iter:   iter,
		append: append,
	}
}

func (a *AppendDB) Close() {
	a.db.Close()
	a.append.Close()
}

func (a *AppendDB) Delete(key []byte) error {
	panic("todo")
}

func (a *AppendDB) DeleteRange(start []byte, end []byte) error {
	panic("todo")
}

type RocksDB struct {
	db *gorocksdb.DB

	wo *gorocksdb.WriteOptions
	ro *gorocksdb.ReadOptions
}

func NewRocksDB(name string) (*RocksDB, error) {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)

	opts.SetCompression(gorocksdb.NoCompression)

	db, err := gorocksdb.OpenDb(opts, name)
	if err != nil {
		return nil, err
	}

	wo := gorocksdb.NewDefaultWriteOptions()
	wo.SetSync(false)

	rdb := &RocksDB{
		db: db,
		wo: wo,
		ro: gorocksdb.NewDefaultReadOptions(),
	}

	return rdb, nil
}

type RocksIterator struct {
	iter *gorocksdb.Iterator
}

func (iter *RocksIterator) Seek(prefix []byte) {
	iter.iter.Seek(prefix)
}

func (iter *RocksIterator) Next() bool {
	if iter.iter.Valid() {
		iter.iter.Key().Free()
		iter.iter.Value().Free()
	}
	iter.iter.Next()
	return iter.iter.Valid()
}

func (iter *RocksIterator) Key() []byte {
	return iter.iter.Key().Data()
}

func (iter *RocksIterator) Value() []byte {
	return iter.iter.Value().Data()
}

func (iter *RocksIterator) Close() {
	iter.iter.Close()
}

func (r *RocksDB) NewIterator() Iterator {
	return &RocksIterator{
		iter: r.db.NewIterator(r.ro),
	}
}

func (r *RocksDB) DeleteRange(start []byte, end []byte) error {
	panic("no")
}

func (r *RocksDB) Set(key []byte, value []byte) error {
	err := r.db.Put(r.wo, key, value)

	return err
}

func (r *RocksDB) Get(key []byte) ([]byte, error) {
	slice, err := r.db.Get(r.ro, key)
	if err != nil {
		return nil, err
	}

	// best to avoid copy
	value := slice.Data()
	rvalue := make([]byte, len(value))
	copy(rvalue, value)
	slice.Free()

	return rvalue, err
}

func (r *RocksDB) Delete(key []byte) error {
	return r.db.Delete(r.wo, key)
}

func (r *RocksDB) Close() {
	r.db.Close()
}

func dump(storage Storage, maxID uint64) {
	var i uint64
	for i = 1; i <= maxID; i++ {
		value, err := storage.Get(encode_uint64(i))
		if err != nil {
			panic(err)
		}

		fmt.Println("key: ", encode_uint64(i), " value len: ", len(value))
	}
}

func testIter(storage Storage) {
	start := time.Now()
	iter := storage.NewIterator()
	cnt := 0
	iter.Seek([]byte(""))

	for iter.Next() {
		value := iter.Value()
		_ = value
		cnt++
	}
	fmt.Println("iterate ", cnt, " keys use ", time.Since(start), float64(cnt)/time.Since(start).Seconds(), " op per second")

	iter.Close()
}

func testWrite(storage Storage) {
	var id uint64
	goroutineNum := 100
	ti := time.Now()

	var wg sync.WaitGroup

	for i := 0; i < goroutineNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			value := make([]byte, *vsize)
			for {
				key := atomic.AddUint64(&id, 1)

				if (int64(key)+1)*(*vsize) > *totalSize {
					return
				}

				err := storage.Set(encode_uint64(key), value)
				if err != nil {
					panic(err)
				}
			}
		}()
	}

	wg.Wait()

	fmt.Println("take: ", time.Since(ti), " ", float64(id-1)/time.Since(ti).Seconds(), " op per second")

	// dump(storage, wnum)
}

func encode_uint64(ts uint64) []byte {
	b := make([]byte, 8)
	for i := 0; i < 8; i++ {
		b[7-i] = byte(ts % 256)
		ts /= 256
	}

	return b
}

func cleanPath(path string) {
	var err error
	err = os.RemoveAll(path)
	if err != nil {
		panic(err)
	}
}

func main() {
	flag.Parse()

	path := "/data1/test"

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	cleanPath(path)

	rdb, err := NewRocksDB(path)
	if err != nil {
		log.Fatal(err)
	}

	testWrite(rdb)
	testIter(rdb)
	rdb.Close()
	cleanPath(path)

	time.Sleep(time.Second * 10)

	bdb, err := NewBadger(path)
	if err != nil {
		log.Fatal(err)
	}
	testWrite(bdb)
	testIter(bdb)
	bdb.Close()
	cleanPath(path)

	time.Sleep(time.Second * 10)

	adb, err := NewAppendDB(path)
	if err != nil {
		log.Fatal(err)
	}
	testWrite(adb)
	testIter(adb)
	adb.Close()
}
