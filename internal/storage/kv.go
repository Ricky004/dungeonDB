package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	u "github.com/Ricky004/dungeonDB/internal/utils"
)

type KV struct {
	Path string
	// internals
	fp   *os.File
	tree BTree
	mmap struct {
		file   int      // file size, can be larger than the database size
		total  int      // mmap size, can be larger than the file size
		chunks [][]byte // multiple mmaps, can be non-continuous
	}
	page struct {
		flushed uint64 // database size in number of pages
		nfree   int    // number of pages taken from the free list
		nappend int    // number of pages to be appended
		// newly allocated or deallocated pages keyed by the pointer.
		// nil value denotes a deallocated page.
		updates map[uint64][]byte
	}
	free FreeList
}

// callback for BTree, dereference a pointer.
func (db *KV) PageGet(ptr uint64) BNode {
	if page, ok := db.page.updates[ptr]; ok {
		u.Assert(page != nil)
		return BNode{page} // for new pages
	}
	return PageGetMapped(db, ptr) // for written pages
}

func PageGetMapped(db *KV, ptr uint64) BNode {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_PAGE_SIZE
		if ptr < end {
			offset := BTREE_PAGE_SIZE * (ptr - start)
			return BNode{chunk[offset : offset+BTREE_PAGE_SIZE]}
		}
		start = end
	}
	panic("bad ptr")
}

// the signature of the database file
const DB_SIG = "DungeonDB01"

// the master page format.
// it contains the pointer to the root and other important bits.
// | sig | btree_root | page_used |
// | 16B | 8B | 8B |
func MasterLoad(db *KV) error {
	if db.mmap.file == 0 {
		// empty file, the master page will be created on the first write.
		db.page.flushed = 1 // reserved for the master page
		return nil
	}
	data := db.mmap.chunks[0]
	root := binary.LittleEndian.Uint64(data[16:])
	used := binary.LittleEndian.Uint64(data[24:])
	// verify the page
	if !bytes.Equal([]byte(DB_SIG), data[:16]) {
		return errors.New("Bad signature.")
	}
	bad := !(1 <= used && used <= uint64(db.mmap.file/BTREE_PAGE_SIZE))
	bad = bad || !(0 <= root && root < used)
	if bad {
		return errors.New("Bad master page.")
	}
	db.tree.root = root
	db.page.flushed = used
	return nil
}

func MasterStore(db *KV) error {
	var data [32]byte
	copy(data[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[16:], db.tree.root)
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	// NOTE: Updating the page via mmap is not atomic.
	// Use the `pwrite()` syscall instead.
	_, err := db.fp.WriteAt(data[:], 0)
	if err != nil {
		return fmt.Errorf("write master page: %w", err)
	}
	return nil
}

// callback for BTree, allocate a new page.
func (db *KV) PageNew(node BNode) uint64 {
	u.Assert(len(node.Data) <= BTREE_PAGE_SIZE)
	ptr := uint64(0)
	if db.page.nfree < db.free.Total() {
		// reuse a deallocated page
		ptr = db.free.Get(db.page.nfree)
		db.page.nfree++
	} else {
		// append a new page
		ptr = db.page.flushed + uint64(db.page.nappend)
		db.page.nappend++
	}
	db.page.updates[ptr] = node.Data
	return ptr
}

// callback for BTree, deallocate a page.
func (db *KV) PageDel(ptr uint64) {
	db.page.updates[ptr] = nil
}

// callback for FreeList, allocate a new page
func (db *KV) PageAppend(node BNode) uint64 {
   u.Assert(len(node.Data) <= BTREE_PAGE_SIZE)
   ptr := db.page.flushed + uint64(db.page.nappend)
   db.page.nappend++
   db.page.updates[ptr] = node.Data
   return ptr
}

// callback for FreeList, reuse a page
func (db *KV) PageUse(ptr uint64, node BNode) {
   db.page.updates[ptr] = node.Data
}