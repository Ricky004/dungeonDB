//go:build linux || darwin
// +build linux darwin

package storage

import (
	"errors"
	"fmt"
	"os"

	u "github.com/Ricky004/dungeonDB/internal/utils"
	"golang.org/x/sys/unix"
)

// create the initial mmap that covers the whole file
// unix specific code for mmap
func MmapInit(fp *os.File) (int, []byte, error) {
	fi, err := fp.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("stat: %w", err)
	}

	if fi.Size()%BTREE_PAGE_SIZE != 0 {
		return 0, nil, errors.New("File size is not a multiple of page size.")
	}

	mmapSize := 64 << 20
	u.Assert(mmapSize%BTREE_PAGE_SIZE == 0)
	for mmapSize < int(fi.Size()) {
		mmapSize *= 2
	}
	// mmapSize can be larger than the file
	chunk, err := unix.Mmap(
		int(fp.Fd()), 0, mmapSize,
		unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED,
	)

	if err != nil {
		return 0, nil, fmt.Errorf("mmap: %s", err)
	}

	return int(fi.Size()), chunk, nil

}

// extend the mmap by adding new mappings
func ExtendMmap(db *KV, npages int) error {
	if db.mmap.total >= npages*BTREE_PAGE_SIZE {
		return nil
	}

	// double the address space
	chunk, err := unix.Mmap(
		int(db.fp.Fd()), int64(db.mmap.total), db.mmap.total,
		unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED,
	)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}

	db.mmap.total += db.mmap.total
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}

// extend the file to at least `npages`.
func extendFile(db *KV, npages int) error {
	filePages := db.mmap.file / BTREE_PAGE_SIZE
	if filePages >= npages {
		return nil
	}
	for filePages < npages {
		// the file size is increased exponentially,
		// so that we don't have to extend the file for every update.
		inc := filePages / 8
		if inc < 1 {
			inc = 1
		}
		filePages += inc
	}
	fileSize := filePages * BTREE_PAGE_SIZE
	err := unix.Fallocate(int(db.fp.Fd()), 0, 0, int64(fileSize))
	if err != nil {
		return fmt.Errorf("fallocate: %w", err)
	}
	db.mmap.file = fileSize
	return nil
}

func (db *KV) Open() error {
	// open or create the DB file
	fp, err := os.OpenFile(db.Path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("OpenFile: %w", err)
	}
	db.fp = fp
	// create the initial mmap
	sz, chunk, err := MmapInit(db.fp)
	if err != nil {
		goto fail
	}
	db.mmap.file = sz
	db.mmap.total = len(chunk)
	db.mmap.chunks = [][]byte{chunk}
	// btree callbacks
	db.tree.Get = db.PageGet
	db.tree.New = db.PageNew
	db.tree.Del = db.PageDel
	// read the master page
	err = MasterLoad(db)
	if err != nil {
		goto fail
	}
	// done
	return nil
fail:
	db.Close()
	return fmt.Errorf("KV.Open: %w", err)
}

// cleanups
func (db *KV) Close() {
	for _, chunk := range db.mmap.chunks {
		err := unix.Munmap(chunk)
		u.Assert(err == nil)
	}
	_ = db.fp.Close()
}

// read the db
func (db *KV) Get(key []byte) ([]byte, bool) {
	// code
	return nil, false
}

func (db *KV) Set(key []byte, val []byte) error {
	db.tree.Insert(key, val)
	return FlushPages(db)
}
func (db *KV) Del(key []byte) (bool, error) {
	deleted := db.tree.Delete(key)
	return deleted, FlushPages(db)
}

// persist the newly allocated pages after updates
func FlushPages(db *KV) error {
	if err := WritePages(db); err != nil {
		return err
	}
	return SyncPages(db)
}

func WritePages(db *KV) error {
	// update the free list
	freed := []uint64{}
	for ptr, page := range db.page.updates {
		if page == nil {
			freed = append(freed, ptr)
		}
	}
	db.free.Update(db.page.nfree, freed)

	// extend the file & mmap if needed
	npages := int(db.page.flushed) + len(db.page.updates)
	if err := extendFile(db, npages); err != nil {
		return err
	}
	if err := ExtendMmap(db, npages); err != nil {
		return err
	}
	
	// copy pages to the file
	for ptr, page := range db.page.updates {
		if page != nil {
		copy(PageGetMapped(db, ptr).Data, page)
		}
	}
	return nil
}

func SyncPages(db *KV) error {
    // flush data to the disk. must be done before updating the master page.
    if err := db.fp.Sync(); err != nil {
        return fmt.Errorf("fsync: %w", err)
    }
    db.page.flushed += uint64(len(db.page.updates))
    db.page.updates = make(map[uint64][]byte)
    // update & flush the master page
    if err := MasterStore(db); err != nil {
        return err
    }
    if err := db.fp.Sync(); err != nil {
        return fmt.Errorf("fsync: %w", err)
    }
    return nil
}
