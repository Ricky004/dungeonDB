package storage

import (
	"bytes"
	"encoding/binary"
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
    // Initialize the updates map first
    db.page.updates = make(map[uint64][]byte)

    // Check if the file is empty
    if db.mmap.file == 0 {
        // If the file is empty, initialize the master page with signature
        db.page.flushed = 1 // Set the page to 1 as the master page is initialized
        fmt.Println("File is empty, initializing master page with signature...")
        err := MasterStore(db) // Store the master page with initialized values
        if err != nil {
            return fmt.Errorf("failed to initialize master page: %w", err)
        }

        // Sync the file to ensure all changes are written to disk
        fmt.Println("Syncing the file after initializing master page...")
        err = db.fp.Sync()
        if err != nil {
            return fmt.Errorf("failed to sync file: %w", err)
        }

        // Re-initialize memory map after writing the master page
        fmt.Println("Re-initializing memory map after master page write...")
        if err := ExtendMmapWindows(db, 1); err != nil {
            return fmt.Errorf("failed to re-initialize mmap after master page write: %w", err)
        }

        // Verify the initialization
        data := db.mmap.chunks[0]
        expectedSig := make([]byte, 16)
        copy(expectedSig, []byte(DB_SIG))
        
        fmt.Printf("Verifying signature - Expected: %x, Got: %x\n", expectedSig, data[:16])
        
        if !bytes.Equal(expectedSig, data[:16]) {
            return fmt.Errorf("signature verification failed after initialization - Expected: %x, Got: %x", 
                expectedSig, data[:16])
        }

        // Initialize tree root and page values for new database
        db.tree.root = 0
        db.page.flushed = 1

        return nil
    }

    // Load the data (first chunk in the memory map)
    data := db.mmap.chunks[0]
    
    // Create properly padded expected signature
    expectedSig := make([]byte, 16)
    copy(expectedSig, []byte(DB_SIG))
    
    // Log the loaded signature and values for debugging
    fmt.Printf("Loaded data signature: %s\n", string(data[:16]))
    fmt.Printf("Raw signature bytes - Expected: %x, Got: %x\n", expectedSig, data[:16])
    
    root := binary.LittleEndian.Uint64(data[16:])
    used := binary.LittleEndian.Uint64(data[24:])
    fmt.Printf("Root: %d, Used: %d\n", root, used)

    // Check if the signature matches with proper padding comparison
    if !bytes.Equal(expectedSig, data[:16]) {
        return fmt.Errorf("Bad signature. Expected: %x, Got: %x", expectedSig, data[:16])
    }

    // Handle the case where the file is newly initialized
    if used == 0 {
        // If this is the first valid page, treat it as initialized but with 0 pages
        fmt.Println("Initializing master page with signature due to 0 pages used.")
        db.page.flushed = 1
        err := MasterStore(db)
        if err != nil {
            return fmt.Errorf("failed to store master page: %w", err)
        }

        // Sync the file to ensure changes are written to disk
        fmt.Println("Syncing the file after writing master page...")
        err = db.fp.Sync()
        if err != nil {
            return fmt.Errorf("failed to sync file after master page write: %w", err)
        }

        // Re-initialize memory map after master page write
        fmt.Println("Re-initializing memory map after master page write...")
        if err := ExtendMmapWindows(db, 1); err != nil {
            return fmt.Errorf("failed to re-initialize mmap after master page write: %w", err)
        }

        return nil
    }

    // Further checks to ensure `used` and `root` values are within valid ranges
    if !(0 <= used && used <= uint64(db.mmap.file/BTREE_PAGE_SIZE)) {
        return fmt.Errorf("Bad master page: used value out of range (used: %d, max: %d)", 
            used, uint64(db.mmap.file/BTREE_PAGE_SIZE))
    }

    if !(0 <= root && root < used) {
        return fmt.Errorf("Bad master page: root value out of range (root: %d, used: %d)", 
            root, used)
    }

    // Set root and flushed page values
    db.tree.root = root
    db.page.flushed = used

    return nil
}

func MasterStore(db *KV) error {
	// Ensure the file pointer is valid before writing
    if db.fp == nil {
        return fmt.Errorf("db.fp is nil, file is not open")
    }
    fmt.Println("File pointer is valid, proceeding with master page store.")
	
    var data [32]byte
    
    // Create a properly padded signature
    sigBytes := []byte(DB_SIG)
    // Pad with zeros if necessary
    if len(sigBytes) < 16 {
        paddedSig := make([]byte, 16)
        copy(paddedSig, sigBytes)
        sigBytes = paddedSig
    }
    
    // Store the padded signature
    copy(data[:16], sigBytes)
    
    // Store the root and flushed values
    binary.LittleEndian.PutUint64(data[16:], db.tree.root)
    binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
    
    // Write the data
    _, err := db.fp.WriteAt(data[:], 0)
    if err != nil {
        return fmt.Errorf("write master page: %w", err)
    }
    
    // Sync to ensure write is flushed to disk
    if err := db.fp.Sync(); err != nil {
        return fmt.Errorf("sync master page: %w", err)
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
