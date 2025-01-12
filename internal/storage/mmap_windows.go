//go:build windows
// +build windows

package storage

import (
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

// create the initial mmap that covers the whole file
// windows specific code for mmap
func MmapInitWindows(fp *os.File) (int, []byte, error) {
	// Truncate file if necessary
	fi, err := fp.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("stat: %w", err)
	}

	fileSize := fi.Size()
	if fileSize%BTREE_PAGE_SIZE != 0 {
		fileSize = (fileSize/BTREE_PAGE_SIZE + 1) * BTREE_PAGE_SIZE
		if err := fp.Truncate(fileSize); err != nil {
			return 0, nil, fmt.Errorf("truncate: %w", err)
		}
	}

	// Close and reopen the file
	fileName := fp.Name()
	fp.Close()

	fp, err = os.OpenFile(fileName, os.O_RDWR, 0644)
	if err != nil {
		return 0, nil, fmt.Errorf("reopen file: %w", err)
	}
	defer fp.Close()

	handle := windows.Handle(fp.Fd())

	// Create file mapping
	mapHandle, err := windows.CreateFileMapping(
		handle,
		nil,
		windows.PAGE_READWRITE,
		0,
		uint32(fileSize),
		nil,
	)
	if err != nil {
		return 0, nil, fmt.Errorf("CreateFileMapping: %w", err)
	}
	defer windows.CloseHandle(mapHandle)

	// Map the file into memory
	addr, err := windows.MapViewOfFile(
		mapHandle,
		windows.FILE_MAP_READ|windows.FILE_MAP_WRITE,
		0,
		0,
		uintptr(fileSize),
	)
	if err != nil {
		return 0, nil, fmt.Errorf("MapViewOfFile: %w", err)
	}

	return int(fileSize), unsafe.Slice((*byte)(unsafe.Pointer(addr)), fileSize), nil
}

func ExtendMmapWindows(db *KV, npages int) error {
	newSize := db.mmap.total + (npages * BTREE_PAGE_SIZE)

	// Use the existing file handle (db.fp) instead of reopening the file
	handle := db.fp.Fd()

	// Create a file mapping for the new size
	mapHandle, err := windows.CreateFileMapping(
		windows.Handle(handle),
		nil,
		windows.PAGE_READWRITE,
		0,
		uint32(newSize),
		nil,
	)
	if err != nil {
		return fmt.Errorf("CreateFileMapping: %w", err)
	}
	defer windows.CloseHandle(mapHandle)

	// Map the extended file into memory
	addr, err := windows.MapViewOfFile(
		mapHandle,
		windows.FILE_MAP_READ|windows.FILE_MAP_WRITE,
		0,
		0,
		uintptr(newSize),
	)
	if err != nil {
		return fmt.Errorf("MapViewOfFile: %w", err)
	}

	// Update the mmap struct
	db.mmap.total = newSize
	chunk := unsafe.Slice((*byte)(unsafe.Pointer(addr)), newSize)
	db.mmap.chunks = append(db.mmap.chunks, chunk)

	return nil
}

func extendFileWindows(db *KV, npages int) error {
	// Calculate the number of pages already in the file
	filePages := db.mmap.file / BTREE_PAGE_SIZE
	if filePages >= npages {
		return nil
	}

	// Increase the file size exponentially, similar to the original logic
	for filePages < npages {
		inc := filePages / 8
		if inc < 1 {
			inc = 1
		}
		filePages += inc
	}

	// Calculate the new file size
	fileSize := filePages * BTREE_PAGE_SIZE

	// Use the existing file handle to resize the file
	if err := db.fp.Truncate(int64(fileSize)); err != nil {
		return fmt.Errorf("failed to truncate file: %w", err)
	}

	// Update the file size in the KV struct
	db.mmap.file = fileSize
	return nil
}

// OpenWindows opens the DB file and sets up the memory-mapping
func (db *KV) OpenWindows() error {
	fmt.Println("Starting OpenWindows...")

	if db.Path == "" {
		return fmt.Errorf("database path is empty")
	}
	fmt.Printf("Opening database file at path: %q\n", db.Path)

	// Open or create the file
	file, err := os.OpenFile(db.Path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("failed to open or create file: %w", err)
	}
	db.fp = file // Assign file pointer to KV struct
	fmt.Println("File pointer assigned:", db.fp)

	fmt.Println("File opened successfully.")

	// Check if the file is empty and initialize the signature if needed
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}
	fmt.Printf("File size before any operations: %d\n", fileInfo.Size())

	// If file is empty, initialize it with the master page
	if fileInfo.Size() == 0 {
		fmt.Println("File is empty, initializing master page with signature...")
		if err := MasterStore(db); err != nil {
			return fmt.Errorf("failed to initialize master page: %w", err)
		}
		fmt.Println("Master page initialized successfully!")
	}

	// Resize the file if necessary
	pagesRequired := 1 // Adjust this based on your use case
	fmt.Println("Ensuring file size is sufficient...")
	if err := extendFileWindows(db, pagesRequired); err != nil {
		return fmt.Errorf("failed to extend file: %w", err)
	}
	fmt.Println("File size ensured.")

	// Initialize memory map
	fmt.Println("Initializing memory map...")
	if err := ExtendMmapWindows(db, pagesRequired); err != nil {
		return fmt.Errorf("failed to initialize mmap: %w", err)
	}
	fmt.Println("Memory map initialized successfully.")

	// Set up BTree callbacks
	db.tree.Get = db.PageGet
	db.tree.New = db.PageNew
	db.tree.Del = db.PageDel
	fmt.Println("BTree callbacks configured.")

	// Load the master page
	fmt.Println("Loading master page...")
	if err := MasterLoad(db); err != nil {
		fmt.Printf("Failed to load master page: %v\n", err)
		goto fail
	}
	fmt.Println("Master page loaded successfully.")

	fmt.Println("OpenWindows completed successfully.")
	return nil

fail:
	fmt.Println("Error occurred, performing cleanup...")
	// Close the file explicitly only at the end
	if err := db.CloseWindows(); err != nil {
		fmt.Printf("Error closing database: %v", err)
	}
	return fmt.Errorf("KV.OpenWindows: %w", err)
}

// CloseWindows performs cleanup of the memory-mapped regions and closes the file
func (db *KV) CloseWindows() error {
	// First, try to write any pending changes
	if err := MasterStore(db); err != nil {
		return fmt.Errorf("failed to store final state: %w", err)
	}

	// Sync to ensure all changes are written to disk
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("failed to sync final changes: %w", err)
	}

	// Then unmap all chunks
	for _, chunk := range db.mmap.chunks {
		err := windows.UnmapViewOfFile(uintptr(unsafe.Pointer(&chunk[0])))
		if err != nil {
			return fmt.Errorf("failed to unmap view: %w", err)
		}
	}

	// Finally close the file
	if db.fp != nil {
        // Ensure the file pointer is closed properly
        if err := db.fp.Close(); err != nil {
            return fmt.Errorf("failed to close database file: %w", err)
        }
        db.fp = nil // Ensure the pointer is set to nil after closing
    }
    
	return nil
}

// read the db
func (db *KV) GetW(key []byte) ([]byte, bool) {
	// code
	return nil, false
}

func (db *KV) SetW(key []byte, val []byte) error {
	db.tree.Insert(key, val)
	return FlushPagesW(db)
}
func (db *KV) DelW(req *DeleteReq) (bool, error) {
	deleted := db.tree.Delete(req.Key)
	return deleted, FlushPagesW(db)
}

func (db *KV) UpdateW(req *InsertReq) (bool, error) {
	// code
	return false, nil
}

// persist the newly allocated pages after updates
func FlushPagesW(db *KV) error {
	if err := WritePagesW(db); err != nil {
		return err
	}
	return SyncPagesW(db)
}

func WritePagesW(db *KV) error {
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
	if err := extendFileWindows(db, npages); err != nil {
		return err
	}
	if err := ExtendMmapWindows(db, npages); err != nil {
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

func SyncPagesW(db *KV) error {
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
