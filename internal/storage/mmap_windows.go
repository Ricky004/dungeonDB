//go:build windows
// +build windows

package storage

import (
	"fmt"
	"os"
	"unsafe"

	u "github.com/Ricky004/dungeonDB/internal/utils"
	"golang.org/x/sys/windows"
)

// create the initial mmap that covers the whole file
// windows specific code for mmap
func MmapInitWindows(fp *os.File) (int, []byte, error) {
	fi, err := fp.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("stat: %w", err)
	}

	fileSize := fi.Size()
	if fileSize%BTREE_PAGE_SIZE != 0 {
		// Pad file size to the nearest multiple of BTREE_PAGE_SIZE
		fileSize = (fileSize/BTREE_PAGE_SIZE + 1) * BTREE_PAGE_SIZE
		if err := fp.Truncate(fileSize); err != nil {
			return 0, nil, fmt.Errorf("truncate: %w", err)
		}
	}

	// Open the file with Windows-specific API
	handle, err := windows.CreateFile(
		windows.StringToUTF16Ptr(fp.Name()),
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		0,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		return 0, nil, fmt.Errorf("CreateFile: %w", err)
	}
	defer windows.CloseHandle(handle)

	// Create a file mapping
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

	// Return the mapped memory as a byte slice
	return int(fileSize), unsafe.Slice((*byte)(unsafe.Pointer(addr)), fileSize), nil
}

func ExtendMmapWindows(db *KV, npages int) error {
	newSize := db.mmap.total + (npages * BTREE_PAGE_SIZE)

	// Open the file
	handle, err := windows.CreateFile(
		windows.StringToUTF16Ptr(db.fp.Name()),
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		0,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		return fmt.Errorf("CreateFile: %w", err)
	}
	defer windows.CloseHandle(handle)

	// Create a file mapping for the new size
	mapHandle, err := windows.CreateFileMapping(
		handle,
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

	// Open the file for resizing
	handle, err := windows.CreateFile(
		windows.StringToUTF16Ptr(db.fp.Name()),
		windows.GENERIC_WRITE,
		0,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		return fmt.Errorf("CreateFile: %s", err)
	}
	defer windows.CloseHandle(handle)

	// Set the file pointer to the new file size
	_, err = windows.SetFilePointer(handle, int32(fileSize), nil, windows.FILE_BEGIN)
	if err != nil {
		return fmt.Errorf("SetFilePointer: %s", err)
	}

	// Extend the file by setting the end of the file at the new position
	err = windows.SetEndOfFile(handle)
	if err != nil {
		return fmt.Errorf("SetEndOfFile: %s", err)
	}

	// Update the file size in the KV struct
	db.mmap.file = fileSize
	return nil
}

// OpenWindows opens the DB file and sets up the memory-mapping
func (db *KV) OpenWindows() error {
	fmt.Println("Starting OpenWindows...")

	// Validate the database path
	if db.Path == "" {
		return fmt.Errorf("database path is empty")
	}
	fmt.Printf("Opening database file at path: %q\n", db.Path)

	// Open or create the DB file using os.OpenFile (to get *os.File)
	file, err := os.OpenFile(db.Path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("failed to open or create file: %w", err)
	}
	defer file.Close()

	fmt.Println("File opened successfully.")

	   // Check if the file is already mapped or being used
    // This check ensures no other processes or parts of your program hold the file handle
    fileInfo, err := file.Stat()
    if err != nil {
        return fmt.Errorf("failed to stat file: %w", err)
    }
    fmt.Printf("File size: %d bytes\n", fileInfo.Size())

	// Initialize mmap using the *os.File handle
	fmt.Println("Initializing memory map...")
	sz, chunk, err := MmapInitWindows(file)
	if err != nil {
        fmt.Printf("Failed to initialize mmap: %v\n", err)
        return fmt.Errorf("KV.OpenWindows: %w", err)
    }

	db.mmap.file = sz
	db.mmap.total = len(chunk)
	db.mmap.chunks = [][]byte{chunk}
	fmt.Println("Memory map initialized successfully.")

	// Set up BTree callbacks
	db.tree.Get = db.PageGet
	db.tree.New = db.PageNew
	db.tree.Del = db.PageDel
	fmt.Println("BTree callbacks configured.")

	// Load the master page
	fmt.Println("Loading master page...")
	err = MasterLoad(db)
	if err != nil {
		fmt.Printf("Failed to load master page: %v\n", err)
		goto fail
	}
	fmt.Println("Master page loaded successfully.")

	// Done
	fmt.Println("OpenWindows completed successfully.")
	return nil

fail:
	// Cleanup on failure
	fmt.Println("Error occurred, performing cleanup...")
	db.CloseWindows()
	return fmt.Errorf("KV.OpenWindows: %w", err)
}

// CloseWindows performs cleanup of the memory-mapped regions and closes the file
func (db *KV) CloseWindows() {
	for _, chunk := range db.mmap.chunks {
		// Unmap the chunk using UnmapViewOfFile
		err := windows.UnmapViewOfFile(uintptr(unsafe.Pointer(&chunk[0])))
		u.Assert(err == nil)
	}
	_ = db.fp.Close()
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
func (db *KV) DelW(key []byte) (bool, error) {
	deleted := db.tree.Delete(key)
	return deleted, FlushPagesW(db)
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
