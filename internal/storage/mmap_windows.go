//go:build windows
// +build windows

package storage

import (
	"errors"
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

	if fi.Size()%BTREE_PAGE_SIZE != 0 {
		return 0, nil, errors.New("File size is not a multiple of page size.")
	}

	mmapSize := 64 << 20
	u.Assert(mmapSize%BTREE_PAGE_SIZE == 0)

	// Adjust mmap size to be larger than the file size
	for mmapSize < int(fi.Size()) {
		mmapSize *= 2
	}
	// mmapSize can be larger than the file
	// open the file
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
		return 0, nil, fmt.Errorf("CreateFile: %s", err)
	}

	defer windows.CloseHandle(handle)
	
	// create a file mapping
	mapHandle, err := windows.CreateFileMapping(
		handle,
		nil,
		windows.PAGE_READWRITE,
		0,
		uint32(mmapSize),
		nil,
	)
    if err != nil {
		return 0, nil, fmt.Errorf("CreateFileMapping: %s", err)
	}
	
	defer windows.CloseHandle(mapHandle)

	// Map the file into memory
	addr, err := windows.MapViewOfFile(
		mapHandle,
		windows.FILE_MAP_READ|windows.FILE_MAP_WRITE,
		0,
		0,
		uintptr(mmapSize),
	)
	if err != nil {
		return 0, nil, fmt.Errorf("MapViewOfFile: %s", err)
	}
     
	// Return the mapped memory as a byte slice
	// We need to cast the pointer to a byte slice and return it
	return int(fi.Size()), (*[1 << 30]byte)(unsafe.Pointer(addr))[:fi.Size()], nil

}

func ExtendMmapWindows(db *KV, npages int) error {
	// Check if the current memory-mapped region is already large enough
	if db.mmap.total >= npages*BTREE_PAGE_SIZE {
		return nil
	}

	// Double the address space size
	newSize := db.mmap.total * 2

	// Open the file again, as we may need to extend the file mapping
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
		return fmt.Errorf("CreateFile: %s", err)
	}
	defer windows.CloseHandle(handle)

	// Create a new file mapping for the extended size
	mapHandle, err := windows.CreateFileMapping(
		handle,
		nil,
		windows.PAGE_READWRITE,
		0,
		uint32(newSize),
		nil,
	)
	if err != nil {
		return fmt.Errorf("CreateFileMapping: %s", err)
	}
	defer windows.CloseHandle(mapHandle)

	// Map the extended region of the file into memory
	addr, err := windows.MapViewOfFile(
		mapHandle,
		windows.FILE_MAP_READ|windows.FILE_MAP_WRITE,
		0,
		0,
		uintptr(newSize),
	)
	if err != nil {
		return fmt.Errorf("MapViewOfFile: %s", err)
	}

	// Update the KV struct with the new memory mapping information
	db.mmap.total = newSize
	// Append the new chunk (the extended region) to the mmap chunks
	chunk := (*[1 << 30]byte)(unsafe.Pointer(addr))[:newSize]
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


// Open opens the DB file and sets up the memory-mapping
func (db *KV) OpenWindows() error {
	// Open or create the DB file using CreateFile (Windows)
	handle, err := windows.CreateFile(
		windows.StringToUTF16Ptr(db.Path),
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		0,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		// If the file does not exist, we need to create it
		if err.Error() == "The system cannot find the file specified." {
			handle, err = windows.CreateFile(
				windows.StringToUTF16Ptr(db.Path),
				windows.GENERIC_READ|windows.GENERIC_WRITE,
				0,
				nil,
				windows.CREATE_NEW,
				windows.FILE_ATTRIBUTE_NORMAL,
				0,
			)
			if err != nil {
				return fmt.Errorf("CreateFile: %w", err)
			}
		} else {
			return fmt.Errorf("CreateFile: %w", err)
		}
	}
	defer windows.CloseHandle(handle)

	// Create the initial mmap (memory map) using MapViewOfFile
	sz, chunk, err := MmapInitWindows(db.fp)
	if err != nil {
		goto fail
	}
	db.mmap.file = sz
	db.mmap.total = len(chunk)
	db.mmap.chunks = [][]byte{chunk}

	// Set up BTree callbacks
	db.tree.Get = db.pageGet
	db.tree.New = db.pageNew
	db.tree.Del = db.pageDel

	// Read the master page
	err = masterLoad(db)
	if err != nil {
		goto fail
	}

	// Done
	return nil

fail:
	// Cleanup on failure
	db.CloseWindows()
	return fmt.Errorf("KV.Open: %w", err)
}

// Close performs cleanup of the memory-mapped regions and closes the file
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
	// extend the file & mmap if needed
	npages := int(db.page.flushed) + len(db.page.temp)
	if err := extendFileWindows(db, npages); err != nil {
		return err
	}
	if err := ExtendMmapWindows(db, npages); err != nil {
		return err
	}
	// copy data to the file
	for i, page := range db.page.temp {
		ptr := db.page.flushed + uint64(i)
		copy(db.pageGet(ptr).Data, page)
	}
	return nil
}

func SyncPagesW(db *KV) error {
	// flush data to the disk. must be done before updating the master page.
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	db.page.flushed += uint64(len(db.page.temp))
	db.page.temp = db.page.temp[:0]
	// update & flush the master page
	if err := masterStore(db); err != nil {
		return err
	}
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	return nil
}

