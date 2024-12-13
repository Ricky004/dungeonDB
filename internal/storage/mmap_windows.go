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
