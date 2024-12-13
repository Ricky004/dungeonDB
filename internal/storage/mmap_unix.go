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
func MmapInitUnix(fp *os.File) (int, []byte, error) {
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
