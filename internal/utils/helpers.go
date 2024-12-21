package utils

import (
	"fmt"
	"log"
	"os"
)

// assert checks a condition and panics with a message if the condition is false
func Assert(condition bool, message ...string) {
	if !condition {
		if len(message) > 0 {
			log.Panicf("Assertion failed: %s", message[0])
		} else {
			log.Panic("Assertion failed")
		}
	}
}

func HexViewr(file *os.File, offset int64, size int) {
	// Reading back node data
    // Use the offset where the node data was written
	_, err := file.Seek(offset, 0) // Seek to the correct position in the file
	if err != nil {
		log.Fatalf("Failed to seek to offset %d: %v", offset, err)
	}

	// Read data from the offset (example: read 64 bytes)
	data := make([]byte, size)
	_, err = file.Read(data)
	if err != nil {
		log.Fatalf("Failed to read from file: %v", err)
	}

	// Print the raw data as a hex dump
	fmt.Printf("Read data (hex dump):\n")
	for i, b := range data {
		if i%8 == 0 {
			fmt.Print("\n")
		}
		fmt.Printf("%02X ", b)
	}
}
