package main

import (
	"fmt"
	"log"
	"os"

	"github.com/Ricky004/dungeonDB/internal/storage"
	u "github.com/Ricky004/dungeonDB/internal/utils"
)

func main() {
	// Define the path to the database file
	dbPath := "test.db"

	// Remove the file if it exists to start fresh (for testing purposes)
	if _, err := os.Stat(dbPath); err == nil {
		os.Remove(dbPath)
	}

	// Create a new KV instance
	db := &storage.KV{
		Path: dbPath,
	}

	// Open the database
	err := db.OpenWindows()
	if err != nil {
		log.Fatalf("Failed to open the database: %v", err)
	}
	defer func() {
		if err := db.CloseWindows(); err != nil {
			log.Printf("Error closing database: %v", err)
		}
	}() // Ensure cleanup happens even if something fails

	fmt.Println("Database opened successfully!")

	// Perform some basic operations (e.g., adding a page, deleting a page)
	// Create a new BNode (sample data)
	node := storage.BNode{
		Data: []byte(`work in progress by Ricky004`),
	}

	// Allocate a new page
	pageID := db.PageAppend(node)
	fmt.Printf("Added a new page with ID: %d\n", pageID)

	// Delete the page
	// db.PageDel(pageID)
	// fmt.Printf("Deleted the page with ID: %d\n", pageID)
	db.PageUse(pageID, node)

	// Verify master page load/store
	err = storage.MasterStore(db)
	if err != nil {
		log.Fatalf("Failed to store the master page: %v", err)
	}
	fmt.Println("Master page stored successfully!")

	fmt.Println("All operations completed successfully!")

	file, err := os.Open(dbPath)
	if err != nil {
		log.Fatalf("Failed to open database file: %v", err)
	}
	defer file.Close()

	u.HexViewr(file, 4096, 64)
}
