/*

A trivial application to illustrate how the dfslib library can be used
from an application in assignment 2 for UBC CS 416 2017W2.

Usage:
go run app.go
*/

package main

// Expects dfslib.go to be in the ./dfslib/ dir, relative to
// this app.go file
import (
	"fmt"
	"os"
	"time"

	"./dfslib"
)

func main() {
	serverAddr := "127.0.0.1:9482"
	localIP := "127.0.0.1"
	localPath := "/tmp/dfs-g4w81/"

	// Connect to DFS.
	dfs, err := dfslib.MountDFS(serverAddr, localIP, localPath)

	fmt.Printf("PRINTING DFS: %+v\n\n", dfs)
	if checkError(err) != nil {
		return
	}

	// Close the DFS on exit.
	// Defers are really cool, check out: https://blog.golang.org/defer-panic-and-recover
	defer dfs.UMountDFS()

	// Check if hello.txt file exists in the global DFS.
	exists, err := dfs.GlobalFileExists("a2")
	fmt.Println("GLOBAL_FILE_EXISTS: a2 %t", exists)
	if checkError(err) != nil {
		return
	}

	// if exists {
	// 	fmt.Println("File already exists, mission accomplished")
	// 	return
	// }

	// Open the file (and create it if it does not exist) for writing.
	fmt.Println("Write mode is: %d, %d", dfslib.WRITE, dfslib.READ)
	f, err := dfs.Open("a2", dfslib.WRITE)

	checkError(err)

	fmt.Println("Sleep app for 12 seconds")
	time.Sleep(12 * time.Second)

	fmt.Println("Awake")
	f, err = dfs.Open("a2", dfslib.WRITE)

	if err != nil {
		checkError(err)
		fmt.Println("SHOULD HAVE BEEN ABLE TO OPEN WRITE NOW ......")

		return
	}

	var chunk dfslib.Chunk
	const str = "EVIL HELLO!"
	copy(chunk[:], str)

	// Write the 0th chunk of the file.
	err = f.Write(0, &chunk)
	if err != nil {
		fmt.Println("Write error")
		checkError(err)
		// return
	}

	for {
	}
	// // Close the file on exit.
	// defer f.Close()

	// // Create a chunk with a string message.
	// var chunk dfslib.Chunk
	// const str = "Hello friends!"
	// copy(chunk[:], str)

	// Write the 0th chunk of the file.
	err = f.Write(0, &chunk)
	if checkError(err) != nil {
		return
	}

	// Read the 0th chunk of the file.
	err = f.Read(0, &chunk)
	checkError(err)
}

// If error is non-nil, print it out and return it.
func checkError(err error) error {
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error ", err.Error())
		return err
	}
	return nil
}
