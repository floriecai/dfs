/*

This package specifies the application's interface to the distributed
file system (DFS) system to be used in assignment 2 of UBC CS 416
2017W2.

*/

package dfslib

import (
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
)

// A Chunk is the unit of reading/writing in DFS.
type Chunk [32]byte

// Represents a type of file access.
type FileMode int

const (
	// Read mode.
	READ FileMode = iota

	// Read/Write mode.
	WRITE

	// Disconnected read mode.
	DREAD
)

////////////////////////////////////////////////////////////////////////////////////////////
// <ERROR DEFINITIONS>

// These type definitions allow the application to explicitly check
// for the kind of error that occurred. Each API call below lists the
// errors that it is allowed to raise.
//
// Also see:
// https://blog.golang.org/error-handling-and-go
// https://blog.golang.org/errors-are-values

// Contains serverAddr
type DisconnectedError string

func (e DisconnectedError) Error() string {
	return fmt.Sprintf("DFS: Not connnected to server [%s]", string(e))
}

// Contains chunkNum that is unavailable
type ChunkUnavailableError uint8

func (e ChunkUnavailableError) Error() string {
	return fmt.Sprintf("DFS: Latest verson of chunk [%s] unavailable", string(e))
}

// Contains filename
type OpenWriteConflictError string

func (e OpenWriteConflictError) Error() string {
	return fmt.Sprintf("DFS: Filename [%s] is opened for writing by another client", string(e))
}

// Contains file mode that is bad.
type BadFileModeError FileMode

func (e BadFileModeError) Error() string {
	return fmt.Sprintf("DFS: Cannot perform this operation in current file mode [%s]", string(e))
}

// Contains filename.
type WriteModeTimeoutError string

func (e WriteModeTimeoutError) Error() string {
	return fmt.Sprintf("DFS: Write access to filename [%s] has timed out; reopen the file", string(e))
}

// Contains filename
type BadFilenameError string

func (e BadFilenameError) Error() string {
	return fmt.Sprintf("DFS: Filename [%s] includes illegal characters or has the wrong length", string(e))
}

// Contains filename
type FileUnavailableError string

func (e FileUnavailableError) Error() string {
	return fmt.Sprintf("DFS: Filename [%s] is unavailable", string(e))
}

// Contains local path
type LocalPathError string

func (e LocalPathError) Error() string {
	return fmt.Sprintf("DFS: Cannot access local path [%s]", string(e))
}

// Contains filename
type FileDoesNotExistError string

func (e FileDoesNotExistError) Error() string {
	return fmt.Sprintf("DFS: Cannot open file [%s] in D mode as it does not exist locally", string(e))
}

// </ERROR DEFINITIONS>
////////////////////////////////////////////////////////////////////////////////////////////

// Represents a file in the DFS system.
type DFSFile interface {
	// Reads chunk number chunkNum into storage pointed to by
	// chunk. Returns a non-nil error if the read was unsuccessful.
	//
	// Can return the following errors:
	// - DisconnectedError (in READ,WRITE modes)
	// - ChunkUnavailableError (in READ,WRITE modes)
	Read(chunkNum uint8, chunk *Chunk) (err error)

	// Writes chunk number chunkNum from storage pointed to by
	// chunk. Returns a non-nil error if the write was unsuccessful.
	//
	// Can return the following errors:
	// - BadFileModeError (in READ,DREAD modes)
	// - DisconnectedError (in WRITE mode)
	// - WriteModeTimeoutError (in WRITE mode)
	Write(chunkNum uint8, chunk *Chunk) (err error)

	// Closes the file/cleans up. Can return the following errors:
	// - DisconnectedError
	Close() (err error)
}

// DFSFileT Implementation of DFSFile
type DFSFileT struct {
	id        int
	requestID int // ID of who made the request on this file, need to know the local path ....
	name      string
	data      []Chunk
}

func (file DFSFileT) Read(chunkNum uint8, chunk *Chunk) (err error) {
	// When reading, must always check server to see if it has latest version
	// if local has latest version, server must block other writes (but allow reads)
	// if local doesn't have latest version, server

	// do something like:
	// rpc.Call('Read', chunkNum, Chunk, file.name, file.id)
	return nil
}

// ChunkToByte is helper function to convert the array of Chunks to a single byte array
func (file DFSFileT) ChunkToByte() (err error) {
	const ChunkLength = 32

	var byteArr [len(file.data)]byte

	for i, chunk := range file.data {
		for j := 0; j < ChunkLength; j++ {
			byteArr[ChunkLength*i+j] = chunk[j]
		}
	}
}

func (file DFSFileT) Write(chunkNum uint8, chunk *Chunk) (err error) {
	file.data[chunkNum] = *chunk

	file.data
	err = ioutil.WriteFile("somepath/"+file.name, chunk, 0644)
	return nil
}

func (file DFSFileT) Close() (err error) {
	return nil
}

// DFSInstance is the implementation of the DFS Interface
type DFSInstance struct {
	id        int
	localIP   string
	localPath string
}

// LocalFileExists returns bool whether file is on client's disk
func (dfs DFSInstance) LocalFileExists(fname string) (exists bool, err error) {
	_, err = os.Stat(dfs.localPath + fname)

	if err == nil {
		return true, nil
	}

	return false, FileDoesNotExistError(fname)
}

// GlobalFileExists returns bool whether file is in DFS
func (dfs DFSInstance) GlobalFileExists(fname string) (exists bool, err error) {
	existsLocally, err := dfs.LocalFileExists(fname)
	if existsLocally {
		return true, nil
	}

	return false, FileDoesNotExistError(fname)
}

// Open returns file of fname
func (dfs DFSInstance) Open(fname string, mode FileMode) (f DFSFile, err error) {

	return nil, FileDoesNotExistError(fname)
}

// UMountDFS unmounts the current DFS and cleans up its resources
func (dfs DFSInstance) UMountDFS() (err error) {
	return nil
}

// DFS Represents a connection to the DFS system.
type DFS interface {
	// Check if a file with filename fname exists locally (i.e.,
	// available for DREAD reads).
	//
	// Can return the following errors:
	// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
	LocalFileExists(fname string) (exists bool, err error)

	// Check if a file with filename fname exists globally.
	//
	// Can return the following errors:
	// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
	// - DisconnectedError
	GlobalFileExists(fname string) (exists bool, err error)

	// Opens a filename with name fname using mode. Creates the file
	// in READ/WRITE modes if it does not exist. Returns a handle to
	// the file through which other operations on this file can be
	// made.
	//
	// Can return the following errors:
	// - OpenWriteConflictError (in WRITE mode)
	// - DisconnectedError (in READ,WRITE modes)
	// - FileUnavailableError (in READ,WRITE modes)
	// - FileDoesNotExistError (in DREAD mode)
	// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
	Open(fname string, mode FileMode) (f DFSFile, err error)

	// Disconnects from the server. Can return the following errors:
	// - DisconnectedError
	UMountDFS() (err error)
}

// Open files and stuff
func Open(fname string, mode FileMode) (f DFSFile, err error) {
	regex, _ := regexp.Compile("^[a-z0-9]{1,16}$")

	if !regex.MatchString(fname) {
		return nil, BadFilenameError(fname)
	}

	if mode == READ {

	} else if mode == DREAD {

	} else if mode == WRITE {

	}

	return nil, nil
}

// DFSSingleton an application's unique library
var DFSSingleton *DFSInstance

// MountDFS is:
// The constructor for a new DFS object instance. Takes the server's
// IP:port address string as parameter, the localIP to use to
// establish the connection to the server, and a localPath path on the
// local filesystem where the client has allocated storage (and
// possibly existing state) for this DFS.
//
// The returned dfs instance is singleton: an application is expected
// to interact with just one dfs at a time.
//
// This call should succeed regardless of whether the server is
// reachable. Otherwise, applications cannot access (local) files
// while disconnected.
//
// Can return the following errors:
// - LocalPathError
// - Networking errors related to localIP or serverAddr
func MountDFS(serverAddr string, localIP string, localPath string) (dfs DFS, err error) {
	// TODO
	// For now return LocalPathError

	_, err = os.Stat(localPath)

	if err != nil {
		return nil, LocalPathError(localPath)
	}

	if DFSSingleton != nil {
		return DFSSingleton, nil
	}

	// Setup DFS
	// 1) Setup heartbeat to server (1 go routine)
	// 2) Setup connection to server ( this is a second thread ??? )
	// 2) Return dfs

	// 	conn = net.Listen(...)
	//   rpc.Register(...)
	//   go rpc.Accept(conn)
	//   server = rpc.Dial(...)
	//   id = fetchOldId()
	//   server.Call("Hello", {clientId: id, clientIp: ip})
	DFSSingleton = &DFSInstance{0, localIP, localPath}

	// 1) Continuousing try to connect to server

	return nil, LocalPathError(localPath)
}
