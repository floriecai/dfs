/*

This package specifies the application's interface to the distributed
file system (DFS) system to be used in assignment 2 of UBC CS 416
2017W2.

*/

package dfslib

import (
	"as2_g4w8/shared"
	"fmt"
	"log"
	"net"
	"net/rpc"
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
	return fmt.Sprintf("DFS: Latest verson of chunk [%d] unavailable", string(e))
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
	requestID int    // ID of who made the request on this file, need to know the local path ....
	name      string // filename
	data      []Chunk
}

func (file DFSFileT) Read(chunkNum uint8, chunk *Chunk) (err error) {
	// When reading, must always check server to see if it has latest version
	// if local has latest version, server must block other writes (but allow reads)
	// if local doesn't have latest version, server

	// do something like:
	// rpc.Call(READ, chunkNum, Chunk, file.name, file.id)
	return nil
}

// ChunkToByte is helper function to convert the array of Chunks to a single byte array
func (file *DFSFileT) ChunkToByte() (err error) {
	// const ChunkLength = 32

	// var byteArr [len(file.data)]byte

	// for i, chunk := range file.data {
	// 	for j := 0; j < ChunkLength; j++ {
	// 		byteArr[ChunkLength*i+j] = chunk[j]
	// 	}
	// }
	return nil
}

func (file *DFSFileT) Write(chunkNum uint8, chunk *Chunk) error {
	fileWriteRequest := shared.FileArgs{dfsSingleton.id, chunkNum, file.name}

	var writeReply shared.WriteRequestReply
	dfsSingleton.server.Call("ServerDfs.RequestWrite", fileWriteRequest, &writeReply)

	if writeReply.CanWrite {

		return nil
	}

	return OpenWriteConflictError(file.name)
}

func (file *DFSFileT) Close() (err error) {
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

type DFSInstance struct {
	id        int
	localIp   string
	localPath string
	server    *rpc.Client
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

	// dfs.server.Call("ServerDfs.")

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

// Open files and stuff
func Open(fname string, mode FileMode) (f DFSFile, err error) {
	regex, _ := regexp.Compile("^[a-z0-9]{1,16}$")

	if !regex.MatchString(fname) {
		return nil, BadFilenameError(fname)
	}

	fmt.Println(os.Getwd())
	fileExists, _ := dfsSingleton.GlobalFileExists(fname)

	// if !fileExists {
	// 	newFile, err := os.Create("." + dfsSingleton.localPath + fname + ".dfs")
	// } else {
	// 	if mode == DREAD {
	// 		newFile, err := dfsSingleton.server.Call("ServerDfs.GetBestFile")
	// 	}
	// 	newFile, err := dfsSingleton.server.Call("ServerDfs.GetFile")
	// }

	args := shared.FileArgs{ClientId: dfsSingleton.id, ChunkNum: 0, Filename: fname}
	switch mode {
	case READ:
		fmt.Println("Reading...")
		if !fileExists {
			// create the file
		} else {
			var dfsFile DFSFileT
			dfsSingleton.server.Call("ServerDfs.GetLatestFile", args)
		}
	case DREAD:
		fmt.Println("Dreading...")
	case WRITE:
		// 1) Request write

		// chunkNum doesn't matter, I'm just reusing data structures
		fileWriteRequest := shared.FileArgs{dfsSingleton.id, 0, fname}
		var writeReply shared.WriteRequestReply
		dfsSingleton.server.Call("ServerDfs.RequestWrite", fileWriteRequest, &writeReply)

		if writeReply.CanWrite {

			// TODO fcai - maybe just simplify it so both calls always call server.rpc
			if dfsSingleton.LocalFileExists(fname) {
				// write directly
			}

			if dfsSingle.GlobalFileExists {

			}
		}

		return OpenWriteConflictError(file.name)
	default:
		return nil, nil
	}

	return nil, nil
}

type ClientDfs struct{}

// func (dfs *ClientDfs) PrintTest(args *shared.InitiateArgs, reply *shared.InitiateReply) error {
// 	fmt.Println("PrintTest in client is: %+v", args)
// 	*reply = shared.InitiateReply{100, true}
// 	return nil
// }

// *************************** RPC THAT SERVER CALLS *********************** //
func (dfs *ClientDfs) GetFile(args *shared.FileArgs, reply *shared.FileReply) error {
	return nil
}

// TODO fcai - i don't think we actually need this .................................
func (dfs *ClientDfs) WriteFile(args *shared.FileArgs, reply *shared.FileReply) error {
	return nil
}

// *************************** RPC THAT SERVER CALLS ENDS ****************** //
// dfsSingleton an application's unique library
var dfsSingleton *DFSInstance

// MountDFS is:
// The constructor for a new DFS object instance. Takes the server's
// IP:port address string as parameter, the localIp to use to
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
// - Networking errors related to localIp or serverAddr
func MountDFS(serverAddr string, localIp string, localPath string) (dfs DFS, err error) {
	logger := log.New(os.Stdout, "[416 dfslib A2] ", log.Lshortfile)
	_, err = os.Stat(localPath)

	if err != nil {
		return nil, LocalPathError(localPath)
	}

	if dfsSingleton != nil {
		logger.Println("Has dfs already")
		return *dfsSingleton, nil
	}

	// Setup DFS
	// 1) Setup heartbeat to server (1 go routine)
	// 2) Setup connection to server ( this is a second thread ??? )
	// 2) Return dfs

	generatedIP := localIp + ":1111" // TODO - can't hardcode this
	// tcpListener, _ := net.ResolveTCPAddr("tcp", generatedIP)
	conn, err := net.Listen("tcp", generatedIP)

	localAddr := conn.Addr()

	clientDfs := new(ClientDfs)
	rpc.Register(clientDfs)
	go rpc.Accept(conn)

	server, err := rpc.Dial("tcp", serverAddr)

	var initReply shared.InitiateReply
	args := shared.InitiateArgs{Ip: localAddr.String(), LocalPath: localPath}
	err = server.Call("ServerDfs.InitiateRPC", &args, &initReply)

	fmt.Printf("Printing reply.... %+v\n", initReply)
	dfsSingleton = &DFSInstance{id: initReply.Id, localIp: localIp, localPath: localPath, server: server}

	// 1) Continuousing try to connect to server

	return *dfsSingleton, nil
}
