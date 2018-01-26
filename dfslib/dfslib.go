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
	"path/filepath"
	"regexp"
)

const ChunkSize = 32
const NumChunks = 256
const FileSize = 8096

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

const LogFile = "dfs.log"

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
type BadFileModeError string

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
	Filename string
	Data     [256]shared.Chunk
	Mode     FileMode
}

// Can return the following errors:
// - DisconnectedError (in READ,WRITE modes)
// - ChunkUnavailableError (in READ,WRITE modes)
func (file DFSFileT) Read(chunkNum uint8, chunk *Chunk) error {
	// When reading, must always check server to see if it has latest version
	// if local has latest version, server must block other writes (but allow reads)
	// if local doesn't have latest version, server

	var fileArgs = &shared.FileArgs{ClientId: dfsSingleton.id, ChunkNum: chunkNum, Filename: file.Filename}
	var chunkReply shared.ChunkReply

	isDisconnected := false
	if file.Mode == WRITE || file.Mode == READ {
		if isDisconnected {
			return DisconnectedError(dfsSingleton.id)
		}

		err := dfsSingleton.server.Call("ServerDfs.GetLatestChunk", fileArgs, &chunkReply)
		*chunk = Chunk(chunkReply.Data)
		if err != nil {
			return ChunkUnavailableError(chunkNum)
		}

	} else {
		// DREAD Mode

		if isDisconnected {
			// Get Local File
		}

		err := dfsSingleton.server.Call("ServerDfs.GetBestChunk", fileArgs, &chunkReply)
		*chunk = Chunk(chunkReply.Data)

		if err != nil {
			return ChunkUnavailableError(chunkNum)
		}

	}
	return nil
}

// - BadFileModeError (in READ,DREAD modes)
// - DisconnectedError (in WRITE mode)
// - WriteModeTimeoutError (in WRITE mode)
func (file DFSFileT) Write(chunkNum uint8, chunk *Chunk) error {
	mode := dfsSingleton.modeMap[file.Filename]
	if mode != WRITE {
		return BadFileModeError(file.Filename)
	}

	isDisconnected := false
	if isDisconnected {
		return DisconnectedError(file.Filename)
	}

	isTimedOut := false
	if isTimedOut {
		return WriteModeTimeoutError(file.Filename)
	}

	// Write locally
	// Write to log
	// Wait to receive confirmation from server
	// Receives, then return
	return nil
}

func (file DFSFileT) Close() error {
	delete(dfsSingleton.modeMap, file.Filename)
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
	modeMap   map[string]FileMode // Whenever a file is opened, record what mode it is in
}

// Only allow alphanumeric files with all lower case, at most 16 letters
func isValidFilename(fname string) bool {
	regex, _ := regexp.Compile("^[a-z0-9]{1,16}$")
	return regex.MatchString(fname)
}

// LocalFileExists returns bool whether file is on client's disk
// Can return the following errors:
// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
func (dfs DFSInstance) LocalFileExists(fname string) (exists bool, err error) {
	if !isValidFilename(fname) {
		return false, BadFilenameError(fname)
	}

	_, err = os.Stat(dfs.localPath + fname + ".dfs")

	if err == nil {
		return true, nil
	}

	return false, nil
}

// GlobalFileExists returns bool whether file is in DFS
// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
// - DisconnectedError
func (dfs DFSInstance) GlobalFileExists(fname string) (exists bool, err error) {
	if !isValidFilename(fname) {
		return false, BadFilenameError(fname)
	}

	var existReply shared.FileExistsReply
	dfsSingleton.server.Call("ServerDfs.DoesFileExist", shared.FileArgs{Filename: fname}, &existReply)

	if existReply {
		return bool(existReply), nil
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

// - OpenWriteConflictError (in WRITE mode)
// - DisconnectedError (in READ,WRITE modes)
// - FileUnavailableError (in READ,WRITE modes)
// - FileDoesNotExistError (in DREAD mode)
func Open(fname string, mode FileMode) (f DFSFile, err error) {
	var dfsFile DFSFileT
	regex, _ := regexp.Compile("^[a-z0-9]{1,16}$")

	if !regex.MatchString(fname) {
		return nil, BadFilenameError(fname)
	}

	fmt.Println(os.Getwd())
	fileExists, _ := dfsSingleton.GlobalFileExists(fname)

	args := &shared.FileArgs{ClientId: dfsSingleton.id, Filename: fname}

	isDisconnected := false
	if isDisconnected {
		switch mode {
		case READ:
			return dfsFile, DisconnectedError(mode)
		case WRITE:
			return dfsFile, DisconnectedError(mode)
		case DREAD:
			file, err := os.Open(fname + ".dfs")

			if err != nil {
				return dfsFile, FileDoesNotExistError(fname)
			}
			chunkData, err := getChunkedFile(file)
			dfsFile.Data = chunkData
			dfsFile.Filename = fname

			return dfsFile, nil
		default:
			return dfsFile, BadFileModeError(mode)
		}
	}

	// CONNECTED MODE
	switch mode {
	case READ:
		fmt.Println("Reading...")
		if !fileExists {
			err = storeFileOnServer(fname, dfsSingleton.localPath, args, &dfsFile)
			return dfsFile, err
		}

		err = dfsSingleton.server.Call("ServerDfs.GetLatestFile", args, &dfsFile)

		if err != nil {
			return nil, FileUnavailableError(fname)
		}

		return dfsFile, nil

	case DREAD:
		if !fileExists {
			err = storeFileOnServer(fname, dfsSingleton.localPath, args, &dfsFile)
			return dfsFile, err
		}

		err = dfsSingleton.server.Call("ServerDfs.GetBestFile", args, &dfsFile)

		if err != nil {
			return nil, FileUnavailableError(fname)
		}

		return dfsFile, nil

	case WRITE:
		if !fileExists {
			err = storeFileOnServer(fname, dfsSingleton.localPath, args, &dfsFile)
			return dfsFile, err
		}

		// If file exists on the server, request the write lock
		fileWriteRequest := shared.FileArgs{ClientId: dfsSingleton.id, Filename: fname}
		var writeReply shared.WriteRequestReply
		dfsSingleton.server.Call("ServerDfs.RequestWrite", fileWriteRequest, &writeReply)

		if writeReply.CanWrite {
			err = dfsSingleton.server.Call("ServerDfs.GetBestFile", args, &dfsFile)
			return dfsFile, err
		}

		return dfsFile, OpenWriteConflictError(fname)
	default:
		return dfsFile, BadFileModeError(mode)
	}
}

type ClientDfs struct{}

// func (dfs *ClientDfs) PrintTest(args *shared.InitiateArgs, reply *shared.InitiateReply) error {
// 	fmt.Println("PrintTest in client is: %+v", args)
// 	*reply = shared.InitiateReply{100, true}
// 	return nil
// }
// ************************  Helper funcs ************************** //

func storeFileOnServer(fname string, localPath string, fileArgs *shared.FileArgs, dfsFile *DFSFileT) error {
	file, err := createFile(fname, dfsSingleton.localPath)
	if err != nil {
		return err
	}

	fileChunks, err := getChunkedFile(file)
	dfsFile.Data = fileChunks
	dfsFile.Filename = fname

	// Send to server that you created a file
	var fileExists shared.FileReply
	err = dfsSingleton.server.Call("ServerDfs.RegisterFile", fileArgs, &fileExists)

	return err
}

// Creates a new file that is written to disk
func createFile(fname string, localPath string) (file *os.File, err error) {
	path := filepath.Join(localPath, fname+".dfs")
	f, err := os.Create(path)

	if err != nil {
		return nil, err
	}

	defer f.Close()

	f.Sync()
	return f, nil
}

// Create chunk given chunk offset
func chunkify(chunkNum uint8, file *os.File) (data shared.Chunk, err error) {
	var chunk [ChunkSize]byte

	chunkBuf := make([]byte, ChunkSize)
	offset := int64(chunkNum) * ChunkSize
	_, err = file.ReadAt(chunkBuf, offset)

	if err != nil {
		return chunk, err
	}

	copy(chunk[:], chunkBuf)

	return chunk, nil
}

// Create lists of chunks for DFSFile
func getChunkedFile(file *os.File) (data [NumChunks]shared.Chunk, err error) {
	var chunkData [NumChunks]shared.Chunk
	for i := 0; i < NumChunks; i++ {
		chunk, err := chunkify(uint8(i), file)

		if err != nil {
			return chunkData, err
		}

		chunkData[i] = chunk
	}

	return chunkData, nil
}

// **********************  Helper funcs end************************* //

// *************************** RPC THAT SERVER CALLS *********************** //

func (dfs *ClientDfs) GetChunk(args *shared.FileArgs, reply *shared.ChunkReply) error {
	file, err := os.Open(args.Filename + ".dfs")

	if err != nil {
		return err
	}

	chunk, err := chunkify(args.ChunkNum, file)
	reply.Data = chunk
	return err
}

func (dfs *ClientDfs) GetFile(args *shared.FileArgs, reply *shared.FileReply) error {
	file, err := os.Open(args.Filename + ".dfs")

	if err != nil {
		return err
	}

	chunkedFile, err := getChunkedFile(file)

	if err != nil {
		return err
	}

	reply.Filename = args.Filename
	reply.Data = chunkedFile
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
