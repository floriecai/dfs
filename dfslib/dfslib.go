/*

This package specifies the application's interface to the distributed
file system (DFS) system to be used in assignment 2 of UBC CS 416
2017W2.

*/

package dfslib

import (
	"as2_g4w8/shared"
	"encoding/json"
	"fmt"
	"io/ioutil"
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

	//
	DEFAULT
)

const LogFile = "dfs.log"

type IdFile struct {
	Id        int    `json:"id"`
	LocalAddr string `json:"localAddr`
}

func (f IdFile) toString() string {
	return toJson(f)
}

func toJson(f interface{}) string {
	bytes, err := json.Marshal(f)
	if err != nil {
		checkError(err)
		return ""
	}

	return string(bytes)
}

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
	return fmt.Sprintf("DFS: Latest version of chunk [%s] unavailable", string(e))
}

// Contains filename
type OpenWriteConflictError string

func (e OpenWriteConflictError) Error() string {
	return fmt.Sprintf("DFS: Filename [%s] is opened for writing by another client", string(e))
}

// Contains file mode that is bad.
type BadFileModeError string

func (e BadFileModeError) Error() string {
	return fmt.Sprintf("DFS: Cannot perform this operation in current file mode [%d]", string(e))
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

type FileNotOpen string

func (e FileNotOpen) Error() string {
	return fmt.Sprintf("DFS: Cannot perform action: [%s] on file as because it is not opened", string(e))
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
	Filepath  string
	Filename  string
	Data      [256]shared.Chunk
	ClientId  int
	Mode      FileMode
	Server    *rpc.Client
	IsInvalid bool
	WriteLock bool
}

// Can return the following errors:
// - DisconnectedError (in READ,WRITE modes)
// - ChunkUnavailableError (in READ,WRITE modes)
// Added: FileNotOpen
func (file DFSFileT) Read(chunkNum uint8, chunk *Chunk) error {
	// When reading, must always check server to see if it has latest version
	// if local has latest version, server must block other writes (but allow reads)
	// if local doesn't have latest version, server

	var fileArgs = &shared.FileArgs{ClientId: file.ClientId, ChunkNum: chunkNum, Filename: file.Filename}

	var chunkReply shared.ChunkReply

	isDisconnected := file.IsInvalid

	if file.Mode == WRITE || file.Mode == READ {
		if isDisconnected {
			return DisconnectedError(file.ClientId)
		}

		log.Println("Read is getting best chunks %+v", fileArgs)
		err := file.Server.Call("ServerDfs.GetBestChunk", fileArgs, &chunkReply)
		*chunk = Chunk(chunkReply.Data)

		checkError(err)
		if err != nil {
			return ChunkUnavailableError(chunkNum)
		}

	} else if file.Mode == DREAD {
		*chunk = Chunk(file.Data[chunkNum])
		return nil

		// // DREAD Mode
		// if isDisconnected {
		// 	// Get Local File

		// }

		// err := file.Server.Call("ServerDfs.GetBestChunk", fileArgs, &chunkReply)
		// *chunk = Chunk(chunkReply.Data)

		// if err != nil {
		// 	return ChunkUnavailableError(chunkNum)
		// }

	} else {
		if isDisconnected {
			return DisconnectedError(file.ClientId)
		}
	}
	return nil
}

// - BadFileModeError (in READ,DREAD modes)
// - DisconnectedError (in WRITE mode)
// - WriteModeTimeoutError (in WRITE mode)
func (file DFSFileT) Write(chunkNum uint8, chunk *Chunk) error {
	log.Println("Write in file .....")

	if file.IsInvalid {
		return DisconnectedError(file.Filename)
	}

	if file.Mode != WRITE {
		return BadFileModeError(file.Mode)
	}

	isTimedOut := false
	if isTimedOut {
		return WriteModeTimeoutError(file.Filename)
	}

	// Write locally
	// Write to log
	// Wait to receive confirmation from server
	// Receives, then return

	path := filepath.Join(file.Filepath, file.Filename+".dfs")
	fmt.Println("Write ... path is: %s", path)
	localFile, err := os.OpenFile(path, os.O_WRONLY, 0644)

	defer localFile.Close()

	if err != nil {
		checkError(err)
		fmt.Println("Write .... open file error ....")
		return nil
	}

	var writeBuf []byte = make([]byte, 32)
	copy(writeBuf[:], (*chunk)[:])
	n, err := localFile.WriteAt(writeBuf, int64(chunkNum)*ChunkSize)

	fmt.Println("Write offset is ... %d, %d bytes written", int64(chunkNum)*ChunkSize, n)
	localFile.Sync()

	if err != nil {
		checkError(err)
		fmt.Println("Write ... error at WriteAt ...")
		return nil
	}

	//TODO
	// Write to log

	fileArgs := &shared.FileArgs{ClientId: file.ClientId, ChunkNum: chunkNum, Filename: file.Filename}
	var reply shared.WriteRequestReply
	err = file.Server.Call("ServerDfs.WriteChunk", fileArgs, &reply)

	fmt.Println("Write ... reply is: %t", reply.CanWrite)
	if err != nil {
		checkError(err)
		return err
	}

	return nil
}

func (file DFSFileT) Close() error {
	isDisconnected := file.IsInvalid
	if isDisconnected {
		return DisconnectedError(file.ClientId)
	}

	if file.Mode == DEFAULT {
		// It's already closed
		return FileNotOpen("Close")
	}
	if file.Mode == WRITE {
		// Return the write lock

	}

	file.IsInvalid = isDisconnected
	file.Mode = DEFAULT
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
	id             int
	localIp        string
	localPath      string
	server         *rpc.Client
	modeMap        map[string]FileMode // Whenever a file is opened, record what mode it is in
	isDisconnected bool
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

	isDisconnected := false
	if isDisconnected {
		return false, DisconnectedError(fname)
	}

	var existReply shared.FileExistsReply
	args := shared.FileExistsArgs(fname)

	err = dfs.server.Call("ServerDfs.DoesFileExist", &args, &existReply)

	if err != nil {
		checkError(err)
		fmt.Println("Error in GlobalFileExists")
	}

	if existReply {
		return bool(existReply), err
	}

	return false, err
}

// UMountDFS unmounts the current DFS and cleans up its resources
func (dfs DFSInstance) UMountDFS() (err error) {

	if dfs.isDisconnected {
		return DisconnectedError("Server")
	}
	for v, k := range dfs.modeMap {
		if k == WRITE {
			// Release all write locks
		}

		delete(dfs.modeMap, v)
	}

	dfs.modeMap = make(map[string]FileMode)
	return nil
}

// - OpenWriteConflictError (in WRITE mode)
// - DisconnectedError (in READ,WRITE modes)
// - FileUnavailableError (in READ,WRITE modes)
// - FileDoesNotExistError (in DREAD mode)
func (dfs DFSInstance) Open(fname string, mode FileMode) (f DFSFile, err error) {
	dfsFile := DFSFileT{
		Mode:     mode,
		Filename: fname,
		ClientId: dfs.id,
		Server:   dfs.server,
		Filepath: dfs.localPath}

	if !isValidFilename(fname) {
		return dfsFile, BadFilenameError(fname)
	}

	fileExists, err := dfs.GlobalFileExists(fname)

	args := &shared.FileArgs{ClientId: dfs.id, Filename: fname}

	err, isDisconnected := err.(DisconnectedError)
	dfs.isDisconnected = isDisconnected
	dfsFile.IsInvalid = isDisconnected
	if isDisconnected {
		switch mode {
		case READ:
			return dfsFile, DisconnectedError(mode)
		case WRITE:
			return dfsFile, DisconnectedError(mode)
		case DREAD:

			existsLocally, _ := dfs.LocalFileExists(fname)

			if !existsLocally {
				return dfsFile, FileDoesNotExistError(fname)
			}

			filePath := filepath.Join(dfs.localPath, fname+".dfs")
			file, _ := os.Open(filePath)

			chunkData, _ := getChunkedFile(filePath)

			defer file.Close()
			// // TODO fcai - remove later
			// if err != nil {
			// 	log.Println("Error while chunkifying file in Open(D) file: %s", fname)
			// }

			dfsFile.Data = chunkData
			dfsFile.Filename = fname

			return dfsFile, nil
		default:
			return dfsFile, BadFileModeError(mode)
		}
	}

	// CONNECTED MODE
	if !fileExists {

		fmt.Println("Before storeFileOnServer .... id is: %+v", *args)
		err = storeFileOnServer(fname, dfs.localPath, dfs.server, args, &dfsFile)

		if err != nil {
			// Became disconnected while trying to register new file to server
			return dfsFile, DisconnectedError(fname)
		}
		return dfsFile, nil
	}

	dfs.modeMap[fname] = mode
	switch mode {
	case READ:
		fmt.Println("Reading...")
		err = dfs.server.Call("ServerDfs.GetBestFile", args, &dfsFile)

		if err != nil {
			return dfsFile, FileUnavailableError(fname)
		}

		return dfsFile, nil

	case DREAD:
		fmt.Println("DReading...")
		err = dfs.server.Call("ServerDfs.GetBestFile", args, &dfsFile)

		if err != nil {
			return dfsFile, FileUnavailableError(fname)
		}

		return dfsFile, nil

	case WRITE:
		fmt.Println("Writing...")

		// If file exists on the server, request the write lock
		fileWriteRequest := &shared.FileArgs{ClientId: dfs.id, Filename: fname}
		var writeReply shared.WriteRequestReply
		dfs.server.Call("ServerDfs.RequestWrite", fileWriteRequest, &writeReply)

		if writeReply.CanWrite {
			err = dfs.server.Call("ServerDfs.GetBestFile", args, &dfsFile)
			dfsFile.WriteLock = true
			return dfsFile, err
		}

		return dfsFile, OpenWriteConflictError(fname)
	default:
		return dfsFile, BadFileModeError(mode)
	}
}

type ClientDfs struct {
	localPath string
	ip        string
}

// func (dfs *ClientDfs) PrintTest(args *shared.InitiateArgs, reply *shared.InitiateReply) error {
// 	fmt.Println("PrintTest in client is: %+v", args)
// 	*reply = shared.InitiateReply{100, true}
// 	return nil
// }

// ************************  Helper funcs ************************** //

func storeFileOnServer(fname string, localPath string, server *rpc.Client, fileArgs *shared.FileArgs, dfsFile *DFSFileT) error {
	_, err := createFile(fname, localPath, ".dfs")
	if err != nil {
		log.Println("storeFileOnServer: Can't createFile")
		return err
	}

	fileChunks, _ := getChunkedFile(filepath.Join(localPath, fname+".dfs"))
	dfsFile.Data = fileChunks
	dfsFile.Filename = fname

	// Send to server that you created a file
	var fileExists shared.FileExistsReply
	err = server.Call("ServerDfs.RegisterFile", fileArgs, &fileExists)

	// Becomes disconnected
	if err != nil {
		checkError(err)
		log.Println("Error on storeFileOnServer")
	}
	return err
}

// Creates a new file that is written to disk
// Closes file
func createFile(fname string, localPath string, extension string) (file *os.File, err error) {
	path := filepath.Join(localPath, fname+extension)
	f, err := os.Create(path)

	if err != nil {
		return nil, err
	}

	defer f.Close()

	f.Sync()
	return f, err
}

// Create chunk given chunk offset
func chunkify(chunkNum uint8, filepath string) (data shared.Chunk, err error) {
	var chunk [ChunkSize]byte
	file, err := os.Open(filepath)

	defer file.Close()

	if err != nil {
		checkError(err)
		return chunk, err
	}

	chunkBuf := make([]byte, ChunkSize)
	offset := int64(chunkNum) * ChunkSize

	// _, err = file.Read(chunkBuf)
	// if err != nil {
	// 	fmt.Println("READ Error in chunkify...., %s", err.Error())
	// }
	// fmt.Println("Print entire file: %s", string(chunkBuf[offset:offset+32]))
	n, err := file.ReadAt(chunkBuf, offset)

	if err != nil {
		fmt.Printf("Read %d bytes .... buf len: %d , offset is: %d\n", n, len(chunkBuf), offset)
		checkError(err)
		return chunk, err
	}

	copy(chunk[:], chunkBuf[:])

	return chunk, nil
}

// Create lists of chunks for DFSFile
func getChunkedFile(filepath string) (data [NumChunks]shared.Chunk, err error) {
	var chunkData [NumChunks]shared.Chunk

	for i := 0; i < NumChunks; i++ {
		chunk, err := chunkify(uint8(i), filepath)

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
	filepath := filepath.Join(dfs.localPath, args.Filename+".dfs")
	fmt.Printf("Printing filepath in getchunk ....... %s\n", filepath)

	chunk, err := chunkify(args.ChunkNum, filepath)
	reply.Data = chunk

	if err != nil {
		fmt.Println("Err in chunkify")
	}
	return err
}

// *************************** RPC THAT SERVER CALLS ENDS ****************** //
// dfsSingleton an application's unique library

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
	logger := log.New(os.Stdout, "[416 dfslib A2] ", log.LstdFlags)
	logger.SetFlags(log.LstdFlags | log.Lshortfile)
	_, err = os.Stat(localPath)

	if err != nil {
		return nil, LocalPathError(localPath)
	}

	// Check for existing id on disk

	dfsSingleton := DFSInstance{localIp: localIp, localPath: localPath}

	var clientId int

	idFilePath := filepath.Join(localPath, "id.json")
	_, err = os.Stat(idFilePath)
	isReconnectingClient := err == nil // File opened, contains id file

	generatedIp := localIp + ":0" // TODO - can't hardcode this
	fmt.Println("Is reconnecting : %t", isReconnectingClient)
	if isReconnectingClient {
		// Old client
		idFile, _ := os.Open(idFilePath)
		idBytes, _ := ioutil.ReadAll(idFile)
		var idFileJson IdFile
		json.Unmarshal(idBytes, &idFileJson)

		// id, _ := strconv.Atoi(string(idBytes))

		clientId = idFileJson.Id
		generatedIp = idFileJson.LocalAddr
	}

	conn, err := net.Listen("tcp", generatedIp)

	checkError(err)
	localAddr := conn.Addr()

	fmt.Printf("\n\nPRINTING NEW LOCAL ADDRESS ...... %s\n\n", localAddr.String())
	clientDfs := &ClientDfs{localPath: localPath, ip: localIp}
	rpc.Register(clientDfs)
	go rpc.Accept(conn)

	server, err := rpc.Dial("tcp", serverAddr)

	// Continuously keep trying to connect
	// if err != nil {
	// 	serverChannel := make(chan *rpc.Client)
	// 	go func(sAddr string) {
	// 		var server *rpc.Client
	// 		for server == nil {
	// 			server, err = rpc.Dial("tcp", sAddr)
	// 		}

	// 		serverChannel <- server
	// 	}(serverAddr)

	// 	server := <-serverChannel

	// }

	// dfsSingleton.server = server

	// New client, write it to disk
	if !isReconnectingClient {
		// Write id to disk
		var initReply shared.InitiateReply
		args := &shared.InitiateArgs{Ip: localAddr.String(), LocalPath: localPath}

		err = server.Call("ServerDfs.InitiateRPC", args, &initReply)

		logger.Printf("Printing reply.... %+v\n\n", initReply)

		_, err := createFile("id", localPath, ".json")

		if err != nil {
			logger.Fatal("WTF MAN .... Create the goddamn ID file")
		}

		oldIdFile, _ := os.OpenFile(idFilePath, os.O_WRONLY, 0644)
		idFileData := IdFile{Id: initReply.Id, LocalAddr: localAddr.String()}
		idData, err := json.Marshal(idFileData)

		if err != nil {
			checkError(err)
			fmt.Println("Json marshalling error ..... ")
		}

		n, err := oldIdFile.Write(idData)

		if err != nil {
			checkError(err)
			fmt.Printf("Write error again..... %v\n", n)
		}

		oldIdFile.Sync()
		defer oldIdFile.Close()
		clientId = initReply.Id
	}

	dfsSingleton.id = clientId
	dfsSingleton.server = server
	dfsSingleton.modeMap = make(map[string]FileMode)

	// 1) Continuousing try to connect to server

	log.Println("Printing dfs singleton ............ %+v", dfsSingleton)
	return dfsSingleton, nil
}

func checkError(err error) error {
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error ", err.Error())
		return err
	}
	return nil
}
