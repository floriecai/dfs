/*

Server that all DFS clients talk to

Usage:
go run server.go
*/

package main

// Expects dfslib.go to be in the ./dfslib/ dir, relative to
// this app.go file

import (
	"as2_g4w8/shared"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
)

// MaxClients is unique clients to ever connect to the server
const MaxClients = 16

type WriteState uint32

const (
	Unlocked WriteState = iota
	Locked
)

type Chunk [32]byte

// dfslib.DFSInstance
// FileInfo for identifying which clients own which files
type FileInfo struct {
	id      int
	version int
}

type FileData struct {
	chunkNum int
	filename string
}

type StackError string

func (e StackError) Error() string {
	return fmt.Sprintf("No items in the stack to pop or peek")
}

type VersionStack []FileOwnerMetadata

func (s *VersionStack) Push(metadata FileOwnerMetadata) {
	*s = append(*s, metadata)
}

func (s *VersionStack) Pop() (fData FileOwnerMetadata, err error) {
	var latestMetadata FileOwnerMetadata
	stack := *s
	if len(stack) == 0 {
		return latestMetadata, StackError("Can't pop")
	}

	latestMetadata = stack[len(*s)-1]
	*s = stack[0 : len(stack)-1]
	return latestMetadata, nil
}

func (s *VersionStack) Peek() (fData FileOwnerMetadata, err error) {
	var latestMetadata FileOwnerMetadata
	stack := *s
	if len(stack) == 0 {
		return latestMetadata, StackError("Can't peek")
	}

	latestMetadata = stack[len(*s)-1]
	return latestMetadata, nil
}

func (s *VersionStack) IsEmpty() bool {
	return len(*s) == 0
}

type WriterDeniedError string

func (e WriterDeniedError) Error() string {
	return fmt.Sprintf("Already has a writer")
}

type FileOwnerMetadata struct {
	owner   int // id of the client that owns the file
	version int // which chunk version, this is also the priority for the version tracking
}

type ClientInfo struct {
	ip        string
	localPath string
}

type ClientConn struct {
	conn      *rpc.Client
	localPath string
}

type ServerDfs struct {
	writeLock  WriteState
	versionMap map[string]VersionStack // filename to {owner, versionNum}
	writerMap  map[string]WriteState
	nextId     int
	clientToId map[ClientInfo]int
	idToClient map[int]ClientConn
}

type ChunkMap [32]VersionStack
type ServerState struct {
	writerMap  map[string]WriteState // filename to it's write state
	versionMap map[string]ChunkMap   // filename to {owner, versionNum}
	nextId     int
	clientToId map[ClientInfo]int
	idToClient map[int]ClientConn
}

var dfsState ServerState

func (dfs *ServerDfs) Read(args shared.FileArgs, reply *shared.FileReply) error {

}

func (dfs *ServerDfs) RequestWrite(args shared.FileArgs, reply *shared.WriteRequestReply) error {
	isLocked := dfsState.writerMap[args.Filename]
	reply.CanWrite = atomic.CompareAndSwapInt32(&isLocked, Unlocked, Locked)
	// if !atomic.CompareAndSwapInt32(&isLocked, Unlocked, Locked) {
	// 	reply.CanWrite = false
	// } else {
	// 	reply.CanWrite = true
	// }

	return nil
}

func (dfs *ServerDfs) Write(chunkNum uint8, chunk *Chunk, fname string, clientId int) error {
	// Give lock
	// writeLock.Lock()
	// open connection for closing call // Will call RPC Write here
	// listen on closing call
	// Update version map
	// versionStack := versionMap[fname]
	// latestVersion := versionStack.Peek().version + 1
	// newMetaData := FileOwnerMetaData{id: clientId, version: latestVersion}
	// versionStack.Push(newMetaData)

	// writeLock.Unlock()
	// Unlock

}

// ****************** RPC METHODS THAT CLIENT CALLS *********** //
// func (t *T) MethodName(argType T1, replyType *T2) error
func (dfs *ServerDfs) GetBestChunk(args *shared.FileArgs, reply *shared.FileReply) error {
	versionStack := dfsState.versionMap[args.Filename][args.ChunkNum]
	for !versionStack.IsEmpty() {
		latestFileData, _ := versionStack.Pop()
		clientInfo := dfsState.idToClient[latestFileData.owner]

		e := clientInfo.conn.Call("GetFile", args, reply)

		if e == nil {
			return nil
		}
	}
	return nil // TODO No file to get
}

func (dfs *ServerDfs) GetLatestChunk(args *shared.FileArgs, reply *shared.FileReply) error {
	versionStack := dfsState.versionMap[args.Filename][args.ChunkNum]
	latestFileData, _ := versionStack.Peek()

	// Attempt to retrieve this chunk version from Client B
	clientInfo := dfsState.idToClient[latestFileData.owner]

	e := clientInfo.conn.Call("GetFile", args, reply)

	if e != nil {
		// TODO return error?
		// *reply = nil
		return e
	}

	fmt.Println("Got the latest file .... %s")

	// Now Client B also has a version so push onto the stack
	versionStack.Push(FileOwnerMetadata{args.ClientId, latestFileData.version})
	return nil
}

func (dfs *ServerDfs) GetLatestFile(args *shared.FileArgs, reply *shared.FileReply) error {
	for i := 0; i < 32; i++ {
		latestVersion, err := dfsState.versionMap[args.Filename][i].Peek()
		latestVersionOwner := latestVersion.owner
		// Server calls Client's RPC of latestVersion.owner

		clientConnInfo := dfsState.idToClient[latestVersionOwner]
		// var clientRpcArgs = shared.FileArgs{args.ClientId, args.chunkNum,}
		var clientRpcReply shared.FileReply
		err = clientConnInfo.conn.Call("ClientDfs.GetChunk", args, &clientRpcReply)

		if err != nil {
			fmt.Println("Wtf happened in GetLatestFile")
			return err
		}
	}
}

func (dfs *ServerDfs) InitiateRPC(args *shared.InitiateArgs, reply *shared.InitiateReply) error {
	client, err := rpc.Dial("tcp", args.Ip)
	RecordClientInfo(client, args, reply, dfs)

	reply.Connected = true
	// err = client.Call("ClientDfs.PrintTest", args, reply)

	if err != nil {
		log.Fatal("CLIENT DFS PRINT TEST FAILED\n")
	}
	// log.Println("REply is :::::: %+v", *reply)
	return err
}

func (dfs *ServerDfs) DoesFileExist(args *shared.FileExistArgs, reply *bool) error {
	versionStack := dfsState.versionMap[args.Filename] // If it's not in the map, it doesn't exist!
	if versionStack == nil {
		*reply = true
	} else {
		*reply = false
	}
	// empty := versionStack.IsEmpty()
	// *reply = empty
	return nil
}

// **************** RPC METHODS THAT CLIENT CALLS END *********** //

// Returns unique id of this client
func RecordClientInfo(clientConn *rpc.Client, args *shared.InitiateArgs, reply *shared.InitiateReply, dfs *ServerDfs) {
	var clientInfo = ClientInfo{ip: args.Ip, localPath: args.LocalPath}
	clientId := dfsState.clientToId[clientInfo]

	if clientId == 0 {
		clientId = dfsState.nextId
		dfsState.clientToId[clientInfo] = clientId
		dfsState.idToClient[clientId] = ClientConn{conn: clientConn, localPath: args.LocalPath}
		dfsState.nextId++
	}

	reply.Id = clientId

}

func main() {
	// serverIp := os.Args[1]

	var logger = log.New(os.Stdout, "[416_a2] ", log.Lshortfile)
	dfsImpl := new(ServerDfs)
	rpc.Register(dfsImpl)

	// Initialize data structures

	mutex := &sync.Mutex{}
	dfsState = ServerState{
		writeLock:  mutex,
		versionMap: make(map[string]VersionStack),
		nextId:     1,
		clientToId: make(map[ClientInfo]int),
		idToClient: make(map[int]ClientConn)}

	// l, err := net.Listen("tcp", serverIp)
	l, err := net.Listen("tcp", ":8257")
	if err != nil {
		logger.Fatal(err)
	}

	// Go routine to accept incoming connections
	go func(l net.Listener) {
		for {
			logger.Println("Printing....")
			conn, _ := l.Accept()
			rpc.ServeConn(conn)
		}
	}(l)

	for {
	}
}
