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
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
)

// MaxClients is unique clients to ever connect to the server
const MaxClients = 16
const ChunkSize = 32
const NumChunks = 256
const WriteTimeout = 10000 // ms

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

type StackReturnError string

func (e StackReturnError) Error() string {
	return fmt.Sprintf("Stack return no items to pop or peek")
}

// Max 16 Clients, so 16 unique versions, but each client can have a version
// i.e. C1 and C2 could have V2, both on the stack
// so max 256 items in this stack
// When pushing, must check for previous version with SAME owner
type VersionStack []FileOwnerMetadata

// If there's 1 item in the list that is v0, it is trivial
func (s *VersionStack) IsTrivial() bool {
	return len(*s) == 1 && (*s)[0].version == 0
}

func (s *VersionStack) Push(metadata FileOwnerMetadata) {
	// Scan for previous versions of same owner

	endPos := len(*s) - 1
	stack := *s
	for i, fMetadata := range stack {
		if fMetadata.owner == metadata.owner {
			if i == endPos {
				head := stack[:endPos]
				*s = append(head, metadata)
			} else if i == 0 {
				head := stack[i+1:]
				*s = append(head, fMetadata)
			} else {
				head := stack[:i]
				tail := stack[i+1:]
				*s = append(append(head, tail...), metadata)
			}

			return
		}
	}

	updatedStack := *s
	*s = append(updatedStack, metadata)
}

func (s *VersionStack) Pop() (fData FileOwnerMetadata, err error) {
	var latestMetadata FileOwnerMetadata
	stack := *s
	if len(stack) == 0 {
		return latestMetadata, StackError("Can't pop")
	}

	latestMetadata = stack[len(stack)-1]
	*s = stack[:len(stack)-1]
	return latestMetadata, nil
}

func (s *VersionStack) Peek() (fData FileOwnerMetadata, err error) {
	var latestMetadata FileOwnerMetadata
	stack := *s
	if len(stack) == 0 {
		return latestMetadata, StackError("Can't peek")
	}

	latestMetadata = stack[len(stack)-1]
	return latestMetadata, nil
}

func (s *VersionStack) IsEmpty() bool {
	return len(*s) == 0
}

type WriterDeniedError string

func (e WriterDeniedError) Error() string {
	return fmt.Sprintf("Already has a writer")
}

type NoFileError string

func (e NoFileError) Error() string {
	return fmt.Sprintf("No file")
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

type WriterInfo struct {
	IsLocked bool
	ClientId int
}
type ServerDfs struct {
	writerMap     map[string]WriterInfo        // filename to it's write state
	versionMap    map[string][256]VersionStack // filename to {owner, versionNum}
	nextId        int
	clientToId    map[ClientInfo]int
	idToClient    map[int]ClientConn
	files         map[string]bool // files that exist
	heartBeatMap  map[int]shared.HeartBeatData
	writeMutex    *sync.Mutex
	logger        *log.Logger
	heartbeatAddr string
}

// ****************** RPC METHODS THAT CLIENT CALLS *********** //
// func (t *T) MethodName(argType T1, replyType *T2) error
func (dfs *ServerDfs) RegisterFile(args *shared.FileArgs, reply *shared.FileExistsReply) error {
	fname := args.Filename
	dfs.files[fname] = true

	var versionStacks [256]VersionStack
	for i := 0; i < NumChunks; i++ {
		versionStack := &VersionStack{}
		var metaData = FileOwnerMetadata{owner: args.ClientId, version: 0}
		versionStack.Push(metaData)

		versionStacks[i] = *versionStack
	}

	dfs.versionMap[args.Filename] = versionStacks

	*reply = true

	return nil
}

func (dfs *ServerDfs) DoesFileExist(args *shared.FileExistsArgs, reply *shared.FileExistsReply) error {
	hasFile := dfs.files[string(*args)]
	*reply = shared.FileExistsReply(hasFile)
	return nil
}

func (dfs *ServerDfs) ConfirmWrite(args *shared.FileArgs, reply *shared.WriteRequestReply) error {
	versionStack := dfs.versionMap[args.Filename][args.ChunkNum]
	versionStackCopy := new(VersionStack)
	*versionStackCopy = versionStack

	for !versionStackCopy.IsEmpty() {
		currentFileData, _ := versionStackCopy.Pop()
		if currentFileData.owner == args.ClientId {
			reply.Version = currentFileData.version
			return nil
		}
	}

	return shared.LatestChunkUnavailable("Could not confirm write")
}

func (dfs *ServerDfs) WriteChunk(args *shared.FileArgs, reply *shared.WriteRequestReply) error {
	lockerHolder := dfs.writerMap[args.Filename].ClientId

	// Times up sir. No writes for you.
	if lockerHolder != args.ClientId {
		reply.CanWrite = false
		return shared.NoWriteLockError(string(args.ClientId))
	}

	versionMap := dfs.versionMap[args.Filename]
	latestMetadata, err := versionMap[args.ChunkNum].Peek()

	// dfs.logger.Printf("Latest meta is: %v\n", latestMetadata)
	if err != nil {
		return err
	}
	newVersion := latestMetadata.version + 1
	metadata := FileOwnerMetadata{owner: args.ClientId, version: newVersion}
	versionMap[args.ChunkNum].Push(metadata)

	reply.CanWrite = true
	reply.Version = newVersion

	return nil
}
func (dfs *ServerDfs) CloseWrite(args shared.FileArgs, reply *shared.WriteRequestReply) error {
	delete(dfs.writerMap, args.Filename)
	reply.CanWrite = false
	return nil
}

func (dfs *ServerDfs) RequestWrite(args shared.FileArgs, reply *shared.WriteRequestReply) error {
	// dfs.logger.Println("Request write .... by %d", args.ClientId)
	dfs.writeMutex.Lock()
	writerInfo := dfs.writerMap[args.Filename]
	// dfs.logger.Println("Writer info .... by %v", writerInfo)
	// dfs.logger.Println("printing writer map: %+v\n\n", dfs.writerMap)
	defer dfs.writeMutex.Unlock()

	// Check if it's the same ID, maybe someone wants to open it twice ....
	if writerInfo.IsLocked && writerInfo.ClientId != args.ClientId {
		// dfs.logger.Println("Write stack is locked")
		// Check if that client is even alive
		writerConn := dfs.idToClient[args.ClientId].conn

		if writerConn == nil {
			// dfs.logger.Println("Person holding lock does'nt exist")
			dfs.writerMap[args.Filename] = WriterInfo{ClientId: args.ClientId, IsLocked: false}
			// tell that client that it's taking too long.
			reply.CanWrite = true
		} else {
			// dfs.logger.Println("LOCKED BY ANOTHER PERSON")
			reply.CanWrite = false
		}
	} else {
		// dfs.logger.Println("Write stack NOT locked")
		dfs.writerMap[args.Filename] = WriterInfo{ClientId: args.ClientId, IsLocked: true}
		reply.CanWrite = true
	}

	return nil
}

func (dfs *ServerDfs) GetBestChunk(args *shared.FileArgs, reply *shared.ChunkReply) error {
	versionStack := dfs.versionMap[args.Filename][args.ChunkNum]

	versionStackCopy := new(VersionStack)
	*versionStackCopy = versionStack

	hasTrivial := versionStackCopy.IsTrivial()

	if hasTrivial {
		// Just return an empty copy
		var trivialChunk [ChunkSize]byte
		trivialData := make([]byte, ChunkSize)
		copy(trivialChunk[:], trivialData)

		reply.Data = shared.Chunk(trivialChunk)
		return nil
	} else {
		for !versionStackCopy.IsTrivial() && !versionStackCopy.IsEmpty() {
			latestFileData, _ := versionStackCopy.Pop()
			clientConnInfo := dfs.idToClient[latestFileData.owner]

			var getChunkReply shared.ChunkReply

			if clientConnInfo.conn == nil {
				return shared.BestChunkUnavailable(string(args.ChunkNum))
			}
			err := clientConnInfo.conn.Call("ClientDfs.GetChunk", args, &getChunkReply)

			if err == nil {

				var temp []byte
				copy(temp[:], getChunkReply.Data[:])
				*reply = getChunkReply

				newVersionMetadata := FileOwnerMetadata{owner: args.ClientId, version: latestFileData.version + 1}

				versionStack.Push(newVersionMetadata)

				return err // Found the latest chunk
			}

			dfs.logger.Println("Error is : %s", err.Error())
		}

		return shared.BestChunkUnavailable(args.Filename) // TODO No file to get
	}
}

// Gets the latest version of each chunk of a file.
// Throws BestChunkUnavailableError if the only chunk available is a trivial version
// and there were non-trivial versions (but were disconnected)
func (dfs *ServerDfs) GetBestFile(args *shared.FileArgs, reply *shared.FileReply) error {
	dfs.logger.Printf("GetBestFile ....... args: %+v\n\n", *args)

	var fileData [NumChunks]shared.Chunk
	var versionData [256]int
	for i := 0; i < NumChunks; i++ {
		versionMapCopy := new(VersionStack)
		*versionMapCopy = dfs.versionMap[args.Filename][i]

		hasTrivial := versionMapCopy.IsTrivial()
		var foundChunk bool

		if hasTrivial {
			// Just return an empty copy
			var trivialChunk [ChunkSize]byte
			trivialData := make([]byte, ChunkSize)
			copy(trivialChunk[:], trivialData)
			fileData[i] = trivialChunk
		} else {
			// We want non-trivial versions if there is one
			for !versionMapCopy.IsTrivial() && !versionMapCopy.IsEmpty() {
				latestFileData, err := versionMapCopy.Pop()
				clientConnInfo := dfs.idToClient[latestFileData.owner]

				// Server calls Client's RPC of latestVersion.owner
				var getChunkReply shared.ChunkReply

				if clientConnInfo.conn == nil {
					return shared.BestChunkUnavailable("Client with chunk is")
				}

				err = clientConnInfo.conn.Call("ClientDfs.GetChunk", args, &getChunkReply)

				// Found the chunk
				if err == nil {
					foundChunk = true
					fileData[i] = getChunkReply.Data
					versionData[i] = latestFileData.version
					break
				}
			}

			if !foundChunk {
				return shared.BestChunkUnavailable(args.Filename)
			}
		}
	}

	reply.Filename = args.Filename
	reply.Data = fileData
	return nil
}

func (dfs *ServerDfs) CloseConnection(args *shared.CloseArgs, reply *shared.CloseReply) error {
	delete(dfs.idToClient, args.Id) // Remove from the list of connections
	reply.Closed = true

	// Release all locks
	for filename, writerInfo := range dfs.writerMap {
		if writerInfo.ClientId == args.Id {
			delete(dfs.writerMap, filename)
		}
	}

	return nil
}

func (dfs *ServerDfs) InitiateRPC(args *shared.InitiateArgs, reply *shared.InitiateReply) error {
	client, err := rpc.Dial("tcp", args.Ip)

	if err != nil {
		return err
	}

	var clientId int
	if args.ClientId == 0 {
		clientId = dfs.nextId
		dfs.nextId++
	} else {
		clientId = args.ClientId
	}

	RecordClientInfo(client, args, reply, dfs, clientId)
	reply.Connected = true
	return nil
}

// **************** RPC METHODS THAT CLIENT CALLS END *********** //

// Returns unique id of this client
func RecordClientInfo(clientConn *rpc.Client, args *shared.InitiateArgs, reply *shared.InitiateReply, dfs *ServerDfs, clientId int) {
	var clientInfo = ClientInfo{ip: args.Ip, localPath: args.LocalPath}
	dfs.clientToId[clientInfo] = clientId
	dfs.idToClient[clientId] = ClientConn{conn: clientConn, localPath: args.LocalPath}

	reply.Id = clientId
}

func main() {
	serverAddr := os.Args[1]

	writerMutex := &sync.Mutex{}
	var logger = log.New(os.Stdout, "[416_a2] ", log.Lshortfile)
	dfs := &ServerDfs{
		writerMap:    make(map[string]WriterInfo),
		versionMap:   make(map[string][256]VersionStack),
		nextId:       1,
		clientToId:   make(map[ClientInfo]int),
		idToClient:   make(map[int]ClientConn),
		files:        make(map[string]bool), //
		heartBeatMap: make(map[int]shared.HeartBeatData),
		writeMutex:   writerMutex,
		logger:       logger}

	rpc.Register(dfs)

	l, err := net.Listen("tcp", serverAddr)
	if err != nil {
		logger.Fatal(err)
	}

	// TODO fcai use this instead
	// heartbeatListener, err := net.Listen("udp", serverAddr)

	// Bind address for server to listen to heartbeats
	go func(dfs *ServerDfs) {
		// hbFile, err := os.Create("./heartbeats.json")

		if err != nil {
			fmt.Println("Couldn't create heartbeats.json")
			return
		}

		serverAddr := "127.0.0.1:9482"
		localAddr, _ := net.ResolveUDPAddr("udp", serverAddr)
		hListener, err := net.ListenUDP("udp", localAddr)

		if err != nil {
			logger.Fatal(err)
		}

		defer hListener.Close()

		udpBuf := make([]byte, 1024)

		for {
			// Reading heartbeat msgs from connection
			n, _, err := hListener.ReadFromUDP(udpBuf)

			if err != nil {
				dfs.logger.Println("Error reading from UDP")
			}

			var heartBeatJson shared.HeartBeatData
			json.Unmarshal(udpBuf[:n], &heartBeatJson)

			dfs.logger.Println("PRINTING HEARTBEATS: %#v", heartBeatJson)
			fmt.Println("after printing the heartbeats  the go routine ........ ")
			dfs.heartBeatMap[heartBeatJson.ClientId] = heartBeatJson
		}

	}(dfs)

	// Go routine to accept incoming connections
	// go func(l net.Listener) {
	for {
		conn, _ := l.Accept()
		go rpc.ServeConn(conn)
	}
	// }(l)
}

func checkError(err error) error {
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error ", err.Error())
		return err
	}
	return nil
}
