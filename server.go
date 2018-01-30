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
)

// MaxClients is unique clients to ever connect to the server
const MaxClients = 16
const ChunkSize = 32
const NumChunks = 256
const WriteTimeout = 10000 // ms

type HeartBeatData struct {
	ClientId int `json: "id"`
	// Timestamp time `json: "timestamp"`
}

type HeartBeatItem struct {
}
type WriteLocked bool

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
	fmt.Println("Before pop ....... %+v", *s)
	var latestMetadata FileOwnerMetadata
	stack := *s
	if len(stack) == 0 {
		return latestMetadata, StackError("Can't pop")
	}

	latestMetadata = stack[len(stack)-1]
	*s = stack[:len(stack)-1]
	fmt.Println("After pop ....... %+v", *s)
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
	openedFile    map[string]int  // filenames to clientIDs who have opened them
	files         map[string]bool // files that exist
	writeMutex    *sync.Mutex
	logger        *log.Logger
	heartbeatAddr string
}

// ****************** RPC METHODS THAT CLIENT CALLS *********** //
// func (t *T) MethodName(argType T1, replyType *T2) error
func (dfs *ServerDfs) RegisterFile(args *shared.FileArgs, reply *shared.FileExistsReply) error {
	fname := args.Filename
	dfs.files[fname] = true
	dfs.openedFile[fname] = args.ClientId

	var versionStacks [256]VersionStack
	for i := 0; i < NumChunks; i++ {
		versionStack := &VersionStack{}
		var metaData = FileOwnerMetadata{owner: args.ClientId, version: 0}
		versionStack.Push(metaData)

		versionStacks[i] = *versionStack
	}

	dfs.versionMap[args.Filename] = versionStacks

	dfs.logger.Println("VERSION MAP OF EVERYTHING IS: %+v \n\n", dfs.versionMap[args.Filename][0])
	*reply = true

	return nil
}

func (dfs *ServerDfs) DoesFileExist(args *shared.FileExistsArgs, reply *shared.FileExistsReply) error {
	dfs.logger.Println("Printing all existing files %+v", dfs.files)
	hasFile := dfs.files[string(*args)]
	*reply = shared.FileExistsReply(hasFile)
	return nil
}

func (dfs *ServerDfs) WriteChunk(args *shared.FileArgs, reply *shared.WriteRequestReply) error {
	versionMap := dfs.versionMap[args.Filename]
	latestMetadata, err := versionMap[args.ChunkNum].Peek()

	// dfs.logger.Printf("Latest meta is: %v\n", latestMetadata)
	if err != nil {
		return err
	}
	newVersion := latestMetadata.version + 1
	metadata := FileOwnerMetadata{owner: args.ClientId, version: newVersion}
	versionMap[args.ChunkNum].Push(metadata)

	dfs.logger.Printf("WriteChunk: version map is: %v\n", dfs.versionMap[args.Filename][0])
	reply.CanWrite = true
	reply.Id = dfs.nextId

	return nil
}
func (dfs *ServerDfs) CloseWrite(args shared.FileArgs, reply *shared.WriteRequestReply) error {
	dfs.logger.Println("WRITER MAP AFTER BEFORE CLOSE .... %#v", dfs.writerMap)
	delete(dfs.writerMap, args.Filename)
	dfs.logger.Println("WRITER MAP AFTER CLOSE .... %#v", dfs.writerMap)
	reply.CanWrite = false
	reply.Id = 100
	return nil
}

// func onlyOne() error {
// 	select {
// 	case <-ch:
// 		return nil

// 	default:
// 		return errors.New("Can't write")
// 	}
// }

// func getWriteLock(dfs *ServerDfs, fname string) bool {
// 	dfs.writeMutex.Lock()
// 	lockedState := dfs.writerMap[fname]

// 	defer dfs.writeMutex.Unlock()
// 	if lockedState.WriteState == Locked {
// 		return false
// 	} else {
// 		return true
// 	}
// }

func (dfs *ServerDfs) RequestWrite(args shared.FileArgs, reply *shared.WriteRequestReply) error {
	dfs.logger.Println("Request write .... by %d", args.ClientId)
	dfs.writeMutex.Lock()
	writerInfo := dfs.writerMap[args.Filename]
	dfs.logger.Println("Writer info .... by %v", writerInfo)
	dfs.logger.Println("printing writer map: %+v\n\n", dfs.writerMap)
	defer dfs.writeMutex.Unlock()

	// Check if it's the same ID, maybe someone wants to open it twice ....
	if writerInfo.IsLocked && writerInfo.ClientId != args.ClientId {
		dfs.logger.Println("Write stack is locked")
		// Check if that client is even alive
		writerConn := dfs.idToClient[args.ClientId].conn

		if writerConn == nil {
			dfs.logger.Println("Person holding lock does'nt exist")
			dfs.writerMap[args.Filename] = WriterInfo{ClientId: args.ClientId, IsLocked: false}
			// tell that client that it's taking too long.
			reply.CanWrite = true
		} else {
			dfs.logger.Println("LOCKED BY ANOTHER PERSON")
			reply.CanWrite = false
		}
	} else {
		dfs.logger.Println("Write stack NOT locked")
		dfs.writerMap[args.Filename] = WriterInfo{ClientId: args.ClientId, IsLocked: true}
		reply.CanWrite = true
	}

	dfs.logger.Printf("PRINTING UPDATED WRITE MAP: %v", dfs.writerMap)
	return nil
}

func (dfs *ServerDfs) GetBestChunk(args *shared.FileArgs, reply *shared.ChunkReply) error {
	versionStack := dfs.versionMap[args.Filename][args.ChunkNum]

	dfs.logger.Println("Version stack is ..... %+v", versionStack)
	// var versionStackCopy VersionStack
	// versionStackCopy = VersionStack{}
	// copy(versionStackCopy, versionStack)

	versionStackCopy := new(VersionStack)
	*versionStackCopy = versionStack

	hasTrivial := versionStackCopy.IsTrivial()

	if hasTrivial {
		dfs.logger.Println("CHUNK IS TRIVIAL ...... >>>>>")
		// Just return an empty copy
		var trivialChunk [ChunkSize]byte
		trivialData := make([]byte, ChunkSize)
		copy(trivialChunk[:], trivialData)

		reply.Data = shared.Chunk(trivialChunk)
		return nil
	} else {
		dfs.logger.Println("Printing version stack copy .... %+v", *versionStackCopy)
		for !versionStackCopy.IsTrivial() && !versionStackCopy.IsEmpty() {
			latestFileData, _ := versionStackCopy.Pop()
			clientConnInfo := dfs.idToClient[latestFileData.owner]

			dfs.logger.Println("Printing latestFile data: %+v", latestFileData)
			dfs.logger.Println("Printing ids: %+v", dfs.idToClient)

			dfs.logger.Println("GetBestChunk ... getting from: %d", clientConnInfo.localPath)
			var getChunkReply shared.ChunkReply
			// getChunkArgs := shared.FileArgs{ClientId: args.ClientId, ChunkNum: args.ChunkNum,Filename: }
			if clientConnInfo.conn == nil {
				dfs.logger.Println("\n>>>>>>>>>>>>>>>>>>>> CLIENT DOESNT EXIST >>>>>>>>>>>>>>>>>>>> \n>>>>>>>>>>")
			}
			err := clientConnInfo.conn.Call("ClientDfs.GetChunk", args, &getChunkReply)

			if err == nil {

				var temp []byte
				copy(temp[:], getChunkReply.Data[:])
				dfs.logger.Println("Got chunk reply ..... %s", string(temp))
				*reply = getChunkReply

				newVersionMetadata := FileOwnerMetadata{owner: args.ClientId, version: latestFileData.version + 1}

				versionStack.Push(newVersionMetadata)

				dfs.logger.Println("BestChunk mode: , ")
				return err // Found the latest chunk
			}

			dfs.logger.Println("Error is : %s", err.Error())
		}

		return shared.BestChunkUnavailable(args.Filename) // TODO No file to get
	}
}

func (dfs *ServerDfs) GetLatestChunk(args *shared.FileArgs, reply *shared.ChunkReply) error {
	versionMap := dfs.versionMap
	versionStack := versionMap[args.Filename][args.ChunkNum]
	latestFileData, _ := versionStack.Peek()

	// Attempt to retrieve this chunk version from Client B
	clientInfo := dfs.idToClient[latestFileData.owner]

	e := clientInfo.conn.Call("ClientDfs.GetChunk", args, reply)

	if e != nil {
		return e
	}

	// Now Client B also has a version so push onto the stack
	versionStack.Push(FileOwnerMetadata{args.ClientId, latestFileData.version})
	return nil
}

// Gets the latest version of each chunk of a file.
// Throws BestChunkUnavailableError if the only chunk available is a trivial version
// and there were non-trivial versions (but were disconnected)
func (dfs *ServerDfs) GetBestFile(args *shared.FileArgs, reply *shared.FileReply) error {
	dfs.logger.Printf("GetBestFile ....... args: %+v\n\n", *args)
	// dfs.logger.Printf("Version map dfs before ............ %+v", dfs.versionMap[args.Filename])
	var fileData [NumChunks]shared.Chunk

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
				err = clientConnInfo.conn.Call("ClientDfs.GetChunk", args, &getChunkReply)

				// dfs.logger.Println("Chunk reply is .... %+v", getChunkReply)
				if err != nil {
					dfs.logger.Println("Printing err .... %s, clientConn: %+v", err.Error())
				}

				// Found the chunk
				if err == nil {
					foundChunk = true
					fileData[i] = getChunkReply.Data
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
	// clientConn := dfs.idToClient[args.Id]
	// reply.Connection = clientConn.conn
	reply.Closed = true

	// for filename, writerInfo := range dfs.writerMap {
	// 	if writerInfo.ClientId == args.Id {
	// 		delete(dfs.writerMap, filename)
	// 	}
	// }

	// clientConn.conn.Close()
	// delete(dfs.idToClient, args.Id)

	return nil
}

// func (dfs *ServerDfs) GetLatestFile(args *shared.FileArgs, reply *shared.FileReply) error {
// 	var fileData [NumChunks]shared.Chunk
// 	versionMap := dfs.versionMap
// 	for i := 0; i < NumChunks; i++ {
// 		versionStack := versionMap[args.Filename][i]
// 		latestVersion, err := versionStack.Peek()
// 		latestVersionOwner := latestVersion.owner

// 		// Server calls Client's RPC of latestVersion.owner
// 		clientConnInfo := dfs.idToClient[latestVersionOwner]
// 		var clientRpcReply shared.ChunkReply
// 		err = clientConnInfo.conn.Call("ClientDfs.GetChunk", args, clientRpcReply)

// 		// Could not get latest file, then it'll fail
// 		if err != nil {
// 			fmt.Println("Wtf happened in GetLatestFile")
// 			return err
// 		}

// 		fileData[i] = clientRpcReply.Data
// 	}

// 	reply.Filename = args.Filename
// 	reply.Data = fileData

// 	return nil
// }

func (dfs *ServerDfs) InitiateRPC(args *shared.InitiateArgs, reply *shared.InitiateReply) error {
	client, err := rpc.Dial("tcp", args.Ip)
	RecordClientInfo(client, args, reply, dfs)

	// dfs.logger.Println("DFS in Initiate is ..... %#v", *dfs)
	reply.Connected = true
	return err
}

// **************** RPC METHODS THAT CLIENT CALLS END *********** //

// Returns unique id of this client
func RecordClientInfo(clientConn *rpc.Client, args *shared.InitiateArgs, reply *shared.InitiateReply, dfs *ServerDfs) {
	var clientInfo = ClientInfo{ip: args.Ip, localPath: args.LocalPath}
	clientId := dfs.clientToId[clientInfo]

	if clientId == 0 {
		clientId = dfs.nextId
		dfs.clientToId[clientInfo] = clientId
		dfs.idToClient[clientId] = ClientConn{conn: clientConn, localPath: args.LocalPath}
		dfs.nextId++
	}

	// dfs.logger.Println("DFS IS ...... %+v", *dfs)
	dfs.logger.Println("Client id is ...... %d", clientId)
	reply.Id = clientId
}

func main() {
	// serverIp := os.Args[1]

	writerMutex := &sync.Mutex{}
	var logger = log.New(os.Stdout, "[416_a2] ", log.Lshortfile)
	dfs := &ServerDfs{
		writerMap:  make(map[string]WriterInfo),
		versionMap: make(map[string][256]VersionStack),
		nextId:     1,
		clientToId: make(map[ClientInfo]int),
		idToClient: make(map[int]ClientConn),
		openedFile: make(map[string]int),  // filenames to clientIDs who have opened them
		files:      make(map[string]bool), //
		writeMutex: writerMutex,
		logger:     logger}

	rpc.Register(dfs)

	// l, err := net.Listen("tcp", serverIp)
	l, err := net.Listen("tcp", ":9482")
	if err != nil {
		logger.Fatal(err)
	}

	// TODO fcai use this instead
	// heartbeatListener, err := net.Listen("udp", serverIp + ":0")

	// Bind address for server to listen to heartbeats
	// go func(dfs *ServerDfs) {
	// 	hbFile, err := os.Create("./heartbeats.json")

	// 	if err != nil {
	// 		fmt.Println("Couldn't create heartbeats.json")
	// 		return
	// 	}

	// 	hListener, err := net.ListenUDP("udp", ":37445")

	// 	if err != nil {
	// 		logger.Fatal(err)
	// 	}

	// 	defer hListener.Close()
	// 	defer hbFile.Close()

	// 	buf := make([]byte, 1024)
	// 	readBuf := make([]byte, 1024)
	// 	for {
	// 		n, err := hListener.ReadFromUDP(buf)
	// 		now := time.Now()
	// 		// var hbData HeartBeatData
	// 		// heartbeats := make([]HeartBeat, 0)
	// 		// hbId := string(buf[:n])
	// 		heartbeatId, _ := stronv.Atoi(string(buf[:n]))
	// 		hbData := HeartBeatData{ClientId: heartbeatId, Timestamp: now}

	// 		hbBytes, err := json.Marshal(hbData)

	// 		if err != nil {
	// 			checkError(err)
	// 		}

	// 		n, err := hbFile.Read(readBuf)

	// 		heartBeatJson := make([]HeartBeatData, 0)
	// 		json.Unmarshal(readBuf, &heartBeatJson)
	// 		previousData := heartBeatJson[heartBeatId]

	// 		if previousData

	// 		n, hbFile.Write(hbBytes)
	// 		// _, err = hbFile.Write(buf)

	// 		if err != nil {
	// 			fmt.Println("Error in writing heartbeats to file")
	// 		}
	// 	}

	// }(dfs)
	// Go routine to accept incoming connections
	// go func(l net.Listener) {
	for {
		logger.Println("Printing....")
		conn, _ := l.Accept()
		go rpc.ServeConn(conn)
	}
	// }(l)

	// for {
	// }
}

func checkError(err error) error {
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error ", err.Error())
		return err
	}
	return nil
}
