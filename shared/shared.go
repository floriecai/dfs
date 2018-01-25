package shared

// type RPCArgs struct {
// 	chunkNum uint8
// 	chunk    *dfslib.Chunk
// }

type OpenArgs struct {
	Filename string
}

// ****************** RPC ARG TYPES THAT CLIENT CALLS ********** //
type InitiateArgs struct {
	Ip        string
	LocalPath string
}

type InitiateReply struct {
	Id        int
	Connected bool
}

type TempArgs struct {
	Id int
}
type FileArgs struct {
	ClientId int // person who is requesting this
	ChunkNum uint8
	Filename string
}

type FileReply Chunk

type WriteRequestReply struct {
	CanWrite bool
}

type FileExistArgs struct {
	Filename string
}

// *****************RPC ARG TYPES THAT CLIENT CALLS END ******** //

// **************** RPC ARG TYPES THAT SERVER CALLS ************ //

// **************** RPC ARG TYPES THAT SERVER CALLS END********* //

type Chunk [32]byte

type DFSInstance struct {
	Id        int
	LocalIP   string
	LocalPath string
}

type DFSFileT struct {
	Owner     int
	RequestID int // ID of who made the request on this file, need to know the local path ....
	Name      string
	Data      []Chunk
}
