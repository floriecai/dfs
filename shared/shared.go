package shared

import (
	"fmt"
)

const NumChunks = 256

type OpenArgs struct {
	Filename string
}

type InitiateArgs struct {
	Ip        string
	LocalPath string
}

type InitiateReply struct {
	Id        int
	Connected bool
}

// type TempArgs struct {
// 	Id int
// }

type ChunkArgs struct {
	ChunkNum int
	Chunk    Chunk
}

type ChunkReply struct {
	Data Chunk
}

type FileArgs struct {
	ClientId int // person who is requesting this
	ChunkNum uint8
	Filename string
}

type FileReply struct {
	Filename string
	Data     [256]Chunk
}

type WriteRequestReply struct {
	CanWrite bool
}

type FileExistsArgs string
type FileExistsReply bool

// Types for dfslib
type Chunk [32]byte

// ************** ERRORS ****************** //
type LatestChunkUnavailable string

func (e LatestChunkUnavailable) Error() string {
	return fmt.Sprintf("Latest Chunk unavailable", string(e))
}

type BestChunkUnavailable string

func (e BestChunkUnavailable) Error() string {
	return fmt.Sprintf("Best Chunk unavailable", string(e))
}

// *********** ERRORS END****************** //
