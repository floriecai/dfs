package shared

import (
	"fmt"
	"time"
)

const NumChunks = 256

type HeartBeatData struct {
	ClientId  int       `json: "id"`
	Timestamp time.Time `json: "timestamp"`
}

type ConfirmWriteArgs struct {
	ChunkNum int
	Data     Chunk
}

type ConfirmWriteReply struct {
	Version int
}

type OpenArgs struct {
	Filename string
}

type InitiateArgs struct {
	Ip        string
	LocalPath string
	ClientId  int
}

type InitiateReply struct {
	Id        int
	Connected bool
}

type CloseArgs struct {
	Id int
}

type CloseReply struct {
	Closed bool
	// Connection *rpc.Client
}

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

// Used for Requesting the Write Lock and Notifying a Write has occured
type WriteRequestReply struct {
	CanWrite bool
	Version  int
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

type NoWriteLockError string

func (e NoWriteLockError) Error() string {
	return fmt.Sprintf("No longer has write lock", string(e))
}

// *********** ERRORS END****************** //
