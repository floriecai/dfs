/*

Server that all DFS clients talk to

Usage:
go run server.go
*/

package main

// Expects dfslib.go to be in the ./dfslib/ dir, relative to
// this app.go file

import (
	"fmt"
	"net/rpc"
	"os"
)

// MaxClients is unique clients to ever connect to the server
const MaxClients = 16

// FileInfo for identifying which clients own which files
type FileInfo struct {
	id      int
	version int
}

type FileData struct {
	chunkNum int
	filename string
}

type VersionStack []int

func (s *VersionStack) Push(id int) {
	*s = append(*s, id)
}

func (s *VersionStack) Pop() int {
	if len(*s) == 0 {
		return -1
	}

	latestVersion := s[len(*s)-1]
	*s = s[0 : len(*s)-1]
	return latestVersion
}

type FileOwnerMetadata struct {
	id        int // id of the client that owns the file
	versionID int // which chunk version, this is also the priority for the version tracking
}

// type PriorityQueue []*FileOwnerMetadata

// func (pq PriorityQueue) Len() int               { return len(pq) }
// func (pq PriorityQueue) Less(i int, j int) bool { return pq[i].versionID > pq[j].versionID }
// func (pq PriorityQueue) Swap(i int, j int)      { pq[i], pq[j] = pq[j], pq[i] }

// func (pq *PriorityQueue) Push(x interface{}) {
// 	n := len(*pq)
// 	fileMetaData := x.(*FileOwnerMetaData)
// 	*pq = append(*pq, item)
// }

func main() {
	serverIP := os.Args[1]

	// Map of localPath to its id
	var clients = make(map[string]int)

	// Map of files, filename to owner and latest version
	var versionMap = make(map[string]FileInfo)

	var versionMap = make(map[FileData]VersionStack)

	rpc.Dial(clientIP)
	fmt.Println("hello")
}

func (dfs *DFSInstance) RegisterClient(clientId string, clientIP string) {
	client = rpc.Dial("TCP", clientIP)
	// recordClientInfo(id, ip, client)
}

func (dfs *DFSInstance) Read(chunkNum uint8, chunk *Chunk, fname string, clientId int) {

}

func (dfs *DFSInstance) Write(chunkNum uint8, chunk *Chunk, fname string, clientId int) {

}
