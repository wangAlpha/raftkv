package shardmaster

import (
	"log"
	"os"
)

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	Ok = iota
	ErrNoneLeader
	ErrTimeout
	ErrWrongLeader
	ErrDuplicateOp
)

var StatusCodeMap = map[int]string{
	Ok:             "OK",
	ErrNoneLeader:  "ErrNoneLeader",
	ErrTimeout:     "ErrTimeout",
	ErrWrongLeader: "ErrWrongLeader",
	ErrDuplicateOp: "ErrDuplicateOp",
}

const (
	OpQuery = iota
	OpJoin
	OpLeave
	OpMove
)

var OpName = map[int]string{
	OpQuery: "Query",
	OpJoin:  "Join",
	OpLeave: "Leave",
	OpMove:  "Move",
}

// var OpName[]string={"Query", "Join", "Leave", "Move"}

type Command struct {
	OpType  int
	Servers map[int][]string // for Join
	Num     int              // for Query, desired config number
	GIDs    []int            // for Leave
	Shard   int              // for Move
	GID     int              // for Move

	ClientId  int64
	CommandId int64
}

type CommandArgs struct {
	Command   Command
	ClientId  int64
	RequestId int64
}

type CommandReply struct {
	StatusCode int
	Config     Config
}

var (
	LogFile = os.Stderr
	INFO    = log.New(LogFile, "INFO ", log.Ltime|log.Lshortfile).Printf
	WARN    = log.New(LogFile, "WARN ", log.Ltime|log.Lshortfile).Printf
)

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

func All(list []int, f func(x int) bool) bool {
	for _, e := range list {
		if !f(e) {
			return false
		}
	}
	return true
}

func deepCopy(src map[int][]string) map[int][]string {
	dst := map[int][]string{}
	for key, values := range src {
		dst[key] = values
	}
	return dst
}
