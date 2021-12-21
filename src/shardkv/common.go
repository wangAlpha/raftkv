package shardkv

import "raftkv/src/shardmaster"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNoneLeader  = "ErrNoneLeader"
	ErrTimeout     = "ErrTimeout"
)

const (
	OpGet = iota
	OpPut
	OpAppend

	OpConfig
	OpMitigate
	OpGc
)

var OpName = map[int]string{
	OpGet:      "Get",
	OpPut:      "Put",
	OpAppend:   "Append",
	OpConfig:   "Config",
	OpMitigate: "Mitigate",
	OpGc:       "Gc",
}

type Err string

type CommandArgs struct {
	OpType int
	Key    string
	Value  string

	RequestId int
	ClientId  int64
}

type CommandReply struct {
	StatusCode Err
	Value      string
}

var (
	INFO = shardmaster.INFO
	WARN = shardmaster.WARN
)
