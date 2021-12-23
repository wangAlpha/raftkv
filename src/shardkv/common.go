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

	OpReConfig
	OpGc
)

var OpName = map[int]string{
	OpGet:      "Get",
	OpPut:      "Put",
	OpAppend:   "Append",
	OpReConfig: "ReConfig",
	OpGc:       "Gc",
}

type Err string

type CommandArgs struct {
	OpType int
	Key    string
	Value  string
	Config shardmaster.Config

	RequestId int
	ClientId  int64
}

type CommandReply struct {
	StatusCode Err
	Value      string
}

type MigrateArgs struct {
	Num    int
	Shards int
}

type MigrateReply struct {
	StatusCode   Err
	Data         [shardmaster.NShards]map[string]string
	RequstRecord map[int64]int
}

type GcArgs struct {
	Num     int
	ShardId int
}

type GcReply struct {
	StatusCode Err
}

var (
	INFO = shardmaster.INFO
	WARN = shardmaster.WARN
)
