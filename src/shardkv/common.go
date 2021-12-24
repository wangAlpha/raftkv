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
	ErrNotReady    = "ErrNotReady"
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

	Data          [shardmaster.NShards]map[string]string
	ResultRecord  map[int64]CommandReply
}

type CommandReply struct {
	ClientId   int64
	RequestId  int
	StatusCode Err
	Value      string
}

type MigrateArgs struct {
	Num    int
	Shards []int
}

type MigrateReply struct {
	StatusCode    Err
	Data          [shardmaster.NShards]map[string]string
	RequstRecord  map[int64]int
	RequestResult map[int64]CommandReply
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

func DeepCopy(kv map[string]string) map[string]string {
	dst := make(map[string]string)
	for k, v := range kv {
		dst[k] = v
	}
	return dst
}
