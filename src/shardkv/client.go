package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"raftkv/src/labrpc"
	"raftkv/src/shardmaster"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	ShardMaster *shardmaster.Clerk
	Config      shardmaster.Config
	MakeEnd     func(string) *labrpc.ClientEnd
	LeaderId    map[int]int
	RequestId   int64
	ClientId    int64
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	shardMaster := shardmaster.MakeClerk(masters)
	config := shardMaster.Query(-1)
	return &Clerk{
		ShardMaster: shardMaster,
		Config:      config,
		MakeEnd:     make_end,
		LeaderId:    make(map[int]int, 1),
		RequestId:   0,
		ClientId:    nrand(),
	}
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) RequestOp(args *CommandArgs) string {
	INFO("Request Command %d %+v %v %v", args.RequestId, OpName[args.OpType], args.Key, args.Value)
	args.RequestId = ck.RequestId
	args.ClientId = ck.ClientId
	for {
		shard := key2shard(args.Key)
		for gid := range ck.Config.Shards {
			if _, ok := ck.LeaderId[gid]; !ok {
				ck.LeaderId[gid] = 0
			}
		}
		gid := ck.Config.Shards[shard]
		if servers, ok := ck.Config.Groups[gid]; ok {
			// try each server for the shard.
			oldLeader := ck.LeaderId[gid]
			leaderId := oldLeader
			for {
				srv := ck.MakeEnd(servers[leaderId])
				var reply CommandReply
				ok := srv.Call("ShardKV.HandleRequestRPC", args, &reply)
				INFO("Op: %v Id: %d ok: %t, reply: %+v ", OpName[args.OpType], args.RequestId, ok, reply)
				if ok && (reply.StatusCode == OK || reply.StatusCode == ErrNoKey) {
					ck.LeaderId[gid] = leaderId
					ck.RequestId++
					return reply.Value
				}
				if ok && (reply.StatusCode == ErrWrongGroup) {
					break
				}
				leaderId = (leaderId + 1) % len(servers)
				if oldLeader == leaderId {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.Config = ck.ShardMaster.Query(-1)
	}

	return ""
}

func (ck *Clerk) Get(key string) string {
	return ck.RequestOp(&CommandArgs{
		OpType: OpGet,
		Key:    key,
		Value:  "",
	})
}

func (ck *Clerk) Put(key string, value string) {
	ck.RequestOp(&CommandArgs{
		OpType: OpPut,
		Key:    key,
		Value:  value,
	})
}

func (ck *Clerk) Append(key string, value string) {
	ck.RequestOp(&CommandArgs{
		OpType: OpAppend,
		Key:    key,
		Value:  value,
	})
}
