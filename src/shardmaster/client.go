package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"math/big"
	"raftkv/src/labrpc"
	"time"
)

type Clerk struct {
	servers    []*labrpc.ClientEnd
	server_id  int
	client_id  int64
	command_id int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	return &Clerk{
		servers:    servers,
		server_id:  0,
		client_id:  nrand(),
		command_id: 0,
	}
}

func (ck *Clerk) RequestOp(cmd Command) Config {
	ck.command_id += 1
	cmd.CommandId = ck.command_id
	cmd.ClientId = ck.client_id
	args := &CommandArgs{
		Command:   cmd,
		ClientId:  ck.client_id,
		RequestId: ck.command_id,
	}
	for {
		server := ck.servers[ck.server_id]
		var reply CommandReply
		ok := server.Call("ShardMaster.HandleRequest", args, &reply)
		INFO("ok: %t, Reply: %+v", ok, reply)
		if ok && reply.StatusCode == Ok {
			return reply.Config
		}
		if !ok || reply.StatusCode == ErrNoneLeader {
			ck.server_id = (ck.server_id + 1) % len(ck.servers)
		} else if reply.StatusCode != ErrDuplicateOp {
			INFO("Request Err: %s", StatusCodeMap[reply.StatusCode])
		}
		// ck.server_id = (ck.server_id + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Query(num int) Config {
	return ck.RequestOp(Command{
		OpType: OpQuery,
		Num:    num,
	})
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.RequestOp(Command{
		OpType:  OpJoin,
		Servers: servers,
	})
}

func (ck *Clerk) Leave(gids []int) {
	ck.RequestOp(Command{
		OpType: OpLeave,
		GIDs:   gids,
	})
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.RequestOp(Command{
		OpType: OpMove,
		Shard:  shard,
		GID:    gid,
	})
}
