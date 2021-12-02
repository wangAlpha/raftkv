package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"

	"mit6.824/src/labrpc"
)

const (
	OpGet = iota
	OpPut
	OpAppend
)

type Command struct {
	Key       string
	Value     string
	OpType    Operator
	ClientId  int
	CommandId int
}

type Clerk struct {
	servers     []*labrpc.ClientEnd
	session_id  int64
	leader_id   int
	sequence_id int
}

// ClientRequest RPC
// Invoked by clients to modify the replicated state.
// Arguments:
// clientId: client invoking request
// sequenceNum: to eliminate duplicates
// command: request for ototo montine }\end{array}$

// RegisterClient RPC
// Result:
// status: OK if state register client
// clientId: unique identifer register client
// leaderHint: address of recent leader, if known

// ClientQuery RPC
// Arguments:
// query: request for state machine, read-only
// Results:
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	return &Clerk{
		servers:     servers,
		session_id:  nrand(),
		leader_id:   0,
		sequence_id: 0,
	}
}

func (clerk *Clerk) RequestOp(command *Command) string {
	args := CommandArgs{
		ClientId:  clerk.session_id,
		LeaderId:  clerk.leader_id,
		CommnadId: clerk.sequence_id,
		Request:   *command,
	}
	for {
		var reply CommandReply
		ok := clerk.servers[clerk.leader_id].Call("KVServer.HandleRequest", &args, &reply)
		INFO("Ok: %t, reply: %+v", ok, reply)
		if ok && reply.StatusCode == Ok {
			clerk.sequence_id += 1
			return reply.Value
		}
		if reply.StatusCode == ErrNoneLeader {
			// if reply.LeaderId != -1 {
			// 	clerk.leader_id = reply.LeaderId
			// } else {
			clerk.leader_id = (clerk.leader_id + 1) % len(clerk.servers)
			// }
		}
		fmt.Printf("request operator error: %s", ErrName[reply.StatusCode])
	}
}

func (clerk *Clerk) Get(key string) string {
	return clerk.RequestOp(&Command{
		Key:       key,
		OpType:    OpGet,
		ClientId:  int(clerk.session_id),
		CommandId: clerk.sequence_id,
	})
}

func (clerk *Clerk) PutAppend(key string, value string, op Operator) {
	clerk.RequestOp(&Command{
		Key:       key,
		Value:     value,
		OpType:    op,
		ClientId:  int(clerk.session_id),
		CommandId: clerk.sequence_id,
	})
}

func (clerk *Clerk) Put(key string, value string) {
	clerk.PutAppend(key, value, OpPut)
}

func (clerk *Clerk) Append(key string, value string) {
	clerk.PutAppend(key, value, OpAppend)
}
