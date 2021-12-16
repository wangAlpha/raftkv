package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"mit6.824/src/labgob"
	"mit6.824/src/labrpc"
	"mit6.824/src/raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type KVStateMachine struct {
	Data map[string]string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	raft    *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxRaftState int // snapshot if log grows this big
	stateMachine *KVStateMachine
	LastOpResult map[int64]CommandReply
	notifyChan   map[int]chan CommandReply
}

func NewKVStateMachine() *KVStateMachine {
	return &KVStateMachine{Data: make(map[string]string)}
}

func (server *KVServer) ExecuteCommand(cmd Command) (string, int) {
	if server.isDuplicate(cmd.ClientId, cmd.CommandId) && cmd.OpType != OpGet {
		result := server.LastOpResult[cmd.ClientId]
		return result.Value, result.StatusCode
	}
	response, status_code := "Error", Ok
	switch cmd.OpType {
	case OpPut:
		server.stateMachine.Data[cmd.Key] = cmd.Value
		response = "Put:" + cmd.Value
	case OpGet:
		if value, ok := server.stateMachine.Data[cmd.Key]; ok {
			response = value
		} else {
			status_code = ErrNoneKey
		}
	case OpAppend:
		server.stateMachine.Data[cmd.Key] += cmd.Value
		response = "Append:" + cmd.Value
	}
	return response, status_code
}

func (server *KVServer) isDuplicate(client_id int64, cmd_id int) bool {
	server.mu.Lock()
	defer server.mu.Unlock()
	if result, ok := server.LastOpResult[client_id]; ok && result.CommnadId >= cmd_id {
		return true
	}
	return false
}

func (server *KVServer) getNotifyChan(index int) chan CommandReply {
	if _, ok := server.notifyChan[index]; !ok {
		server.notifyChan[index] = make(chan CommandReply, 1)
		// INFO("There don't exist notifyChan, %+v", server.notifyChan)
	} else {
		// INFO("There exist notifyChan, %+v", server.notifyChan)
	}
	return server.notifyChan[index]
}

func (server *KVServer) retrieveLastOpResult(client_id int64) CommandReply {
	return server.LastOpResult[client_id]
}

func (server *KVServer) removeNotifyChan(index int) {
	delete(server.notifyChan, index)
}

func (server *KVServer) HandleRequest(args *CommandArgs, reply *CommandReply) {
	// INFO("HandleRequest %v", *args)
	command := args.RequestOp
	if server.isDuplicate(args.ClientId, args.CommandId) && command.OpType != OpGet {
		*reply = server.retrieveLastOpResult(args.ClientId)
		reply.Value = "Dup_" + reply.Value
		return
	}
	index, _, is_leader := server.raft.Start(command)
	if !is_leader {
		reply.StatusCode = ErrNoneLeader
		reply.LeaderId = server.raft.GetLeaderId()
		return
	}
	server.mu.Lock()
	ch := server.getNotifyChan(index)
	server.mu.Unlock()
	select {
	case msg := <-ch:
		if command.ClientId == msg.ClientId && command.CommandId == msg.CommnadId {
			// INFO("ClientID:%d %d Op:%d result,  value: %s, code: %s", command.ClientId, command.CommandId, OpName[command.OpType], msg.Value, ErrName[msg.StatusCode])
			*reply = msg
		} else {
			reply.Value = "ERROR"
			reply.StatusCode = ErrOperator
		}
	case <-time.After(OverTime):
		INFO("timeout")
		reply.Value = "ErrorTimeout"
		reply.StatusCode = ErrTimeout
	}
	server.mu.Lock()
	server.removeNotifyChan(index)
	server.mu.Unlock()
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.raft.Kill()
}

// receiver operator and execute its.
func (server *KVServer) handleCommand() {
	for msg := range server.applyCh {
		if msg.CommandValid {
			cmd := msg.Command.(Command)
			value, status_code := server.ExecuteCommand(cmd)
			reply := CommandReply{
				ClientId:   cmd.ClientId,
				CommnadId:  cmd.CommandId,
				StatusCode: status_code,
				Value:      value,
			}
			server.mu.Lock()
			server.LastOpResult[cmd.ClientId] = reply
			ch := server.getNotifyChan(msg.CommandIndex)
			server.mu.Unlock()
			ch <- reply

			if server.maxRaftState != -1 && server.raft.GetRaftStateSize() > server.maxRaftState {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(server.LastOpResult)
				e.Encode(server.stateMachine.Data)
				// INFO("Encode: %+v", server.stateMachine.Data)
				go server.raft.MakeRaftSnaphot(w.Bytes(), msg.CommandIndex)
			}
		} else if len(msg.Snapshot) > 0 {
			INFO("handle command use snapshot")
			r := bytes.NewReader(msg.Snapshot)
			d := labgob.NewDecoder(r)
			var last_log_index int // unused placeholder
			var last_log_term int  // unused placeholder
			d.Decode(&last_log_index)
			d.Decode(&last_log_term)
			d.Decode(&server.LastOpResult)
			d.Decode(&server.stateMachine.Data)
			// INFO("Decode: %+v", server.stateMachine.Data)
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxRaftState bytes,
// in order to allow Raft to garbage-collect its log. if maxRaftState is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxRaftState int) *KVServer {
	labgob.Register(Command{})
	labgob.Register(CommandReply{})
	server := &KVServer{
		mu:           sync.Mutex{},
		me:           me,
		applyCh:      make(chan raft.ApplyMsg, 128),
		dead:         0,
		maxRaftState: maxRaftState,
		stateMachine: NewKVStateMachine(),
		LastOpResult: make(map[int64]CommandReply),
		notifyChan:   make(map[int]chan CommandReply),
	}
	server.raft = raft.Make(servers, me, persister, server.applyCh)

	go server.handleCommand()
	return server
}
