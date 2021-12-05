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
	data map[string]string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	raft    *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxRaftState int // snapshot if log grows this big
	stateMachine *KVStateMachine
	lastOpResult map[int64]CommandReply
	notifyChan   map[int]chan CommandReply
}

func NewKVStateMachine() *KVStateMachine {
	return &KVStateMachine{data: make(map[string]string)}
}

func (server *KVServer) ExecuteCommand(cmd Command) (string, int) {
	if server.isDuplicate(cmd.ClientId, cmd.CommandId) && cmd.OpType != OpGet {
		result := server.lastOpResult[cmd.ClientId]
		return result.Value, result.StatusCode
	}
	response, status_code := "Error", Ok
	switch cmd.OpType {
	case OpPut:
		server.stateMachine.data[cmd.Key] = cmd.Value
		response = "Put:" + cmd.Value
	case OpGet:
		if value, ok := server.stateMachine.data[cmd.Key]; ok {
			response = value
		} else {
			status_code = ErrNoneKey
		}
	case OpAppend:
		server.stateMachine.data[cmd.Key] += cmd.Value
		response = "Append:" + cmd.Value
	}
	return response, status_code
}

func (server *KVServer) isDuplicate(client_id int64, cmd_id int) bool {
	if result, ok := server.lastOpResult[client_id]; ok && result.CommnadId >= cmd_id {
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
	return server.lastOpResult[client_id]
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
	server.getNotifyChan(index)
	server.mu.Unlock()
	select {
	case msg := <-server.notifyChan[index]:
		if command.ClientId == msg.ClientId && command.CommandId == msg.CommnadId {
			INFO("ClientID:%d %d Op:%d result,  value: %s, code: %s", command.ClientId, command.CommandId, command.OpType, msg.Value, ErrName[msg.StatusCode])
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
		// INFO("get applyCh: %+v", msg)
		// server.mu.Lock()
		if msg.UseSnapshot {
			w := new(bytes.Buffer)
			d := labgob.NewDecoder(w)
			var last_log_index int // unused placeholder
			var last_log_term int  // unused placeholder
			d.Decode(&last_log_index)
			d.Decode(&last_log_term)
			d.Decode(&server.lastOpResult)
			d.Decode(&server.stateMachine.data)
		} else if msg.CommandValid {
			cmd := msg.Command.(Command)
			value, status_code := server.ExecuteCommand(cmd)
			reply := CommandReply{
				ClientId:   cmd.ClientId,
				CommnadId:  cmd.CommandId,
				StatusCode: status_code,
				Value:      value,
			}
			server.lastOpResult[cmd.ClientId] = reply
			ch := server.getNotifyChan(msg.CommandIndex)
			ch <- reply

			if server.maxRaftState != -1 && server.raft.GetRaftStateSize() > server.maxRaftState {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(server.lastOpResult)
				e.Encode(server.stateMachine.data)
				go server.raft.MakeRaftSnaphot(w.Bytes(), msg.CommandIndex)
			}
		}
		// server.mu.Unlock()
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
	server := &KVServer{
		mu:           sync.Mutex{},
		me:           me,
		applyCh:      make(chan raft.ApplyMsg, 64),
		dead:         0,
		maxRaftState: maxRaftState,
		stateMachine: NewKVStateMachine(),
		lastOpResult: make(map[int64]CommandReply),
		notifyChan:   make(map[int]chan CommandReply),
	}
	server.raft = raft.Make(servers, me, persister, server.applyCh)

	go server.handleCommand()
	return server
}
