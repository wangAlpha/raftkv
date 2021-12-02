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

	maxRaftState  int // snapshot if log grows this big
	stateMachine  *KVStateMachine
	lastOperation map[int64]int
	notifyChan    map[int]chan CommandReply
}

func NewKVStateMachine() *KVStateMachine {
	return &KVStateMachine{data: make(map[string]string)}
}

func (kv *KVStateMachine) ExecuteCommand(command Command) (string, int) {
	if command.OpType == OpPut {
		kv.data[command.Key] = command.Value
		return "Ok", Ok
	} else if command.OpType == OpGet {
		if value, ok := kv.data[command.Key]; ok {
			return value, Ok
		}
	} else if command.OpType == OpAppend {
		kv.data[command.Key] += command.Value
		return "Ok", Ok
	}
	return "Error", ErrOperator
}

func (server *KVServer) isDuplicate(client_id int64, command_id int) bool {
	if cmd_id, ok := server.lastOperation[client_id]; ok && cmd_id >= command_id {
		return true
	}
	return false
}

func (server *KVServer) getNotifyChan(index int) chan CommandReply {
	if _, ok := server.notifyChan[index]; !ok {
		server.notifyChan[index] = make(chan CommandReply, 1)
	}
	return server.notifyChan[index]
}

func (server *KVServer) HandleRequest(args *CommandArgs, reply *CommandReply) {
	INFO("Handling request")
	server.mu.Lock()
	defer server.mu.Unlock()
	command := args.Request
	if server.isDuplicate(args.ClientId, args.CommnadId) {
		INFO("Duplicate command")
		return
	}
	index, _, is_leader := server.raft.Start(command)
	if !is_leader {
		reply.StatusCode = ErrNoneLeader
		reply.LeaderId = server.raft.GetLeaderId()
		return
	}

	ch := server.getNotifyChan(index)
	select {
	case msg := <-ch:
		INFO("msg: %v", msg)
		reply.Value = msg.Value
		reply.StatusCode = msg.StatusCode

	case <-time.After(OverTime):
		INFO("timeout")
		reply.Value = "ErrorTimeout"
		reply.StatusCode = ErrTimeout
	}
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.raft.Kill()
}

// receiver operator and execute its.
func (server *KVServer) handleCommand() {
	for {
		msg := <-server.applyCh
		if msg.CommandValid {
			command := msg.Command.(Command)
			// fmt.Printf("msg: %+v", msg.Command)
			value, status_code := server.stateMachine.ExecuteCommand(command)
			reply := CommandReply{Value: value, StatusCode: status_code}
			ch := server.getNotifyChan(msg.CommandIndex)
			ch <- reply
			if server.maxRaftState != -1 && server.raft.GetRaftStateSize() > server.maxRaftState {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(server.lastOperation)
				e.Encode(server.stateMachine.data)
				go server.raft.MakeRaftSnaphot(msg.Snapshot, msg.CommandIndex)
			}
		} else if msg.UseSnapshot {
			w := new(bytes.Buffer)
			d := labgob.NewDecoder(w)
			var last_log_index int // unused placeholder
			var last_log_term int  // unused placeholder
			d.Decode(&last_log_index)
			d.Decode(&last_log_term)
			d.Decode(&server.lastOperation)
			d.Decode(&server.stateMachine.data)
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
	server := &KVServer{
		me:           me,
		maxRaftState: maxRaftState,
		applyCh:      make(chan raft.ApplyMsg),
		stateMachine: NewKVStateMachine(),
		notifyChan:   make(map[int]chan CommandReply),
	}

	server.raft = raft.Make(servers, me, persister, server.applyCh)
	go server.handleCommand()
	return server
}
