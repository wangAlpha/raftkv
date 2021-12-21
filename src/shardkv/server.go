package shardkv

// import "raftkv/src/shardmaster"
import (
	"raftkv/src/labgob"
	"raftkv/src/labrpc"
	"raftkv/src/raft"
	"raftkv/src/shardmaster"
	"sync"
	"time"
)

type Command struct{}

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxRaftState int // snapshot if log grows this big
	mck          *shardmaster.Clerk
	config       shardmaster.Config
	data         map[string]string

	chanNotify   map[int]chan CommandReply
	lastOpResult map[int64]CommandReply
	lastRequest  map[int64]int
}

func (kv *ShardKV) isDuplicateRequest(clientId int64, cmdId int) bool {
	kv.mu.RLock()
	kv.mu.RUnlock()
	if id, ok := kv.lastRequest[clientId]; ok && id >= cmdId {
		return true
	}
	return false
}

// Client command RPC handlerk
func (kv *ShardKV) HandleRequest(args *CommandArgs, reply *CommandReply) {
	cmd := *args
	if kv.isDuplicateRequest(cmd.ClientId, cmd.RequestId) {
		return
	}
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.StatusCode = ErrNoneLeader
		return
	}
	kv.mu.Lock()
	if _, ok := kv.chanNotify[index]; !ok {
		kv.chanNotify[index] = make(chan CommandReply, 1)
	}
	kv.mu.Unlock()
	select {
	case *reply = <-kv.chanNotify[index]:
		// TODO: CHECK REPLY
		INFO("reply: %+v", *reply)
	case <-time.After(500 * time.Millisecond):
		reply.StatusCode = ErrTimeout
	}
	kv.mu.Lock()
	delete(kv.chanNotify, index)
	kv.mu.Unlock()
}

func (kv *ShardKV) Kill() {
	kv.rf.Kill()
}

// handle command
func (kv *ShardKV) handleCommand() {
	for msg := range kv.applyCh {
		cmd := msg.Command.(CommandArgs)
		var reply CommandReply

		if msg.CommandValid {
			switch cmd.OpType {
			case OpPut:
				kv.data[cmd.Key] = cmd.Value
				reply.Value = cmd.Value
				reply.StatusCode = OK
			case OpGet:
				var value string
				if _, ok := kv.data[cmd.Key]; !ok {
					value = ""
				} else {
					value = kv.data[cmd.Key]
				}
				reply.Value = value
				reply.StatusCode = OK
			case OpAppend:
				kv.data[cmd.Key] += cmd.Value
				reply.Value = kv.data[cmd.Key]
				reply.StatusCode = OK
			case OpConfig:
				// TODO: Update config
				// reply.Value = kv.data[cmd.Key]
				reply.StatusCode = OK
			case OpMitigate:
				reply.StatusCode = OK
				// TODO: Mitigate data
			case OpGc:
				reply.StatusCode = OK
				// TODO: Garbage collection
			}
			kv.mu.RLock()
			if _, ok := kv.chanNotify[msg.CommandIndex]; !ok {
				kv.chanNotify[msg.CommandIndex] = make(chan CommandReply, 1)
			}
			ch := kv.chanNotify[msg.CommandIndex]
			kv.mu.RUnlock()
			ch <- reply
			if kv.maxRaftState != -1 && kv.maxRaftState < kv.rf.GetRaftStateSize() {
				// TODO: Make snapshot
				INFO("Make snapshot files")
			}
		} else if len(msg.Snapshot) > 0 {
			INFO("Receive snapshot msg")
			// TODO:
		}
	}
}

func (kv *ShardKV) fetchConfig(syncTime time.Duration) {
	for {
		// if _, isLeader := kv.rf.GetState(); isLeader {
		// 	config := kv.mck.Query(-1)
		// 	kv.mu.RLock()
		// 	if config.Num != kv.config.Num {
		// 		index, _, isLeader := kv.rf.Start(config)
		// 		INFO("index: %d, isLeader: %t", index, isLeader)
		// 	}
		// 	kv.mu.RUnlock()
		// }
		time.Sleep(syncTime * time.Millisecond)
	}
}

func (kv *ShardKV) migrateData() {
	for {
		time.Sleep(100 * time.Millisecond)
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister,
	maxRaftState int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	labgob.Register(CommandArgs{})
	labgob.Register(CommandReply{})

	kv := &ShardKV{
		mu:           sync.RWMutex{},
		me:           me,
		applyCh:      make(chan raft.ApplyMsg, 128),
		make_end:     make_end,
		gid:          gid,
		masters:      masters,
		maxRaftState: maxRaftState,
		mck:          shardmaster.MakeClerk(masters),
		config:       shardmaster.Config{},
		data:         make(map[string]string, 1),
		chanNotify:   map[int]chan CommandReply{},
		lastOpResult: map[int64]CommandReply{},
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// start a command coroutine
	go kv.handleCommand()
	// start a fetch config coroutine
	go kv.fetchConfig(100)
	// start a k/v migrate coroutine
	go kv.migrateData()
	return kv
}
