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
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	mck          *shardmaster.Clerk

	chanNotify   map[int]chan CommandReply
	lastOpResult map[int64]CommandReply
}

func (kv *ShardKV) IsDuplicateRequest(clientId int64, cmdId int64) bool {
	// TODO
	return true
}

// Client command RPC handlerk
func (kv *ShardKV) HandleRequest(args *CommandArgs, reply *CommandReply) {
	// check request whether is duplicate request
	cmd := *args

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
	case reply := <-kv.chanNotify[index]:
		shardmaster.INFO("reply: %+v", reply)
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

func (kv *ShardKV) Run() {
	for cmd := range kv.applyCh {
		// 
	}
}

func (kv *ShardKV) FetchConfig(syncTime time.Duration) {
	for {
		// fetch latest config
		time.Sleep(syncTime * time.Millisecond)
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
	maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	labgob.Register(CommandArgs{})
	labgob.Register(CommandReply{})

	kv := &ShardKV{
		me:           me,
		maxraftstate: maxraftstate,
		make_end:     make_end,
		gid:          gid,
		masters:      masters,
		applyCh:      make(chan raft.ApplyMsg, 128),
		mck:          shardmaster.MakeClerk(masters),
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// start a command coroutine
	go kv.Run()
	// start a fetch config coroutine
	go kv.FetchConfig(100)
	// start a k/v migrate coroutine
	return kv
}
