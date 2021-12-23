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

type StoreComponent struct {
	data map[string]string
}

func MakeStore() *StoreComponent {
	return &StoreComponent{
		data: make(map[string]string, 1),
	}
}

func (db *StoreComponent) UpdateDB(op int, key string, value string) (string, Err) {
	var status Err = OK
	switch op {
	case OpGet:
		if _, ok := db.data[key]; !ok {
			status = ErrNoKey
		} else {
			value = db.data[key]
		}
	case OpPut:
		db.data[key] = value
		value = db.data[key]
	case OpAppend:
		db.data[key] += value
		value = db.data[key]
	}
	return value, status
}

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
	db           *StoreComponent

	chanNotify   map[int]chan CommandReply
	lastOpResult map[int64]CommandReply
	lastRequest  map[int64]int
}

func (kv *ShardKV) isDuplicateRequest(clientId int64, cmdId int) bool {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	if id, ok := kv.lastRequest[clientId]; ok && id >= cmdId {
		return true
	}
	return false
}

// Check whether the key belongs to the shard
func (kv *ShardKV) checkKeyShard(key string) bool {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return kv.config.Shards[key2shard(key)] == kv.gid
}

// Client command RPC handlerk
func (kv *ShardKV) HandleRequest(args *CommandArgs, reply *CommandReply) {
	cmd := *args
	if kv.isDuplicateRequest(cmd.ClientId, cmd.RequestId) {
		return
	}
	if !kv.checkKeyShard(cmd.Key) {
		reply.StatusCode = ErrWrongGroup
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
		reply := CommandReply{}

		if msg.CommandValid {
			switch cmd.OpType {
			case OpPut:
				reply.Value, reply.StatusCode = kv.db.UpdateDB(cmd.OpType, cmd.Key, cmd.Value)
			case OpGet:
				reply.Value, reply.StatusCode = kv.db.UpdateDB(cmd.OpType, cmd.Key, cmd.Value)
			case OpAppend:
				reply.Value, reply.StatusCode = kv.db.UpdateDB(cmd.OpType, cmd.Key, cmd.Value)
			case OpReConfig:
				reply.Value, reply.StatusCode = kv.applyReConfig(cmd)
			case OpGc:
				reply.StatusCode, reply.StatusCode = kv.applyGC(cmd)
			}
			kv.mu.RLock()
			if ch, ok := kv.chanNotify[msg.CommandIndex]; !ok {
				kv.chanNotify[msg.CommandIndex] = make(chan CommandReply, 1)
			} else {
				for len(ch) > 0 {
					select {
					case <-ch:
					default:
						break
					}
				}
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

func (kv *ShardKV) equalConfig(cfg1 *shardmaster.Config, cfg2 *shardmaster.Config) bool {
	if cfg1.Num == cfg2.Num && cfg1.Shards == cfg2.Shards {
		return true
	}
	return false
}

// 当前配置中本group持有的没有的shards，而新的配置要分给本group的shards
func (kv *ShardKV) obtainMigrateShards(newConfig shardmaster.Config) map[int][]int {
	shardsGroups := map[int][]int{}
	for i := 0; i < shardmaster.NShards; i++ {
		if newConfig.Shards[i] != kv.gid && kv.config.Shards[i] == kv.gid {
			gid := newConfig.Shards[i]
			if _, ok := shardsGroups[gid]; !ok && gid != 0 {
				shardsGroups[gid] = make([]int, 1)
			}
			shardsGroups[gid] = append(shardsGroups[gid], i)
		}
	}
	return shardsGroups
}

func (kv *ShardKV) generateConfig(newConfig shardmaster.Config) (CommandArgs, bool) {
	args := CommandArgs{
		OpType: OpReConfig,
	}
	migrateShardsGroups := kv.obtainMigrateShards(newConfig)
	var wg sync.WaitGroup
	// TODO: send config and receive shard data
	// var mutex sync.Mutex
	// for gid, shards := range migrateShardsGroups {
	// migrateArgs := &MigrateArgs{
	// 	Num:     newConfig.Num,
	// 	ShardKV: shards,
	// }
	// wg.Add(1)
	// go func(args *migrateArgs) {
	// 	wg.Done()
	// 	ok := kv.MigrateGroups()
	// }(&migrateArgs)
	// go func() {
	// 	wg.Done()
	// }()
	// }
	wg.Wait()
	return args, true
}

func (kv *ShardKV) updateConfig(syncTime time.Duration) {
	for {
		if _, isLeader := kv.rf.GetState(); isLeader {
			nextConfig := kv.mck.Query(kv.config.Num + 1)
			// See hint 1
			if nextConfig.Num == kv.config.Num+1 {
				// TODO: send config and exchange data
				if cmd, ok := kv.generateConfig(nextConfig); ok {
					// TODO: APPLY CONFIG AND DATA TO THIS GROUP
				}
			}
		}
		time.Sleep(syncTime * time.Millisecond)
	}
}

func (kv *ShardKV) applyReConfig(cmd CommandArgs) (string, Err) {
	// TODO:
	// 1.merge request record
	// 2.apply pulled  data
	// 3.apply this config
	// 4.send GC RPC
	return "", OK
}

func (kv *ShardKV) applyGC(cmd CommandArgs) (string, Err) {
	return "", OK
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
	// labgob.Register(Command{})
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
		db:           MakeStore(),
		chanNotify:   map[int]chan CommandReply{},
		lastOpResult: map[int64]CommandReply{},
		lastRequest:  map[int64]int{},
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// start a command coroutine
	go kv.handleCommand()
	// start a update config and data coroutine
	go kv.updateConfig(50)
	return kv
}
