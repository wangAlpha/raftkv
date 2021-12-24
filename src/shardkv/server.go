package shardkv

import (
	"raftkv/src/labgob"
	"raftkv/src/labrpc"
	"raftkv/src/raft"
	"raftkv/src/shardmaster"
	"sync"
	"time"
)

type StoreComponent struct {
	data [shardmaster.NShards]map[string]string
}

func MakeStore() *StoreComponent {
	data := [shardmaster.NShards]map[string]string{}
	for i := 0; i < shardmaster.NShards; i++ {
		data[i] = make(map[string]string)
	}
	return &StoreComponent{
		data: data,
	}
}

func (db *StoreComponent) UpdateDB(op int, key string, value string) (string, Err) {
	var status Err = OK
	shard := key2shard(key)
	switch op {
	case OpGet:
		if _, ok := db.data[shard][key]; !ok {
			status = ErrNoKey
		} else {
			value = db.data[shard][key]
		}
	case OpPut:
		db.data[shard][key] = value
		value = db.data[shard][key]
	case OpAppend:
		db.data[shard][key] += value
		value = db.data[shard][key]
	}
	return value, status
}

func (db *StoreComponent) UpdateFrom(data [shardmaster.NShards]map[string]string) {
	for shard, kv := range data {
		for k, v := range kv {
			db.data[shard][k] = v
		}
	}
}

func (db *StoreComponent) CopyFrom(shards []int) [shardmaster.NShards]map[string]string {
	data := [shardmaster.NShards]map[string]string{}
	for i := 0; i < shardmaster.NShards; i++ {
		data[i] = make(map[string]string)
	}
	for _, shard := range shards {
		data[shard] = DeepCopy(db.data[shard])
	}
	return data
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
		*reply = kv.lastOpResult[cmd.ClientId]
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
	ch := kv.getNotifyChan(index)
	kv.mu.Unlock()
	select {
	case *reply = <-ch:
		if args.ClientId == reply.ClientId && args.RequestId == reply.RequestId {
			// TODO: CHECK REPLY
			INFO("reply: %+v", *reply)
		} else {
			INFO("Warning, reply is not match args: %+v", *reply)
		}
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

func (kv *ShardKV) getNotifyChan(index int) chan CommandReply {
	if ch, ok := kv.chanNotify[index]; !ok {
		kv.chanNotify[index] = make(chan CommandReply, 1)
	} else {
		for len(ch) > 0 {
			select {
			case <-ch:
			default:
				break
			}
		}
	}
	return kv.chanNotify[index]
}

// handle command
func (kv *ShardKV) handleCommand() {
	for msg := range kv.applyCh {
		cmd := msg.Command.(CommandArgs)
		reply := CommandReply{}

		if msg.CommandValid {
			if !kv.isDuplicateRequest(cmd.ClientId, cmd.RequestId) {
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
					reply.Value, reply.StatusCode = kv.applyGC(cmd)
				}
				kv.lastRequest[cmd.ClientId] = cmd.RequestId
				kv.lastOpResult[cmd.ClientId] = reply
			} else {
				reply = kv.lastOpResult[cmd.ClientId]
			}
			kv.mu.RLock()
			ch := kv.getNotifyChan(msg.CommandIndex)
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

// 提取当前配置中本group持有的没有的shards，而新的配置要分给本group的shards
func (kv *ShardKV) extractMigrateShards(newConfig shardmaster.Config) map[int][]int {
	shardsGroups := map[int][]int{}
	for i := 0; i < shardmaster.NShards; i++ {
		if newConfig.Shards[i] != kv.gid && kv.config.Shards[i] == kv.gid {
			gid := newConfig.Shards[i]
			if _, ok := shardsGroups[gid]; !ok && gid != 0 {
				shardsGroups[gid] = make([]int, 0)
			}
			shardsGroups[gid] = append(shardsGroups[gid], i)
		}
	}
	return shardsGroups
}

// Migrate data RPC
func (kv *ShardKV) MigrateData(args *MigrateArgs, reply *MigrateReply) bool {
	if args.Num > kv.config.Num {
		reply.StatusCode = ErrNotReady
		return false
	}
	for clientId, record := range kv.lastRequest {
		reply.RequstRecord[clientId] = record
	}
	for clientId, result := range kv.lastOpResult {
		reply.RequestResult[clientId] = result
	}
	reply.Data = kv.db.CopyFrom(args.Shards)
	return true
}

func (kv *ShardKV) sendMigrateGroups(gid int, args *MigrateArgs, reply *MigrateReply) Err {
	for _, server := range kv.config.Groups[gid] {
		srv := kv.make_end(server)
		if srv.Call("ShardKV.MigrateData", args, reply) {
			return reply.StatusCode
		}
	}
	return ErrNoneLeader
}

func (kv *ShardKV) forwardConfig(newConfig shardmaster.Config) (CommandArgs, bool) {
	// TODO: send config and receive shard data
	migrateShardsGroups := kv.extractMigrateShards(newConfig)

	data := [shardmaster.NShards]map[string]string{}
	requestRecord := map[int64]int{}
	resultRecord := map[int64]CommandReply{}
	for i := 0; i < shardmaster.NShards; i++ {
		data[i] = make(map[string]string)
	}

	var wg sync.WaitGroup
	var mutex sync.Mutex
	for gid, shards := range migrateShardsGroups {
		migrateArgs := &MigrateArgs{
			Num:    newConfig.Num,
			Shards: shards,
		}
		wg.Add(1)
		go func(gid int, args *MigrateArgs, reply *MigrateReply) {
			defer wg.Done()
			if statusCode := kv.sendMigrateGroups(gid, args, reply); statusCode == OK {
				mutex.Lock()
				for shard, kv := range reply.Data {
					for k, v := range kv {
						data[shard][k] = v
					}
					for clientId, record := range reply.RequstRecord {
						requestRecord[clientId] = record
						resultRecord[clientId] = reply.RequestResult[clientId]
					}
				}
				mutex.Unlock()
			}
		}(gid, migrateArgs, &MigrateReply{})
	}
	wg.Wait()
	args := CommandArgs{
		OpType:        OpReConfig,
		Data:          data,
		RequestRecord: requestRecord,
		ResultRecord:  resultRecord,
		Config:        newConfig,
	}
	return args, true
}

func (kv *ShardKV) applyCommand(args CommandArgs) bool {
	// TODO: Check if the command is repeated
	ok := true
	index, _, isLeader := kv.rf.Start(args)
	if !isLeader {
		return false
	}
	kv.mu.Lock()
	if _, ok := kv.chanNotify[index]; !ok {
		kv.chanNotify[index] = make(chan CommandReply, 1)
	}
	ch := kv.chanNotify[index]
	kv.mu.Unlock()
	select {
	case <-ch:
	case <-time.After(time.Millisecond * 240):
		ok = false
	}
	kv.mu.Lock()
	delete(kv.chanNotify, index)
	kv.mu.Unlock()
	return ok
}

func (kv *ShardKV) updateConfig(syncTime time.Duration) {
	for {
		if _, isLeader := kv.rf.GetState(); isLeader {
			nextConfig := kv.mck.Query(kv.config.Num + 1)
			// See hint 1
			if nextConfig.Num == kv.config.Num+1 {
				// TODO: send config and exchange data
				if cmd, ok := kv.forwardConfig(nextConfig); ok {
					// TODO: APPLY CONFIG AND DATA TO THIS GROUP
					kv.applyCommand(cmd)
					// INFO("GROUPS:%d, apply cmd:%+v, result: %t", kv.gid, cmd, ok)
				}
			}
		}
		time.Sleep(syncTime * time.Millisecond)
	}
}

func (kv *ShardKV) applyReConfig(args CommandArgs) (string, Err) {
	// TODO:
	// 1.merge request record
	for clientId, record := range args.RequestRecord {
		if cmdId, ok := kv.lastRequest[clientId]; !ok || cmdId < record {
			kv.lastRequest[clientId] = record
			kv.lastOpResult[clientId] = args.ResultRecord[clientId]
		}
	}
	// 2.apply pulled  data
	kv.db.UpdateFrom(args.Data)
	// 3.apply this config
	// lastConfig := kv.config
	kv.config = args.Config
	// 4.send GC RPC
	// 已经应用了这些DATA，就再申请移除相关的数据
	kv.sendGarbageCollection(kv.gid, args.Data)
	return "", OK
}

func (kv *ShardKV) sendGarbageCollection(gid int, data [shardmaster.NShards]map[string]string) {
	// TODO: send GC RPC
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
	labgob.Register(MigrateArgs{})
	labgob.Register(MigrateReply{})
	labgob.Register(GcArgs{})
	labgob.Register(GcReply{})
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
