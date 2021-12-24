package shardkv

import (
	"bytes"
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
}

func (kv *ShardKV) isDuplicateRequest(clientId int64, cmdId int) bool {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	if result, ok := kv.lastOpResult[clientId]; ok && result.RequestId >= cmdId {
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
		INFO("Group: %d shard:  %+v", kv.gid, kv.config.Shards)
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
			INFO("reply: %+v", *reply)
		} else {
			INFO("Warning, reply is not match args: %+v", *reply)
		}
	case <-time.After(300 * time.Millisecond):
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
		select {
		case <-ch:
		default:
			break
		}
	}
	return kv.chanNotify[index]
}

// handle command
func (kv *ShardKV) handleCommand() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			cmd := msg.Command.(CommandArgs)
			reply := CommandReply{
				ClientId:   cmd.ClientId,
				RequestId:  cmd.RequestId,
				StatusCode: OK,
				Value:      "",
			}
			if !kv.isDuplicateRequest(cmd.ClientId, cmd.RequestId) || cmd.OpType == OpReConfig || cmd.OpType == OpGc {
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
				kv.lastOpResult[cmd.ClientId] = reply
			} else {
				reply = kv.lastOpResult[cmd.ClientId]
			}
			kv.mu.Lock()
			ch := kv.getNotifyChan(msg.CommandIndex)
			kv.SaveSnapshot(msg.CommandIndex)
			kv.mu.Unlock()
			ch <- reply

		} else if len(msg.Snapshot) > 0 {
			kv.mu.Lock()
			kv.ReadFromSnapshot(msg.Snapshot)
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) SaveSnapshot(index int) {
	if kv.maxRaftState != -1 && kv.maxRaftState < kv.rf.GetRaftStateSize() {
		INFO("Group:%d,db:%d maxRaftState: %d, get raft state size: %d", kv.gid, len(kv.db.data), kv.maxRaftState, kv.rf.GetRaftStateSize())
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.lastOpResult)
		e.Encode(kv.config)
		e.Encode(kv.db.data)
		go kv.rf.MakeRaftSnaphot(w.Bytes(), index)
	}
}

func (kv *ShardKV) ReadFromSnapshot(snapshot []byte) {
	r := bytes.NewReader(snapshot)
	d := labgob.NewDecoder(r)
	var last_log_index int // unused placeholder
	var last_log_term int  // unused placeholder
	d.Decode(&last_log_index)
	d.Decode(&last_log_term)

	d.Decode(&kv.lastOpResult)
	d.Decode(&kv.config)
	d.Decode(&kv.db.data)

	INFO("Group:%d, read snapshot size: %d, data len: %d", kv.gid, len(snapshot), len(kv.db.data))
}

// 提取当前配置中本group持有的没有的shards，而新的配置要分给本group的shards
func (kv *ShardKV) extractMigrateShards(newConfig shardmaster.Config) map[int][]int {
	shardsGroups := map[int][]int{}
	for i := 0; i < shardmaster.NShards; i++ {
		if newConfig.Shards[i] == kv.gid && kv.config.Shards[i] != kv.gid {
			gid := kv.config.Shards[i]
			if gid == 0 {
				continue
			}
			if _, ok := shardsGroups[gid]; !ok {
				shardsGroups[gid] = make([]int, 0)
			}
			shardsGroups[gid] = append(shardsGroups[gid], i)
		}
	}
	return shardsGroups
}

// Migrate data RPC
func (kv *ShardKV) MigrateData(args *MigrateArgs, reply *MigrateReply) {
	INFO("Group:%d Migrate data", kv.gid)
	if args.Num > kv.config.Num {
		reply.StatusCode = ErrNotReady
		return
	}
	reply.RequestResult = make(map[int64]CommandReply, 1)
	for clientId, result := range kv.lastOpResult {
		reply.RequestResult[clientId] = result
	}
	reply.Data = kv.db.CopyFrom(args.Shards)
	reply.StatusCode = OK
	INFO("Migrate data reply: %*+v", reply)
}

func (kv *ShardKV) sendMigrateGroups(gid int, args *MigrateArgs, reply *MigrateReply) Err {
	INFO("Group: %d, gid: %d, config: %+v", kv.gid, gid, kv.config)
	for _, server := range kv.config.Groups[gid] {
		srv := kv.make_end(server)
		ok := srv.Call("ShardKV.MigrateData", args, reply)
		INFO("Ok: %t, args: %+v, reply:%+v", ok, args, reply)
		if ok && reply.StatusCode == OK {
			return OK
		}
		if ok && reply.StatusCode == ErrNotReady {
			return ErrNotReady
		}
	}
	return OK
}

func (kv *ShardKV) forwardConfig(newConfig shardmaster.Config) (CommandArgs, bool) {
	migrateShardsGroups := kv.extractMigrateShards(newConfig)

	data := [shardmaster.NShards]map[string]string{}
	resultRecord := map[int64]CommandReply{}
	for i := 0; i < shardmaster.NShards; i++ {
		data[i] = make(map[string]string)
	}
	ok := true
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
			reply.RequestResult = make(map[int64]CommandReply)
			statusCode := kv.sendMigrateGroups(gid, args, reply)
			if statusCode != OK {
				ok = false
			}
			INFO("Group: %d, statusCode: %v, reply:%+v", kv.gid, statusCode, *reply)
			mutex.Lock()
			for shard, kv := range reply.Data {
				for k, v := range kv {
					data[shard][k] = v
				}
				for clientId, result := range reply.RequestResult {
					resultRecord[clientId] = result
				}
			}
			mutex.Unlock()
		}(gid, migrateArgs, &MigrateReply{})
	}
	wg.Wait()
	args := CommandArgs{
		OpType:       OpReConfig,
		Config:       newConfig,
		Data:         data,
		ResultRecord: resultRecord,
	}
	return args, ok
}

func (kv *ShardKV) applyCommand(args CommandArgs) bool {
	// TODO: Check if the command is repeated
	ok := true
	index, _, isLeader := kv.rf.Start(args)
	if !isLeader {
		return false
	}
	kv.mu.Lock()
	ch := kv.getNotifyChan(index)
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
				if cmd, ok := kv.forwardConfig(nextConfig); ok {
					kv.applyCommand(cmd)
				}
			}
		}
		time.Sleep(syncTime * time.Millisecond)
	}
}

func (kv *ShardKV) applyReConfig(args CommandArgs) (string, Err) {
	// 1.merge request record
	for clientId, record := range args.ResultRecord {
		if result, ok := kv.lastOpResult[clientId]; !ok || result.RequestId < record.RequestId {
			kv.lastOpResult[clientId] = record
		}
	}
	// 2.apply pulled  data
	kv.db.UpdateFrom(args.Data)
	// 3.apply this config
	// lastConfig := kv.config
	kv.config = args.Config
	// INFO("CONFIG: %+v", kv.config)
	// 4.send GC RPC
	// 已经应用了这些DATA，就再申请移除相关的数据
	// TODO: To implement GC
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
	labgob.Register(CommandArgs{})
	labgob.Register(CommandReply{})
	labgob.Register(MigrateArgs{})
	labgob.Register(MigrateReply{})
	labgob.Register(shardmaster.Config{})
	labgob.Register(StoreComponent{})
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
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// start a command coroutine
	go kv.handleCommand()
	// start a update config and data coroutine
	go kv.updateConfig(50)
	return kv
}
