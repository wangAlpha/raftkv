package shardmaster

import (
	"sync"
	"time"

	"mit6.824/src/labgob"
	"mit6.824/src/labrpc"
	"mit6.824/src/raft"
)

type OpResult struct {
	Command Command
	Result  CommandReply
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	lastOperation map[int64]int64
	lastOpResult  map[int]chan OpResult
	configs       []Config // indexed by config num
}

func (master *ShardMaster) IsDuplicateRequest(args *CommandArgs) bool {
	master.mu.Lock()
	defer master.mu.Unlock()
	if cmd_id, ok := master.lastOperation[args.ClientId]; ok && cmd_id >= args.RequestId {
		return false
	}
	master.lastOperation[args.ClientId] = args.RequestId
	return true
}

func (master *ShardMaster) HandleRequest(args *CommandArgs, reply *CommandReply) {
	if master.IsDuplicateRequest(args) {
		reply.StatusCode = ErrDuplicateOp
		return
	}
	cmd_index, _, is_leader := master.rf.Start(args.Command)
	if !is_leader {
		reply.StatusCode = ErrNoneLeader
		return
	}
	master.mu.Lock()
	if _, ok := master.lastOpResult[cmd_index]; !ok {
		master.lastOpResult[cmd_index] = make(chan OpResult)
	}
	ch := master.lastOpResult[cmd_index]
	master.mu.Unlock()

	select {
	case op_result := <-ch:
		if op_result.Command.ClientId == args.ClientId && op_result.Command.CommandId == args.RequestId {
			*reply = op_result.Result
		}
	case <-time.After(time.Millisecond * 400):
		reply.StatusCode = ErrTimeout
	}
	master.mu.Lock()
	delete(master.lastOpResult, cmd_index)
	master.mu.Unlock()
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (master *ShardMaster) Kill() {
	master.rf.Kill()
}

// needed by shardkv tester
func (master *ShardMaster) Raft() *raft.Raft {
	return master.rf
}

func (master *ShardMaster) FindLeastShardGid(shardGroup map[int][]int) int {
	minGid := 0
	minNum := int(0xfffffff)
	for gid, groups := range shardGroup {
		if len(groups) < minNum {
			minGid = gid
			minNum = len(groups)
		}
	}
	return minGid
}

func (master *ShardMaster) FindGreatestShardGid(shardGroup map[int][]int) int {
	maxGid := 0
	maxNum := -1
	for gid, groups := range shardGroup {
		if len(groups) > maxNum {
			maxGid = gid
			maxNum = len(groups)
		}
	}
	return maxGid
}

func (master *ShardMaster) ReBalanceShards(shardGroup map[int][]int, shard [NShards]int) [NShards]int {
	for {
		// 判断是否平衡
		if master.IsBalance(shardGroup) {
			INFO("Now shardGroup is balance : %+v", shardGroup)
			break
		}
		// 找到具有最少shard的gid
		leastShardGid := master.FindLeastShardGid(shardGroup)
		// 找到最多分片的gid
		greatestShardGid := master.FindGreatestShardGid(shardGroup)
		// 将最多的shard的gid分分一部分给对应的gid
		len := len(shardGroup[greatestShardGid])
		moveShard := shardGroup[greatestShardGid][len-1]
		shardGroup[leastShardGid] = append(shardGroup[leastShardGid], moveShard)
		shardGroup[greatestShardGid] = shardGroup[greatestShardGid][:len-1]
	}
	newShards := [NShards]int{}
	// 将shards划分到newShard切片中
	for gid, serverList := range shardGroup {
		for _, server := range serverList {
			newShards[server] = gid
		}
	}
	return newShards
}

func (master *ShardMaster) IsBalance(shardGroups map[int][]int) bool {
	if len(shardGroups) == 0 {
		return true
	}
	maxGroupLen := -1
	minGroupLen := int(0xffffffff)
	for _, serverList := range shardGroups {
		serverLen := len(serverList)
		maxGroupLen = Max(maxGroupLen, serverLen)
		minGroupLen = Min(minGroupLen, serverLen)
	}
	return maxGroupLen-minGroupLen <= 1
}

func (master *ShardMaster) ConstructShardGroups(shards [NShards]int, groups map[int][]string) map[int][]int {
	shardGroups := map[int][]int{}
	for gid := range groups {
		shardGroups[gid] = make([]int, 0)
	}
	is_zero := func(x int) bool { return x == 0 }
	// 假如 shards都是零，直接将其平均分配给gid
	if All(shards[:], is_zero) {
		gids := make([]int, 0)
		for gid := range groups {
			gids = append(gids, gid)
		}
		for i, _ := range shards {
			gid := gids[i%len(gids)]
			shardGroups[gid] = append(shardGroups[gid], i)
		}
	} else {
		// 现有gid->shardsList分配情况
		for i, gid := range shards {
			shardGroups[gid] = append(shardGroups[gid], i)
		}
	}
	INFO("shards: %+v", shards)
	INFO("shardGroups: %+v", shardGroups)
	return shardGroups
}

// gid -> serverList
func (master *ShardMaster) Join(servers map[int][]string) {
	INFO("Join %+v", servers)

	lastConfig := master.configs[len(master.configs)-1]
	newConfig := Config{len(master.configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}
	for gid, serverList := range servers {
		newConfig.Groups[gid] = serverList
	}
	shardsGroups := master.ConstructShardGroups(newConfig.Shards, newConfig.Groups)
	INFO("after shards: %+v, Groups: %+v", newConfig.Shards, newConfig.Groups)
	newConfig.Shards = master.ReBalanceShards(shardsGroups, newConfig.Shards)
	INFO("before shards: %+v, Groups: %+v", newConfig.Shards, newConfig.Groups)
	master.configs = append(master.configs, newConfig)
}

func (master *ShardMaster) Move(shard int, gid int) {
	lastConfig := master.configs[len(master.configs)-1]
	newConfig := Config{len(master.configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}

	newConfig.Shards[shard] = gid

	master.configs = append(master.configs, newConfig)
}

func (master *ShardMaster) Leave(gids []int) {
	lastConfig := master.configs[len(master.configs)-1]
	newConfig := Config{len(master.configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}
	for gid := range gids {
		delete(newConfig.Groups, gid)
	}
	master.configs = append(master.configs, newConfig)
}

func (master *ShardMaster) Query(num int) Config {
	if num == -1 || num >= len(master.configs) {
		return master.configs[len(master.configs)-1]
	}
	return master.configs[num]
}

func (master *ShardMaster) Run() {
	for command := range master.applyCh {
		// INFO("cmd: %+v", command)
		if !command.CommandValid {
			continue
		}
		reply := CommandReply{StatusCode: Ok}
		cmd := command.Command.(Command)
		INFO("OP: %+v Num: %d GIDs: %v, Shard: %v GID:%v, Servers:%v",
			OpName[cmd.OpType], cmd.Num, cmd.GIDs, cmd.Shard, cmd.GID, cmd.Servers)
		switch cmd.OpType {
		case OpJoin:
			master.Join(cmd.Servers)
			reply.Config = master.configs[len(master.configs)-1]
		case OpMove:
			master.Move(cmd.GID, cmd.Shard)
			reply.Config = master.configs[len(master.configs)-1]
		case OpLeave:
			master.Leave(cmd.GIDs)
			reply.Config = master.configs[len(master.configs)-1]
		case OpQuery:
			reply.Config = master.Query(cmd.Num)
			INFO("Query Result: %+v", reply.Config)
		}

		master.mu.Lock()
		if _, ok := master.lastOpResult[command.CommandIndex]; !ok {
			master.lastOpResult[command.CommandIndex] = make(chan OpResult)
		}
		ch := master.lastOpResult[command.CommandIndex]
		master.mu.Unlock()
		result := OpResult{Command: cmd, Result: reply}

		ch <- result
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	labgob.Register(Command{})
	server := &ShardMaster{
		mu:            sync.Mutex{},
		me:            me,
		applyCh:       make(chan raft.ApplyMsg),
		lastOperation: map[int64]int64{},
		lastOpResult:  map[int]chan OpResult{},
		configs:       make([]Config, 1),
	}
	config := Config{
		Num:    0,
		Shards: [NShards]int{},
		Groups: map[int][]string{},
	}
	server.configs[0] = config
	server.rf = raft.Make(servers, me, persister, server.applyCh)

	go server.Run()

	return server
}
