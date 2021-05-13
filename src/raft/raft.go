package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"
type StateMachine = int32

const StateCandidate = 0
const StateFollower = 1
const StateLeader = 2

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int32
	Index   int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
// TODO:Your data here (2A, 2B, 2C).
// Look at the paper's Figure 2 for a description of what
// state a Raft server must maintain.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int32               // this peer's index into peers[]
	dead      int32               // set by Kill()
	msg       chan ApplyMsg

	currentTerm int32
	log         []LogEntry
	commitIndex []int32
	lastApplied int32

	voted    int32 // whether voted
	votedFor map[int32]int32
	state    int32
	leaderId int32     // leader id
	lastTime time.Time // 上次心跳时间
}

// return currentTerm and whether this server
// believes it is the leader.
// TODO:Your code here (2A).
func (rf *Raft) GetState() (int, bool) {
	currentTerm := int(rf.currentTerm)
	isLeader := (rf.me == rf.leaderId)

	return currentTerm, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
// TODO: 持久化调用这个可以持久化State
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
// TODO:Your data here (2A, 2B).
type RequestVoteArgs struct {
	Term         int32 // current term
	CandidatedId int32 // candidate requesting vote
	LastLogIndex int32 // candidate requesting vote
	LastLogTerm  int32 // term of candidiate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
// TODO:Your data here (2A).
type RequestVoteReply struct {
	Term        int32 // candidate's term
	VoteGranted bool  // whether candidate received vote
}

type VotedResultArgs struct {
	Asked    bool
	VotedFor map[int32]int32
}

type VotedResultReply struct {
	State    StateMachine
	LeaderId int32
	VotedFor map[int32]int32
}

type LogEntryArgs struct {
	Term             int32
	LastAppliedIndex int32
	CommitIndex      int32
	Entry            LogEntry
}

type LogEntryReply struct {
	Term        int32
	lastApplied int32
	CommitIndex int32
}

//
// example RequestVote RPC handler.
// TODO:Your code here (2A, 2B).
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	log.Printf("SERVER:%d, RequestVote", rf.me)
	if args.Term > rf.currentTerm && args.LastLogIndex >= rf.lastApplied {
		// 如果是Leader或者Candidate都会变成 Follower
		if rf.voted != 1 {
			atomic.StoreInt32(&rf.voted, 1)
			rf.votedFor[args.CandidatedId] += 1
			reply.VoteGranted = true
		} else {
			log.Printf("SERVER: %d, voted for %v", rf.me, rf.votedFor)
		}
	} else {
		// 不符合投票条件
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm

	// return nil
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
// TODO: current need to change to last term
func (rf *Raft) sendRequestVote() {
	log.Printf("SERVER: %d sendRequestVote", rf.me)

	args := RequestVoteArgs{rf.currentTerm, rf.me, rf.lastApplied, rf.currentTerm}
	votedResult := make(map[int32]int32)
	for i, peer := range rf.peers {
		if int32(i) != rf.me {
			reply := RequestVoteReply{}
			peer.Call("Raft.RequestVote", &args, &reply)
			// if err {
			// 	log.Fatalf("RequestVote fail")
			// }
			if reply.VoteGranted {
				votedResult[int32(i)] += 1

			}
		}
	}

	rf.votedFor = votedResult
}

func (rf *Raft) AppendEntries(args *RequestVoteArgs, reply *RequestVoteReply) error {
	log.Printf("SERVER:%d, AppendEntries", args.Term)

	if args.Term == -1 {
		rf.lastTime = time.Now()
	} else {
		// TODO: append entries
		// Update log entries
	}
	return nil
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Ask each server information include state and voted,
// and send voted result
func (rf *Raft) broadcastElectResult() int32 {
	log.Printf("SERVER: %d, state: %d, broadcastElectResult: %v", rf.me, rf.state, rf.votedFor)

	// 1. Ask the voted result.
	peerState := map[int32]StateMachine{}
	votedResult := rf.votedFor

	args := VotedResultArgs{}
	args.Asked = false
	args.VotedFor = rf.votedFor

	for i, peer := range rf.peers {
		if int32(i) != rf.me {
			reply := VotedResultReply{}
			peer.Call("Raft.BroadcastResult", &args, &reply)

			for k, v := range reply.VotedFor {
				votedResult[k] += v
				peerState[k] = reply.State
			}
		}
	}
	// 2. Broadcast the voted result to each server.
	log.Printf("SERVER:%d, Broadcast result: %v", rf.me, votedResult)
	args.Asked = true
	args.VotedFor = votedResult
	reply := VotedResultReply{}
	for i, peer := range rf.peers {
		// Only boardcast
		if i != int(rf.me) && peerState[int32(i)] == StateCandidate {
			peer.Call("Raft.BroadcastResult", &args, &reply)
		}
	}
	var leaderId int32 = -1
	var voted int32 = -1
	for i, v := range votedResult {
		if v > voted {
			leaderId = i
			voted = v
		}
	}
	return leaderId
}

// Asked by other servers.
func (rf *Raft) BroadcastResult(args *VotedResultArgs, reply *VotedResultReply) {
	if !args.Asked {
		reply.State = rf.me
		reply.VotedFor = rf.votedFor
	} else {
		if reply.LeaderId != -1 {
			atomic.StoreInt32(&rf.leaderId, reply.LeaderId)
		}
		log.Printf("BroadcastResult: %d", reply.LeaderId)
	}
	// return nil
}

// periodically send heartbreak to Followers.
func (rf *Raft) reportLive(state chan StateMachine) {
	log.Printf("SERVER: %d, reportLive", rf.me)
	for {
		time.Sleep(time.Millisecond * 100)
		go func() {
			args := RequestVoteArgs{}
			reply := RequestVoteReply{}
			rf.peers[rf.me].Call("Raft.AppendEntries", &args, &reply)
		}()
		if rf.state != StateLeader {
			log.Printf("SERVER: %d, is change to %d\n", rf.me, rf.state)
			break
		}
	}
}

// reset timer
func (rf *Raft) resetTimer() {
	rf.lastTime = time.Now()
}

func (rf *Raft) resetVote() {
	atomic.StoreInt32(&rf.voted, 0)
	rf.votedFor = map[int32]int32{}
}

// monitor leader live
func (rf *Raft) monitorLive(stateChan chan StateMachine) {
	log.Printf("SERVER: %d, monitorLive", rf.me)
	for {
		timeout := rand.Intn(50) + 150 // ms
		time.Sleep(time.Millisecond * time.Duration(timeout))
		if rf.state == StateFollower {

			elapsed := time.Now().Sub(rf.lastTime)
			if elapsed.Milliseconds() > 100*time.Millisecond.Microseconds() && rf.voted != -1 {
				stateChan <- StateCandidate // 转变为候选人
				return
			}
		} else {
			return
			// break
		}
	}
}

func (rf *Raft) applyMsg() {
	log.Printf("SERVER: %d, applyMsg", rf.me)
	for msg := range rf.msg {
		if rf.state == StateLeader {
			if msg.CommandValid {
				entry := LogEntry{rf.currentTerm, msg.CommandIndex, msg.Command}
				rf.log = append(rf.log, entry)
				rf.lastApplied = int32(msg.CommandIndex)
				go rf.sendAppendEntries()
			} else {
				log.Fatalf("ERR: SERVER:%d recevide a invalid command: %v", rf.me, msg)
			}
		} else {
			log.Printf("SERVER:%d, State is not Leader, %d", rf.me, rf.state)
			return
		}
	}
}

func (rf *Raft) sendAppendEntries() {
	log.Printf("SERVER: %d, sendAppendEntries", rf.me)

	for i, peer := range rf.peers {
		if i != int(rf.me) {
			args := LogEntryArgs{}
			reply := LogEntryReply{}

			args.Term = rf.currentTerm

			args.LastAppliedIndex = int32(rf.lastApplied)
			args.CommitIndex = rf.commitIndex[len(rf.commitIndex)-1]

			peer.Call("Raft.AppendEntries", &args, &reply)
			// if err {
			// 	log.Fatalf("AppendEntries fail")
			// }
		}
	}
}

func (rf *Raft) run() {
	log.Printf("SERVER:%d is running", rf.me)
	stateChan := make(chan StateMachine)
	defer close(stateChan)

	for {

		// Leaders:
		//  - 选举成功：发送 initial empty AppendEntries RPCs(heartbeat)给其他服务器
		//  - 收到client的command: append entry，之后发送entry给 state machine
		//  - if last log index >= nextIndex for a followrr: 发送AppendEntries RPC给server
		//    - 成功后: follower更新 nextIndex和matchIndex
		//    - 失败后: 自降nextIndex、重试
		//  -当前任期的log.term == currentTerm，set commitIndex = N
		if rf.state == StateLeader {
			go rf.reportLive(stateChan) // 周期性发送心跳
			go rf.applyMsg()            // 处理应用消息
		} else if rf.state == StateFollower {
			// Followers:
			// - 响应candidates和leaders
			//   - RequestVote, AppendEntries
			// - 没有定时收到心跳或者投票请求：转化为candidate
			go rf.monitorLive(stateChan)
		} else if rf.state == StateCandidate {
			// Candidates:
			// 转化为Candidates，开始选举
			//  - currentTerm自增
			//  - 投票给自己
			//  - 重制election timer
			//  - 发送RequestVote RPC给其他servers
			rf.currentTerm += 1
			rf.voted += 1
			rf.votedFor[rf.me] += 1
			for {
				rf.sendRequestVote()

				leaderId := rf.broadcastElectResult()
				log.Printf("election result: %d", leaderId)
				if leaderId == rf.me {
					rf.resetTimer()
					rf.state = StateLeader
					break
				} else if leaderId == -1 {
					log.Printf("re-election")
					timeout := rand.Intn(150) + 150
					time.Sleep(time.Millisecond * time.Duration(timeout))
				} else {
					break
				}
			}
			rf.resetVote()
			go func(s StateMachine, stateChan chan StateMachine) {
				stateChan <- s
			}(rf.state, stateChan)
			select {
			case <-stateChan:
			}
			// case <-rf.dead:
			if rf.dead == 1 {
				log.Printf("SERVER: %d, Recevice Kill signal", rf.me)
				return
			}
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
// TODO:Your initialization code here (2A, 2B, 2C).
// Create a Raft server.
// Ports of all the Raft servers: peers[]
// channel: Raft to send ApplyMsg messages
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	log.Println("Raft start initialize.")

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = int32(me)
	rf.state = StateFollower
	rf.msg = applyCh

	rf.currentTerm = 0
	rf.lastApplied = -1
	rf.leaderId = -1
	rf.lastTime = time.Now()
	rf.state = StateCandidate
	rf.votedFor = make(map[int32]int32)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.run()
	return rf
}

// Make()
// Create a new Raft server
// 领导是否存在
// 随机时间开始成为candidate
// 请求投票 & 重新投票
// 选出新的Leader
// 新的Leader开启后台hearbreaks
// Leader执行所有的Command
