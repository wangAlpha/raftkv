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
	"os"
	"sync"
	"sync/atomic"
	"time"

	"mit6.824/src/labrpc"
)

// import "bytes"
// import "mit6.824/src/labgob"

type StateMachine int

const (
	STATE_FOLLOWER = iota
	STATE_CANDIDATE
	STATE_LEADER
)

var StateString = map[int]string{
	STATE_LEADER:    "Leader",
	STATE_CANDIDATE: "Candidate",
	STATE_FOLLOWER:  "Follower",
}

// file, err := os.OpenFile("logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
var (
	INFO = log.New(os.Stderr, "INFO:", os.O_APPEND|os.O_CREATE|os.O_WRONLY)
	WARN = log.New(os.Stderr, "WARN:", os.O_APPEND|os.O_CREATE|os.O_WRONLY)
	ERR  = log.New(os.Stderr, "ERR:", os.O_APPEND|os.O_CREATE|os.O_WRONLY)
)

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
	Term    int
	Index   int
	Command ApplyMsg
	// Command interface{}
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
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	chanMsg   chan ApplyMsg

	currentTerm int
	log         []LogEntry
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	votedFor    int // voted for client
	votedResult map[int]int
	state       int
	leaderId    int // current lead id

	chanVoteGranted chan bool
	chanWinElect    chan bool
	chanHeartbeat   chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
// TODO:Your code here (2A).
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	isLeader := (rf.me == rf.leaderId)
	rf.mu.Unlock()

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
	// e := labgob.NewEncode(w)
	// e.Encode(rf.currentTerm)
	// e.Encode(rf.state)
	// e.Encode(rf.log)
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
	Term         int // current term
	CandidatedId int // candidate requesting vote
	LastLogIndex int // candidate requesting vote
	LastLogTerm  int // term of candidiate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
// TODO:Your data here (2A).
type RequestVoteReply struct {
	Term        int  // candidate's term
	VoteGranted bool // whether candidate received vote
}

type LogEntryArgs struct {
	Term         int // Leade's Term
	LeaderId     int
	PrevLogIndex int // Index of log entry immediately preceding new ones

	PrevLogTerm int // Term of prevLogTerm
	Entries     []LogEntry

	CommitIndex int // leader's commitIndex

	LeaderCommit int
}

type LogEntryReply struct {
	Term        int
	Success     bool
	LastApplied int
	CommitIndex int
}

//
// RequestVote RPC handler.
// TODO:Your code here (2A, 2B).
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	INFO.Printf("SERVER:%d, RequestVote", rf.me)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term > rf.currentTerm && args.LastLogIndex >= rf.lastApplied {
		if rf.votedFor == -1 {
			rf.votedFor = args.CandidatedId
			reply.VoteGranted = true
			rf.chanVoteGranted <- true
		}
	}
}

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
// look at the comments in mit6.824/src/labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
// TODO: current need to change to last term
func (rf *Raft) BroadcastRequestVote() {
	INFO.Printf("BROADCAST request vote, SERVER: %d sendRequestVote", rf.me)

	args := &RequestVoteArgs{rf.currentTerm, rf.me, rf.lastApplied, rf.LastLogTerm()}

	for peer := range rf.peers {
		if peer != rf.me && rf.state == STATE_CANDIDATE {
			go rf.SendRequestVote(peer, args, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) SendRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[peer].Call("Raft.RequestVote", args, reply)
	if !ok {
		ERR.Fatalf("Raft.RequestVote occur errr, SERVER: %d", peer)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.ToFollower()
		return
	}
	if reply.VoteGranted {
		rf.votedResult[peer] += 1
	}

	majority := len(rf.peers)/2 + 1
	vote := len(rf.votedResult)
	if len(rf.peers)%2 == 0 && vote == majority {
		INFO.Printf("ELECT FAIL, the appear the same number of votes, vote: %d", vote)
	} else if vote >= majority {
		rf.chanWinElect <- true
		rf.persist()
	}
	INFO.Printf("BROADCAST request vote result, SERVER:%d, Result: %d, %v", rf.me, rf.votedFor, rf.votedResult)
}

func PrintLog(msg string, logs []LogEntry) {
	// commands := []interface{}
	var cmds []interface{}
	for _, v := range logs {
		cmds = append(cmds, v.Command)
	}
	INFO.Printf("%s, Command: %+v", msg, cmds)
}

func (rf *Raft) AppendEntries(args *LogEntryArgs, reply *LogEntryReply) {
	INFO.Printf("SERVER:%d, AppendEntries, State: %s, currentTerm: %d, log: %v", rf.me, StateString[rf.state], rf.currentTerm, rf.log)
	baseIndex := len(rf.log) - 1
	PrintLog("ApendEntries begin", rf.log)
	// 1. Reply false if term < currentTerm
	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		reply.LastApplied = -1
		reply.CommitIndex = -1
		return
	}
	rf.currentTerm = args.Term

	// 2. Reply false if log does't contain an entry at prevLogIndex
	//	 whose term matches prevLogTerm
	if args.PrevLogIndex > len(rf.log)-1 {
		reply.LastApplied = rf.lastApplied
		reply.CommitIndex = rf.commitIndex
		return
	}

	rf.leaderId = args.LeaderId
	rf.chanHeartbeat <- true

	// TODO
	if args.PrevLogIndex > rf.lastApplied {
		if rf.log[rf.lastApplied].Term == rf.currentTerm {
			for i := 0; i < args.PrevLogIndex-rf.lastApplied; i++ {
				rf.log = append(rf.log, args.Entries[i])
			}
			reply.LastApplied = args.PrevLogIndex
			reply.CommitIndex = rf.commitIndex
			reply.Success = true
		}
	}
	if args.PrevLogIndex < rf.lastApplied {
		index := args.PrevLogIndex
		for rf.log[args.PrevLogIndex].Term != rf.currentTerm && index > 0 {
			index--
		}
		reply.LastApplied = index
		// reply.CommitIndex = index
		reply.Success = true
	}

	if l := args.PrevLogIndex - rf.lastApplied; l == len(args.Entries) && l > 0 {
		logIndex := 0
		index := rf.lastApplied
		for index < args.PrevLogIndex {
			rf.log[index] = args.Entries[logIndex]
			index++
			logIndex++
		}
		rf.lastApplied = index
		rf.commitIndex = index
		reply.LastApplied = index
		reply.CommitIndex = index
		reply.Success = true
	}
	go func() {
		// INFO.Printf("")
		PrintLog("AppendEntries end", rf.log)
		for i := baseIndex; i < len(rf.log); i++ {
			rf.chanMsg <- rf.log[i].Command
		}
	}()
	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that follow
	// 4. Append any new entries not already in the log
	// 5. If leaderCommit > commitIndex, set commitIndex =
	//	min(leaderCommit, index of last new entry)
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
	// INFO.Printf("Test send a command: %d", command)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := (rf.me == rf.leaderId)
	if isLeader {
		index = rf.BuildMsg(command).CommandIndex
		term = rf.currentTerm
	}

	return index, term, isLeader
}

func (rf *Raft) BuildMsg(command interface{}) ApplyMsg {
	if len(rf.log) > 0 {
		rf.lastApplied = rf.log[len(rf.log)-1].Index
	}
	msg := ApplyMsg{}
	msg.CommandValid = true
	msg.Command = command
	msg.CommandIndex = rf.lastApplied
	rf.lastApplied++

	logEntry := LogEntry{rf.currentTerm, msg.CommandIndex, msg}

	rf.log = append(rf.log, logEntry)

	go rf.PublishMsg(false)

	return msg
}

func (rf *Raft) PublishMsg(heartbeat bool) {
	if rf.state != STATE_LEADER {
		log.Printf("Server:%d, PublishMsg fail!Command:%v", rf.me, rf.log)
		return
	}
	if heartbeat {
		INFO.Printf("Heartbeat,Server: %d, state: %s", rf.me, StateString[rf.state])
	} else {
		INFO.Printf("ApplyState,Server: %d, leader: %d", rf.me, rf.leaderId)
	}

	args := &LogEntryArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex

	if args.LeaderCommit > rf.lastApplied {
		args.LeaderCommit = rf.lastApplied
	}

	var applied int32 = 0
	wg := sync.WaitGroup{}
	for peer := range rf.peers {
		nextIndex := rf.nextIndex[peer] - 1
		if nextIndex < 0 {
			nextIndex = 0
		}

		args.PrevLogIndex = rf.LastLogIndex()
		args.PrevLogTerm = rf.LastLogTerm()

		if peer == rf.me && rf.state == STATE_LEADER {
			// TODO: To persistence self data
		} else {
			if nextIndex <= rf.lastApplied && len(rf.log) > 0 {
				args.Entries = rf.log[nextIndex:]
			}
			reply := &LogEntryReply{}
			// Send entries without command whether message is heartbeats
			go func(peer int) {
				wg.Add(1)
				defer wg.Done()
				ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
				if !ok {
					ERR.Printf("Push Msg fail, %d->%d", rf.me, peer)
					return
				}
				if reply.Success {
					atomic.AddInt32(&applied, 1)
				} else {
					INFO.Printf("Send Args:%+v fail! Reply: %+v", args, reply)
					if reply.Term > rf.currentTerm {
						rf.ToFollower()
						return
					}
					rf.nextIndex[peer] = reply.LastApplied
				}
			}(peer)
		}
	}
	wg.Wait()

	majority := int32(len(rf.peers))/2 + 1

	// TODO: send msg to peers, and majority of peer return true, the leader will
	// commit msg to applyMsg
	if applied >= majority {
		INFO.Printf("Commit command %v,", rf.log[rf.commitIndex].Command)
		rf.commitIndex = rf.lastApplied
		rf.chanMsg <- rf.log[rf.commitIndex].Command
	}
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

func (rf *Raft) LastLogIndex() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) LastLogTerm() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Term
}

// periodically send heartbreak to Followers.
func (rf *Raft) BroadcastHeartbeats() {
	// INFO.Printf("SERVER: %d, BroadcastHeartbeats", rf.me)
	rf.PublishMsg(true)
}

func (rf *Raft) ToFollower() {
	rf.votedResult = make(map[int]int)
	rf.state = STATE_FOLLOWER
	rf.votedFor = -1
}

func (rf *Raft) ToLeader() {
	rf.state = STATE_LEADER
	rf.leaderId = rf.me
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.lastApplied + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
}

func (rf *Raft) Run() {
	INFO.Printf("SERVER:%d is run", rf.me)

	for {
		switch rf.state {
		case STATE_FOLLOWER:
			select {
			case <-rf.chanVoteGranted:
			case <-rf.chanHeartbeat:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(200)+200)):
				rf.state = STATE_CANDIDATE
				// rf.persist()
				INFO.Printf("State change, SERVER:%d become Follower to Candidate", rf.me)
			}
		case STATE_LEADER:
			go rf.BroadcastHeartbeats()
			time.Sleep(time.Millisecond * 60)
		case STATE_CANDIDATE:
			rf.mu.Lock()
			term := rf.currentTerm
			rf.leaderId = -1
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.mu.Unlock()
			go rf.BroadcastRequestVote()

			select {
			case <-rf.chanWinElect:
				rf.ToLeader()
				INFO.Printf("ELECT Result, %d", rf.me)
			case <-rf.chanHeartbeat:
				rf.ToFollower()
				INFO.Printf("SELECT OVER, Server: %d, State: %s", rf.me, StateString[rf.state])
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(300)+200)):
				rf.currentTerm = term // Recover term
				INFO.Printf("SELECT OVERTIME, SERVER: %d, State: %s", rf.me, StateString[rf.state])
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
// channel: Raft to send ApplyMsg messages to state machine
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = STATE_FOLLOWER
	rf.chanMsg = applyCh

	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.leaderId = -1
	rf.votedFor = -1
	rf.votedResult = make(map[int]int)
	rf.log = append(rf.log, LogEntry{Term: 0})
	// rf.nextIndex = mak

	rf.chanWinElect = make(chan bool, 64)
	rf.chanVoteGranted = make(chan bool, 64)
	rf.chanHeartbeat = make(chan bool, 64)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.Run()
	return rf
}
