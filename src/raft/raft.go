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
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"mit6.824/src/labgob"
	"mit6.824/src/labrpc"
)

// import "bytes"
// import "mit6.824/src/labgob"

type StateMachine int

const (
	StateFollower = iota
	StateCandidate
	StateLeader
)

var StateString = map[int]string{
	StateLeader:    "Leader",
	StateCandidate: "Candidate",
	StateFollower:  "Follower",
}

// file, err := os.OpenFile("logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
var (
	INFO = log.New(os.Stderr, "INFO:", log.Ltime|log.Lshortfile)
	WARN = log.New(os.Stderr, "WARN:", log.Ltime|log.Lshortfile)
	// ERR  = log.New(os.Stderr, "ERR:", log.Ldate|log.Lshortfile)
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
	CommandIndex int
	Command      interface{}

	UseSnapshot bool
	Snapshot    []byte
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
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
	commitIndex int // Index of highest log entry known to be committed (initialized to 0)
	lastApplied int // Index of highest log entry applied to state machine (initialized to 0)

	nextIndex  []int // State on leader, next log index to send to that server
	matchIndex []int // State on leader, index of highest log entry known to be replicated on server

	votedFor    int         // voted for client
	votedResult map[int]int // received from others server

	state    int
	leaderId int // current lead id

	chanVoteGranted chan bool
	chanWinElect    chan bool
	chanHeartbeat   chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
// TODO:Your code here (2A).
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm

	return currentTerm, rf.state == StateLeader
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

func (rf *Raft) RecoverFromSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	last_included_index := 0
	last_included_term := 0
	d.Decode(&last_included_index)
	d.Decode(&last_included_term)
	rf.commitIndex = last_included_index
	rf.lastApplied = last_included_index
	rf.TrimLog(last_included_index, last_included_term)
	msg := ApplyMsg{
		UseSnapshot: true,
		Snapshot:    snapshot,
	}
	rf.chanMsg <- msg
	// d.Decode()
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

	CommitIndex  int // leader's commitIndex
	LeaderCommit int
}

type LogEntryReply struct {
	Term         int
	Success      bool
	LastApplied  int
	CommitIndex  int
	NextTryIndex int
}

type InstallSnapshotArg struct {
	Term              int // leader' term
	LeaderId          int // leader's id
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

//
// RequestVote RPC handler.
// TODO:Your code here (2A, 2B).
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// INFO.Printf("ID:%d, RequestVote", rf.me)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.StateSet(StateFollower)
		rf.currentTerm = args.Term
	}

	term := args.LastLogTerm
	last_log := args.LastLogIndex
	is_log_updated := term > rf.LastLogTerm() || (term == rf.LastLogTerm() && last_log >= rf.LastLogIndex())
	if rf.votedFor == -1 && is_log_updated {
		rf.votedFor = args.CandidatedId
		reply.VoteGranted = true
		rf.chanVoteGranted <- true
	}
	INFO.Printf("ID: %d, votedFor: %d", rf.me, rf.votedFor)
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
	INFO.Printf("ID:%d Broad RequestVote", rf.me)
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidatedId: rf.me,
		LastLogIndex: rf.LastLogIndex(),
		LastLogTerm:  rf.LastLogTerm(),
	}

	for peer := range rf.peers {
		if peer != rf.me && rf.state == StateCandidate {
			go rf.SendRequestVote(peer, args, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) SendRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[peer].Call("Raft.RequestVote", args, reply)
	if !ok || rf.state != StateCandidate || rf.currentTerm != args.Term {
		return ok
	}
	defer rf.persist()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.StateSet(StateFollower)
		return ok
	}

	if reply.VoteGranted {
		rf.votedResult[peer] += 1
		if len(rf.votedResult) >= len(rf.peers)/2 {
			rf.StateSet(StateLeader)
		}
	}
	return ok
}

func PrintLog(msg string, logs []LogEntry) {
	// commands := []interface{}
	var cmds []interface{}
	for _, v := range logs {
		cmds = append(cmds, v)
	}
	INFO.Printf("%s, Command: %+v", msg, cmds)
}

func (rf *Raft) sendAppendEntries(server int, args *LogEntryArgs, reply *LogEntryReply) bool {
	INFO.Printf("ID:%d %d Term:%d log: %d Entries:%d ", rf.me, server, rf.currentTerm, len(rf.log), len(args.Entries))
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok || rf.state != StateLeader || rf.currentTerm != args.Term {
		fmt.Printf("Failed ok%t state: %s term: %d\n", ok, StateString[rf.state], rf.currentTerm)
		return ok
	}
	defer rf.persist()

	if reply.Term > rf.currentTerm {
		rf.StateSet(StateFollower)
		rf.currentTerm = reply.Term
		return ok
	}

	if !reply.Success {
		rf.nextIndex[server] = min(reply.NextTryIndex, rf.LastLogIndex())
	} else {
		if len(args.Entries) > 0 {
			rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		}
	}
	// TODO: to apply log
	// Match log index
	baseIndex := rf.log[0].Index
	for N := rf.LastLogIndex(); N >= rf.commitIndex && rf.log[N-baseIndex].Term == rf.currentTerm; N-- {
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			go rf.applyLog()
			break
		}
	}
	return ok
}

func (rf *Raft) AppendEntries(args *LogEntryArgs, reply *LogEntryReply) {
	INFO.Printf("ID:%d, AppendEntries,state: %s log: %d Entries: %d", rf.me, StateString[rf.state], len(rf.log), len(args.Entries))
	defer INFO.Printf("ID:%d, log: %d END", rf.me, len(rf.log))
	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		// reject and return currentTerm
		INFO.Printf("ID:%d Term:%dRequest term < current Term", rf.me, rf.currentTerm)
		return
	}
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.StateSet(StateFollower)
		rf.leaderId = args.LeaderId
		rf.currentTerm = args.Term
		INFO.Printf("ID:%d received heartbeat: %d", rf.me, len(args.Entries))
	}
	rf.chanHeartbeat <- true
	rf.LogMatch(args, reply)
}

func (rf *Raft) LogMatch(args *LogEntryArgs, reply *LogEntryReply) {
	if args.PrevLogIndex > rf.LastLogIndex() {
		INFO.Printf("PrevLogIndex:%d, LastLogIndex:%d", args.PrevLogIndex, rf.LastLogIndex())
		reply.NextTryIndex = rf.LastLogIndex() + 1
		return
	}
	baseIndex := rf.log[0].Index
	if args.PrevLogIndex >= baseIndex && args.PrevLogTerm != rf.log[args.PrevLogIndex-baseIndex].Term {
		term := rf.log[args.PrevLogIndex-baseIndex].Term
		index := args.PrevLogIndex - 1
		for ; index >= baseIndex; index-- {
			if term != rf.log[index-baseIndex].Term {
				break
			}
		}
		reply.NextTryIndex = index + 1
		INFO.Printf("%d %d", args.PrevLogIndex, reply.NextTryIndex)
	} else if args.PrevLogIndex >= baseIndex-1 {
		INFO.Printf("prevIndex, NextIndex %d %d", args.PrevLogIndex, reply.NextTryIndex)
		rf.log = rf.log[:args.PrevLogIndex-baseIndex+1]
		rf.log = append(rf.log, args.Entries...)

		reply.Success = true
		reply.NextTryIndex = args.PrevLogIndex + len(args.Entries)

		if rf.commitIndex < args.LeaderCommit {
			rf.commitIndex = min(args.LeaderCommit, rf.LastLogIndex())
			go rf.applyLog()
		}
	}
}

func (rf *Raft) applyLog() {
	baseIndex := rf.log[0].Index
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{
			CommandValid: true,
			CommandIndex: i,
			Command:      rf.log[i-baseIndex].Command,
		}
		rf.chanMsg <- msg
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) SendInstallSnapshot(peer int, args *InstallSnapshotArg, reply *InstallSnapshotReply) bool {
	ok := rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)
	if !ok || rf.state != StateLeader || rf.currentTerm != args.Term {
		WARN.Printf("ID: %d call InstallSnapshot Fail", peer)
		return ok
	}
	defer rf.persist()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.StateSet(StateFollower)
		return ok
	}
	rf.nextIndex[peer] = args.LastIncludedIndex + 1
	rf.matchIndex[peer] = args.LastIncludedIndex
	return ok
}

func (rf *Raft) GetRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return w.Bytes()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArg, reply *InstallSnapshotReply) {
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.StateSet(StateFollower)
	}
	rf.chanHeartbeat <- true
	if args.LastIncludedIndex > rf.commitIndex {
		rf.TrimLog(args.LastIncludedIndex, args.LastIncludedTerm)
		rf.lastApplied = args.LastIncludedIndex
		rf.commitIndex = args.LastIncludedIndex
		rf.persister.SaveStateAndSnapshot(rf.GetRaftState(), args.Data)
		msg := ApplyMsg{UseSnapshot: true, Snapshot: args.Data}
		rf.chanMsg <- msg
	}
}

// trim log by last_index and last_term
func (rf *Raft) TrimLog(last_included_index int, last_included_term int) {
	// newLog := LogEntry{Term: last_included_term, Index: }
	new_log := make([]LogEntry, 0)
	new_log = append(new_log, LogEntry{Term: last_included_term, Index: last_included_index})
	index := len(rf.log) - 1
	for ; index > 0; index-- {
		if rf.log[index].Index == last_included_index && rf.log[index].Term == last_included_term {
			break
		}
	}
	rf.log = append(new_log, rf.log[index:]...)
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
	// defer INFO.Printf("Test send a command: %dEnd", command)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term, index := -1, -1
	isLeader := (rf.state == StateLeader)
	if isLeader {
		term = rf.currentTerm
		index = rf.LastLogIndex() + 1
		rf.log = append(rf.log, LogEntry{Index: index, Term: term, Command: command})
		rf.persist()
	}

	return index, term, isLeader
}

// periodically send heartbreak to Followers.
func (rf *Raft) BroadcastHeartbeats() {
	baseIndex := rf.log[0].Index

	for peer := range rf.peers {
		if peer != rf.me && rf.state == StateLeader {
			if baseIndex < rf.LastLogIndex() {
				args := &LogEntryArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[peer] - 1,
					// PrevLogTerm:  rf.LastLogTerm(),
					LeaderCommit: rf.commitIndex,
				}
				if args.PrevLogIndex >= baseIndex {
					args.PrevLogTerm = rf.log[args.PrevLogIndex-baseIndex].Term
				}
				if rf.nextIndex[peer] <= rf.LastLogIndex() {
					args.Entries = rf.log[rf.nextIndex[peer]-baseIndex:]
				}
				go rf.sendAppendEntries(peer, args, &LogEntryReply{})
			} else {
				args := &InstallSnapshotArg{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.log[0].Index,
					LastIncludedTerm:  rf.log[0].Term,
					Offset:            rf.persister.SnapshotSize(),
					Data:              rf.persister.ReadRaftState(),
				}
				go rf.SendInstallSnapshot(peer, args, &InstallSnapshotReply{})
			}
		}
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
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) LastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) StateSet(state int) {
	rf.state = state
	if state == StateFollower {
		rf.votedResult = make(map[int]int)
		rf.votedFor = -1
	}
	if state == StateLeader {
		rf.leaderId = rf.me
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := range rf.nextIndex {
			rf.nextIndex[i] = rf.LastLogIndex() + 1
		}
		rf.chanWinElect <- true
	}
}

func (rf *Raft) Run() {
	// INFO.Printf("SERVER:%d is run", rf.me)
	for {
		switch rf.state {
		case StateFollower:
			select {
			case <-rf.chanVoteGranted:
			case <-rf.chanHeartbeat:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(200)+200)):
				rf.StateSet(StateCandidate)
				// rf.persist()
				INFO.Printf("ID:%dbecome to Candidate", rf.me)
			}
		case StateLeader:
			go rf.BroadcastHeartbeats()
			time.Sleep(time.Millisecond * 60)
		case StateCandidate:
			rf.mu.Lock()
			// term := rf.currentTerm
			rf.leaderId = -1
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.votedResult = make(map[int]int)
			rf.votedResult[rf.me] += 1
			rf.mu.Unlock()
			go rf.BroadcastRequestVote()

			select {
			case <-rf.chanWinElect:
				// rf.ToLeader()
				INFO.Printf("ID:%d log:%d, become leader", rf.me, len(rf.log))
			case <-rf.chanHeartbeat:
				rf.StateSet(StateFollower)
				// INFO.Printf("ID:%d, State: %s, receive heartbeat", rf.me, StateString[rf.state])
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(300)+200)):
				// rf.currentTerm = term // Recover term
				// INFO.Printf("ID:%d, State: %s,voteResult: %v overtime", rf.me, StateString[rf.state], rf.votedResult)
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

	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		state:       StateFollower,
		currentTerm: 0,

		commitIndex: 0,
		lastApplied: 0,
		leaderId:    0,

		votedFor:    -1,
		votedResult: make(map[int]int),
		// log:         make([]LogEntry{Term: 0}, 1),

		chanMsg:         applyCh,
		chanVoteGranted: make(chan bool, 64),
		chanWinElect:    make(chan bool, 64),
		chanHeartbeat:   make(chan bool, 64),
	}

	rf.log = append(rf.log, LogEntry{Term: 0})

	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())
	// rf.RecoverFromSnapshot(rf.persister.ReadSnapshot())

	go rf.Run()
	return rf
}
