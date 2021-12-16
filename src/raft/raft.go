package raft

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

type StateMachine int

const (
	StateFollower = iota
	StateCandidate
	StateLeader
)

var StateName = map[int]string{
	StateLeader:    "L",
	StateCandidate: "C",
	StateFollower:  "F",
}

var (
	// LogFile, _ = os.OpenFile("o|utput.log", os.O_CREATE|os.O_WRONLY, 0666)
	LogFile = os.Stderr
	INFO    = log.New(LogFile, "INFO ", log.Ltime|log.Lshortfile).Printf
	WARN    = log.New(LogFile, "WARN ", log.Ltime|log.Lshortfile).Printf
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

	// UseSnapshot bool
	Snapshot []byte
}

type LogEntry struct {
	Term    int
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
func (rf *Raft) GetState() (int, bool) {
	currentTerm := rf.currentTerm
	return currentTerm, rf.state == StateLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.state)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
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

func (rf *Raft) GetStateSize() int {
	return rf.persister.RaftStateSize()
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
		CommandValid: false,
		Snapshot:     snapshot,
	}
	rf.chanMsg <- msg
	INFO("recover from snapshot last index: %d, term: %d", last_included_index, last_included_term)
}

type RequestVoteArgs struct {
	Term         int // current term
	CandidatedId int // candidate requesting vote
	LastLogIndex int // candidate requesting vote
	LastLogTerm  int // term of candidiate's last log entry
}

type RequestVoteReply struct {
	Term        int  // candidate's term
	VoteGranted bool // whether candidate received vote
}

type LogEntryArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int // Index of log entry immediately preceding new ones
	PrevLogTerm  int // Term of prevLogTerm
	Entries      []LogEntry

	LeaderCommit int
}

type LogEntryReply struct {
	Term         int
	Success      bool
	CommitIndex  int
	NextTryIndex int
}

type InstallSnapshotArg struct {
	Term              int // leader' term
	LeaderId          int // leader's id
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	defer rf.persist()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		fmt.Printf("ID:%d Term%d<currentTerm%d", rf.me, args.Term, rf.currentTerm)
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
}

func (rf *Raft) CountVotes(peer int, reply *RequestVoteReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.StateSet(StateFollower)
		return false
	}

	if reply.VoteGranted {
		rf.votedResult[peer] += 1
		if len(rf.votedResult) > len(rf.peers)/2 {
			rf.StateSet(StateLeader)
		}
	}
	return true
}

func (rf *Raft) BroadcastVoteRequest() {
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidatedId: rf.me,
		LastLogIndex: rf.LastLogIndex(),
		LastLogTerm:  rf.LastLogTerm(),
	}

	for peer := range rf.peers {
		if peer != rf.me && rf.state == StateCandidate {
			go func(peer int, args *RequestVoteArgs) {
				reply := &RequestVoteReply{}
				ok := rf.peers[peer].Call("Raft.RequestVote", args, reply)
				if ok && rf.state == StateCandidate && rf.currentTerm == args.Term {
					rf.CountVotes(peer, reply)
				}
			}(peer, args)
		}
	}
}

func (rf *Raft) ParseHeartbeatReply(server int, args *LogEntryArgs, reply *LogEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if reply.Term > rf.currentTerm {
		rf.StateSet(StateFollower)
		rf.currentTerm = reply.Term
		return
	}
	// INFO("ID: %d=>%d reply: %+v, my log: %d last log: %d", rf.me, server, *reply, len(rf.log), rf.LastLogIndex())
	if !reply.Success {
		rf.nextIndex[server] = min(reply.NextTryIndex, rf.LastLogIndex())
	} else {
		if len(args.Entries) > 0 {
			rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		}
	}
	rf.UpdateCommitIndex()
	// INFO("ID: %d=>%d nextIndex: %+v", rf.me, server, rf.nextIndex)
}

// Check follower and update Leader's commitIndex
func (rf *Raft) UpdateCommitIndex() {
	baseIndex := rf.log[0].Index
	for N := rf.LastLogIndex(); N > rf.commitIndex && rf.log[N-baseIndex].Term == rf.currentTerm; N-- {
		count := 1

		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}
		// INFO("ID: %d, Apply Log, count: %d", rf.me, count)
		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			go rf.ApplyLog()
			break
		}
	}
}

// Broad heartbeat RPC handler.
func (rf *Raft) AppendEntries(args *LogEntryArgs, reply *LogEntryReply) {
	defer rf.persist()
	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		// reject and return currentTerm
		reply.NextTryIndex = rf.LastLogIndex() + 1
		INFO("ID:%d Term:%dRequest term < current Term", rf.me, rf.currentTerm)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.leaderId = args.LeaderId
		rf.StateSet(StateFollower)
	}
	rf.chanHeartbeat <- true
	rf.ResolveConflictLogs(args, reply)
}

func (rf *Raft) ResolveConflictLogs(args *LogEntryArgs, reply *LogEntryReply) {
	if args.PrevLogIndex > rf.LastLogIndex() {
		INFO("ID:%d PrevLogIndex:%d, LastLogIndex:%d", rf.me, args.PrevLogIndex, rf.LastLogIndex())
		reply.NextTryIndex = rf.LastLogIndex() + 1
		return
	}
	baseIndex := rf.log[0].Index
	if args.PrevLogIndex >= baseIndex && args.PrevLogTerm != rf.log[args.PrevLogIndex-baseIndex].Term {
		term := rf.log[args.PrevLogIndex-baseIndex].Term
		for i := args.PrevLogIndex - 1; i >= baseIndex; i-- {
			if term != rf.log[i-baseIndex].Term {
				reply.NextTryIndex = i + 1
				break
			}
		}
	} else if args.PrevLogIndex >= baseIndex-1 {
		rf.log = rf.log[:args.PrevLogIndex-baseIndex+1]
		rf.log = append(rf.log, args.Entries...)

		reply.Success = true
		reply.NextTryIndex = args.PrevLogIndex + len(args.Entries)

		if rf.commitIndex < args.LeaderCommit {
			rf.commitIndex = min(args.LeaderCommit, rf.LastLogIndex())
			go rf.ApplyLog()
		}
	}
}

func (rf *Raft) ApplyLog() {
	// FIXME
	baseIndex := rf.log[0].Index
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		command_valid := rf.log[i-baseIndex].Command != nil
		msg := ApplyMsg{
			CommandValid: command_valid,
			CommandIndex: i,
			Command:      rf.log[i-baseIndex].Command,
		}
		rf.chanMsg <- msg
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) ParseInstallSnapshotReply(peer int, args *InstallSnapshotArg, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	INFO("ID: %d=>%d my Term: %d parseInstallSnapshotReply: %d", rf.me, peer, rf.currentTerm, *reply)
	if reply.Term <= rf.currentTerm {
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
		rf.matchIndex[peer] = args.LastIncludedIndex
	} else {
		rf.currentTerm = reply.Term
		rf.StateSet(StateFollower)
	}
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) GetRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return w.Bytes()
}

func (rf *Raft) MakeRaftSnaphot(snapshot []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	base_index, last_index := rf.log[0].Index, rf.LastLogIndex()
	if base_index >= index || last_index < index {
		// INFO("ERR: %d base_index: %+v, %d ", len(rf.log), rf.log[0], rf.LastLogIndex())
		return
	}
	rf.TrimLog(rf.log[index-base_index].Index, rf.log[index-base_index].Term)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log[0].Index)
	e.Encode(rf.log[0].Term)
	snapshot = append(w.Bytes(), snapshot...)
	rf.persister.SaveStateAndSnapshot(rf.GetRaftState(), snapshot)
}

// InstallSnapshot RPC handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArg, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// defer func() { INFO("ID:%d After InstallSnapshot, args: %+v log %+v", rf.me, *args, rf.log) }()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.StateSet(StateFollower)
	}
	rf.chanHeartbeat <- true
	// Apply snapshot to channel
	if args.LastIncludedIndex > rf.commitIndex {
		rf.TrimLog(args.LastIncludedIndex, args.LastIncludedTerm)
		rf.lastApplied = args.LastIncludedIndex
		rf.commitIndex = args.LastIncludedIndex
		rf.persister.SaveStateAndSnapshot(rf.GetRaftState(), args.Data)
		msg := ApplyMsg{CommandValid: false, Snapshot: args.Data}
		rf.chanMsg <- msg
	}
}

// trim log by last_index and last_term
func (rf *Raft) TrimLog(last_included_index int, last_included_term int) {
	new_log := make([]LogEntry, 0)
	new_log = append(new_log, LogEntry{Term: last_included_term, Index: last_included_index})
	for index := len(rf.log) - 1; index >= 0; index-- {
		if rf.log[index].Index == last_included_index && rf.log[index].Term == last_included_term {
			new_log = append(new_log, rf.log[index+1:]...)
			break
		}
	}
	rf.log = new_log
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
	// INFO("Test send a command: %d", command)
	// defer INFO("Test send a command: %dEnd", command)
	rf.mu.Lock()
	term, index := -1, -1
	isLeader := (rf.state == StateLeader)
	if isLeader {
		term = rf.currentTerm
		index = rf.LastLogIndex() + 1
		rf.log = append(rf.log, LogEntry{Index: index, Term: term, Command: command})
		rf.persist()
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

func (rf *Raft) GetLeaderId() int {
	if rf.state == StateCandidate {
		return -1
	}
	return rf.leaderId
}

// periodically send heartbreak to Followers.
func (rf *Raft) BroadcastHeartbeats() {
	baseIndex := rf.log[0].Index

	for peer := range rf.peers {
		if peer != rf.me && rf.state == StateLeader {
			if rf.nextIndex[peer] > baseIndex {
				args := &LogEntryArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[peer] - 1,
					PrevLogTerm:  rf.LastLogTerm(),
					LeaderCommit: rf.commitIndex,
				}
				if args.PrevLogIndex >= baseIndex {
					args.PrevLogTerm = rf.log[args.PrevLogIndex-baseIndex].Term
				}
				if rf.nextIndex[peer] <= rf.LastLogIndex() {
					args.Entries = rf.log[rf.nextIndex[peer]-baseIndex:]
				}
				// INFO("ID:%d=>%d %v %d %d", rf.me, peer, rf.nextIndex, rf.nextIndex[peer], args.PrevLogIndex)
				go func(server int, args *LogEntryArgs) {
					reply := &LogEntryReply{}
					ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
					if ok {
						rf.ParseHeartbeatReply(server, args, reply)
					} else {
						// INFO("ID: %d=>%d Failed to call Raft AppendEntries:%+v", rf.me, server, reply)
					}
					// INFO("ID: %d=>%d, nextIndex: %+v", rf.me, server, rf.nextIndex)
				}(peer, args)
			} else {
				rf.persist()
				snapshot := rf.persister.ReadSnapshot()
				r := bytes.NewReader(snapshot)
				d := labgob.NewDecoder(r)
				var last_included_index, last_included_term int
				d.Decode(&last_included_index)
				d.Decode(&last_included_term)
				args := &InstallSnapshotArg{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.log[0].Index,
					LastIncludedTerm:  rf.log[0].Term,
					Data:              snapshot,
				}
				go func(peer int, args *InstallSnapshotArg) {
					reply := &InstallSnapshotReply{}
					ok := rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)
					if ok && rf.state == StateLeader {
						rf.ParseInstallSnapshotReply(peer, args, reply)
					}
				}(peer, args)
			}
		}
	}
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
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
		rf.votedFor = -1
		rf.votedResult = make(map[int]int)
	} else if state == StateLeader {
		rf.leaderId = rf.me
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := range rf.nextIndex {
			rf.nextIndex[i] = rf.LastLogIndex() + 1
		}
		rf.chanWinElect <- true
	} else if state == StateCandidate {
		rf.leaderId = -1
		rf.votedFor = rf.me
		rf.votedResult = make(map[int]int)
		rf.votedResult[rf.me] += 1
	}
	INFO("ID: %d, state: %s", rf.me, StateName[rf.state])
}

func (rf *Raft) Run() {
	for {
		switch rf.state {
		case StateFollower:
			select {
			case <-rf.chanVoteGranted:
			case <-rf.chanHeartbeat:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(200)+200)):
				rf.mu.Lock()
				rf.StateSet(StateCandidate)
				rf.persist()
				rf.mu.Unlock()
				// INFO("ID:%dTo C", rf.me)
			}
		case StateLeader:
			go rf.BroadcastHeartbeats()
			time.Sleep(time.Millisecond * 60)
		case StateCandidate:
			rf.currentTerm += 1
			go rf.BroadcastVoteRequest()

			select {
			case <-rf.chanWinElect:
				// INFO("ID:%dTo L log:%d,", rf.me, len(rf.log))
			case <-rf.chanHeartbeat:
				rf.mu.Lock()
				rf.StateSet(StateFollower)
				rf.persist()
				rf.mu.Unlock()
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(300)+200)):
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
		log:         []LogEntry{{Term: 0, Index: 0}},

		chanMsg:         applyCh,
		chanVoteGranted: make(chan bool, 128),
		chanWinElect:    make(chan bool, 128),
		chanHeartbeat:   make(chan bool, 128),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.RecoverFromSnapshot(rf.persister.ReadSnapshot())

	go rf.Run()
	return rf
}
