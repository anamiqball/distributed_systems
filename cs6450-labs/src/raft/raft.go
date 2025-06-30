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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"fmt"
	"6.5840/labgob"
	"6.5840/labrpc"
)


// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

type ServerState int
type VoteState int
type AppendState int

var Election_Timeout_Max = 160 * time.Millisecond


const (
	Follower ServerState = iota
	Candidate
	Leader
)

const (
	Normal VoteState = iota
	Killed
	Expire
	Voted
)

const (
	AppNormal    AppendState = iota
	AppOutOfDate
	AppKilled
	AppCommitted
	Mismatch
)

type LogEntry struct {
	Command interface{}
	Term    int
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state ServerState
	
	currentTerm int
	votedFor    int
	log 	    []LogEntry
	
	electionTimer time.Time
	electionTimeout time.Duration
	commitIndex int
	lastApplied int
	
	nextIndex  []int
	matchIndex []int
	
	overtime time.Duration
	timer    *time.Ticker
	applyChan chan ApplyMsg
	
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	AppState    AppendState
	IndexNext int
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int
	CandidateId int

	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term int
	VoteGranted bool
	VoteState   VoteState
}

func (rf *Raft) GetStateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
    defer rf.mu.Unlock() 
	var term int
	var isleader bool
	// Your code here (3A).
	
	term = rf.currentTerm
	isleader = rf.state == Leader
	
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.Save(data, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
	if data == nil || len(data) < 1 {
	return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		fmt.Println("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}



// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
 	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("Server %d received vote request from %d in term %d\n", rf.me, args.CandidateId, args.Term)
	if rf.killed() {
		reply.VoteState = Killed
		reply.Term = -1
		reply.VoteGranted = false
		return
	}

	if args.Term < rf.currentTerm {
		reply.VoteState = Expire
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	if rf.votedFor == -1 {
		lastLogTerm := 0
		if len(rf.log) > 0 {
			lastLogTerm = rf.log[len(rf.log)-1].Term
		}

		if args.LastLogTerm < lastLogTerm || (len(rf.log) > 0 && args.LastLogTerm == lastLogTerm && args.LastLogIndex < len(rf.log)) {
			reply.VoteState = Expire
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			rf.persist()
			return
		}

		rf.votedFor = args.CandidateId
		reply.VoteState = Normal
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.persist()

		rf.timer.Reset(rf.overtime)

	} else {
		reply.VoteState = Voted
		reply.VoteGranted = false

		if rf.votedFor != args.CandidateId {
			return
		} else {
			rf.state = Follower
		}

		rf.timer.Reset(rf.overtime)

	}

	return
}

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
// within a timeout interval, Call() returns true;  
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteNums *int) bool {

	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		return false
	}

	if reply.VoteState == Expire {
		rf.state = Follower
		rf.timer.Reset(rf.overtime)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
		}
	} else if reply.VoteState == Normal || reply.VoteState == Voted {
		if reply.VoteGranted && reply.Term == rf.currentTerm && *voteNums <= (len(rf.peers)/2) {
			*voteNums++
		}

		if *voteNums >= (len(rf.peers)/2)+1 {
			*voteNums = 0
			if rf.state == Leader {
				return ok
			}

			rf.state = Leader
			rf.nextIndex = make([]int, len(rf.peers))
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.log) + 1
			}
			rf.timer.Reset(Election_Timeout_Max)
		}
	} else if reply.VoteState == Killed {
		return false
	}

	return ok
}



func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("Server %d received AppendEntries from %d for term %d\n", rf.me, args.LeaderId, args.Term)

	if rf.killed() {
		reply.AppState = AppKilled
		reply.Term = -1
		reply.Success = false
		return
	}

	if args.Term < rf.currentTerm {
		reply.AppState = AppOutOfDate
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.PrevLogIndex > 0 && (len(rf.log) < args.PrevLogIndex || rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
		reply.AppState = Mismatch
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.IndexNext = rf.lastApplied + 1
		return
	}

	if args.PrevLogIndex != -1 && rf.lastApplied > args.PrevLogIndex {
		reply.AppState = AppCommitted
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.IndexNext = rf.lastApplied + 1
		return
	}

	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	rf.state = Follower
	rf.timer.Reset(rf.overtime)

	reply.AppState = AppNormal
	reply.Term = rf.currentTerm
	reply.Success = true

	if args.Entries != nil {
		rf.log = rf.log[:args.PrevLogIndex]
		rf.log = append(rf.log, args.Entries...)
		//fmt.Printf("Server %d appended log entries: %v\n", rf.me, args.Entries)
	}
	rf.persist()

	for rf.lastApplied < args.LeaderCommit {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.lastApplied,
			Command:      rf.log[rf.lastApplied-1].Command,
		}
		rf.applyChan <- applyMsg
		rf.commitIndex = rf.lastApplied
		//fmt.Printf("Server %d applied command %v at index %d\n", rf.me, applyMsg.Command, applyMsg.CommandIndex)
	}

	return
}



func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, appendNums *int) {

	if rf.killed() {
		return
	}

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
		if rf.killed() {
			return
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	switch reply.AppState {
	case AppKilled:
		return
	case AppNormal:
		if reply.Success && reply.Term == rf.currentTerm && *appendNums <= len(rf.peers)/2 {
			*appendNums++
		}

		if rf.nextIndex[server] > len(rf.log)+1 {
			return
		}
		rf.nextIndex[server] += len(args.Entries)
		if *appendNums > len(rf.peers)/2 {
			*appendNums = 0

			if len(rf.log) == 0 || rf.log[len(rf.log)-1].Term != rf.currentTerm {
				return
			}

			for rf.lastApplied < len(rf.log) {
				rf.lastApplied++
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[rf.lastApplied-1].Command,
					CommandIndex: rf.lastApplied,
				}
				rf.applyChan <- applyMsg
				rf.commitIndex = rf.lastApplied
			}
		}
		return
	case Mismatch, AppCommitted:
		if reply.Term > rf.currentTerm {
			rf.state = Follower
			rf.votedFor = -1
			rf.timer.Reset(rf.overtime)
			rf.currentTerm = reply.Term
			rf.persist()
		}
		rf.nextIndex[server] = reply.IndexNext
	case AppOutOfDate:
		rf.state = Follower
		rf.votedFor = -1
		rf.timer.Reset(rf.overtime)
		rf.currentTerm = reply.Term
		rf.persist()
	}

	return
}



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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	if rf.killed() {
		return index, term, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return index, term, false
	}

	isLeader = true

	appendLog := LogEntry{Term: rf.currentTerm, Command: command}
	rf.log = append(rf.log, appendLog)
	index = len(rf.log)
	term = rf.currentTerm

	rf.persist()
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	rf.timer.Stop()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.timer.C:
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			if rf.state == Follower {
				rf.state = Candidate
				//fmt.Printf("Server %d became Candidate in term %d\n", rf.me, rf.currentTerm)
			}
			if rf.state == Candidate {
				rf.currentTerm += 1
				rf.votedFor = rf.me
				votedNums := 1
				rf.persist()

				rf.overtime = time.Duration(170+rand.Intn(200)) * time.Millisecond
				rf.timer.Reset(rf.overtime)

				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}

					voteArgs := RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: len(rf.log),
						LastLogTerm:  0,
					}
					if len(rf.log) > 0 {
						voteArgs.LastLogTerm = rf.log[len(rf.log)-1].Term
					}

					voteReply := RequestVoteReply{}

					go rf.sendRequestVote(i, &voteArgs, &voteReply, &votedNums)
				}
			}
			if rf.state == Leader {
				appendNums := 1
				rf.timer.Reset(Election_Timeout_Max)
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: 0,
						PrevLogTerm:  0,
						Entries:      nil,
						LeaderCommit: rf.commitIndex,
					}

					reply := AppendEntriesReply{}

					args.Entries = rf.log[rf.nextIndex[i]-1:]

					if rf.nextIndex[i] > 0 {
						args.PrevLogIndex = rf.nextIndex[i] - 1
					}

					if args.PrevLogIndex > 0 {
						args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
					}

					go rf.sendAppendEntries(i, &args, &reply, &appendNums)
				}
			}
			rf.mu.Unlock()
		}
	}
}


// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	rf.applyChan = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.state = Follower
	rf.overtime = time.Duration(170+rand.Intn(200)) * time.Millisecond
	rf.timer = time.NewTicker(rf.overtime)

	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()

	return rf
}
