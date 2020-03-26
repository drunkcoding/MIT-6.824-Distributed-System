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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

type StateType int

const (
	FOLLOWER  StateType = 0
	CANDIDATE StateType = 1
	LEADER    StateType = 2
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	currentTerm int
	votedFor    int
	voteCount   int
	state       StateType

	// volatile state
	commitIndex int
	lastApplied int

	// volatile state for leader
	nextIndex  []int
	matchIndex []int

	// utils
	keepAliveChan      chan bool
	electionLeaderChan chan bool
	voteGrantedChan    chan bool
}

type TermError struct {
	current int
	peer    int
}

func (e *TermError) Error() string {
	if e.current > e.peer {
		return fmt.Sprintf("Peer term too old; current:%d|peer:%d", e.current, e.peer)
	}
	return fmt.Sprintf("Current term too old; current:%d|peer:%d", e.current, e.peer)
}

func (rf *Raft) ResetTermMeta() {
	rf.votedFor = -1
	rf.voteCount = 0
}

func (rf *Raft) CheckTerm(term int) (bool, error) {
	if rf.currentTerm > term {
		return true, &TermError{rf.currentTerm, term}
	}

	if rf.currentTerm < term {
		old := rf.currentTerm
		rf.currentTerm = term
		rf.state = FOLLOWER
		rf.ResetTermMeta()
		return false, &TermError{old, term}
	}

	return true, nil
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isleader = rf.currentTerm, rf.state == LEADER

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateID int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term    int
	Granted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Granted = false

	termValid, err := rf.CheckTerm(args.Term)

	if termValid && err != nil {
		fmt.Println(rf.me, "RequestVote", err)
		return
	}

	if !termValid {
		fmt.Println(rf.me, "RequestVote", err)
		rf.keepAliveChan <- true
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		rf.votedFor = args.CandidateID
		reply.Granted = true
		fmt.Printf("%v grant vote request from %v\n", rf.me, rf.votedFor)
		rf.voteGrantedChan <- true
		rf.state = FOLLOWER
	}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) (ok bool) {
	ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != CANDIDATE {
		return
	}

	rf.CheckTerm(reply.Term)

	if reply.Granted {
		rf.voteCount++
		if rf.voteCount > len(rf.peers)/2 {
			rf.state = LEADER
			rf.ResetTermMeta()
			rf.electionLeaderChan <- true
		}
	}

	return
}

func (rf *Raft) broadcastRequestVote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for index := range rf.peers {
		if index != rf.me && rf.state == CANDIDATE {
			// fmt.Printf("%v RequestVote %v\n", rf.me, index)
			args := RequestVoteArgs{}
			args.CandidateID = rf.me
			args.Term = rf.currentTerm
			go func(i int, args RequestVoteArgs) {
				reply := RequestVoteReply{}
				rf.sendRequestVote(i, &args, &reply)
			}(index, args)
		}
	}
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}
type AppendEntriesReply struct {
	Term int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	termValid, err := rf.CheckTerm(args.Term)

	if termValid && err != nil {
		fmt.Println(rf.me, "AppendEntries", err)
		return
	}

	rf.keepAliveChan <- true

	if !termValid {
		fmt.Println(rf.me, "AppendEntries", err)
		rf.keepAliveChan <- true
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (ok bool) {
	ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		return
	}

	if rf.state != LEADER {
		return
	}

	if args.Term != rf.currentTerm {
		return
	}

	termValid, err := rf.CheckTerm(args.Term)

	if !termValid {
		fmt.Println(rf.me, "sendAppendEntries", err)
		return
	}

	return
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for index := range rf.peers {
		if index != rf.me && rf.state == LEADER {
			// fmt.Printf("%v heartbeat %v\n", rf.me, index)
			args := AppendEntriesArgs{}
			args.LeaderId = rf.me
			args.Term = rf.currentTerm
			go func(i int, args AppendEntriesArgs) {
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(i, &args, &reply)
			}(index, args)
		}
	}
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
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) DoCandidateRoutine() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.mu.Unlock()

	go rf.broadcastRequestVote()
	select {
	case <-time.After(time.Duration(rand.Int63()%300+500) * time.Millisecond):
		fmt.Printf("%v election timeout; term %v\n", rf.me, rf.currentTerm)
	case <-rf.electionLeaderChan:
		fmt.Printf("%v win election; term %v\n", rf.me, rf.currentTerm)
	}
}

func (rf *Raft) DoLeaderRoutine() {
	rf.broadcastAppendEntries()
	time.Sleep(50 * time.Millisecond)
}

func (rf *Raft) DoFollowerRoutine() {
	select {
	case <-rf.voteGrantedChan:
		fmt.Printf("%v vote granted; term %v\n", rf.me, rf.currentTerm)
	case <-rf.keepAliveChan:
		// fmt.Printf("%v follower keep alive\n", rf.me)
	case <-time.After(time.Duration(rand.Int63()%300+500) * time.Millisecond):
		fmt.Printf("%v follower timeout\n", rf.me)
		rf.mu.Lock()
		rf.state = CANDIDATE
		rf.mu.Unlock()
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.voteCount = 0
	rf.votedFor = -1
	rf.state = FOLLOWER

	rf.keepAliveChan = make(chan bool)
	rf.electionLeaderChan = make(chan bool)
	rf.voteGrantedChan = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			switch rf.state {
			case CANDIDATE:
				rf.DoCandidateRoutine()
			case FOLLOWER:
				rf.DoFollowerRoutine()
			case LEADER:
				rf.DoLeaderRoutine()
			}
		}
	}()

	return rf
}
