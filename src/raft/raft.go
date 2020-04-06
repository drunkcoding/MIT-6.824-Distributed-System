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
	"encoding/gob"
	"math/rand"
	"sync"
	"time"

	"../labrpc"
)

// import "bytes"
// import "encoding/gob"

const (
	LEADER = iota
	CANDIDATE
	FOLLOWER
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	CommandIndex int
	Command      interface{}
	CommandValid bool
	UseSnapshot  bool   // ignore for lab2; only used in lab3
	Snapshot     []byte // ignore for lab2; only used in lab3
}

type Log struct {
	Term    int
	Index   int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	State       int
	VoteCount   int
	CurrentTerm int
	VotedFor    int
	Entry       []Log
	CommitIndex int
	LastApplied int

	NextIndex  []int
	MatchIndex []int

	Heartbeatchan chan bool
	IsLeaderchan  chan bool
	VotedForchan  chan bool
	GrantVotechan chan bool
	CommitLogchan chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isleader = rf.CurrentTerm, rf.State == LEADER

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.Entry)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.LastApplied)
	e.Encode(rf.CommitIndex)
	e.Encode(rf.VotedFor)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.Entry)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.LastApplied)
	d.Decode(&rf.CommitIndex)
	d.Decode(&rf.VotedFor)
}

type InstallSnapArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapReply struct {
	Term int
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.CurrentTerm
	isLeader := rf.State == LEADER
	if isLeader {
		index = rf.Entry[len(rf.Entry)-1].Index + 1
		rf.Entry = append(rf.Entry, Log{Term: term, Command: command, Index: index})
		rf.persist()
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

	// Your initialization code here.
	rf.State = FOLLOWER
	rf.VotedFor = -1
	rf.CurrentTerm = 0
	rf.CommitIndex = 0
	rf.VoteCount = 0
	rf.LastApplied = 0
	rf.Heartbeatchan = make(chan bool, 100)
	rf.IsLeaderchan = make(chan bool, 100)
	rf.VotedForchan = make(chan bool, 100)
	rf.GrantVotechan = make(chan bool, 100)
	rf.CommitLogchan = make(chan bool, 100)
	rf.Entry = append(rf.Entry, Log{Term: 0, Index: 0})

	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			switch rf.State {
			case FOLLOWER:
				select {
				case <-rf.GrantVotechan:
				case <-rf.Heartbeatchan:
				case <-time.After(time.Duration(rand.Int63()%300+500) * time.Millisecond):
					rf.mu.Lock()
					rf.persist()
					rf.State = CANDIDATE
					rf.mu.Unlock()
				}
			case LEADER:
				go rf.broadcastAppendEntries()
				time.Sleep(40 * time.Millisecond)
			case CANDIDATE:
				rf.mu.Lock()
				rf.CurrentTerm++
				rf.VotedFor = rf.me
				rf.VoteCount = 1
				rf.persist()
				rf.mu.Unlock()
				// fmt.Printf("%v become candidate Term %v\n",rf.me,rf.CurrentTerm)
				go rf.broadcastRequestVote()
				select {
				case <-time.After(time.Duration(rand.Int63()%300+500) * time.Millisecond):
				case <-rf.Heartbeatchan:
					rf.mu.Lock()
					rf.persist()
					rf.State = FOLLOWER
					rf.mu.Unlock()
				case <-rf.IsLeaderchan:
					//fmt.Printf("%v become leader! Term %v\n",rf.me,rf.CurrentTerm)
					rf.mu.Lock()
					rf.persist()
					rf.State = LEADER
					rf.NextIndex = make([]int, len(rf.peers))
					rf.MatchIndex = make([]int, len(rf.peers))
					for index := range rf.peers {
						rf.NextIndex[index] = rf.Entry[len(rf.Entry)-1].Index + 1
						rf.MatchIndex[index] = 0
					}
					rf.mu.Unlock()
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-rf.CommitLogchan:
				rf.mu.Lock()
				for i := rf.LastApplied + 1; i <= rf.CommitIndex; i++ {
					msg := ApplyMsg{}
					msg.CommandValid = true
					msg.Command = rf.Entry[i].Command
					msg.CommandIndex = i
					applyCh <- msg
				}
				rf.LastApplied = rf.CommitIndex
				rf.mu.Unlock()
			}
		}
	}()

	return rf
}
