package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	// fmt.Printf("%v %v",rf.CurrentTerm,args.Term)
	if rf.CurrentTerm > args.Term {
		return
	}

	if rf.CurrentTerm < args.Term {
		rf.State = FOLLOWER
		rf.VotedFor = -1
		rf.CurrentTerm = args.Term
	}

	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateID {
		lastLogTerm := rf.Entry[len(rf.Entry)-1].Term
		lastLogIndex := rf.Entry[len(rf.Entry)-1].Index

		if (lastLogTerm < args.LastLogTerm) ||
			(lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
			// fmt.Printf("%d Vote for %d\n",rf.me,args.CandidateID)
			reply.VoteGranted = true
			rf.State = FOLLOWER
			rf.VotedFor = args.CandidateID
			rf.GrantVotechan <- true
		}
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
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.State != CANDIDATE {
			return ok
		}
		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.State = FOLLOWER
			rf.VoteCount = 0
			rf.VotedFor = -1
			rf.persist()
		}
		if reply.VoteGranted {
			rf.VoteCount += 1
			if rf.VoteCount > len(rf.peers)/2 {
				rf.IsLeaderchan <- true
				rf.State = LEADER
				rf.persist()
			}
		}
	}
	return ok
}

func (rf *Raft) broadcastRequestVote() {
	rf.mu.Lock()
	args := RequestVoteArgs{}
	args.Term = rf.CurrentTerm
	args.CandidateID = rf.me
	args.LastLogIndex = rf.Entry[len(rf.Entry)-1].Index
	args.LastLogTerm = rf.Entry[len(rf.Entry)-1].Term
	rf.mu.Unlock()
	for index := range rf.peers {
		if rf.State == CANDIDATE && index != rf.me {
			go func(index int) {
				reply := RequestVoteReply{}
				rf.sendRequestVote(index, args, &reply)
			}(index)
		}
	}
}
