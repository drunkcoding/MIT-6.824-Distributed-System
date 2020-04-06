package raft

type AppendEntriesArgs struct {
	Term         int
	LeadID       int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
	NextLogIndex int
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Term = rf.CurrentTerm
	reply.Success = false

	if rf.CurrentTerm > args.Term {
		return
	}

	rf.Heartbeatchan <- true
	reply.Term = args.Term

	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.State = FOLLOWER
		rf.VotedFor = -1
	}

	if rf.Entry[len(rf.Entry)-1].Index < args.PrevLogIndex {
		reply.NextLogIndex = rf.Entry[len(rf.Entry)-1].Index + 1
		return
	}

	prevlogterm := rf.Entry[args.PrevLogIndex].Term
	if prevlogterm != args.PrevLogTerm {
		for i := args.PrevLogIndex - 1; i >= 0; i-- {
			if rf.Entry[i].Term != prevlogterm {
				reply.NextLogIndex = i + 1
				return
			}
		}
	} else {
		reply.Success = true
		rf.Entry = rf.Entry[:args.PrevLogIndex+1]
		rf.Entry = append(rf.Entry, args.Entries...)
	}

	if args.LeaderCommit > rf.CommitIndex {
		if args.LeaderCommit > rf.Entry[len(rf.Entry)-1].Index {
			rf.CommitIndex = rf.Entry[len(rf.Entry)-1].Index
		} else {
			rf.CommitIndex = args.LeaderCommit
		}
		rf.CommitLogchan <- true
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.State != LEADER {
			return ok
		}

		if args.Term != rf.CurrentTerm {
			return ok
		}

		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.State = FOLLOWER
			rf.VotedFor = -1
			rf.persist()
			return ok
		}
		if reply.Success {
			if len(args.Entries) > 0 {
				rf.NextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
				//fmt.Printf("Success %v %v len %v\n", server, rf.NextIndex[server], len(args.Entries))
				rf.MatchIndex[server] = rf.NextIndex[server] - 1
			}
		} else {
			rf.NextIndex[server] = reply.NextLogIndex
		}
	}
	return ok
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Index := rf.CommitIndex
	for i := rf.CommitIndex + 1; i <= rf.Entry[len(rf.Entry)-1].Index; i++ {
		Num := 1
		for index := range rf.peers {
			if index != rf.me && rf.State == LEADER && rf.MatchIndex[index] >= i && rf.Entry[i].Term == rf.CurrentTerm {
				Num++
			}
		}
		if 2*Num > len(rf.peers) {
			Index = i
		}
	}

	if Index != rf.CommitIndex {
		rf.CommitIndex = Index
		rf.CommitLogchan <- true
	}

	for index := range rf.peers {
		if index != rf.me && rf.State == LEADER {
			args := AppendEntriesArgs{}
			args.LeadID = rf.me
			args.Term = rf.CurrentTerm
			args.LeaderCommit = rf.CommitIndex
			args.PrevLogIndex = rf.NextIndex[index] - 1
			args.PrevLogTerm = rf.Entry[args.PrevLogIndex].Term
			args.Entries = append(args.Entries, rf.Entry[args.PrevLogIndex+1:]...)
			go func(i int, args AppendEntriesArgs) {
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(i, args, &reply)
			}(index, args)
		}
	}

}
