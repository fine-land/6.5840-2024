package raft

func (rf *Raft) SendInstallSnapshot(serverId int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
	}
	args.Data = make([]byte, len(rf.data))

	if args.Data == nil {
		PrettyDebug(dError, "S%d,term=%d,nil snapshot, error")
	}

	copy(args.Data, rf.data)
	reply := InstallSnapshotReply{}

	PrettyDebug(dSnap, "S%d->S%d, installsnap, term=%d,lastIncludedIndex=%d,lastincludeterm=%d", rf.me, serverId, rf.currentTerm, args.LastIncludedIndex, args.LastIncludedTerm)

	rf.mu.Unlock()
	ok := rf.peers[serverId].Call("Raft.InstallSnapshotHandler", &args, &reply)
	rf.mu.Lock()

	if !ok {
		PrettyDebug(dWarn, "S%d->S%d, term=%d, send install snapshot fail", rf.me, serverId, rf.currentTerm)
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.resetNon()
		return
	}
	if args.Term == rf.currentTerm && rf.state == Leader {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = Follower
			rf.resetNon()
		} else {
			PrettyDebug(dSnap, "S%d->S%d, installsnap success, term=%d,lastIncludedIndex=%d,lastincludeterm=%d",
				rf.me, serverId, rf.currentTerm, args.LastIncludedIndex, args.LastIncludedTerm)
			newNextIndex := args.LastIncludedIndex + 1
			newMatchIndex := args.LastIncludedIndex
			if rf.nextIndex[serverId] < newNextIndex {
				rf.nextIndex[serverId] = newNextIndex
			}
			if rf.matchIndex[serverId] < newMatchIndex {
				rf.matchIndex[serverId] = newMatchIndex
			}

			rf.tryUpdateCommitIndexNon()
		}
	}
}

func (rf *Raft) InstallSnapshotHandler(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Data == nil {
		PrettyDebug(dError, "S%d,term=%d,, get nil snapshot, args.LeaderId=%d,args.lastIncludedindex/term=%d,%d,rf.lastincludedindex/term=%d,%d, lenlog=%d",
			rf.me, rf.currentTerm, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm, rf.lastIncludedIndex, rf.lastIncludedTerm, len(rf.log)-1)
		return
	}

	PrettyDebug(dSnap, "S%d,InstallSnapshotHandler, args.LeaderId=%d, args.term=%d,lastincludedindex=%d,lastincludedterm=%d,", rf.me, args.LeaderId, args.Term,
		args.LastIncludedIndex, args.LastIncludedTerm)

	if args.Term < rf.currentTerm {
		PrettyDebug(dSnap, "S%d,InstallSnapshotHandler, notbe leader, args.LeaderId=%d, rf.currentTerm=%d, args.term=%d,lastincludedindex=%d,lastincludedterm=%d,",
			rf.me, args.LeaderId, rf.currentTerm, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = rf.currentTerm

	//有可能发送过期的snapshot
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		PrettyDebug(dSnap, "S%d,InstallSnapshotHandler, outdata snapshot, args.LeaderId=%d,args.term=%d,lastincludedindex=%d,lastincludedterm=%d,rf.lastIncludedIndex=%d", rf.me, args.LeaderId, args.Term,
			args.LastIncludedIndex, args.LastIncludedTerm, rf.lastIncludedIndex)
		return
	}

	//rf.lastIncludedIndex = args.LastIncludedIndex
	//rf.lastIncludedTerm = args.LastIncludedTerm
	//store the snapshot
	//rf.data = args.Data
	rf.data = make([]byte, len(args.Data))
	copy(rf.data, args.Data)
	rf.hasSnapshot = true

	PrettyDebug(dSnap, "S%d,term=%d, snap before, loglen=%d, rf.lastincludedindex=%d,args.lastincludedindex=%d",
		rf.me, rf.currentTerm, len(rf.log)-1, rf.lastIncludedIndex, args.LastIncludedIndex)
	for i := 1; i < len(rf.log); i++ {
		//logindex := rf.lastIncludedIndex + i
		logindex := rf.Log2GlobalNon(i)
		if logindex == args.LastIncludedIndex && rf.log[i].Term == args.LastIncludedTerm {
			log := rf.log[0:1]
			log = append(log, rf.log[(i+1):]...)
			rf.log = make([]LogEntry, len(log))
			copy(rf.log, log)

			rf.lastIncludedIndex = args.LastIncludedIndex
			rf.lastIncludedTerm = args.LastIncludedTerm

			PrettyDebug(dSnap, "S%d,term=%d,installsnapHandler success,rf.lastIncludedIndex=%d, rf.lastIncludedTerm=%d,lenlog=%d",
				rf.me, rf.currentTerm, rf.lastIncludedIndex, rf.lastIncludedTerm, len(rf.log)-1)

			rf.cond.Signal()
			return
		}
	}

	//rf.log = rf.log[0:1]
	log := make([]LogEntry, 1)
	rf.log = make([]LogEntry, 1)
	copy(rf.log, log)

	//PrettyDebug(dSnap, "S%d, lenlog=%d", rf.me, len(rf.log)-1)
	rf.PrintLog()
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	//if rf.lastApplied < rf.lastIncludedIndex {
	//	rf.lastApplied = rf.lastIncludedIndex
	//}
	if rf.commitIndex < rf.lastIncludedIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}
	//now apply
	rf.hasSnapshot = true
	PrettyDebug(dSnap, "S%d,term=%d,installsnapHandler success, loglenshould 0,rf.lastIncludedIndex=%d, rf.lastIncludedTerm=%d,lenlog=%d",
		rf.me, rf.currentTerm, rf.lastIncludedIndex, rf.lastIncludedTerm, len(rf.log)-1)
	rf.cond.Signal()
}
