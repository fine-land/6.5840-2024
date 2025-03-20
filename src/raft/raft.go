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
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}

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
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry

	flag := false
	if d.Decode(&currentTerm) != nil {
		flag = true
		DPrintf3C("decode currentTerm fail\n")
	}
	if d.Decode(&votedFor) != nil {
		flag = true
		DPrintf3C("decode votedfor fail\n")
	}
	if d.Decode(&log) != nil {
		flag = true
		DPrintf3C("decode log fail\n")
	}
	if !flag {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log[:]
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
// 这个函数是RequestVote处理函数，当Candidate发送RPC后，其他服务器会调用这个函数；
// 当前服务器有三种状态，Leader，Candidate，Follower
// Leader:如果args.CandidateTerm比自己的新，直接变成Follower，然后投票；如果args.Term <= term
// 拒绝投票
// Candidate：args.term <= term, 拒绝投票，因为Candidate默认投票给自己
// args.term > term, 退回Follower，然后投票
// Follower: 如果term或日志不够新，拒绝投票
// 同时还要防止重复投票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.CandidiateTerm < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	//for debug message
	reply.Term = rf.currentTerm
	if args.CandidiateTerm > rf.currentTerm {
		rf.state = Follower
		rf.votedFor = -1
		rf.currentTerm = args.CandidiateTerm
		//persist
		rf.persist()
	}

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	up2date := (args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex))
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && up2date {
		DPrintf("[VOTEGRAND]: server[%d]Term[%d] --> server[%d]Term[%d]\n", rf.me, rf.currentTerm, args.CandidateId, args.CandidiateTerm)
		rf.votedFor = args.CandidateId

		//persist
		rf.persist()

		reply.VoteGranted = true

		//一旦投过票，证明在这个term内，自己是不能成为leader
		//最好进行一次reset
		rf.resetNon()
		return
	}

	//something wrong happen
	reply.VoteGranted = false
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		DPrintf("server[%d]Term[%d] not leader, just return\n", rf.me, rf.currentTerm)
		return -1, -1, false
	}

	newlog := LogEntry{
		Op:   command,
		Term: rf.currentTerm,
	}
	rf.log = append(rf.log, newlog)
	//persist
	rf.persist()
	rf.nextIndex[rf.me] = len(rf.log)
	rf.matchIndex[rf.me] = len(rf.log) - 1
	DPrintf3B("[Start]: server[%d]state[%d] get log, log len[%d], get log[%v]\n", rf.me, rf.state, len(rf.log)-1, command)

	go rf.sendAppendEntries()
	return len(rf.log) - 1, rf.currentTerm, true
}

// 只有日志的时候才会进行这个函数，不是循环
func (rf *Raft) sendAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i, _ := range rf.peers {
		if i != rf.me && len(rf.log)-1 >= rf.nextIndex[i] {
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
				Entries:      rf.log[rf.nextIndex[i]:], //[rf.nextIndex ...]
				LeaderCommit: rf.commitIndex,
			}
			DPrintf("server[%d]Term[%d] --> AElog for server[%d]\n", rf.me, rf.currentTerm, i)
			go rf.appendEntries(i, &args)
		}
	}
}

func (rf *Raft) appendEntries(serverId int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	//这里如果当前服务器已经不是leader了，按照paper应当立刻停止发送；
	//但是这样需要持有锁来检测当前服务器状态，但是发送RPC又不能持有锁；
	//所以我的想法是，即使一直发送，发送成功时，如果当前服务器不是leader了，那么
	//在下面的逻辑中也不会处理
	//for !rf.peers[serverId].Call("Raft.AppendEntriesHandler", args, &reply) && rf.killed() == false {
	//	time.Sleep(time.Duration(5) * time.Millisecond)
	//}
	ok := rf.peers[serverId].Call("Raft.AppendEntriesHandler", args, &reply)
	if !ok {
		//DPrintf3B("send AE error")
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//这里也有一个bug
	//leader1网络分区后，重连回来，立刻发送两个heartbeat给其他服务器；
	//但是发送给Follower的先回来，修改了此服务器的term，导致从leader返回的heartbeat无法被处理
	//防止过去太长时间

	if args.Term == rf.currentTerm && rf.state == Leader {
		DPrintf("[AE]: server[%d] handler reply of server[%d] log", rf.me, serverId)
		if reply.Success {

			//very small bug ,
			//如果在返回的时候，leader又更新了log，就会出现问题
			//rf.nextIndex[serverId] = len(rf.log)
			//rf.matchIndex[serverId] = len(rf.log) - 1
			newNextIndex := args.PrevLogIndex + len(args.Entries) + 1
			newMatchIndex := args.PrevLogIndex + len(args.Entries)
			if newNextIndex > rf.nextIndex[serverId] {
				rf.nextIndex[serverId] = newNextIndex
			}
			if newMatchIndex > rf.matchIndex[serverId] {
				rf.matchIndex[serverId] = newMatchIndex
			}
			rf.tryUpdateCommitIndexNon()
		} else {
			if reply.Term > rf.currentTerm {
				rf.state = Follower
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				//persist
				rf.persist()
				//一旦不是leader，调整完毕直接退出AE
				DPrintf3B("server[%d]Term[%d] not leader now\n", rf.me, rf.currentTerm)
				return
			} else {
				//这里由于日志不一致的错误，而非term的错误
				//此时调整nextIndex，然后retry
				if rf.nextIndex[serverId] == 1 {
					return
				}
				//rf.nextIndex[serverId]--
				if reply.XTerm == -1 {
					rf.nextIndex[serverId] = reply.XLen
				} else {
					//这里可以二分查找来加速,wait to do
					for i := args.PrevLogIndex; i >= 0; i-- {
						if rf.log[i].Term > reply.XTerm {
							continue
						} else if rf.log[i].Term == reply.XTerm {
							rf.nextIndex[serverId] = i
							break
						} else {
							rf.nextIndex[serverId] = reply.XIndex
							break
						}
					}
				}

				go rf.appendEntries(serverId, &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[serverId] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[serverId]-1].Term,
					Entries:      rf.log[rf.nextIndex[serverId]:],
					LeaderCommit: rf.commitIndex,
				})
			}
		}

	}
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 将electionTimeout设置为当前时间+1s+[0, 300]ms
func (rf *Raft) resetNon() {
	// 获取当前时间
	now := time.Now()

	// 添加 1 秒
	rf.electionTimeout = now.Add(time.Second)

	// 创建一个新的随机数生成器
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// 生成 [0, 300] 毫秒之间的随机时间
	randomM := time.Duration(r.Intn(301)) * time.Millisecond

	// 添加随机时间
	rf.electionTimeout = rf.electionTimeout.Add(randomM)
}

// hold lock
func (rf *Raft) callSendRequestVote(vote *int, args *RequestVoteArgs, serverId int) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(serverId, args, &reply)
	if !ok {
		DPrintf("[RV]: may be loss vote\n")
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < reply.Term {
		DPrintf("[RV]: ID[%d]TERM[%d] <-- ID[%d]TERM[%d], get greater term\n", rf.me, rf.currentTerm, serverId, reply.Term)
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		//persist
		rf.persist()
	}
	//收到reply，可能过去很长时间；确认term没有改变
	if rf.currentTerm == args.CandidiateTerm && rf.state == Candidate {
		if reply.VoteGranted {
			*vote++
			DPrintf("[VOTE]: server[%d]Term[%d] <-- server[%d]Term[%d]\n", rf.me, rf.currentTerm, serverId, reply.Term)
			if *vote > len(rf.peers)/2 && rf.state != Leader /* only one go routines heartbeat*/ {
				//became leader,AE
				rf.state = Leader
				DPrintf3B("server[%d] became leader\n", rf.me)
				rf.updateNextIndexNon()
				//go rf.checkConsistency()
				DPrintf("server[%d]Term[%d] became leader\n", rf.me, rf.currentTerm)
				DPrintf3B("server[%d]Term[%d] start send heartbeat\n", rf.me, rf.currentTerm)
				go rf.sendHeartBeats()
			}
		}
	}
}

// 这个函数实际上就是检查是否一致
// 实际上就是调用一次AE，如果日志不一致，则进行日志统一
func (rf *Raft) checkConsistency() {
	rf.sendAppendEntries()
}

func (rf *Raft) electionNon() {
	rf.currentTerm++
	rf.votedFor = rf.me
	//persist
	rf.persist()

	rf.state = Candidate
	vote := 1

	args := RequestVoteArgs{
		CandidiateTerm: rf.currentTerm,
		CandidateId:    rf.me,
		LastLogIndex:   len(rf.log) - 1,
		LastLogTerm:    rf.log[len(rf.log)-1].Term,
	}
	for i, _ := range rf.peers {
		if i != rf.me {
			go rf.callSendRequestVote(&vote, &args, i)
		}
	}
}

// 变成Leader的时候，马上启动这个；
// 直到自己变成其他状态或者dead
// Hold lock
func (rf *Raft) sendHeartBeats() {
	for rf.killed() == false {
		rf.mu.Lock()
		//only NonLeader can do this
		if rf.state != Leader { //dead lock fuck
			DPrintf("server[%d] not leader now, quit\n", rf.me)
			rf.mu.Unlock()
			return
		}

		for i, _ := range rf.peers {
			if i != rf.me {
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
					//maybe not bug, just to pass the test
					//Entries:      make([]LogEntry, 0), //empty entries for heartbeat
					//很快AE就会退化成HB，而且多次发送同一个AE不会有问题
					Entries:      rf.log[rf.nextIndex[i]:],
					LeaderCommit: rf.commitIndex,
				}
				DPrintf("i am server[%d], send HeartBeat for server[%d]\n", rf.me, i)
				go rf.appendEntries(i, &args)
			}
		}
		rf.mu.Unlock()

		time.Sleep(time.Duration(125) * time.Millisecond)
	}
}

/*
// need to do something about log
func (rf *Raft) appendEntries(serverId int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.peers[serverId].Call("Raft.AppendEntriesHandler", args, &reply)
	if !ok {
		DPrintf("[AE]: something wrong happen\n")
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//防止过去太长时间
	if args.Term == rf.currentTerm && rf.state == Leader {
		if reply.Success {
			//donothing now
			DPrintf("[AE]: server[%d]Term[%d] --> server[%d]Term[%d] success\n", rf.me, rf.currentTerm, serverId, reply.Term)
		} else {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.resetNon()
			} else {
				//consistency check false
				DPrintf3B("heartbeat but concsistency check false\n")
			}
		}
	}
}
*/

func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf3B("get [AE]: server[%d]Term[%d]->server[%d]Term[%d], %v\n loglen[%d]\n", args.LeaderId, args.Term, rf.me, rf.currentTerm,
		*args, len(args.Entries))
	DPrintf3B("server[%d] log before:\n", rf.me)
	rf.PrintLog()
	if args.Term < rf.currentTerm { //cannot be the leader
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	//5.3
	if len(rf.log)-1 < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[AEH]: consistency fail")
		reply.Success = false

		//roll back quickly,XTerm,XIndex,XLen
		if len(rf.log)-1 < args.PrevLogIndex {
			reply.XTerm = -1
			reply.XLen = len(rf.log)
		} else {
			reply.XTerm = rf.log[args.PrevLogIndex].Term
			for i := args.PrevLogIndex; i >= 0; i-- {
				if rf.log[i].Term != reply.XTerm {
					reply.XIndex = i + 1
					break
				}
			}
		}
		return
	}
	//不能直接append，因为有可能出现一个bug：{1， 103，102}日志和{1， 103}日志同时发送过来；
	//但是{1， 103， 102}日志处理的时候成功了，所以需要apply；但是后边那个日志会顶掉前面的102；
	//导致apply越界
	//rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	//len1 := len(rf.log) - args.PrevLogIndex - 1
	//len2 := len(args.Entries)
	logEntryIndex := 0
	logIndex := args.PrevLogIndex + 1
	conflict := false
	for logEntryIndex < len(args.Entries) && logIndex < len(rf.log) {
		if args.Entries[logEntryIndex].Term != rf.log[logIndex].Term {
			//conflict
			rf.log = rf.log[:logIndex]
			//persist
			rf.persist()
			conflict = true
			break
		}
		logEntryIndex++
		logIndex++
	}

	if conflict {
		rf.log = append(rf.log, args.Entries[logEntryIndex:]...)
		rf.persist()
	} else {
		//no conflict
		if logEntryIndex == len(args.Entries) && logIndex == len(rf.log) {
			//do nothing
		} else if logEntryIndex == len(args.Entries) {
			//do nothing
		} else if logIndex == len(rf.log) {
			rf.log = append(rf.log, args.Entries[logEntryIndex:]...)
			rf.persist()
		}
	}

	DPrintf3B("server[%d]Term[%d] log append success\n append after:\n", rf.me, rf.currentTerm)
	rf.PrintLog()
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = minNumber(args.LeaderCommit, len(rf.log)-1)
		rf.cond.Signal()
	}
	DPrintf("[AEH]: server[%d] append log success, log len is [%d]\n", rf.me, len(rf.log))
	//rf.PrintLog()

	if rf.currentTerm < args.Term {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		//persist
		rf.persist()
	}

	rf.state = Follower
	rf.resetNon()
	reply.Success = true
	reply.Term = rf.currentTerm
}

// 这个ticker包括两个timeout；
// 如果状态是Follower, 切换成Candidate，开始Election
// 如果状态是Candidate，直接进入ReElection
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state == Leader {
			rf.resetNon()
		}
		if time.Now().After(rf.electionTimeout) {
			DPrintf("[TIMEOUT]: server[%d]Term[%d] timeout, start election\n", rf.me, rf.currentTerm)
			rf.resetNon()
			rf.electionNon()
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applyCommand(ApplyCh chan ApplyMsg) {
	for rf.killed() == false {
		rf.mu.Lock()
		rf.cond.Wait()

		entries := make([]ApplyMsg, 0)
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Op,
				CommandIndex: rf.lastApplied,
			}

			//rf.mu.Unlock()
			//ApplyCh <- msg
			//rf.mu.Lock()
			entries = append(entries, msg)
		}
		//rf.PrintLog()
		rf.mu.Unlock()

		for i, _ := range entries {
			ApplyCh <- entries[i]
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
	rf.currentTerm = 0
	rf.votedFor = -1             //null
	rf.log = make([]LogEntry, 1) //log start from 1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	for i, _ := range rf.peers {
		rf.nextIndex[i] = 1 //lastIogIndex+1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.state = Follower
	rf.cond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyCommand(applyCh)
	return rf
}
