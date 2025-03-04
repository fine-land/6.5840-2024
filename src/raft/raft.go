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

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
	// Your code here (3A, 3B).
	//怎么样才会投票？
	//至少args的日志和此服务器的一样新；
	//Term至少一样新
	//如果args的term不够新，直接拒绝投票！

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[VoteRequest]: server[%d]Term[%d] ask vote from server[%d]Term[%d]\n", args.CandidateId, args.CandidiateTerm, rf.me, rf.currentTerm)

	//如果是Leader或者Candidate收到投票请求:
	//查看term，如果term小于对方的term
	//转换成Follower状态，然后执行Follower的逻辑
	//如果term大于等于对方的term，拒绝投票
	//并把自己的term放入reply
	if rf.state == Leader || rf.state == Candidate {
		if args.CandidiateTerm <= rf.currentTerm {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		} else {
			WhenStateChanged(rf.me, rf.state, Follower) //log
			rf.state = Follower
		}
	}

	if args.CandidiateTerm < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	//日志必须足够新
	//term相同，长的更新
	//term不同，term大的更新
	//lastlogindex := rf.loglength
	//lastlogterm := rf.log[rf.loglength].term
	//if (args.LastLogTerm < lastlogterm) ||
	//	(args.CandidiateTerm == lastlogterm && args.LastLogIndex < lastlogindex) {
	//	reply.VoteGranted = false
	//	return
	//}
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		//已经投过票了，且不是投给的这个服务器
		reply.VoteGranted = false
	}

	rf.votedFor = args.CandidateId
	//this line use for labrpc warning
	//reply.Term = rf.currentTerm          //把自己的老term返回去，maybe有用???
	rf.currentTerm = args.CandidiateTerm //更新自己的term
	reply.VoteGranted = true
	//一旦在该term里投过票，不应该进入Candidate？？？
	rf.resetTimeout()
	//fmt.Printf("[VoteRequest]: server[%d] vote for candidate[%d], term is [%d]\n", rf.me, args.CandidateId, args.CandidiateTerm)
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

func (rf *Raft) AppendEntries() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.state != Leader {
			rf.cond.Wait()
		}
		args := AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}
		rf.mu.Unlock()
		reply := AppendEntriesReply{}
		for i := range rf.peers {
			if i != rf.me {
				rf.mu.Lock()
				DPrintf("[AppendEntries]: server[%d]Term[%d] AE for server[%d]\n", rf.me, rf.currentTerm, i)
				rf.mu.Unlock()
				for !rf.peers[i].Call("Raft.AppendEntriesHandler", &args, &reply) {
				}
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					DPrintf("[AppendEntries]: server[%d]Term[%d] get max Term from server[%d]Term[%d]\n", rf.me, rf.currentTerm, i, reply.Term)
					rf.currentTerm = reply.Term
					WhenStateChanged(i, rf.state, Follower)
					rf.state = Follower
					break
				}
				rf.mu.Unlock()
			}
		}

		time.Sleep(200 * time.Millisecond) //5times per second; 200ms*5 = 1s
	}

}

func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[AppendEntriesHandler]: server[%d]Term[%d] get AE from server[%d]Term[%d]\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.currentTerm = args.Term
	rf.state = Follower
	rf.resetTimeout() //not need lock
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 这里少了一步关键步骤，如果在VoteRequest请求过程中，丢包之类的；
// 导致花费太多时间，超过超时时间；此时应该立刻中断此次选举，重新进行选举
// 但这个实现没有解决这个问题
func (rf *Raft) election() {
	//Start Leader Election
	rf.mu.Lock()
	//Leader not need Election
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}

	rf.currentTerm++
	vote := 1                       //vote for itself
	majority := len(rf.peers)/2 + 1 // 4/2+1==3, 5/2+1==3
	rf.votedFor = rf.me
	WhenStateChanged(rf.me, rf.state, Candidate)
	rf.state = Candidate

	args := RequestVoteArgs{
		CandidateId:    rf.me,
		CandidiateTerm: rf.currentTerm,
		//LastLogIndex:   rf.loglength,
		//LastLogTerm:    rf.log[rf.loglength].term,
	}
	reply := RequestVoteReply{}

	rf.mu.Unlock()

	//这里不能使用for循环；因为有可能一个服务器崩了；导致一直接收不到RPC；
	for i := range rf.peers {
		if i != rf.me {
			ok := rf.peers[i].Call("Raft.RequestVote", &args, &reply)
			if !ok {
				DPrintf("[VoteRequest] sever[%d]--> server[%d] lost VoteRequestRPC, maybe crashed", rf.me, i)
				continue
			}
			rf.mu.Lock()
			//如果term小，不用看了
			if reply.Term > rf.currentTerm {
				WhenStateChanged(rf.me, rf.state, Follower)
				rf.state = Follower
				break
			} else if reply.VoteGranted {
				vote++
			}
			rf.mu.Unlock()
		}

		//得票超过一半
		//if vote >= majority {
		//	Assert(true, "get majority votes")
		//	rf.mu.Lock()
		//	WhenStateChanged(rf.me, rf.state, Leader)
		//	rf.state = Leader
		//	rf.mu.Unlock()
		//	//马上发送AppendEntries去抑制其他服务器
		//	rf.cond.Signal()
		//	break
		//}

	}

	if vote >= majority {
		Assert(true, "get majority votes")
		rf.mu.Lock()
		WhenStateChanged(rf.me, rf.state, Leader)
		rf.state = Leader
		rf.mu.Unlock()
		//马上发送AppendEntries去抑制其他服务器
		rf.cond.Signal()
	}

}

func (rf *Raft) resetTimeout() {
	ms := 150 + (rand.Int63() % 150)
	rf.electionTimeout = time.Now().Add(time.Duration(ms) * time.Millisecond) //wait
}

// 如何设计呢？
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		if time.Now().After(rf.electionTimeout) {
			DPrintf("server[%d]: electiontimeout, time for election", rf.me)
			rf.election() //Hold Lock
			rf.mu.Lock()
			rf.resetTimeout() //这个函数不能持有锁；因为在AppendEntriesHandler会持有锁调用这个函数，
			rf.mu.Unlock()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	rf.cond = sync.NewCond(&rf.mu)
	rf.currentTerm = 0
	rf.votedFor = -1 //null
	rf.log = make([]LogEntry, 0)
	rf.loglength = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.state = Follower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.AppendEntries()

	return rf
}
