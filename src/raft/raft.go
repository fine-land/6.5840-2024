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
	"fmt"
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

	//persistent state on all servers
	currentTerm int        //当前服务器的term
	votedFor    int        //存放投票给哪个服务器了
	log         []LogEntry //从1开始
	loglength   int

	commitIndex int
	lastApplied int //最高的被应用到状态机的下标；用于follower提交到application

	nextIndex  []int //对每个follower维护一个nextIndex，指向下一个要发送的log下标
	matchIndex []int

	electionTimeout time.Time //选举超时时间
	voteTimeout     time.Time //投票超时时间
	state           int       //Follower, Candidate, Leader?
}

type LogEntry struct {
	op   interface{}
	term int
}

const (
	Follower = iota
	Candidate
	Leader
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
// only send by Candidate!! use for Leader Election &&
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	CandidiateTerm int //Candidate的term
	CandidateId    int //Candidate的id，用于votedfor
	LastLogIndex   int //Candidate的日志至少和其他服务器的一样新！,否则拒绝投票给Candidate
	LastLogTerm    int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  //Candidate是否需要更新term？
	VoteGranted bool //是否投票？
}

type AppendEntriesArgs struct {
	Term int //Leader term
}

type AppendEntriesReply struct {
	Term    int
	Success bool
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
	lastlogindex := rf.loglength
	lastlogterm := rf.log[rf.loglength].term
	if (args.LastLogTerm < lastlogterm) ||
		(args.CandidiateTerm == lastlogterm && args.LastLogIndex < lastlogindex) {
		reply.VoteGranted = false
		return
	}
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		//已经投过票了，且不是投给的这个服务器
		reply.VoteGranted = false
	}

	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	fmt.Printf("[VoteRequest]: server[%d] vote for candidate[%d], term is [%d]\n", rf.me, args.CandidateId, args.CandidiateTerm)
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
	for {
		args := AppendEntriesArgs{
			Term: rf.currentTerm,
		}
		reply := AppendEntriesReply{}
		rf.mu.Lock()
		for i := range rf.peers {
			if i != rf.me {
				if rf.state == Leader {
					break
				}
				for !rf.peers[i].Call("Raft.AppendEntriesHandler", args, reply) {
				}
				if reply.Success == false {
					if reply.Term > rf.currentTerm {
						rf.state = Follower
						break
					}
				}
			}
		}
		rf.mu.Unlock()

		time.Sleep(200 * time.Millisecond) //5times per second; 200ms*5 = 1s
	}

}

func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	ResetTicker()

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

func (rf *Raft) Execute() {
	for rf.killed() == false {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm++

		if rf.state == Follower {
			//ElectionTimeout
			WhenStateChanged(rf.me, Follower, Candidate)
			rf.state = Candidate
		}
		votes := 1                      //vote for itself
		majority := len(rf.peers)/2 + 1 //assert odd servers
		for i := range rf.peers {
			if i != rf.me {
				args := RequestVoteArgs{
					CandidiateTerm: rf.currentTerm,
					CandidateId:    rf.me,
					LastLogIndex:   rf.loglength,
					LastLogTerm:    rf.log[rf.loglength].term,
				}
				reply := RequestVoteReply{}
				for !rf.sendRequestVote(i, &args, &reply) {
				}
				if reply.Term > rf.currentTerm {
					//如果发现某个人的term大于自己，进入follower
					rf.currentTerm = reply.Term
					WhenStateChanged(rf.me, rf.state, Follower)
					rf.state = Follower
					break
				}
			}
		}
	}
}

// 这个ticker包括两个timeout；
// 如果状态是Follower, 切换成Candidate，开始Election
// 如果状态是Candidate，直接进入ReElection
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

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
	rf.currentTerm = 0
	rf.votedFor = -1 //null
	rf.log = make([]LogEntry, 0)
	rf.loglength = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.state = Follower

	go rf.Execute()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
