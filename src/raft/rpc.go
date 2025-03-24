package raft

import (
	"sync"
	"time"

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

	commitIndex int //当前已经commit过的最高id？
	lastApplied int //最高的被应用到状态机的下标；用于follower提交到application

	nextIndex  []int //对每个follower维护一个nextIndex，指向下一个要发送的log下标
	matchIndex []int // ?

	electionTimeout time.Time //选举超时时间
	state           int       //Follower, Candidate, Leader?
	cond            *sync.Cond
}

type LogEntry struct {
	Op   interface{}
	Term int
}

const (
	Follower = iota
	Candidate
	Leader
)

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
	Term         int //Leader term
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	//three member for roll back quickly
	XTerm  int
	XIndex int
	XLen   int
}

// non hold lock
// fix bugs
// 每次选举成功都要重新初始化
func (rf *Raft) updateNextIndexNon() {
	for i, _ := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
}

func maxNumber(x1, x2 int) int {
	if x1 > x2 {
		return x1
	} else {
		return x2
	}
}

// 不需要上锁
// leader自己的matchIndex就是len(rf.log)-1
func (rf *Raft) tryUpdateCommitIndexNon() {
	PrettyDebug(dLeader, "S%d,term=%d,updatecommit index before=%d", rf.me, rf.currentTerm, rf.commitIndex)
	rf.matchIndex[rf.me] = len(rf.log) - 1
	maxMatchIndex := -1
	for _, v := range rf.matchIndex {
		maxMatchIndex = maxNumber(maxMatchIndex, v)
	}

	for N := maxMatchIndex; N > rf.commitIndex; N-- {
		count := 0
		for _, v := range rf.matchIndex {
			if v >= N {
				count++
			}
		}
		if count > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
			rf.commitIndex = N
			break
		}

	}
	PrettyDebug(dLeader, "S%d,term=%d,updatecommit index after=%d", rf.me, rf.currentTerm, rf.commitIndex)
	rf.cond.Signal()
}
