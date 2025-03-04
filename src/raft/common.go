package raft

import (
	"sync"
	"time"

	"6.5840/labrpc"
)

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
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	cond      *sync.Cond
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
	state           int       //Follower, Candidate, Leader?
}

type LogEntry struct {
	//op   interface{}
	//term int
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
	//LastLogIndex   int //Candidate的日志至少和其他服务器的一样新！,否则拒绝投票给Candidate
	//LastLogTerm    int
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
