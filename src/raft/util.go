package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = false

// 收到RPC，[R]
// 发送RPC， [S]
// [ID][TERM]: STATE1 <-- STATE2
// [VOTE]: [ID][TERM] <-- [ID][TERM]
func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

const Debug3B = false

func DPrintf3B(format string, a ...interface{}) {
	if Debug3B {
		log.Printf(format, a...)
	}
}

const Debug3C = false

func DPrintf3C(format string, a ...interface{}) {
	if Debug3C {
		log.Printf(format, a...)
	}
}

const Debug3D = true

func DPrintf3D(format string, a ...interface{}) {
	if Debug3C {
		log.Printf(format, a...)
	}
}

func (rf *Raft) PrintLog() {
	DPrintf3D("server[%d] log len[%d], log is :\n", rf.me, len(rf.log)-1)
	for i, v := range rf.log {
		DPrintf3D("%v %d", v, i)
	}
}

func minNumber(x1, x2 int) int {
	if x1 < x2 {
		return x1
	} else {
		return x2
	}
}

func (args AppendEntriesArgs) String() string {
	s := fmt.Sprintf("Term[%d] LeaderId[%d] PrevLogIndex[%d] PrevLogTerm[%d] LeaderCommit[%d]\n", args.Term,
		args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)

	for _, v := range args.Entries {
		s += fmt.Sprintf("command[%v] Term[%d]\n", v.Op, v.Term)
	}
	return s

}

func (rf *Raft) Global2LogNon(index int) int {
	return index - rf.lastIncludedIndex
}

func (rf *Raft) Log2GlobalNon(index int) int {
	return index + rf.lastIncludedIndex
}

func (rf *Raft) GetTermFromGlobalIndex(index int) int {
	tmp := rf.Global2LogNon(index)
	if tmp == 0 {
		return rf.lastIncludedTerm
	} else if tmp > 0 {
		return rf.log[tmp].Term
	} else {
		PrettyDebug(dError, "S%d,term=%d,error index=%d,", rf.me, rf.currentTerm, tmp)
	}
	return -100
}

func (rf *Raft) GetTermFromLogIndex(index int) int {
	if index == 0 {
		return rf.lastIncludedTerm
	} else {
		return rf.log[index].Term
	}
}

/*
var stateNames = map[int]string{
	Follower:  "Follower",
	Candidate: "Candidate",
	Leader:    "Leader",
}

func WhenStateChanged(id int, old int, new int) {
	if _, ok := stateNames[old]; !ok {
		fmt.Printf("Error: Invalid old state %d\n", old)
		return
	}
	if _, ok := stateNames[new]; !ok {
		fmt.Printf("Error: Invalid new state %d\n", new)
		return
	}

	// 打印状态切换信息
	fmt.Printf("server [%d] State transition: [%s] -> [%s]\n", id, stateNames[old], stateNames[new])

}
*/
