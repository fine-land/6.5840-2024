package raft

import (
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
