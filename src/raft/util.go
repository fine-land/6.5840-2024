package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func Assert(condition bool, discription string) {
	if !condition {
		log.Fatalf("assert [%v] failure", discription)
	}
}

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
