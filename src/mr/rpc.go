package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type RPCHandlerArgs struct {
}

type RPCHandlerReply struct {
	TaskId   int
	File     string // input file, for mapper
	NReduce  int    //Reduce nums
	TaskType int
}

// 监视整个mr执行阶段
const (
	Mapper = iota
	Reducer
	Done
)

// map/reduce 任务的状态
const (
	NotYetStarted = iota
	InProgess
	Finished
)

// TaskType
const (
	Map = iota
	Reduce
	AllDone
	Wait
)

type FinishArgs struct {
	TaskId   int
	TaskType int //Map, Reduce

}

type FinishReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
