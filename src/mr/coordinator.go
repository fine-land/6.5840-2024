package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu       sync.Mutex
	index    int //input files的数量
	NReduce  int
	tasks    map[int]*Task //MapTaskId -- Task, mapper tasks
	reducers map[int]*Task //ReduceTaskId -- Task reducer tasks
	State    int
}

type Task struct {
	File      string
	TaskState int //notyetstarted, running, finished
	StartTime time.Time
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RPCHandler(args *RPCHandlerArgs, reply *RPCHandlerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.State {
	case Mapper:
		for k, v := range c.tasks {
			if v.TaskState == NotYetStarted {
				c.tasks[k].TaskState = InProgess
				c.tasks[k].StartTime = time.Now()
				reply.TaskId = k
				reply.File = v.File
				reply.NReduce = c.NReduce
				reply.TaskType = Map
				return nil
			}
		}
	case Reducer:
		for k, v := range c.reducers {
			if v.TaskState == NotYetStarted {
				c.reducers[k].TaskState = InProgess
				c.reducers[k].StartTime = time.Now()
				reply.TaskId = k
				reply.NReduce = c.NReduce
				reply.TaskType = Reduce
				return nil
			}
		}
	case Done:
		reply.TaskType = AllDone
		return nil
	}

	//not Done,not if
	//need to wait
	reply.TaskType = Wait
	return nil
}

func (c *Coordinator) HandleFinish(args *FinishArgs, reply *FinishReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.TaskType {
	case Map:
		c.tasks[args.TaskId].TaskState = Finished
		count := 0
		for _, v := range c.tasks {
			if v.TaskState != Finished {
				count++
			}
		}
		if count == 0 {
			c.State = Reducer
		}
	case Reduce:
		c.reducers[args.TaskId].TaskState = Finished
		count := 0
		for _, v := range c.reducers {
			if v.TaskState != Finished {
				count++
			}
		}
		if count == 0 {
			c.State = Done
		}
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.State == Done {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReduce:  nReduce,
		index:    0,
		tasks:    make(map[int]*Task),
		reducers: make(map[int]*Task),
		State:    Mapper,
	}

	for _, file := range files {
		task := &Task{
			File:      file,
			TaskState: NotYetStarted,
		}
		c.tasks[c.index] = task
		c.index++
	}

	for i := 0; i < nReduce; i++ {
		task := &Task{
			TaskState: NotYetStarted,
		}
		c.reducers[i] = task
	}

	go c.CrashDetector()

	// Your code here.
	//fmt.Println("Coordinator.index is ", c.index, " NReduce is ", c.NReduce) debug

	// for k, v := range c.tasks { debug
	// 	fmt.Println("task id: ", k, "task input file: ", v)
	// }

	c.server()
	return &c
}

func (c *Coordinator) CrashDetector() {
	for {
		time.Sleep(1 * time.Second)
		c.mu.Lock()
		switch c.State {
		case Mapper:
			for _, v := range c.tasks {
				if v.TaskState == InProgess && time.Since(v.StartTime) > 10*time.Second {
					v.TaskState = NotYetStarted
					v.StartTime = time.Now()
				}
			}
		case Reducer:
			for _, v := range c.reducers {
				if v.TaskState == InProgess && time.Since(v.StartTime) > 10*time.Second {
					v.TaskState = NotYetStarted
					v.StartTime = time.Now()
				}
			}
		case Done:
			return
		}
		c.mu.Unlock()
	}
}
