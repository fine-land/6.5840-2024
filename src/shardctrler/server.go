package shardctrler


import "6.5840/raft"
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	buffer map[int64]LastResult
	channelMap map[int]chan LastResult // maps logindex to channel for waiting replies
	lastAppliedIndex int
	configs []Config // indexed by config num
}


type LastResult struct {
	CommandId int64 
	Err	   Err
	WrongLeader bool
	Config Config // for Query operation
}

type Op struct {
	// Your data here.
	OpType    string // "Join", "Leave", "Move", "Query"
	Servers   map[int][]string // for Join
	GIDs      []int // for Leave
	Shard     int // for Move
	GID       int // for Move
	ClientId  int64 // for deduplication
	CommandId int64 // for deduplication
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	if lr, ok := sc.buffer[args.ClientId]; ok && args.CommandId < lr.CommandId {
		reply.Err = ErrOldRequest
		reply.WrongLeader = false
		sc.mu.Unlock()
		return
	} else if ok && args.CommandId == lr.CommandId {
		reply.Err = lr.Err
		reply.WrongLeader = lr.WrongLeader
		sc.mu.Unlock()
		return 
	} else if !ok || args.CommandId > lr.CommandId {
		// do nothing
	}

	op := Op {
		OpType:    "Join",
		Servers:   args.Servers,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	kv.mu.Unlock()
	index, _, isleader := sc.rf.Start(op)
	if !isleader {
		reply.WrongLeader = true
		return 
	}

	ch := make(chan LastResult)
	sc.mu.Lock()
	sc.channelMap[index] = ch
	sc.mu.Unlock()
	defer func(index int) {
		kv.mu.Lock()
		if _, ok := kv.channelMap[index]; ok { // 如果存在这个channel，就把它关闭并从map中删除
		close(kv.channelMap[index])
		delete(kv.channelMap, index)
	}
		kv.mu.Unlock()
	}(index)

	select {
	case lr := <-ch:
			reply.Err = lr.Err
			reply.WrongLeader = lr.WrongLeader
			return 
	case <-time.After(100 * time.Millisecond):
			reply.Err = ErrTimeout
			return 
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}


// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.buffer = make(map[int64]LastResult)
	sc.configs[0].Num = 0
	sc.configs[0].Shards = [NShards]int{}

	go sc.applyCommands()
	return sc
}


func (sc *ShardCtrler) applyCommands() {
	for {
		msg := <- sc.applyCh
		if msg.CommandValid {
			sc.mu.Lock()
			if msg.CommandIndex <= sc.lastAppliedIndex {
				sc.mu.Unlock()
				continue // ignore old commands
			}
			sc.lastAppliedIndex = msg.CommandIndex
			op := msg.Command.(Op)

			kv.mu.Unlock()
			isLeader := true
			if _, l := sc.rf.GetState(); !l{
				isLeader = false
			}

			kv.mu.Lock()
			var lr LastResult
			switch op.OpType {
			case "Join":
				c := &Config{}
				c.Num = sc.configs[len(sc.configs)-1].Num + 1  
				for gid, ss := range op.Servers {
					c.Groups[gid] = ss  //wait to do ...
				}
			}
		} else if msg.SnapshotValid {
			//do nothing, config do not need snapshot
		}
	}
}