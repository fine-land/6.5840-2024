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
	Num 	 int // for Query, desired config number
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
		OpType:    "Leave",
		GIDs:      args.GIDs,
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

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
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
		OpType:    "Move",
		Shard:     args.Shard,	
		GID:       args.GID,
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

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
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
		reply.Config = lr.Config
		sc.mu.Unlock()
		return 
	} else if !ok || args.CommandId > lr.CommandId {
		// do nothing
	}
	op := Op {
		OpType:    "Query",
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
		Num:       args.Num,
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
		reply.Config = lr.Config
		return
	case <-time.After(100 * time.Millisecond):
		reply.Err = ErrTimeout
		reply.WrongLeader = false
		return
	}
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
				c.Groups = make(map[int][]string)
				for gid, servers := range sc.configs[len(sc.configs)-1].Groups {
					c.Groups[gid] = append([]string{}, servers...) // deep copy
				}
				for gid, servers := range op.Servers{
					if _, exists := c.Groups[gid]; !exists {
						c.Groups[gid] = append([]string{}, servers...)  //may do not need deep copy，but for safety
					}
				}
				rebalanceShards(c)
				lr = LastResult{
					CommandId: op.CommandId,
					Err:       OK,
					WrongLeader: !isLeader,
				}
				sc.configs = append(sc.configs, *c)
			case "Leave":
				c := &Config{}
				c.Num = sc.configs[len(sc.configs)-1].Num + 1
				c.Groups = make(map[int][]string)
				for gid, servers := range sc.configs[len(sc.configs)-1].Groups {
					c.Groups[gid] = append([]string{}, servers...) // deep copy
				}
				for _, gid := range op.GIDs {
					if _, exists := c.Groups[gid]; exists {
						delete(c.Groups, gid) // remove the group
					}
				}
				rebalanceShards(c)
				lr = LastResult{
					CommandId: op.CommandId,
					Err:       OK,
					WrongLeader: !isLeader,
				}
				sc.configs = append(sc.configs, *c)
			case "Move":
				c := &Config{}
				c.Num = sc.configs[len(sc.configs)-1].Num + 1
				c.Groups = make(map[int][]string)
				for gid, servers := range sc.configs[len(sc.configs)-1].Groups {
					c.Groups[gid] = append([]string{}, servers...) // deep copy
				}
				if op.Shard < NShards && op.Shard >= 0 {
					c.Shards[op.Shard] = op.GID // move shard to new group
				}
				lr = LastResult{
					CommandId: op.CommandId,
					Err:       OK,
					WrongLeader: !isLeader,
				}
				sc.configs = append(sc.configs, *c)
			case "Query":
				if op.Num == -1 || op.Num >= len(sc.configs) {
					lr = LastResult{
						CommandId: op.CommandId,
						Err:       OK,
						WrongLeader: !isLeader,
						Config:    sc.configs[len(sc.configs)-1], // return the latest config
					}
				} else {
					lr = LastResult{
						CommandId: op.CommandId,
						Err:       OK,
						WrongLeader: !isLeader,
						Config:    sc.configs[op.Num], // return the requested config
					}
				}
			}
			sc.buffer[op.ClientId] = lr // store the result for deduplication
			if ch, ok := sc.channelMap[msg.CommandIndex]; ok {
				if !isLeader {
					lr.WrongLeader = true // if not leader, set WrongLeader to true
				}
				sc.mu.Unlock() // unlock before sending to channel
				ch <- lr // send the result to the waiting channel
			} else {
				sc.mu.Unlock() // unlock if no channel to send
			}
			//sc.mu.Unlock()
		} else if msg.SnapshotValid {
			//do nothing, config do not need snapshot
		}
	}
}


/*
   由于go的map的不确定性，必须保证所有的servers都是相同的分配策略
   这里我采用的是非常简单的策略；把map shards-gid，改成数组的形式，并按照gid排序；
   首先拿到平均avg和取模k；
   然后统计每个
*/
func rebalanceShards(config *Config){
	// Rebalance shards across groups
	// This is a simplified version, you may want to implement a more sophisticated balancing algorithm
	shardCount := len(config.Shards)
	groupCount := len(config.Groups)

	if groupCount == 0 {
		for i := 0; i < shardCount; i++ {
			config.Shards[i] = 0 // all shards assigned to group 0
		}
		return
	}

	var gids []int
	for gid := range config.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids) 

	//map gid -- []shardId
	shardsByGroup := make(map[int][]int)
	for gid := range config.Groups {
		shardsByGroup[gid] = []int{}
	}
	for i, gid := range config.Shards {
		if _, ok := shardsByGroup[gid]; ok {
			shardsByGroup[gid] = append(shardsByGroup[gid], i)
		}
	}

	/*
         remain个grp有avg+1个shards；
		 其他的grp有avg个shards；
	*/
	avg 	:= shardCount / groupCount
	remain  := shardCount % groupCount

	/*
	    分配策略如下：
		按照顺序遍历group，如果shards > 
	*/
	var toMove []int 
}