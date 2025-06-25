package shardctrler


import "6.5840/raft"
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"
import "sort"
import "time"

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
	sc.mu.Unlock()
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
		sc.mu.Lock()
		if _, ok := sc.channelMap[index]; ok { // 如果存在这个channel，就把它关闭并从map中删除
		close(sc.channelMap[index])
		delete(sc.channelMap, index)
	}
		sc.mu.Unlock()
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
	sc.mu.Unlock()
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
		sc.mu.Lock()
		if _, ok := sc.channelMap[index]; ok { // 如果存在这个channel，就把它关闭并从map中删除
			close(sc.channelMap[index])
			delete(sc.channelMap, index)
		}
		sc.mu.Unlock()
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
	sc.mu.Unlock()
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
		sc.mu.Lock()
		if _, ok := sc.channelMap[index]; ok { // 如果存在这个channel，就把它关闭并从map中删除
			close(sc.channelMap[index])
			delete(sc.channelMap, index)
		}
		sc.mu.Unlock()
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
	sc.mu.Unlock()
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
		sc.mu.Lock()
		if _, ok := sc.channelMap[index]; ok { // 如果存在这个channel，就把它关闭并从map中删除
			close(sc.channelMap[index])
			delete(sc.channelMap, index)
		}
		sc.mu.Unlock()
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
	sc.channelMap = make(map[int]chan LastResult)
	sc.lastAppliedIndex = 0 // initialize last applied index to 0

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

			sc.mu.Unlock()
			isLeader := true
			if _, l := sc.rf.GetState(); !l{
				isLeader = false
			}

			sc.mu.Lock()
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
				c.Shards = sc.configs[len(sc.configs)-1].Shards // copy the shards from the last config
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
					for i := 0; i < NShards; i++ {
						if sc.configs[len(sc.configs)-1].Shards[i] == gid {
							c.Shards[i] = 0 // reassign the shard to group 0
						} else {
							c.Shards[i] = sc.configs[len(sc.configs)-1].Shards[i] // keep the shard in the same group
						}
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
					//c.Groups[gid] = servers
				}
				//Shards是数组而非切片，go会自动拷贝数组
				c.Shards = sc.configs[len(sc.configs)-1].Shards 
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
			} //end switch
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
	DPrintf5("config: %v\n", config)
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

	avg   := shardCount / groupCount
	extra := shardCount % groupCount
    
	// gid -> count of shards in that group
	shardsPerGroup := make(map[int]int)
	for _, gid := range config.Shards {
		shardsPerGroup[gid]++
	}

	//统计所有不同的gid，配合shardsPerGroup使用
	gids := make([]int, 0, len(config.Groups))
	for gid := range config.Groups {
		gids = append(gids, gid)
	}

	//shards多的排在前面；
	//shards相同，gid小的排在前面
	sort.Slice(gids, func(i, j int) bool {
		if shardsPerGroup[gids[i]] != shardsPerGroup[gids[j]] {
			return shardsPerGroup[gids[i]] > shardsPerGroup[gids[j]]
		} 
		return gids[i] < gids[j] 
	})
	//存储需要move的shardsId
	needMove := make([]int, 0)
	
	//前groupCount个gid，平均分配avg+1个shards
	//后边的gid，平均分配avg个shards
	//采用的方法是，首先取出所有需要进行move的shardsId，
	// 然后从前往后遍历gids，直到每个gid的shards数目达到avg+1或avg
	for i := 0; i < shardCount; i++ {
		if config.Shards[i] == 0 {
			needMove = append(needMove, i) // collect unassigned shards
		}
	}

	for i, gid := range gids {
		var target int

		//前extra个gid，平均分配avg+1个shards
		// 后边的gid，平均分配avg个shards
		if i < extra {
			target = avg + 1 
		} else {
			target = avg
		}

		//如果gid的shards大于target，则从头开始遍历shards，取序号小的进入move数组
		if shardsPerGroup[gid] > target {
			diff := shardsPerGroup[gid] - target
			for j := 0; j < shardCount; j++ {
				if config.Shards[j] == gid && diff > 0 {
					needMove = append(needMove, j) // collect shards to move
					diff--
				}
			}
		} else if shardsPerGroup[gid] < target {
			diff := target - shardsPerGroup[gid]
			for _, j := range needMove {
				if diff > 0 {
					config.Shards[j] = gid
					diff--
				}
			}
		} else {
			continue
		}
	}

	DPrintf5("after rebalanceShards, config: %v\n", config)
	if len(needMove) != 0 {
		DPrintf5A("something wrong happen in rebalanceShards\n")
	}
}