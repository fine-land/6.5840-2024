package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	// "log"
	"sync"
	"sync/atomic"
	"time"
)




type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string // key for Get, Put, Append
	Value     string // value for Put, Append
	OpType    string // "Get", "Put", or "Append"
	ClientId  int64 // unique client id
	CommandId int64 // sequence number for operations
	Term      int   //there one bug, if not have this; see also (4);
}

type LastResult struct {
	Value	 string // value for Get
	Err	     Err    // error for Get, Put, Append
	CommandId int64  // sequence number for operations
} 

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store map[string]string // key-value store
	// to keep track of the last command applied by each client
	lastApplied map[int64]LastResult // maps clientId to LastResult
	channelMap map[int]chan LastResult // maps logindex to channel for waiting replies
	lastAppliedIndex int
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// Check if the command has been applied before
	// if lr, ok := kv.lastApplied[args.ClientId]; ok && lr.CommandId >= args.CommandId {
	// 	reply.Err = lr.Err
	// 	reply.Value = lr.Value
	// 	return
	// }
	kv.mu.Lock()
	if lr, ok := kv.lastApplied[args.ClientId];
		ok && args.CommandId < lr.CommandId {
			// If the command has been applied before LastResult,just return
			reply.Err = ErrOldRequest
			kv.mu.Unlock()
			return
		} else if ok && args.CommandId == lr.CommandId {
			// If the command has been applied before, just return the value
			reply.Err = lr.Err
			reply.Value = lr.Value
			kv.mu.Unlock()
			return
		} else if !ok || args.CommandId > lr.CommandId {
			// If the command has not been applied before, we need to process it
			//just continue
		}

	// Create a new operation	
	op := Op{
		Key:       args.Key,
		Value:     "",
		OpType:    "Get",
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	//avoid raft deadlock
	kv.mu.Unlock()
	// Send the operation to Raft
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op.Term = term
	// Create a channel to wait for the reply
	ch := make(chan LastResult)

	kv.mu.Lock()
	kv.channelMap[index] = ch
	kv.mu.Unlock()
	defer func(index int) {
		kv.mu.Lock()
		if _, ok := kv.channelMap[index]; ok { // 如果存在这个channel，就把它关闭并从map中删除
		close(kv.channelMap[index])
		delete(kv.channelMap, index)
	}
		kv.mu.Unlock()
	}(index)
	// Wait for the reply
	select {
		case lr := <-ch:
			reply.Err = lr.Err
			reply.Value = lr.Value
			return
		case <- time.After(1000 * time.Millisecond): // timeout
			reply.Err = ErrTimeout
			return
	}
}


func (kv *KVServer) putAppend(args *PutAppendArgs, reply *PutAppendReply, opType string) {
	// Check if the command has been applied before
	// if lr, ok := kv.lastApplied[args.ClientId]; ok && lr.CommandId >= args.CommandId {
	// 	reply.Err = lr.Err
	// 	return
	// }
	kv.mu.Lock()
	if lr, ok := kv.lastApplied[args.ClientId];
		ok && args.CommandId < lr.CommandId {
			// If the command has been applied before LastResult, just return
		DPrintf("[server][%v], applied before lr, clientid: %v, commandid %v, key: %v, value: %v",
			opType, args.ClientId, args.CommandId, args.Key, args.Value)	
			reply.Err = ErrOldRequest
			kv.mu.Unlock()
			return
		} else if ok && args.CommandId == lr.CommandId {
			// If the command has been applied before, just return the error
			reply.Err = lr.Err
			kv.mu.Unlock()
			DPrintf("[server][%v], ok && args.c==lr.c, clientid: %v, commandid %v, key: %v, value: %v",
			opType, args.ClientId, args.CommandId, args.Key, args.Value)
			return
		} else if !ok || args.CommandId > lr.CommandId {
			// If the command has not been applied before, we need to process it
			// just continue
			DPrintf("[server][%v], !ok | args.c > lr.c, ok: %v, lr.CommandId: %v, clientid: %v, commandid %v, key: %v, value: %v",
			opType, ok, lr.CommandId, args.ClientId, args.CommandId, args.Key, args.Value)
		}
		kv.mu.Unlock()
	// Create a new operation
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		OpType:    opType,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	// Send the operation to Raft
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op.Term = term
	// Create a channel to wait for the reply
	ch := make(chan LastResult)
	kv.mu.Lock()
	kv.channelMap[index] = ch
	kv.mu.Unlock()
	defer func(index int) {
		kv.mu.Lock()
		if _, ok := kv.channelMap[index]; ok { // 如果存在这个channel，就把它关闭并从map中删除
		close(kv.channelMap[index])
		delete(kv.channelMap, index)
	}
		kv.mu.Unlock()
	}(index)

		DPrintf("[server][%v], Raft Start, logindex: %v, clientid: %v, commandid %v, key: %v, value: %v",
			opType, index, args.ClientId, args.CommandId, args.Key, args.Value)
	// Wait for the reply
	select {
	case lr := <-ch:
		reply.Err = lr.Err
		return
	case <- time.After(1000 * time.Millisecond): // timeout
	DPrintf("[server][%v], timeout, clientId: %v, commandid: %v, key: %v, value: %v",
		opType, args.ClientId, args.CommandId, args.Key, args.Value)
		reply.Err = ErrTimeout
		return
	}
}


func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.putAppend(args, reply, "Put")
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.putAppend(args, reply, "Append")
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.store = make(map[string]string) // initialize the key-value store
	kv.lastApplied = make(map[int64]LastResult) // initialize the last applied map
	kv.channelMap = make(map[int]chan LastResult) // initialize the channel map
	kv.lastAppliedIndex = 0 // initialize the last applied index

	go kv.applyLoop() // start the apply loop to process Raft messages
	return kv
}


func (kv *KVServer) applyLoop() {
	for !kv.killed(){
		isL := true
		DPrintf("try get msg, now lastAppliedIndex: %v", kv.lastAppliedIndex)
		msg := <- kv.applyCh  //something wrong happened ...
		DPrintf("get msg, index: %v", msg.CommandIndex)
		if msg.CommandValid {
			kv.mu.Lock()
			if msg.CommandIndex <= kv.lastAppliedIndex {
				kv.mu.Unlock()
				continue
			}
			kv.lastAppliedIndex = msg.CommandIndex
			op := msg.Command.(Op) // type assertion to get the Op from the Command
			DPrintf("[server],getapplyCh, clientId: %v, commandId: %v, op: %v, logindex: %v, lastAppliedIndex: %v, key: %v, value: %v",
			op.ClientId, op.CommandId, op.OpType, msg.CommandIndex, kv.lastAppliedIndex, op.Key, op.Value)
			//bugs need to fix
			if lrop, ok := kv.lastApplied[op.ClientId]; ok && op.CommandId <= lrop.CommandId {
				DPrintf("[server] commandId: %v, ok: %v, op.commandId <= commandId, errrrrrrr", lrop.CommandId, ok)
				//如果多次发送同一个指令，则只会处理一次；
				//虽然我觉得是不可能的，但是还是加上这个判断
				//这种情况下，raft的日志小于等于最后一次的结果，证明
				//这个操作已经被处理过了，我们直接返回lastresult
				//并且不需要提交到state machine了
				lr := LastResult{
					Err: ErrOldRequest,
					Value: "",
				}
				if ch, ok := kv.channelMap[msg.CommandIndex]; ok {
					kv.mu.Unlock()
					ch <- lr
				} else {
					kv.mu.Unlock()
				}
				continue
			}
			
			kv.mu.Unlock()
			if _, isLeader := kv.rf.GetState(); !isLeader {
				isL = false
			} 

			kv.mu.Lock()
			//update kv.store, kv.lastApplied, kv.channelMap
			var lr LastResult
			switch(op.OpType) {
			case "Get":
				if value, ok := kv.store[op.Key]; ok {
					lr = LastResult{
						Value:     value,
						Err:       OK,
						CommandId: op.CommandId,
					}
				} else {
					lr = LastResult{
						Value: "",
						Err:   ErrNoKey,
						CommandId: op.CommandId,
					}
				}
				//may be error
			case "Put":
				kv.store[op.Key] = op.Value // update the store
				lr = LastResult{
					Err:       OK,
					CommandId: op.CommandId,
				}
			case "Append":
				kv.store[op.Key] += op.Value // append to the value
				lr = LastResult{
					Err:       OK,
					CommandId: op.CommandId,
				}
			}
			kv.lastApplied[op.ClientId] = lr // update the last applied map
			//kv.mu.Unlock() // unlock the mutex before sending the result
			if ch, ok := kv.channelMap[msg.CommandIndex]; ok {
				kv.mu.Unlock()
				//此时，记录的是最新的一次日志；但是返回的是ErrWrongLeader
				if isL == false {
					lr.Err = ErrWrongLeader
				}
				ch <- lr // send the result to the channel
			}else {
				//如果这个log的channel已经被关闭了，说明timeout了，直接continue；因为结果已经被
				//记录到lastApplied里面了；无所谓丢失；
				kv.mu.Unlock()
				continue
			}
		} else if msg.SnapshotValid {
			// kv.mu.Unlock()
			//wait to do ...
		}
	} //end for loop
}