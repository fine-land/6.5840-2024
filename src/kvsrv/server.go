package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kvmap  map[string]string
	dupDet map[int64]string //duplication detector map,version--return value
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if value, exists := kv.dupDet[args.Version]; exists {
		reply.Value = value
		return
	}
	if value, exists := kv.kvmap[args.Key]; exists {
		reply.Value = value
	} else {
		reply.Value = ""
	}
	kv.dupDet[args.Version] = reply.Value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, exists := kv.dupDet[args.Version]; exists {
		return
	}

	kv.kvmap[args.Key] = args.Value
	kv.dupDet[args.Version] = ""
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if value, exists := kv.dupDet[args.Version]; exists {
		reply.Value = value
		return
	}

	oldvalue := kv.kvmap[args.Key]
	if oldvalue != "" {
		kv.kvmap[args.Key] = oldvalue + args.Value
	} else {
		kv.kvmap[args.Key] = args.Value
	}

	reply.Value = oldvalue
	kv.dupDet[args.Version] = oldvalue
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvmap = make(map[string]string)
	kv.dupDet = make(map[int64]string)
	return kv
}

func (kv *KVServer) Finish(args *FinishArgs, reply *FinishReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.dupDet, args.Version)
}
