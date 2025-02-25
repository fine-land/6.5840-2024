package kvsrv

import (
	"crypto/rand"

	//"log"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	//finish map[int64]struct{}
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	//ck.finish = make(map[int64]struct{})
	//go ck.Delete()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key:     key,
		Version: nrand(),
	}
	reply := GetReply{}
	ok := false
	for !ok {
		ok = ck.server.Call("KVServer.Get", &args, &reply)
		//fmt.Println("[client][Get]: error from Call,retry")
	}
	//ck.finish[args.Version] = struct{}{}
	ok = false
	for !ok {
		ok = ck.server.Call("KVServer.Finish", &FinishArgs{args.Version}, &FinishReply{})
	}
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:     key,
		Value:   value,
		Version: nrand(),
	}
	reply := PutAppendReply{}

	ok := false
	for !ok {
		ok = ck.server.Call("KVServer."+op, &args, &reply)
	}
	//ck.finish[args.Version] = struct{}{}

	ok = false
	for !ok {
		ok = ck.server.Call("KVServer.Finish", &FinishArgs{args.Version}, &FinishReply{})
	}
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}

/*
func (ck *Clerk) Delete() {
	fmt.Println("Call Delete")
	for version, _ := range ck.finish {
		ok := false
		for !ok {
			ok = ck.server.Call("KVServer.Finish", &FinishArgs{version},
				&FinishReply{})
		}
	}
}

*/
