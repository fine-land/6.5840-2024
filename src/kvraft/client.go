package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "time"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId    int64 // unique client id
	commandId   int64 // sequence number for operations
	leaderId    int // current leader id
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand() // generate a unique client id
	ck.commandId = 0      // initialize command id to 0
	ck.leaderId = 0 // initialize leader id to 0 (no leader known yet)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	ck.commandId++ // increment command id for the next operation	
	reply := GetReply{}
	for {
		DPrintf("[client][Get], try get, key: %v, clientId: %v, commandId: %v, leaderId: %v", 
		key, ck.clientId, ck.commandId, ck.leaderId)

		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			// If the call fails or the leader is wrong, try the next server.
			ck.leaderId = (ck.leaderId + 1) % (len(ck.servers))
			DPrintf("[client][Get], err--!ok | WrongLeader | Timeout, ok: %v, clientId: %v, commandId: %v,reply.Err: %v", 
	ok, args.ClientId, args.CommandId, reply.Err)
			continue
		} else if reply.Err == ErrNoKey {
			// If the key does not exist, return an empty string.
			DPrintf("[client][Get], NoKey ,key: %v, clientId: %v, commandId: %v,",
			 args.Key, args.ClientId, args.CommandId)
			return ""
		} else if reply.Err == OK {
			// If the operation is successful, return the value.
			DPrintf("[client][Get], OK ,key: %v, clientId: %v, commandId: %v,",
			 args.Key, args.ClientId, args.CommandId)
			return reply.Value
		} else if reply.Err == ErrOldRequest {
			//do nothing
			DPrintf("[client][Get], OldRequest ,key: %v, clientId: %v, commandId: %v,",
			 args.Key, args.ClientId, args.CommandId)
		} else {
			//other fail
			// ck.leaderId = (ck.leaderId + 1) % (len(ck.servers))
		}
		time.Sleep(100 * time.Millisecond) // wait before retrying
	}
	DPrintf("unreachableGet: %v", reply.Err)
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,	
	}
	ck.commandId++ // increment command id for the next operation
	reply := PutAppendReply{}
	for {
		DPrintf("[client][%v], try, key: %v, value: %v, clientId: %v, commandId: %v, leaderId: %v", 
		op, args.Key, args.Value, args.ClientId, args.CommandId, ck.leaderId)
		ok := ck.servers[ck.leaderId].Call("KVServer."+op, &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			// If the call fails or the leader is wrong, try the next server.
			DPrintf("[client][%v], !ok | WrongLeader | Timeout, key: %v, value: %v, clientId: %v, commandId: %v, leaderId: %v, ok: %v, Err: %v", 
		op, args.Key, args.Value, args.ClientId, args.CommandId, ck.leaderId, ok, reply.Err)
			ck.leaderId = (ck.leaderId + 1) % (len(ck.servers))
			continue
		} else if reply.Err == OK {
			// If the operation is successful, return.
			DPrintf("[client][%v], OK, clientId: %v, commandId: %v", op, args.ClientId, args.CommandId)
			return
		} else if reply.Err == ErrOldRequest {
			//do nothing
			DPrintf("[client][%v], OldRequest, clientId: %v, commandId: %v", op, args.ClientId, args.CommandId)
		} else {
			//other fail
			// ck.leaderId = (ck.leaderId + 1) % (len(ck.servers))
		}
		time.Sleep(100 * time.Millisecond) // wait before retrying
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
