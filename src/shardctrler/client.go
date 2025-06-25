package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "time"
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId  int64 // unique client id
	commandId int64 // sequence number for operations
	leaderId  int // current leader id
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
	// Your code here.
	ck.clientId = nrand() // generate a unique client id
	ck.commandId = 0      // initialize command id to 0
	ck.leaderId = 0       // initialize leader id to 0 

	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
		Num:       num,
	}
	ck.commandId++ 
	// Your code here.
	//args.Num = num
	for {
		var reply QueryReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, &reply)
		if !ok || reply.Err == ErrTimeout || reply.WrongLeader {
			// If the call fails or the leader is wrong, retry.
			DPrintf5A("Query, maybe !ok | timeout | wrongleader, clientId: %v, commandId: %v, leaderId: %v, ok: %v, reply.Err: %v, reply.WrongLeader: %v", 
			args.ClientId, args.CommandId, ck.leaderId, ok, reply.Err, reply.WrongLeader)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		} else if ok && reply.WrongLeader == false {
			return reply.Config
		}
		time.Sleep(100 * time.Millisecond) 
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		Servers:   servers,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	ck.commandId++ // increment command id for the next operation
	for {
		var reply JoinReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, &reply)
		if !ok || reply.Err == ErrTimeout || reply.WrongLeader {
			// If the call fails or the leader is wrong, retry.
			DPrintf5A("Join, maybe !ok | timeout | wrongleader, clientId: %v, commandId: %v, leaderId: %v, ok: %v, reply.Err: %v, reply.WrongLeader: %v",
			args.ClientId, args.CommandId, ck.leaderId, ok, reply.Err, reply.WrongLeader)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		} else if ok && reply.WrongLeader == false {
			return
		}
		time.Sleep(100 * time.Millisecond) 
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
		GIDs:      gids,
	}
	// Your code here.
	//args.GIDs = gids

	ck.commandId++ // increment command id for the next operation
	for {
		var reply LeaveReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, &reply)
		if !ok || reply.Err == ErrTimeout || reply.WrongLeader {
			// If the call fails or the leader is wrong, retry.
			DPrintf5A("Leave, maybe !ok | timeout | wrongleader, clientId: %v, commandId: %v, leaderId: %v, ok: %v, reply.Err: %v, reply.WrongLeader: %v", 
			args.ClientId, args.CommandId, ck.leaderId, ok, reply.Err, reply.WrongLeader)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		} else if ok && reply.WrongLeader == false {
			return
		}
		time.Sleep(100 * time.Millisecond) 
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		Shard:     shard,
		GID:       gid,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	ck.commandId++ // increment command id for the next operation
	for {
		var reply MoveReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, &reply)
		if !ok || reply.Err == ErrTimeout || reply.WrongLeader {
			// If the call fails or the leader is wrong, retry.
			DPrintf5A("Move, maybe !ok | timeout | wrongleader, clientId: %v, commandId: %v, leaderId: %v, ok: %v, reply.Err: %v, reply.WrongLeader: %v", 
			args.ClientId, args.CommandId, ck.leaderId, ok, reply.Err, reply.WrongLeader)

			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		} else if ok && reply.WrongLeader == false {
			return
		}
		time.Sleep(100 * time.Millisecond) 
	}
}
