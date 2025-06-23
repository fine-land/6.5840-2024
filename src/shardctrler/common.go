package shardctrler

//
// Shard controller: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
/*
	主要的配置信息；
	标识该配置的版本号
	shards到group的映射，即每个group管理哪些shards； shards的下标就是shardId
	groups到servers的映射，即每个group对应的服务器列表；即一组raft服务器集群
*/
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK = "OK"
	// ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrOldRequest  = "ErrOldRequest" 
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings

	ClientId int64 // for deduplication
	CommandId int64 // for deduplication
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int

	ClientId int64 // for deduplication
	CommandId int64 // for deduplication
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int

	ClientId int64 // for deduplication
	CommandId int64 // for deduplication
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number

	ClientId int64 // for deduplication
	CommandId int64 // for deduplication
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}


const Debug5A = false

func DPrintf5A(format string, a ...interface{}) {
	if Debug5A {
		log.Printf(format, a...)
	}
}


func (e Err) String() string {
	switch e {
	case OK:
		return "OK"
	case ErrTimeout:
		return "ErrTimeout"
	default:
		return "Unknown Error"
	}
}