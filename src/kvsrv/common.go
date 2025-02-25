package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Version int64 //UniqueVersionNumber
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Version int64 //UniqueVersionNumber
}

type GetReply struct {
	Value string
}

type FinishArgs struct {
	Version int64
}

type FinishReply struct {
}
