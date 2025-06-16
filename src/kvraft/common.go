package kvraft
import "log"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrOldRequest = "ErrOldRequest"     // commandid < maxcommandid
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64 // unique client id
	CommandId int64 // sequence number for operations
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64 // unique client id
	CommandId int64 // sequence number for operations
}

type GetReply struct {
	Err   Err
	Value string
}


const Debug = true


//[client/server][OP]: description, args/reply
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (e Err) String() string {
	switch e {
	case OK:
		return "OK"
	case ErrNoKey:
		return "ErrNoKey"
	case ErrWrongLeader:
		return "ErrWrongLeader"
	case ErrTimeout:
		return "ErrTimeout"
	case ErrOldRequest:
		return "ErrOldRequest"
	default:
		return "Unknown Error"
	}
}

