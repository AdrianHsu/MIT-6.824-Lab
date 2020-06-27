package kvpaxos

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	Hash  int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
	FailSrv int
}

type GetArgs struct {
	Key  string
	Hash int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
	FailSrv int
}
