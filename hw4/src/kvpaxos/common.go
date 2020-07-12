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
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID  int64
	Seq       int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key  string
	Op   string
	// You'll have to add definitions here.
	ClientID  int64
	Seq       int
}

type GetReply struct {
	Err   Err
	Value string
}
