package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op string
	HashVal int64
}

type PutAppendReply struct {
	Err Err
}

// Put or Append
type PutAppendSyncArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op string
	HashVal int64
	Primary string

}

type PutAppendSyncReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
type BootstrapArgs struct {
	Database map[string]string
	HashVals map[int64]bool
}

type BootstrapReply struct {
	Err   Err
}