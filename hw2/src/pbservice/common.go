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
	Op string
	HashVal int64
	Me string
	// Field names must start with capital letters,
	// otherwise RPC will break.

}

type PutAppendReply struct {
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
type RsyncArgs struct {
	Database map[string]string
	HashVals map[int64]bool
}

type RsyncReply struct {
	Err   Err
}