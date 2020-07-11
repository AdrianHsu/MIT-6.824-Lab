package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

type Err string

type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append"

	ID        int64 // client ID (each client has an unique id)
	// client's seq. everytime he performs put/get/append
	// his seq will += 1
	Seq       int
	ConfigNum int // Number in the clients' config
	Shard     int // from 0 ~ 9. the index of shards
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string

	ID        int64
	Seq       int
	ConfigNum int
	Shard     int
}

type GetReply struct {
	Err   Err
	Value string
}

type UpdateArgs struct {
	Database     map[string]string
	MaxClientSeq map[int64]int

	ID           int64
	Seq          int
	Shard        int
	ConfigNum    int
}

type UpdateReply struct {
	Err string
}