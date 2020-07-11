package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "reflect"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardState struct {
	maxClientSeq map[int64]int
	database     map[string]string
}

type Op struct {
	Operation string
	Value     interface{}
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid    int64 // my replica group ID
	config shardmaster.Config

	lastApply  int
	shardState map[int]*ShardState
	isRecieved map[int]bool
}

func MakeShardState() *ShardState {
	shardState := &ShardState{}
	shardState.database = make(map[string]string)
	shardState.maxClientSeq = make(map[int64]int)
	return shardState
}

func (kv *ShardKV) Apply(op Op) {

	log.Printf("Apply %v, gid %v, me %v", op, kv.gid, kv.me)
	switch op.Operation {
	case "Get":
		if op.Value != nil {
			args := op.Value.(GetArgs)
			log.Printf("Get %v, %v", args.Key, kv.shardState[args.Shard].database[args.Key])

			if args.Seq > kv.shardState[args.Shard].maxClientSeq[args.ID] {
				kv.shardState[args.Shard].maxClientSeq[args.ID] = args.Seq
			}
		}
	case "Put":
		args := op.Value.(PutAppendArgs)
		stateMachine := kv.shardState[args.Shard]
		stateMachine.database[args.Key] = args.Value

		if args.Seq > kv.shardState[args.Shard].maxClientSeq[args.ID] {
			kv.shardState[args.Shard].maxClientSeq[args.ID] = args.Seq
		}
	case "Append":
		args := op.Value.(PutAppendArgs)
		stateMachine := kv.shardState[args.Shard]

		value, ok := stateMachine.database[args.Key]
		if !ok {
			value = ""
		}
		stateMachine.database[args.Key] = value + args.Value

		log.Printf("After append, %v", kv.shardState[args.Shard].database[args.Key])

		if args.Seq > kv.shardState[args.Shard].maxClientSeq[args.ID] {
			kv.shardState[args.Shard].maxClientSeq[args.ID] = args.Seq
		}
	case "Update":
		args := op.Value.(UpdateArgs)
		stateMachine := kv.shardState[args.Shard]

		kv.isRecieved[args.Shard] = true
		log.Printf("Update Recieved, config num %v, shard %d, gid %d, me %d",
			kv.config.Num, args.Shard, kv.gid, kv.me)
		stateMachine.database = args.Database
		stateMachine.maxClientSeq = args.MaxClientSeq

		if args.Seq > kv.shardState[args.Shard].maxClientSeq[args.ID] {
			kv.shardState[args.Shard].maxClientSeq[args.ID] = args.Seq
		}
	default:
		break
	}
	kv.lastApply++
}

func (kv *ShardKV) Wait(seq int) Op {
	sleepTime := 10 * time.Microsecond
	for {
		decided, value := kv.px.Status(seq)
		if decided == paxos.Decided {
			return value.(Op)
		}
		time.Sleep(sleepTime)
		if sleepTime < 10*time.Second {
			sleepTime *= 2
		}
	}
}

func (kv *ShardKV) Propose(op Op) {
	for seq := kv.lastApply + 1; ; seq++ {
		kv.px.Start(seq, op)
		value := kv.Wait(seq)
		if seq > kv.lastApply {
			kv.Apply(value)
		}
		if reflect.DeepEqual(value, op) {
			break
		}
	}
	kv.px.Done(kv.lastApply)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Num != args.ConfigNum {
		reply.Err = ErrWrongGroup
		return nil
	}
	if args.Seq > kv.shardState[args.Shard].maxClientSeq[args.ID] {
		op := Op{Operation: "Get", Value: *args}
		kv.Propose(op)
	}
	value, ok := kv.shardState[args.Shard].database[args.Key]
	if !ok {
		reply.Err = ErrNoKey
		reply.Value = "NO KEY"
	} else {
		reply.Value = value
		reply.Err = OK
	}
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// log.Printf("config num %v, args %v", kv.config.Num, *args)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Num != args.ConfigNum {
		reply.Err = ErrWrongGroup
		return nil
	}
	if args.Seq <= kv.shardState[args.Shard].maxClientSeq[args.ID] {
		reply.Err = OK
		return nil
	}
	op := Op{Operation: args.Op, Value: *args}
	kv.Propose(op)
	reply.Err = OK
	return nil
}

func (kv *ShardKV) Update(args *UpdateArgs, reply *UpdateReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//log.Printf("args %v", *args)

	if args.Seq <= kv.shardState[args.Shard].maxClientSeq[args.ID] {
		reply.Err = OK
		return nil
	}
	if kv.config.Num != args.ConfigNum {
		reply.Err = ErrWrongGroup
		return nil
	}
	op := Op{Operation: "Update", Value: *args}
	kv.Propose(op)
	reply.Err = OK
	return nil
}

func (kv *ShardKV) Send(shard int, newConfig shardmaster.Config) {

	args := &UpdateArgs{
		Shard:        shard,
		ID:           kv.gid,
		Seq:          kv.config.Num,
		ConfigNum:    kv.config.Num,
		Database:     kv.shardState[shard].database,
		MaxClientSeq: kv.shardState[shard].maxClientSeq}
	reply := UpdateReply{}

	gid := newConfig.Shards[shard]
	servers, ok := newConfig.Groups[gid]
	for {

		if ok {
			for _, srv := range servers {
				log.Printf("Send shard %d to gid %d, srv %v, args %v", shard, gid, srv, args)
				ok := call(srv, "ShardKV.Update", args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					log.Printf("Success: shard %d to gid %d, srv %v, args %v", shard, gid, srv, args)
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					log.Printf("Err group: shard %d to gid %d, srv %v, args %v", shard, gid, srv, args)
					continue
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	newConfig := kv.sm.Query(kv.config.Num + 1)

	if newConfig.Num != kv.config.Num {
		isProducer := false
		isConsumer := false

		for shard := 0; shard < shardmaster.NShards; shard++ {
			if kv.config.Shards[shard] == kv.gid && newConfig.Shards[shard] != kv.gid {
				isProducer = true
			}
			if kv.config.Shards[shard] != 0 &&
				kv.config.Shards[shard] != kv.gid && newConfig.Shards[shard] == kv.gid {
				isConsumer = true
			}
		}
		op := Op{Operation: "Get"}

		if isProducer {
			kv.Propose(op)
			for shard := 0; shard < shardmaster.NShards; shard++ {
				if kv.config.Shards[shard] == kv.gid && newConfig.Shards[shard] != kv.gid {
					kv.Send(shard, newConfig)
				}
			}
		}
		if isConsumer {
			kv.Propose(op)
			allRecieved := true
			for shard := 0; shard < shardmaster.NShards; shard++ {
				if kv.config.Shards[shard] != 0 && kv.config.Shards[shard] != kv.gid && newConfig.Shards[shard] == kv.gid {
					if !kv.isRecieved[shard] {
						allRecieved = false
						log.Printf("gid %v, me %v, shard %v not recieved, Config %v", kv.gid, kv.me, shard, kv.config.Num)
						break
					}
				}
			}
			if !allRecieved {
				return
			}
		}

		log.Printf("gid %d, me %d Config promote %v -> %v", kv.gid, kv.me, kv.config.Num, newConfig.Num)
		for shard := 0; shard < shardmaster.NShards; shard++ {
			if kv.config.Shards[shard] == kv.gid && newConfig.Shards[shard] != kv.gid {
				kv.shardState[shard] = MakeShardState()
			}
		}
		kv.config = newConfig
		kv.isRecieved = make(map[int]bool)
	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})
	gob.Register(PutAppendArgs{})
	gob.Register(UpdateArgs{})
	gob.Register(GetArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.config = shardmaster.Config{Num: 0, Groups: map[int64][]string{}}
	kv.shardState = make(map[int]*ShardState)
	for shard := 0; shard < shardmaster.NShards; shard++ {
		kv.shardState[shard] = MakeShardState()
	}

	// Your initialization code here.
	// Don't call Join().

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}