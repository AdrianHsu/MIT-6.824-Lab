package shardkv

import (
	"errors"
	"net"
)
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
	// <key, value> pair in this particular shard
	database     map[string]string
	// the max seq that ShardKV has seen from this client
	// <clientID, seq number>
	maxClientSeq map[int64]int
}

type Op struct {
	Operation string
	Value     interface{}
}

const (
	Put = "Put"
	Append = "Append"
	Get = "Get"
	Update = "Update"
	Reconfigure = "Reconfigure"
)

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid    int64 // my replica group ID
	config shardmaster.Config // my latest config

	lastApply  int // same idea as shardMaster.lastApply
	shardState [shardmaster.NShards]*ShardState
}

func MakeShardState() *ShardState {
	shardState := &ShardState{}
	shardState.database = make(map[string]string)
	shardState.maxClientSeq = make(map[int64]int)
	return shardState
}

func (kv *ShardKV) Apply(op Op) {

	log.Printf("Apply %v, gid %v, me %v", op, kv.gid, kv.me)
	if op.Operation == Get {
		args := op.Value.(GetArgs)
		log.Printf("Get %v, %v", args.Key, kv.shardState[args.Shard].database[args.Key])

		if args.Seq > kv.shardState[args.Shard].maxClientSeq[args.ID] {
			kv.shardState[args.Shard].maxClientSeq[args.ID] = args.Seq
		}
	} else if op.Operation == Put {
		args := op.Value.(PutAppendArgs)
		stateMachine := kv.shardState[args.Shard]
		stateMachine.database[args.Key] = args.Value

		if args.Seq > kv.shardState[args.Shard].maxClientSeq[args.ID] {
			kv.shardState[args.Shard].maxClientSeq[args.ID] = args.Seq
		}
	} else if op.Operation == Append {
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
	} else if op.Operation == Update {
		reply := op.Value.(UpdateReply)
		stateMachine := kv.shardState[reply.Shard]

		log.Printf("Update Received, config num %v, shard %d, gid %d, me %d, db %v",
			kv.config.Num, reply.Shard, kv.gid, kv.me, reply.Database)
		stateMachine.database = reply.Database
		stateMachine.maxClientSeq = reply.MaxClientSeq

		if reply.Seq > kv.shardState[reply.Shard].maxClientSeq[reply.ID] {
			kv.shardState[reply.Shard].maxClientSeq[reply.ID] = reply.Seq
		}
	} else if op.Operation == Reconfigure {
		// do nothing
	}
}

func (kv *ShardKV) Wait(seq int) (Op, error) {
	sleepTime := 10 * time.Microsecond
	for iters := 0; iters < 15; iters++ {
		decided, value := kv.px.Status(seq)
		if decided == paxos.Decided {
			return value.(Op), nil
		}
		time.Sleep(sleepTime)
		if sleepTime < 10 * time.Second {
			sleepTime *= 2
		}
	}
	return Op{}, errors.New("Wait for too long")

}

func (kv *ShardKV) Propose(xop Op) error {
	for seq := kv.lastApply + 1; ; seq++ {
		kv.px.Start(seq, xop)
		op, err := kv.Wait(seq)
		if err != nil {
			return err
		}
		kv.Apply(op)
		kv.lastApply += 1
		if reflect.DeepEqual(op, xop) {
			break
		}
	}
	kv.px.Done(kv.lastApply)
	return nil
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
		err := kv.Propose(op)
		if err != nil {
			return err
		}
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
	err := kv.Propose(op)
	if err != nil {
		return err
	}
	reply.Err = OK
	return nil
}

func (kv *ShardKV) Update(args *UpdateArgs, reply *UpdateReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Seq <= kv.shardState[args.Shard].maxClientSeq[args.ID] {
		reply.Err = OK
		return nil
	}
	if kv.config.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return nil
	}

	reply.Database = make(map[string]string)
	reply.MaxClientSeq = make(map[int64]int)
	for k, v := range kv.shardState[args.Shard].database {
		reply.Database[k] = v
	}
	for k, v := range kv.shardState[args.Shard].maxClientSeq {
		reply.MaxClientSeq[k] = v
	}
	reply.ID = args.ID
	reply.Shard = args.Shard
	reply.Seq = args.Seq

	reply.Err = OK
	return nil
}

func (kv *ShardKV) Send(shard int) {

	args := &UpdateArgs{
		Shard:        shard,
		ID:           kv.gid,
		Seq:          kv.config.Num,
		ConfigNum:    kv.config.Num}

	gid := kv.config.Shards[shard]
	servers, ok := kv.config.Groups[gid]
	for {
		if ok {
			for _, srv := range servers {
				var reply UpdateReply
				ok := call(srv, "ShardKV.Update", args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					op := Op{Operation: "Update", Value: reply}
					kv.Propose(op)
					return
				}
				if ok && reply.Err == ErrNotReady {
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
		isConsumer := false
		isProducer := false
		for shard := 0; shard < shardmaster.NShards; shard++ {
			if kv.config.Shards[shard] == kv.gid && newConfig.Shards[shard] != kv.gid {
				isProducer = true
			}
			if kv.config.Shards[shard] != 0 &&
				kv.config.Shards[shard] != kv.gid && newConfig.Shards[shard] == kv.gid {
				isConsumer = true
			}
		}
		op := Op{Operation: "Reconfigure"}
		if isProducer {
			kv.Propose(op)
		} else if isConsumer {
			kv.Propose(op)
			for shard := 0; shard < shardmaster.NShards; shard++ {
				if kv.config.Shards[shard] != 0 && kv.config.Shards[shard] != kv.gid && newConfig.Shards[shard] == kv.gid {
					kv.Send(shard)
				}
			}
		}

		kv.config = newConfig
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
	gob.Register(GetArgs{})
	gob.Register(UpdateReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.config = shardmaster.Config{Num: 0, Groups: map[int64][]string{}}
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