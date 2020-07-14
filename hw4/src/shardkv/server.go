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
	Database     map[string]string
	// the max seq that ShardKV has seen from this client
	// <clientID, seq number>
	MaxClientSeq map[int64]int
	ProducerGrpConfigNum map[int64]int
}

type Op struct {
	Operation string
	Value     interface{}
}

const (
	Put = "Put"
	Append = "Append"
	Get = "Get"
	Bootstrap = "Bootstrap"
	Reconfigure = "Reconfigure"
	CatchUp = "CatchUp"
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

func (ss *ShardState) Init() {
	ss.Database = make(map[string]string)
	ss.MaxClientSeq = make(map[int64]int)
	ss.ProducerGrpConfigNum = make(map[int64]int)
}
func (ss *ShardState) Bootstrap(other *ShardState) {
	for k, v := range other.Database {
		ss.Database[k] = v
	}
	for k, v := range other.MaxClientSeq {
		ss.MaxClientSeq[k] = v
	}
	for k, v := range other.ProducerGrpConfigNum {
		ss.ProducerGrpConfigNum[k] = v
	}
}
func MakeShardState() *ShardState {
	var shardState ShardState
	shardState.Init()
	return &shardState
}

func (kv *ShardKV) Apply(op Op) {

	//log.Printf("Apply %v, gid %v, me %v", op, kv.gid, kv.me)
	if op.Operation == Get {
		args := op.Value.(GetArgs)
		//log.Printf("Get %v, %v", args.Key, kv.shardState[args.Shard].Database[args.Key])

		if args.Seq > kv.shardState[args.Shard].MaxClientSeq[args.ID] {
			kv.shardState[args.Shard].MaxClientSeq[args.ID] = args.Seq
		}
	} else if op.Operation == Put {
		args := op.Value.(PutAppendArgs)
		stateMachine := kv.shardState[args.Shard]
		stateMachine.Database[args.Key] = args.Value

		//log.Printf("After put, %v", kv.shardState[args.Shard].Database[args.Key])
		if args.Seq > kv.shardState[args.Shard].MaxClientSeq[args.ID] {
			kv.shardState[args.Shard].MaxClientSeq[args.ID] = args.Seq
		}
	} else if op.Operation == Append {
		args := op.Value.(PutAppendArgs)
		stateMachine := kv.shardState[args.Shard]
		value, ok := stateMachine.Database[args.Key]
		if !ok {
			value = ""
		}
		stateMachine.Database[args.Key] = value + args.Value

		//log.Printf("After append, %v", kv.shardState[args.Shard].Database[args.Key])
		if args.Seq > kv.shardState[args.Shard].MaxClientSeq[args.ID] {
			kv.shardState[args.Shard].MaxClientSeq[args.ID] = args.Seq
		}
	} else if op.Operation == Bootstrap {
		reply := op.Value.(BootstrapReply)
		stateMachine := kv.shardState[reply.Shard]

		//log.Printf("Bootstrap Received, config num %v, shard %d, gid %d, me %d, db %v",
		//	kv.config.Num, reply.Shard, kv.gid, kv.me, reply.ShardState.Database)
		stateMachine.Bootstrap(&reply.ShardState)

		if reply.ConfigNum > kv.shardState[reply.Shard].ProducerGrpConfigNum[reply.ProducerGID] {
			kv.shardState[reply.Shard].ProducerGrpConfigNum[reply.ProducerGID] = reply.ConfigNum
		}
	} else if op.Operation == CatchUp {
		// do nothing
	} else if op.Operation == Reconfigure {
		// do nothing
		args := op.Value.(ReconfigureArgs)
		kv.config = kv.sm.Query(args.NewConfigNum)
	}
}

func (kv *ShardKV) Wait(seq int) (Op, error) {
	sleepTime := 10 * time.Millisecond
	for iters := 0; iters < 15; iters++ {
		decided, value := kv.px.Status(seq)
		if decided == paxos.Decided {
			return value.(Op), nil
		} else if decided == paxos.Forgotten {
			// error
			//log.Printf("Forgotten %v", value)
			return Op{}, errors.New("ShardKV: Forgotten")
		}
		time.Sleep(sleepTime)
		if sleepTime < 10 * time.Second {
			sleepTime *= 2
		}
	}
	return Op{}, errors.New("ShardKV: Wait for too long")
}

func (kv *ShardKV) Propose(xop Op) error {
	for  {
		kv.px.Start(kv.lastApply + 1, xop)
		op, err := kv.Wait(kv.lastApply + 1)
		if err != nil {
			return err
		}
		kv.Apply(op)
		kv.lastApply += 1
		if reflect.DeepEqual(op, xop) {
			break
		}
		kv.px.Done(kv.lastApply)
	}
	kv.px.Done(kv.lastApply)
	return nil
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {

	kv.mu.Lock()
	defer kv.mu.Unlock()
	// if client's config num is not equal to mine -> it means it is sending the wrong group (old one)
	if kv.config.Num != args.ConfigNum {
		reply.Err = ErrWrongGroup
		return nil
	}

	if args.Seq <= kv.shardState[args.Shard].MaxClientSeq[args.ID] {
		reply.Err = OK
		reply.Value = kv.shardState[args.Shard].Database[args.Key]
		return nil
	}

	op := Op{Operation: "Get", Value: *args}
	err := kv.Propose(op)
	if err != nil {
		return err
	}
	value, ok := kv.shardState[args.Shard].Database[args.Key]
	if !ok {
		reply.Err = ErrNoKey
		reply.Value = ""
	} else {
		reply.Value = value
		reply.Err = OK
	}
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.config.Num != args.ConfigNum {
		reply.Err = ErrWrongGroup
		return nil
	}

	if args.Seq <= kv.shardState[args.Shard].MaxClientSeq[args.ID] {
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

func (kv *ShardKV) Bootstrap(args *BootstrapArgs, reply *BootstrapReply) error {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// it must be `<=` but not `<` so that we can ensure no future GET/PUT/APPEND will lost
	if kv.config.Num <= args.ConfigNum {
		reply.Err = ErrNotReady
		//log.Printf("ErrNotReady, Producer ConfigNum %v, Consumer ConfigNum %v, kv.gid %d, me %d",
		//	kv.config.Num, args.ConfigNum, kv.gid, kv.me)
		return nil
	}

	// We don't need this proposal! since that we will check config Num if it is the latest one.
	// and when we were doing that, we also do Reconfigure Proposal -> so it must be the latest version
	//op := Op{Operation: "Reconfigure"}
	//kv.Propose(op)

	reply.ShardState.Init()
	for k, v := range kv.shardState[args.Shard].Database {
		reply.ShardState.Database[k] = v
	}
	for k, v := range kv.shardState[args.Shard].MaxClientSeq {
		reply.ShardState.MaxClientSeq[k] = v
	}
	for k, v := range kv.shardState[args.Shard].ProducerGrpConfigNum {
		reply.ShardState.ProducerGrpConfigNum[k] = v
	}
	reply.Err = OK
	return nil
}

func (kv *ShardKV) Migrate(shard int) (bool, *BootstrapReply) {

	gid := kv.config.Shards[shard]
	servers, ok := kv.config.Groups[gid]
	if !ok {
		return false, nil
	}
	if kv.shardState[shard].ProducerGrpConfigNum[gid] >= kv.config.Num {
		// Repeated Bootstrap Received, config num 6, shard 9, kv.gid 102, gid 100, me 1
		// i.e., kv.shardState[shard].MaxClientSeq[gid], gid=100,
		// is also = 6 but not 5 or less anymore
		// (since this happened before) Bootstrap Received, config num 6, shard 9, gid 102, me 1

		//log.Printf("Dismissed: Migrate, config num %v, shard %d, kv.gid %d, gid %d, me %d",
		//	kv.config.Num, shard, kv.gid, gid, kv.me)
		return true, nil
	}
	args := &BootstrapArgs{
		Shard:     shard,
		ConfigNum: kv.config.Num}

	done0 := make(chan bool)
	var reply BootstrapReply
	go func (args *BootstrapArgs, reply *BootstrapReply, gid int64, servers []string) {
		for _, srv := range servers {
			reply.Shard = args.Shard
			newState := MakeShardState()
			ok := call(srv, "ShardKV.Bootstrap", args, &reply)
			if ok && (reply.Err == OK || reply.Err == ErrNoKey) { // exclude ErrNotReady -> this is an error
				newState.Bootstrap(&reply.ShardState)
				reply.ShardState = *newState
				reply.ProducerGID = gid
				reply.ConfigNum = args.ConfigNum
				done0 <- true
				return
			}
		}
	}(args, &reply, gid, servers)
	select {
	case <-done0:
		return true, &reply
	case <-time.After(1 * time.Second):
		//log.Printf("[timeout] deadlock! %v, %v", kv.me, gid)
		return false, nil
	}
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	//log.Printf("tick: config num %v, kv.gid %d, me %d",
	//	kv.config.Num, kv.gid, kv.me)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//log.Printf("tick lock acquired: config num %v, kv.gid %d, me %d",
	//	kv.config.Num, kv.gid, kv.me)

	newConfig := kv.sm.Query(kv.config.Num + 1)

	if newConfig.Num != kv.config.Num {
		op := Op{Operation: "CatchUp"}
		var isCustomer = false
		for shard := 0; shard < shardmaster.NShards; shard++ {
			if kv.config.Shards[shard] != 0 && kv.config.Shards[shard] != kv.gid &&
				newConfig.Shards[shard] == kv.gid {
				isCustomer = true
			}
		}
		if isCustomer {
			kv.Propose(op)
			for shard := 0; shard < shardmaster.NShards; shard++ {
				if kv.config.Shards[shard] != 0 && kv.config.Shards[shard] != kv.gid &&
					newConfig.Shards[shard] == kv.gid {
					// this ShardKV is the consumer -> ask the producer to migrate its latest ShardState
					ok, reply := kv.Migrate(shard)
					if !ok {
						waitTime := 5 * (( kv.gid - 100) + 2) // 5 * ((100 - 100) + 2)
						time.Sleep(time.Millisecond * time.Duration(waitTime))
						return
					} else {
						if reply != nil {
							op := Op{Operation: "Bootstrap", Value: *reply}
							kv.Propose(op)
						} // else : dismissed one -> still need to reconfigure later
					}
				}
			}
		}
		xop := Op{Operation: "Reconfigure", Value: ReconfigureArgs{NewConfigNum: kv.config.Num + 1}}
		kv.Propose(xop)
		// we do reconfiguration in paxos instead of replacing it directly
		// either way should work fine
		// kv.config = newConfig
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
	gob.Register(BootstrapReply{})
	gob.Register(ReconfigureArgs{})

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