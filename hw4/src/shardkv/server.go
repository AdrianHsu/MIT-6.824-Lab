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
	// <key, value> pair in this particular shard (range from shard 0 ~ 9)
	Database     map[string]string
	// the max seq that this ShardKV has seen from this particualr client
	// <clientID, seq number>
	// to ensure that no repetitive client request is processed more than once
	MaxClientSeq map[int64]int
	// The latest ConfigNum from the producer group (it could be either one out of 3 ShardKV)
	// We need to record this as when we are doing migration & bootstrap
	// It's important for the Consumer to check whether this Producer
	// has sent me its ShardState or not. If yes, then we should not Bootstrap once again.
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
	Bootstrap = "Bootstrap" // replace the ShardState for the `i`th shard
	Reconfigure = "Reconfigure" // change kv.config
	CatchUp = "CatchUp" // nothing happens
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
	// [10] shards, each with a separate shardState
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

// Hint: Don't directly update the stored key/value database in the Put/Append/Get
// handlers; instead, attempt to append a Put, Append, or Get operation to the
// Paxos log, and then call your log-reading function to find out what happened
// Apply() is the log-reading function. It will find out what happened
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

		// Hint: perhaps a reconfiguration was entered in the log just before the Put/Append/Get
		// and that is why we need this log-reading function Apply() instead of
		// applying these Put/Append/Get directly.
		args := op.Value.(ReconfigureArgs)
		kv.config = kv.sm.Query(args.NewConfigNum)
	}
}

func (kv *ShardKV) Wait(seq int) (Op, error) {
	sleepTime := 10 * time.Millisecond
	for iters := 0; iters < 15; iters++ {
		// You are allowed to assume that a majority of servers in each Paxos
		// replica group are alive and can talk to each other
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
// Hint: You should have a function `Propose()` whose job is to (1) examine recent entries
// in the Paxos log and (2) apply them to the state of the shardkv server.
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
		// You must call px.Done() to let Paxos free old parts of its log.
		kv.px.Done(kv.lastApply)
	}
	kv.px.Done(kv.lastApply)
	return nil
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {

	kv.mu.Lock()
	defer kv.mu.Unlock()
	// if client's config num is not equal to mine -> it means it is sending the wrong group (old one)

	// When a server realized ErrWrongGroup, we don't need to update maxClientSeq
	if kv.config.Num != args.ConfigNum || kv.gid != kv.config.Shards[key2shard(args.Key)] {
		reply.Err = ErrWrongGroup
		return nil
	}

	if args.Seq <= kv.shardState[args.Shard].MaxClientSeq[args.ID] {
		reply.Err = OK
		reply.Value = kv.shardState[args.Shard].Database[args.Key]
		return nil
	}
	// A Clerk.Get() should see the value written by the most recent Put/Append to the same key.
	// This will get tricky when Gets or Puts arrive at about the same time as configuration changes.
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
// Hint: Don't directly update the stored key/value database in the Put/Append/Get
// handlers; instead, attempt to append a Put, Append, or Get operation to the
// Paxos log
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Your server should respond with an ErrWrongGroup error to a client RPC with a key
	// that the server isn't responsible for
	// (i.e. for a key whose shard is not assigned to the server's group).
	// Either way are fine. But the `kv.config.Num != args.ConfigNum` can 100% ensure that
	// client is sending the right group
	if kv.config.Num != args.ConfigNum || kv.gid != kv.config.Shards[key2shard(args.Key)] {
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
// Hint: During re-configuration, replica groups will have to send each other the keys and values for some shards.
func (kv *ShardKV) Bootstrap(args *BootstrapArgs, reply *BootstrapReply) error {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Hint: Make sure your Get/Put/Append handlers make this decision correctly
	// in the face of a concurrent re-configuration.
	// it must be `<=` but not `<` so that we can ensure no future GET/PUT/APPEND will lost
	// Why? let's say configNum = 3 is like Shards=[100,100,100,100,100,101,101,101,101,101]
	// And now we are gonna perform leave(101)...
	// so in configNum = 4, shards will be [100,100,100,100,100,100,100,100,100,100]
	// That is: group gid=101 will be the "Producer" and gid=100 will be the "Consumer"
	// and the Consumer will send an `Bootstrap` RPC to the Producer (i.e., pulling their ShardState)
	// so that Producer can put is shardState into the reply and send it back
	// Therefore, when consumer's args.ConfigNum = 3 (as it should be)
	// and also producer's kv.config.Num is also = 3. That means what?
	// It means that the producer (gid=100) is still possible to aceept some Put/Get/Append for shard 5~9
	// Which is definitely wrong! We want our producer will NOT propose any ops after it replies to the customers
	// And the solution is: The consumer should just WAIT FOR the producer itself to realize that
	// the config has changed, and then it will realize that it is not responsible for shard 5~9 anymore
	// So that no future ops will be accepted by this producers (gid=100)
	// And thus, the customer can now do the Bootstrap with ease.

	// Response to migration: since groups are join / leave one by one,
	// the same group will not move in and out of the SAME shards at the same time,
	// that is, the groups migrating in and out of shards will NOT intersect.
	// e.g., see the deadlock example figure
	// Therefore, when G1 is in CFG2, G1 is no longer responsible for S1,
	// and can move out of S1 at any time in response to migration.
	// That is: when G1 cfg.num > G2 cfg.num -> can finally respond to migration
	// i.e., G1 cfg.num <= G2 cfg.num -> should return ErrNotReady
	// otherwise you must wait for G1 update to complete
	// (to avoid deadlock in mutual requests).

	if kv.config.Num <= args.ConfigNum {
		reply.Err = ErrNotReady
		// log.Printf("ErrNotReady, Producer ConfigNum %v, Consumer ConfigNum %v, kv.gid %d, me %d",
		//	kv.config.Num, args.ConfigNum, kv.gid, kv.me)
		return nil
	}

	// We don't need this proposal! since that we will check config Num if it is the latest one.
	// and when we were doing that, we also do Reconfigure Proposal -> so it must be the latest version
	// op := Op{Operation: "Reconfigure"}
	// kv.Propose(op)

	reply.ShardState.Init()
	for k, v := range kv.shardState[args.Shard].Database {
		reply.ShardState.Database[k] = v
	}
	// Be careful about implementing at-most-once semantic for RPC.
	// When a server sends shards to another, the server needs to send the clients state as well.
	// Think about how the receiver of the shards should update its own clients state.
	// Is it ok for the receiver to replace its clients state with the received one? YES!
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
		for {
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
		}
	}(args, &reply, gid, servers)
	select {
	case <-done0:
		return true, &reply
	case <-time.After(5000 * time.Millisecond):
		log.Printf("Timeout: deadlock! %v, %v", kv.me, gid)
		return false, nil
	}
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	// Hint: Your server will need to periodically check with the shardmaster
	// to see if there's a new configuration; do this in tick().

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// I always query the config Num one by one (e.g., config.Num = 5 -> 6 -> 7 -> 8...)
	// so that no config will be missing. and thus, no migration will be lost.
	newConfig := kv.sm.Query(kv.config.Num + 1)
	// A single instance of the shardmaster service will assign shards to replica groups.
	// When this assignment changes, replica groups will have to hand off shards to each other.
	if newConfig.Num != kv.config.Num {
		op := Op{Operation: "CatchUp"}
		var isCustomer = false
		for shard := 0; shard < shardmaster.NShards; shard++ {
			if kv.config.Shards[shard] != 0 && kv.config.Shards[shard] != kv.gid &&
				newConfig.Shards[shard] == kv.gid {

				// Example: configuration is from cfg1 to CFG2, and shard S1 is from G1 to G2
				// How to migrate - push or pull? should G1 push S1 or G2 pull S1?
				// Ans. G2 should pull when doing re-configuration.
				isCustomer = true
			}
		}
		if isCustomer {
			kv.Propose(op)
			ops := make([]Op, 10)
			for shard := 0; shard < shardmaster.NShards; shard++ {
				if kv.config.Shards[shard] != 0 && kv.config.Shards[shard] != kv.gid &&
					newConfig.Shards[shard] == kv.gid {
					// this ShardKV is the consumer -> ask the producer to migrate its latest ShardState
					ok, reply := kv.Migrate(shard)
					if !ok {
						waitTime := 100 * ((kv.gid - 100) + 2) // 100 * ((100 - 100) + 2)
						time.Sleep(time.Millisecond * time.Duration(waitTime))
						return
					} else {
						if reply != nil {
							op := Op{Operation: "Bootstrap", Value: *reply}
							ops = append(ops, op)

							// we should not propose here!
							//kv.Propose(op)
						} // else : dismissed one -> still need to reconfigure later
					}
				}
			}
			// only when ALL shards are migrated successfully will we REALLY put them into Paxos
			// otherwise, we will want to re-do tick() and try again.
			// If we did'nt do that and instead we perform Propose() in each shard separately
			// that may cause some shards are successfully bootstrapped but some are not.
			// that is why we push back all the ops and perform them in batch.
			// this is inspired by the idea of `allReceived` -> we want to ensure all bootstrap are received
			for _, op := range ops {
				kv.Propose(op)
			}
		}
		// we do reconfiguration in paxos instead of replacing it directly!
		// kv.config = newConfig
		xop := Op{Operation: "Reconfigure", Value: ReconfigureArgs{NewConfigNum: kv.config.Num + 1}}
		kv.Propose(xop)
		// Hint: After a server has moved to a new view, it can leave the shards that it is
		// not owning in the new view undeleted (i.e, the producer will remain the same)
		// This will simplify the server implementation.
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