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


const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	OpID      int64
	// Put, Get, Append
	Operation string
	Key       string
	Value     string
}


type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid        int64 // my replica group ID

	// Your definitions here.
	config     shardmaster.Config // cache config
	database   sync.Map
	hashVals   sync.Map
	seq        int
}

func (kv *ShardKV) SyncUp(xop Op) {

	to := 10 * time.Millisecond
	doing := false
	for {
		status, op := kv.px.Status(kv.seq)

		if status == paxos.Decided {

			op := op.(Op)
			if xop.OpID == op.OpID {
				break
			} else if op.Operation == "Put" || op.Operation == "Append" {
				kv.doPutAppend(op.Operation, op.Key, op.Value, op.OpID)
			}
			kv.seq += 1
			doing = false
		} else {
			if !doing {

				kv.px.Start(kv.seq, xop)
				doing = true
			}
			time.Sleep(to)

			to += 10 * time.Millisecond
		}
	}

	kv.px.Done(kv.seq)
	kv.seq += 1
}

// added by Adrian
func (kv *ShardKV) doGet(Key string) (string, bool) {
	val, ok := kv.database.Load(Key)
	if !ok {
		return "", false
	} else {
		return val.(string), true
	}
}
// added by Adrian
func (kv *ShardKV) doPutAppend(Operation string, Key string, Value string, hash int64) {

	val, ok := kv.database.Load(Key)
	if !ok { // if not exists
		kv.database.Store(Key, Value)
	} else { // load
		if Operation == "Put" {
			kv.database.Store(Key, Value)
		} else if Operation == "Append" {
			vals := val.(string)
			_, ok := kv.hashVals.Load(hash)
			if !ok {
				kv.database.Store(Key, vals+Value)
			}
		}
	}
	kv.hashVals.Store(hash, 1)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{args.Hash, "Get", args.Key, ""}
	kv.SyncUp(op)
	reply.Value, _ = kv.doGet(args.Key)
	reply.Err = OK
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{args.Hash, args.Op, args.Key, args.Value}
	kv.SyncUp(op)
	kv.doPutAppend(args.Op, args.Key, args.Value, op.OpID)
	reply.Err = OK
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {

	config := kv.sm.Query(-1)
	for i, v := range config.Shards {
		if v != kv.config.Shards[i] {
			//DPrintf("new config %v replaced, %v", config.Num, config.Shards)
			kv.config = config
			break
		}
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

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	kv.config = kv.sm.Query(-1)

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
