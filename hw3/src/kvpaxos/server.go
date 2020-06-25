package kvpaxos

import (
	"net"
	"time"
)
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"


const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpID      int64
	Operation string
	Key       string
	Value     string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	database   sync.Map
	seq        int
}

func (kv *KVPaxos) SyncUp(xop Op) {
	to := 10 * time.Millisecond
	for {
		status, op := kv.px.Status(kv.seq)
		if status == paxos.Decided {
			op := op.(Op)
			if xop.OpID == op.OpID {
				break
			} else if op.Operation == "Put" || op.Operation == "Append" {
				kv.doPutAppend(op.Operation, op.Key, op.Value)

			} else {
				//value, _ := kv.doGet(op.Key)
				//DPrintf("get: %v", value)
			}
			kv.seq += 1
		} else {
			kv.px.Start(kv.seq, xop)
		}
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
	kv.seq += 1
}

func (kv *KVPaxos) doGet(Key string) (string, bool) {
	val, ok := kv.database.Load(Key)
	if !ok {
		return "", false
	} else {
		return val.(string), true
	}
}

func (kv *KVPaxos) doPutAppend(Operation string, Key string, Value string) {
	val, ok := kv.database.LoadOrStore(Key, Value)
	if !ok { // store
		// init
	} else { // load
		if Operation == "Put" {
			kv.database.Store(Key, Value)
		} else if Operation == "Append" {
			vals := val.(string)
			kv.database.Store(Key, vals + Value)
		}
	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	op := Op{args.Hash, "Get", args.Key, ""}
	kv.SyncUp(op)
	reply.Value, _ = kv.doGet(args.Key)
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	op := Op{args.Hash, args.Op, args.Key, args.Value}

	kv.SyncUp(op)
	kv.doPutAppend(args.Op, args.Key, args.Value)
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.seq = 0

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
