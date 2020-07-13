package kvpaxos

import (
	"errors"
	"net"
	"reflect"
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


const (
	Put = "Put"
	Append = "Append"
	Get = "Get"
)

type Op struct {
	// Put, Get, Append
	Operation string
	Args      interface{}
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	lastApply  int
	database   map[string]string
	maxClientSeq  map[int64]int
}


func (kv *KVPaxos) Apply(op Op) {
	if op.Operation == Get {
		args := op.Args.(GetArgs)
		if args.Seq > kv.maxClientSeq[args.ClientID] {
			kv.maxClientSeq[args.ClientID] = args.Seq
		}
	} else if op.Operation == Put {
		args := op.Args.(PutAppendArgs)
		kv.database[args.Key] = args.Value
		if args.Seq > kv.maxClientSeq[args.ClientID] {
			kv.maxClientSeq[args.ClientID] = args.Seq
		}
	} else if op.Operation == Append {
		args := op.Args.(PutAppendArgs)
		value, ok := kv.database[args.Key]
		if !ok {
			value = ""
		}
		kv.database[args.Key] = value + args.Value
		if args.Seq > kv.maxClientSeq[args.ClientID] {
			kv.maxClientSeq[args.ClientID] = args.Seq
		}
	}
}

func (kv *KVPaxos) Wait(seq int) (Op, error) {
	sleepTime := 10 * time.Microsecond
	for iters := 0; iters < 15; iters ++ {
		decided, op := kv.px.Status(seq)
		if decided == paxos.Decided {
			return op.(Op), nil
		}
		// as we correctly do `done()` forgetten one should not be shown
		//else if decided == paxos.Forgotten {
		//	break
		//}
		time.Sleep(sleepTime)
		if sleepTime < 10 * time.Second {
			sleepTime *= 2
		}
	}
	return Op{}, errors.New("Wait for too long")
}

func (kv *KVPaxos) Propose(xop Op) error {
	for {
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
		// do this everytime lastApply +1 -> to prevent any possible mem overflow possibilities
		kv.px.Done(kv.lastApply)
	}
	kv.px.Done(kv.lastApply)
	return nil
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Seq <= kv.maxClientSeq[args.ClientID]  {
		reply.Err = OK
		reply.Value = kv.database[args.Key]
		return nil
	}
	op := Op{Operation: "Get", Args: *args}
	err := kv.Propose(op)
	if err != nil {
		return err
	}

	value, ok := kv.database[args.Key]
	if !ok {
		reply.Err = ErrNoKey
		reply.Value = ""
	} else {
		reply.Err = OK
		reply.Value = value
	}
	return nil

}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Seq <= kv.maxClientSeq[args.ClientID] {
		reply.Err = OK
		return nil
	}

	op := Op{Args: *args, Operation: args.Op}
	err := kv.Propose(op)
	if err != nil {
		return err
	}
	reply.Err = OK
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	//DPrintf("Kill(%d): die\n", kv.me)
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
	gob.Register(GetArgs{})
	gob.Register(PutAppendArgs{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.database = make(map[string]string)
	kv.maxClientSeq = make(map[int64]int)

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
