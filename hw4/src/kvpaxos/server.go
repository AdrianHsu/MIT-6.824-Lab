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

	// OpID is a hash key attached by the client. Each time when client
	// retried an operation, it will always use a fixed OpID
	OpID      int64
	// Put, Get, Append
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
	// hashVals acts as the state to filter duplicates
	// if an operation has already been performed on `database`,
	// it should not be performed again
	hashVals   sync.Map
	// each KVPaxos has a seq number recording the current progress.
	// seq starts from 0 and gradually increase by 1
	// if seq = 12, it means that the paxos server instances 0 to 11 has all been decided
	// and also they should have been called Done()
	seq        int
}

// added by Adrian
// when every time clients sends a Get/PutAppend,
// the first thing our KVPaxos replica do is NOT to perform the Get/PutAppend
// on Paxos directly; instead, we will first make sure all the previous
// operations that have been decided by some other majority (exclude me)
// are successfully fetched into my database. The way we did that is to
// Start() a new operation with the seq number. And since that n_a, v_a has been
// decided, as a result we will get the value which all other majority has reached an agreement to.
func (kv *KVPaxos) SyncUp(xop Op) {
	to := 10 * time.Millisecond
	doing := false
	// sync on all seq number instances that I have not yet recorded
	// and after they are all done, we perform our own xop by calling Start()
	for {
		status, op := kv.px.Status(kv.seq)
		// DPrintf("server %v, seq %v, status %v", kv.me, kv.seq, status)
		// KVPaxos servers interact with each other through the Paxos log.
		if status == paxos.Decided {
			// this Decided() could be 2 cases.
			// case 1. this kv.seq instances has been decided by others and thus when I called Start(),
			// the instance n_a, v_a is taken from some other majority's agreement.
			// case 2. I am the initiator. No one has reached an agreement (not Decided) on this seq number yet
			// and thus the xop.OpID == op.OpID
			op := op.(Op)

			if xop.OpID == op.OpID {
				// if it was case 2. then we don't do doPutAppend() as we will do it later out of this function
				break
			} else if op.Operation == "Put" || op.Operation == "Append" {
				// if it was case 1, then we have to make compensation.
				// we have to catch up on some others' progress. so we perform the PutAppend
				// according to the paxos log we have the consensus on
				kv.doPutAppend(op.Operation, op.Key, op.Value, op.OpID)
			} else {
				// if it was case 1, then even though it is a Get I was previously not aware of,
				// I still don't need to do anything as it will not affect my `database`
				//value, _ := kv.doGet(op.Key)
				//DPrintf("get: %v", value)
			}
			// we could do Done() here. but as it checks all seq num from 0 ~ kv.seq.
			// so we can elegantly do it outside of this for loop for simplicity.
			// kv.px.Done(kv.seq)

			// once we catched up on this instance, we can finally increase our seq num by 1
			kv.seq += 1
			// also we have to set that our Start() is over. We might need to initiate another Start() though
			doing = false
		} else {
			if !doing {
				// your server should try to assign the next available Paxos instance (sequence number)
				// to each incoming client RPC. However, some other kvpaxos replica may also be trying
				// to use that instance for a different client's operation.
				// e.g., KVPaxos server 1 do Put(1, 15) and server 2 do Put(1, 32). they are both seq=3 now
				// Acceptor 1: P1 x   A1-15(ok)    P2 A2-32(ok) // p.s., Proposal 22 arrives A1 a bit late
				// Acceptor 2: P1 P2  A1-15(fail)     A2-32(ok)
				// Acceptor 3: P1 P2  A1-15(fail)     A2-32(ok)
				// as a result, Put(1, 32) will be accepted instead of Put(1, 15)(got overrided)
				// although these 2 servers are both doing this on seq=3

				// Hint: if one of your kvpaxos servers falls behind (i.e. did not participate
				// in the agreement for some instance), it will later need to find out what (if anything)
				// was agree to. A reasonable way to to this is to call Start(), which will either
				// discover the previously agreed-to value, or cause agreement to happen

				// Think about what value would be reasonable to pass to Start() in this situation.
				// Ans. Just pass in the value we want to agree on previously (the `xop`) as a matter of fact
				// if the instance on seq = 3 has been Decided, then when we call Start() on the prepare phase
				// the V_a will definitely be replaced by the V_a that some other majority has agreed to
				// Let's say srv 0 are not at seq = 3, and it wants to do Put(1, 15)
				// and yet srv 1, 2, 3 has already reached seq = 8, that is, their seq 3 ~ 7 are all decided
				// thus, srv 0 will do Start() on seq 3 ~ 7 but the value will got substituded.
				// and finally the Put(1, 15) will only be accepted when seq = 8.
				kv.px.Start(kv.seq, xop)
				//DPrintf("%v: do start for seq: %v, value=%v", kv.me, kv.seq, xop.Value)
				// now I'm doing Start(). So don't call Start() again on the same seq, same xop.
				// not until I finished doing this xop will I initiate another Start()
				doing = true
			}
			time.Sleep(to)
			// your code will need to wait for Paxos instances to complete agreement.
			// A good plan is to check quickly at first, and then more slowly:
			to += 10 * time.Millisecond
		}
	}
	// don't forget to call the Paxos Done() method when a kvpaxos has processed
	// an instance and will no longer need it or any previous instance.
	// When will the px.Forget() to be called? when EVERY KVPaxos call Done() on seq = 3,
	// then Min() will be 4. -> when doing next Start() (as we are piggybacked() the proposer
	// will clean up those old instances by calling Forget()
	kv.px.Done(kv.seq)
	kv.seq += 1
}
// added by Adrian
func (kv *KVPaxos) doGet(Key string) (string, bool) {
	val, ok := kv.database.Load(Key)
	// no effect for Get even the hashVal may has duplicates
	if !ok {
		return "", false
	} else {
		return val.(string), true
	}
}
// added by Adrian
func (kv *KVPaxos) doPutAppend(Operation string, Key string, Value string, hash int64) {
	// first, we check if the key is already exists
	val, ok := kv.database.Load(Key)
	if !ok { // if not exists

		// init. are the same for either put / append
		kv.database.Store(Key, Value)

	} else { // load
		if Operation == "Put" {
			kv.database.Store(Key, Value)
		} else if Operation == "Append" {
			vals := val.(string)
			// you will need to uniquely identify client operations
			// to ensure that they execute just once.
			_, ok := kv.hashVals.Load(hash)
			if !ok {
				// check if the hashVals has been added
				kv.database.Store(Key, vals+Value)
			}
		}
	}
	// we have to store this hash whether it is the first time this pair was pushed or not
	// therefore I put this outside of the if-else branch condition
	kv.hashVals.Store(hash, 1) // an arbitrary value 1
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// these values in Get Op are basically dummy value,
	// we will not use them when meeting one of them in SyncUp()
	op := Op{args.Hash, "Get", args.Key, ""}
	// a kvpaxos server should not complete a Get() RPC if it is not part of a majority
	// (so that it does not serve stale data).
	// -> instead, it will endlessly try syncing and wait for it to be `Decided`
	kv.SyncUp(op)
	reply.Value, _ = kv.doGet(args.Key)
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// It should enter a Get Op in the Paxos log, and then "interpret" the the log **before that point**
	// to make sure its key/value database reflects all recent Put()s.
	// ps. An Append Paxos log entry should contain the Append's arguments,
	// but not the resulting value, since the result might be large.
	op := Op{args.Hash, args.Op, args.Key, args.Value}
	kv.SyncUp(op)
	kv.doPutAppend(args.Op, args.Key, args.Value, args.Hash)
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
