package shardkv

import (
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


const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type DataStore struct {
	Db  [shardmaster.NShards]map[string]string
	Hash   [shardmaster.NShards]map[int64]int
}

type ShardData struct {
	Db map[string]string
	Hash map[int64]int
	ShardNum int
}

type Op struct {
	// Your definitions here.
	OpID      int64
	// Put, Get, Append
	Operation string
	Key       string
	Value     string
	Extra     interface{}
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
	datastore  DataStore
	seq        int
}

func (kv *ShardKV) SyncUp(xop Op) {

	to := 10 * time.Millisecond
	doing := false
	for {
		status, op := kv.px.Status(kv.seq)
		DPrintf("me=%v, status=%v, op=%v, seq=%v, gid=%v", kv.me, status, op, kv.seq, kv.gid)
		if status == paxos.Decided {

			op := op.(Op)
			DPrintf("done: me=%v, status=%v, op=%v, seq=%v, gid=%v", kv.me, status, op, kv.seq, kv.gid)

			if xop.OpID == op.OpID {
				break
			} else if op.Operation == "Put" || op.Operation == "Append" {
				kv.doPutAppend(op.Operation, op.Key, op.Value, op.OpID)
			} else if op.Operation == "Reconfigure" {
				extra := op.Extra.(ShardData)
				//DPrintf("reconf: %v, %v, %v", kv.me, extra, kv.seq)
				kv.doReconfigure(&extra)
				//DPrintf("done reconf: %v, %v, %v", kv.me, extra, kv.seq)
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


	db := kv.datastore.Db[key2shard(Key)]
	val, ok := db[Key]
	if !ok {
		return "", false
	} else {
		return val, true
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Shards[key2shard(args.Key)] != kv.gid {
		reply.Err = ErrWrongGroup
		//DPrintf("wrong group")
		return nil
	}
	//DPrintf("start Get %v", kv.me)
	op := Op{nrand(), "Get", args.Key, "", nil}
	kv.SyncUp(op)
	reply.Value, _ = kv.doGet(args.Key)
	reply.Err = OK
	//DPrintf("end Get %v", kv.me)
	return nil
}
// added by Adrian
func (kv *ShardKV) doPutAppend(Operation string, Key string, Value string, hash int64) {



	db := kv.datastore.Db[key2shard(Key)]
	h := kv.datastore.Hash[key2shard(Key)]

	val, ok := db[Key]
	if !ok { // if not exists
		db[Key] = Value
	} else { // load
		if Operation == "Put" {
			db[Key] = Value
		} else if Operation == "Append" {
			_, ok := h[hash]
			if !ok {
				db[Key] = val + Value
			}
		}
	}
	h[hash] = 1
}
// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Shards[key2shard(args.Key)] != kv.gid {
		reply.Err = ErrWrongGroup
		return nil
	}

	op := Op{nrand(), args.Op, args.Key, args.Value, nil}
	kv.SyncUp(op)
	kv.doPutAppend(args.Op, args.Key, args.Value, op.OpID)
	reply.Err = OK
	return nil
}

func (kv *ShardKV) doReconfigure(shardData *ShardData) {

	for k, v := range shardData.Db {
		kv.datastore.Db[shardData.ShardNum][k] = v
	}
	for k, v := range shardData.Hash {
		kv.datastore.Hash[shardData.ShardNum][k] = v
	}
}

func (kv *ShardKV) Reconfigure(args *ReconfigureArgs, reply *ReconfigureReply) error {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{nrand(), "Sync", "", "", nil}
	kv.SyncUp(op)

	var db = make(map[string]string)
	var hash = make(map[int64]int)
	for k, v := range kv.datastore.Db[args.ShardNum] {
		db[k] = v
	}
	for k, v := range kv.datastore.Hash[args.ShardNum] {
		hash[k] = v
	}

	reply.Err = OK
	reply.Database = db
	reply.HashVal = hash
	return nil
}

func (kv *ShardKV) checkShard(config shardmaster.Config) bool {

	DPrintf("start: me=%v, shards=%v, gid=%v", kv.me, kv.config.Shards, kv.gid)

	for i, oldGid := range kv.config.Shards {
		newGid := config.Shards[i]

		if oldGid != 0 && oldGid != kv.gid && newGid == kv.gid {
			DPrintf("me: %v. change shard %v -> %v (is now mine), shard: %v. gid: %v", kv.me, oldGid, newGid, i, kv.gid)
			for _, srv := range kv.config.Groups[oldGid] {

				args := &ReconfigureArgs{ShardNum: i}
				var reply = ReconfigureReply{}
				ok := call(srv, "ShardKV.Reconfigure", args, &reply)
				if ok && reply.Err == OK {
					shardData := ShardData{reply.Database, reply.HashVal, args.ShardNum}

					op := Op{nrand(), "Reconfigure", "", "", shardData}
					kv.SyncUp(op)
					kv.doReconfigure(&shardData)
					break
				}
			}
		}
	}
	DPrintf("end: me=%v, shards=%v, gid=%v", kv.me, kv.config.Shards, kv.gid)
	return true
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{nrand(), "Tick", "", "", nil}
	kv.SyncUp(op)

	config := kv.sm.Query(-1)
	kv.checkShard(config)

	kv.config.Num = config.Num
	group := make(map[int64][]string)
	for k, v := range config.Groups {
		group[k] = v
	}
	kv.config.Groups = group

	for i, v := range config.Shards {
		kv.config.Shards[i] = v
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
	gob.Register(ShardData{})
	gob.Register(DataStore{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	for i, _ := range kv.datastore.Db {
		kv.datastore.Db[i] = make(map[string]string)
		kv.datastore.Hash[i] = make(map[int64]int)
	}

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
