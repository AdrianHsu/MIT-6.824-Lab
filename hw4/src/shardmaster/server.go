package shardmaster

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

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs [] Config // indexed by config num
	// added by adrian
	exist      map[int64]bool
}

func nrand() int64 {
	bigx := rand.Int63()
	return bigx
}

type Op struct {
	// Your data here.
	HashID       int64
	Operation    string // Join, Leave, Move, Query
	GID          int64
	Servers      [] string
	ShardNum     int
}

func (sm *ShardMaster) Len() int {
	return len(sm.configs)
}
func (sm *ShardMaster) Tail() Config {
	return sm.configs[sm.Len() - 1]
}

// created by Adrian
func (sm *ShardMaster) SyncUp(xop Op) {

	to := 10 * time.Millisecond
	doing := false
	for {
		status, op := sm.px.Status(sm.Len())
		if status == paxos.Decided {

			op := op.(Op)
			if op.HashID == xop.HashID {
				break
			} else if op.Operation == "Join" {
				sm.join(op)
			} else if op.Operation == "Leave" {
				sm.leave(op)
			} else if op.Operation == "Move" {
				sm.move(op)
			}

			doing = false
			sm.px.Done(sm.Len())
		} else {
			if !doing {
				sm.px.Start(sm.Len(), xop)
				doing = true
			}
			time.Sleep(to)
			to += 10 * time.Millisecond
		}
	}
	sm.px.Done(sm.Len())
}

func (sm *ShardMaster) join(op Op) {
	groups := sm.Tail().Groups
	num := sm.Tail().Num
	shards := sm.Tail().Shards

	newGroups := map[int64][]string{}
	for k,v := range groups {
		newGroups[k] = v
	}
	for _, s := range op.Servers {
		newGroups[op.GID] = append(newGroups[op.GID], s)
	}

	var i = 0
	var done = false
	for done == false {
		for k, _ := range newGroups {
			shards[i] = k
			i += 1
			if i == NShards {
				done = true
				break
			}
		}
	}

	num += 1
	sm.configs = append(sm.configs, Config{num,shards, newGroups})
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.exist[args.GID] == true {
		return nil
	} else {
		sm.exist[args.GID] = true
	}
	op := Op{nrand(), "Join",args.GID, args.Servers, -1}
	sm.SyncUp(op)
	sm.join(op)
	return nil
}

func (sm *ShardMaster) leave(op Op) {
	groups := sm.Tail().Groups
	num := sm.Tail().Num
	shards := sm.Tail().Shards
	newGroups := map[int64][]string{}

	for k, v := range groups {
		if k == op.GID {
			// ignore this.
		} else {
			newGroups[k] = v
		}
	}

	var i = 0
	var done = false
	for done == false {
		for k, _ := range newGroups {

			shards[i] = k
			i += 1
			if i == NShards {
				done = true
				break
			}
		}
	}

	num += 1
	sm.configs = append(sm.configs, Config{num,shards, newGroups})
}
func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.exist[args.GID] == false {
		return nil
	} else {
		sm.exist[args.GID] = false
	}

	op := Op{nrand(), "Leave",args.GID, nil, -1}
	sm.SyncUp(op)
	sm.leave(op)
	return nil
}

func (sm *ShardMaster) move(op Op) {
	groups := sm.Tail().Groups
	num := sm.Tail().Num
	shards := sm.Tail().Shards
	newGroups := map[int64][]string{}

	for k, v := range groups {
		newGroups[k] = v
	}

	shards[op.ShardNum] = op.GID
	num += 1
	sm.configs = append(sm.configs, Config{num,shards, newGroups})
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.exist[args.GID] == false {
		return nil
	}

	op := Op{nrand(), "Move",args.GID, nil, args.Shard}
	sm.SyncUp(op)
	sm.move(op)
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if args.Num == -1 {
		reply.Config = sm.Tail()
	} else {
		reply.Config = sm.configs[args.Num]
	}
	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1) // len = 1
	sm.configs[0].Groups = map[int64][]string{}
	sm.exist = map[int64]bool{}

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
