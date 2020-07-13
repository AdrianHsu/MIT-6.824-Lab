package shardmaster

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

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num
	lastApply  int
}


const (
	Join = "Join"
	Leave = "Leave"
	Move = "Move"
	Query = "Query"
)

type Op struct {
	// Your data here.
	Operation string
	Args interface{}
}

func (sm *ShardMaster) Rebalance(config *Config, deleteGID int64) {
	nGroup := len(config.Groups)
	limit := NShards / nGroup

	for i := 0; i < NShards; i++ {
		if config.Shards[i] == deleteGID {
			// let's say we want to delete gid = 101
			// and Shards is now [101, 101, 100, 101, 102, ...]
			// then it becomes [0, 0, 100, 0, 102, ...]
			config.Shards[i] = 0
		}
	}
	gidCounts := make(map[int64]int)
	for i := 0; i < NShards; i++ {
		// occurrences of gids in these 10 shards

		// ps. the DELETED gid will also has a gidCounts
		// and our goal is just making it decrease to 0 (all distributed)
		gidCounts[config.Shards[i]] += 1
	}

	for i := 0; i < NShards; i++ {
		gid := config.Shards[i]
		// if `i`th shard's group is now deleted
		// OR if `i`th shard's group need to manage too many shards
		// -> find someone to replace it and to take care of `i`th shard
		// how do we find who is the best choice?
		if gid == 0 || gidCounts[gid] > limit {

			// bestGid is the best replacement gid that we could find now
			bestGid := int64(-1) // init value
			// minGidCount is the # of shards that the group `bestGid`
			// is taking care of.
			// e.g., [0, 0, 0, 101, 101, 102, 101, 0, 102, 101]
			// then bestGid = 102 as its minGidCount = 2
			// in contrast, gid 101 is not the best as it is already
			// taking care of 4 shards
			minGidCount := -1 // init value

			// enumerate all existing groups
			for currGid, _ := range config.Groups {
				// if init OR
				// group `currGid` is taking care of less # of shards
				// compared to minGidCount
				// update our best choice Gid (the one will MINIMUM count)
				if bestGid == -1 || gidCounts[currGid] < minGidCount {
					bestGid = currGid
					minGidCount = gidCounts[currGid]
				}
			}
			// if the current gid on shard `i` is deleted
			// we MUST need to give it a new gid
			// and so new the deleted group's gidCount will -= 1
			// and the replacement group will += 1
			if gid == 0 {
				gidCounts[gid] -= 1
				gidCounts[bestGid] += 1
				config.Shards[i] = bestGid
			} else {
				// if the current gid is not the deleted one
				// i.e., it is just `gid` group taking care of too many shards
				// then we should reduce its burden. But NOT all the time. When?

				// only if our replacement could be meaningful
				// e.g. [100, 100, 100, 100, 101, 101, 101, 102, 102, 102]
				// for gid = 100, it has now gidCount = 4
				// and for gid = 101, it has now gidCount = 3
				// then if we make gidCount[100] -= 1 and gidCount[101] += 1
				// they will be 3 and 4 respectively...it does not help at all
				// e.g. [100, 100, 100, 101, 101, 101, 101, 102, 102, 102]
				// so we will prefer doing nothing
				if gidCounts[gid] - gidCounts[bestGid] > 1 {
					gidCounts[gid] -= 1
					gidCounts[bestGid] += 1
					config.Shards[i] = bestGid
				} else {
					// do nothing
				}
			}
		}
	}
}

func (sm *ShardMaster) Apply(op Op) {
	lastConfig := sm.configs[sm.lastApply]
	var newConfig Config
	newConfig.Num = lastConfig.Num
	newConfig.Groups = make(map[int64][]string)
	for k, v := range lastConfig.Groups {
		newConfig.Groups[k] = v
	}
	for i := 0; i < NShards; i++ {
		newConfig.Shards[i] = lastConfig.Shards[i]
	}

	if op.Operation == Join {
		joinArgs := op.Args.(JoinArgs)
		newConfig.Groups[joinArgs.GID] = joinArgs.Servers
		newConfig.Num += 1
		sm.Rebalance(&newConfig, 0)
	} else if op.Operation == Leave {
		leaveArgs := op.Args.(LeaveArgs)
		delete(newConfig.Groups, leaveArgs.GID)
		newConfig.Num += 1
		sm.Rebalance(&newConfig, leaveArgs.GID)
	} else if op.Operation == Move {
		moveArgs := op.Args.(MoveArgs)
		newConfig.Shards[moveArgs.Shard] = moveArgs.GID
		newConfig.Num += 1
	} else if op.Operation == Query {
		// do nothin
	}

	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) Wait(seq int) (Op, error) {
	sleepTime := 10 * time.Millisecond
	for iters := 0; iters < 15; iters ++ {
		decided, op := sm.px.Status(seq)
		if decided == paxos.Decided {
			return op.(Op), nil
		}
		time.Sleep(sleepTime)
		if sleepTime < 10 * time.Second {
			sleepTime *= 2
		}
	}
	return Op{}, errors.New("ShardMaster: Wait for too long")
}

func (sm *ShardMaster) Propose(xop Op) error {
	for {
		sm.px.Start(sm.lastApply + 1, xop)
		op, err := sm.Wait(sm.lastApply + 1)
		if err != nil {
			return err
		}
		sm.Apply(op)
		sm.lastApply += 1
		if reflect.DeepEqual(op, xop) {
			break
		}
		sm.px.Done(sm.lastApply)
	}
	sm.px.Done(sm.lastApply)
	return nil
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{Args: *args, Operation: Join}
	err := sm.Propose(op)
	if err != nil {
		return err
	}
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{Args: *args, Operation: Leave}
	err := sm.Propose(op)
	if err != nil {
		return err
	}
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{Args: *args, Operation: Move}
	err := sm.Propose(op)
	if err != nil {
		return err
	}
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{Args: *args, Operation: Query}
	err := sm.Propose(op)
	if err != nil {
		return err
	}

	// config.Num is not necessarily equal to its index in sm.configs
	// e.g., sm.configs[1203].Num -> this value could be != 1203
	// e.g., sm.configs[6].Num = 3, sm.configs[16].Num = 5
	// why? since that WE ONLY add Num when Join/Leave/Move
	// but we don't add Num when doing Query
	// however, sm.configs will be appended even if it was Query
	// thus, len of configs grows FASTER than Num
	for i := 0; i < sm.lastApply; i++ {
		if sm.configs[i].Num == args.Num {
			reply.Config = sm.configs[i]
			//log.Printf("i=%v, num=%v", i, args.Num)
			return nil
		}
	}
	// args.Num == -1 OR args.Num is larger than any other Num in configs
	reply.Config = sm.configs[sm.lastApply]
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
	gob.Register(Op{})
	gob.Register(JoinArgs{})
	gob.Register(LeaveArgs{})
	gob.Register(MoveArgs{})
	gob.Register(QueryArgs{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

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