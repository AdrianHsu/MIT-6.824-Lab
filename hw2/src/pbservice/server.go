package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"



type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	// To the vs, this PBServer is acting like a clerk.
	// so we set up a clerk ptr to do Ping and some stuff.
	vs         *viewservice.Clerk


	// Your declarations here.
	currview *viewservice.View
	database map[string]string
	// hashVals acts as the state to filter duplicates
	hashVals map[int64]bool

}

// edited by Adrian
// the new backup got bootstrapped.
func (pb *PBServer) Bootstrapped(args *BootstrapArgs, reply *BootstrapReply) error {

	pb.mu.Lock()
	pb.database = args.Database
	pb.hashVals = args.HashVals
	defer pb.mu.Unlock()
	return nil
}

// edited by Adrian
// initiate by the Primary when bootstrapping new backup
func (pb *PBServer) Bootstrapping(backup string) error {

	args := &BootstrapArgs{pb.database, pb.hashVals}
	var reply BootstrapReply

	ok := false
	for ok == false {
		ok = call(backup, "PBServer.Bootstrapped", args, &reply)
		if ok {
			break
		} else {
			time.Sleep(viewservice.PingInterval)
		}
	}
	return nil
}

// edited by Adrian
// to leverage determinism of the state machine
// forward any state necessary for backup to `mimic` the execution
func (pb *PBServer) Forward(args *PutAppendArgs, reply *PutAppendReply) error {

	// the backup first need to check if the primary is still the current primary
	if args.Primary != pb.currview.Primary {
		// the caller is not primary anymore
		reply.Err = "you are not the current primary..."
	} else {
		pb.PutAppend(args, reply)
	}
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
	reply.Value = pb.database[args.Key]
	pb.mu.Unlock()
	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.

	pb.mu.Lock()
	if args.Op == "Put" {
		pb.database[args.Key] = args.Value
	} else if args.Op == "Append" {

		// detect duplicates
		if pb.hashVals[args.HashVal] != true {

			// Append should use an empty string for the previous value
			// if the key doesn't exist
			pb.database[args.Key] += args.Value
			pb.hashVals[args.HashVal] = true
		}
	}
	// defer statement defers the execution of a function until the surrounding function returns.
	defer pb.mu.Unlock()

	if pb.isPrimaryInCache() {
		args.Primary = pb.me
		ok := call(pb.currview.Backup, "PBServer.Forward", args, &reply)
		//log.Printf("forward: %v", ok)
		if ok == false {
			// there might be 2 cases:
			// 1. the primary is no longer the primary (split-brain)
			// 2. the backup is dead or not exist

			if pb.vs.Primary() != pb.me {
				reply.Err = "forward PutAppend fails..."
			} else {
				// it should be fine. The problem is bc of the backup, not the primary iteself.
			}
		}
	}
	return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	newview, _ := pb.vs.Ping(pb.currview.Viewnum)

	if pb.isPrimaryInCache() {
		// case 1. {s1, _} -> {s1, s2} // s2 is the new backup
		// case 2. {s1, s2} -> s2 dies -> {s1, s3} // s3 is the new backup
		// note that in case 2, `b` will not be "" at that intermediate state
		// as it was already replaced when primary got notified
		if pb.currview.Backup != newview.Backup {
			pb.Bootstrapping(newview.Backup)
		}
	}
	pb.currview = &newview
}

// edited by Adrian
// this PBServer regards itself as the primary according to its local cache
// but of course, there is no promise that it IS the current primary
func (pb *PBServer) isPrimaryInCache() bool {
	return pb.currview.Primary == pb.me
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.

	pb.currview = &viewservice.View{}
	pb.database = make(map[string]string)
	pb.hashVals = make(map[int64]bool)
	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
