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
	hashVals map[int64]bool

}

// edited by Adrian
func (pb *PBServer) RsyncReceive(args *RsyncArgs, reply *RsyncReply) error {

	pb.mu.Lock()
	pb.database = args.Database
	pb.hashVals = args.HashVals
	pb.mu.Unlock()
	return nil
}

// edited by Adrian
// initiate by the Primary
func (pb *PBServer) RsyncSend(backup string) error {
	args := &RsyncArgs{pb.database, pb.hashVals}
	var reply RsyncReply
	call(backup, "PBServer.RsyncReceive", args, reply)
	return nil
}

// edited by Adrian
func (pb *PBServer) Forward(args *PutAppendArgs, reply *PutAppendReply) error {

	if args.Me != pb.currview.Primary { // the caller is not primary anymore
		reply.Err = "not primary..."
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
	key := args.Key
	value := args.Value
	op := args.Op
	hashVal := args.HashVal

	pb.mu.Lock()
	if op == "Put" {
		pb.database[key] = value
	} else if op == "Append" {
		// Append should use an empty string for the previous value
		// if the key doesn't exist
		if pb.hashVals[hashVal] == true {
			// skip
		} else {
			pb.database[key] += value
			pb.hashVals[hashVal] = true
		}
	}
	defer pb.mu.Unlock()

	if pb.currview.Primary == pb.me {
		args.Me = pb.me
		call(pb.currview.Backup, "PBServer.Forward", args, &reply)
		//log.Printf("forward: %v", ok)
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

	if pb.currview.Primary == pb.me {
		// case 1. {s1, _} -> {s1, s2} // s2 is the new backup
		// case 2. {s1, s2} -> s2 dies -> {s1, s3}
		// note that in case 2, `b` will not be "" at that intermediate state
		if pb.currview.Backup != newview.Backup {
			pb.RsyncSend(newview.Backup)
		}
	}
	pb.currview = &newview
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
