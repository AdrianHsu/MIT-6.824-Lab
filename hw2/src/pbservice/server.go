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
	// defer statement defers the execution of a function until the surrounding function returns.
	// to make sure that when I'm doing Forward for my backup, my own map will not be modified by others.
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
	rwm     sync.RWMutex

	// hashVals acts as the state to filter duplicates
	hashVals map[int64]bool

}

// edited by Adrian
// the new backup got bootstrapped.
func (pb *PBServer) Bootstrapped(args *BootstrapArgs, reply *BootstrapReply) error {

	pb.rwm.Lock()
	log.Printf("database1: %v, database2: %v", len(pb.database["0"]), len(args.Database["0"]))
	// WRONG:
	// pb.database = args.Database  // cannot do like that. this is not copy
	for k,v := range args.Database {
		pb.database[k] = v
	}
	for k,v := range args.HashVals {
		pb.hashVals[k] = v
	}
	pb.rwm.Unlock()
	return nil
}

// edited by Adrian
// initiate by the Primary when bootstrapping new backup
func (pb *PBServer) Bootstrapping(backup string) error {

	pb.rwm.Lock()
	args := &BootstrapArgs{pb.database, pb.hashVals}
	pb.rwm.Unlock()
	var reply BootstrapReply

	ok := false
	for ok == false {
		//log.Printf("%v, %v", pb.me, backup)
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

	// basically you don't need this.
	// if the backup is dead and then the primary do `Forward` -> the connection will fail
	//if pb.isdead() {
	//	reply.Err = "I'm already dead"
	//	return nil
	//}

	if args.Primary != pb.currview.Primary {
		// the backup first need to check if the primary is still the current primary
		// the caller is not primary anymore
		reply.Err = "Forward fails: you are not the current primary..."
	} else {
		pb.PutAppend(args, reply)
	}
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.rwm.Lock()
	reply.Value = pb.database[args.Key]
	pb.rwm.Unlock()
	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.rwm.Lock()
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

	// quick check. as the backup will also call this PutAppend(). not just the primary.
	if pb.isPrimaryInCache(pb.currview.Primary) {
		args.Primary = pb.me

		// IMPORTANT:
		// only if the primary and the backup is `externally consistent`
		// will the primary respond to the client, i.e., to make this change `externally visible`
		var tmpBackup = pb.currview.Backup
		ok := tmpBackup == "" // if there is no backup currently -> don't do Forward

		for ok == false {
			//log.Printf("do forward %v->%v, val=%v", pb.me, pb.currview.Backup, args.Value)
			ok = call(pb.currview.Backup, "PBServer.Forward", args, &reply)
			//log.Printf("%v, do forward %v->%v, val=%v", ok, pb.me, pb.currview.Backup, args.Value)
			//log.Printf("forward: %v", ok)
			if ok == true {
				break
			} else {
				// there might be 3 cases:
				newview, _ := pb.vs.Get()
				//if newview.Primary != pb.me {
					// 1. the primary is no longer the primary (split-brain)
					// I found that this will not happen -> if it is not P, it MUST BE DEAD
					//reply.Err = "PutAppend fails: I am no longer the primary. Sorry!"
					//break
				//} else
				tmpBackup = newview.Backup
				if tmpBackup == "" {
					reply.Err = "PutAppend fails: the backup not exists now. break."
					break
				} else {
					// 2. the backup is dead but the primary doesn't discover yet. {s1, s2} but is in fact {s1, _}
					// it should be fine. The problem is bc of the backup, not the primary itself. -> retry later
					// (wait for tick() to update the new backup)
					// 3. the reason is the unreliable network between P & B. -> retry later.
					time.Sleep(viewservice.PingInterval) // wait for the tick() to update view service's view
				}
			}
		}
	}
	pb.rwm.Unlock()

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

	if pb.isPrimaryInCache(newview.Primary) {
		// case 1. {s1, _} -> {s1, s2} // s2 is the new backup
		// case 2. {s1, s2} -> s2 dies -> {s1, s3} // s3 is the new backup
		// note that in case 2, `b` will not be "" at that intermediate state since we called backupByIdleSrv()
		// -> it was already replaced when primary got notified
		log.Printf("%v is the new primary", newview.Primary)
		if pb.currview.Backup != newview.Backup {

			log.Printf("do bootstrap")
			pb.Bootstrapping(newview.Backup)
		}
	}
	pb.currview = &newview
	log.Printf("me=%v, len=%v, n=%v, p=%v, b=%v", pb.me, len(pb.database["0"]), pb.currview.Viewnum, pb.currview.Primary, pb.currview.Backup)
}

// edited by Adrian
// this PBServer regards itself as the primary according to its local cache
// but of course, there is no promise that it IS the current primary
func (pb *PBServer) isPrimaryInCache(primary string) bool {
	return primary == pb.me
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
