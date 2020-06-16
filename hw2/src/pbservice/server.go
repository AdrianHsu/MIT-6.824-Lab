package pbservice

import (
	"errors"
	"net"
)
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
	//  A read/write mutex allows all the readers to access
	// the map at the same time, but a writer will lock out everyone else.
	rwm        sync.RWMutex

	// Your declarations here.
	currview *viewservice.View
	database map[string]string
	// hashVals acts as the state to filter duplicates
	hashVals map[int64]bool

}

// edited by Adrian
// the new backup got bootstrapped.
func (pb *PBServer) Bootstrapped(args *BootstrapArgs, reply *BootstrapReply) error {

	pb.rwm.Lock()
	for k, v := range args.Database {
		pb.database[k] = v
	}
	for k, v := range args.HashVals {
		pb.hashVals[k] = v
	}
	pb.rwm.Unlock()
	return nil
}

// edited by Adrian
// initiate by the Primary when bootstrapping new backup
func (pb *PBServer) Bootstrapping(backup string) error {

	args := &BootstrapArgs{pb.database, pb.hashVals}
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

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.rwm.Lock()
	reply.Value = pb.database[args.Key]
	pb.rwm.Unlock()
	return nil
}

func (pb *PBServer) Update(key string, value string, op string, hashVal int64) {
	if op == "Put" {
		pb.database[key] = value
	} else if op == "Append" {

		// detect duplicates
		if pb.hashVals[hashVal] != true {

			// Append should use an empty string for the previous value
			// if the key doesn't exist
			pb.database[key] += value
			pb.hashVals[hashVal] = true
		}
	}
}
// edited by Adrian
// to leverage determinism of the state machine
// forward any state necessary for backup to `mimic` the execution
func (pb *PBServer) Forward(sargs *PutAppendSyncArgs, sreply *PutAppendSyncReply) error {

	pb.rwm.Lock()
	// defer statement defers the execution of a function until the surrounding function returns.
	// to make sure that when I'm doing Forward for my backup, my own map will not be modified by others.
	defer pb.rwm.Unlock()

	if sargs.Primary != pb.currview.Primary {
		// the backup first need to check if the primary is still the current primary
		// the caller is not primary anymore
		sreply.Err = "ForwardTest: NOT CURRENT PRIMARY"
		return errors.New("ForwardTest: NOT CURRENT PRIMARY")
	} else {
		pb.Update(sargs.Key, sargs.Value, sargs.Op, sargs.HashVal)
	}
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	if pb.me != pb.currview.Primary {
		reply.Err = "PutAppend: NOT THE PRIMARY YET"
		// e.g., (p1, p3) -> (p3, _)
		// client: already know that p3 is now the new primary
		// p3: according to its cache, it still think (p1, p3) -> still dont think it is the primary
		// p3: wait for tick() until it knows (p3, _) and solved

		// the backup (at least it thought by itself) should reject a direct client request
		return errors.New("PutAppend: NOT THE PRIMARY YET")
	}


	pb.rwm.Lock()
	// Step 1. Update Primary itself
	pb.Update(args.Key, args.Value, args.Op, args.HashVal)

	// defer statement defers the execution of a function until the surrounding function returns.
	// to make sure that when I'm doing Forward for my backup, my own map will not be modified by others.
	defer pb.rwm.Unlock()
	sargs := PutAppendSyncArgs{args.Key, args.Value, args.Op, args.HashVal, pb.me}
	sreply := PutAppendSyncReply{}

	// Step 2. Update Backup if exists
	// IMPORTANT:
	// only if the primary and the backup is `externally consistent`
	// will the primary respond to the client, i.e., to make this change `externally visible`
	ok := pb.currview.Backup == "" // if there is no backup currently -> don't do Forward

	for ok == false {
		ok = call(pb.currview.Backup, "PBServer.Forward", sargs, &sreply)
		if ok == true {
			// everything works fine
			break
		} else {
			// case 1. you are no longer the primary
			if sreply.Err == "ForwardTest: NOT CURRENT PRIMARY" {
				reply.Err = sreply.Err
				return errors.New("PutAppendTest: NOT CURRENT PRIMARY") // don't need to update anymore
			}
			time.Sleep(viewservice.PingInterval)
			// case 2. check if the backup was still alive
			pb.tick() // do tick() by force
			ok = pb.currview.Backup == ""
		}
	}

	return nil
}


func (pb *PBServer) checkNewBackup(newview viewservice.View) {

	if newview.Primary == pb.me && pb.currview.Backup != newview.Backup && newview.Backup != "" {
		// case 1. {s1, _} -> {s1, s2} // s2 is the new backup. s1 is me.
		// case 2. {s1, s2} -> s2 dies -> {s1, s3} // s3 is the new backup. s1 is me.
		// note that in case 2, `b` will not be "" at that intermediate state since we called backupByIdleSrv()
		// -> it was already replaced when primary got notified
		// case 3. {s1, s2} -> {s2, s3} // s3 is the new backup. s2 is me -> therefore we use newview.Primary
		//log.Printf("%v do bootstrapping: %v", pb.me, newview.Backup)
		pb.Bootstrapping(newview.Backup)
	}
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
	//log.Printf("v=%v, p=%v, b=%v", newview.Viewnum, newview.Primary, newview.Backup)
	pb.checkNewBackup(newview)

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
