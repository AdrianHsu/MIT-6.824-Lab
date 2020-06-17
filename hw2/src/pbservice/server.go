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
	// To the view service, this PBServer is acting like a clerk.
	// so we set up a clerk pointer to do Ping and some other stuff.
	vs         *viewservice.Clerk

	// Your declarations here.
	currview *viewservice.View
	database map[string]string
	// hashVals acts as the state to filter duplicates
	hashVals map[int64]bool
	// A read/write mutex allows all the readers to access
	// the map at the same time, but a writer will lock out everyone else.
	rwm        sync.RWMutex
}

// edited by Adrian
// the new backup got bootstrapped.
func (pb *PBServer) Bootstrapped(args *BootstrapArgs, reply *BootstrapReply) error {

	pb.rwm.Lock()
	defer pb.rwm.Unlock()
	for k, v := range args.Database {
		pb.database[k] = v
	}
	for k, v := range args.HashVals {
		pb.hashVals[k] = v
	}
	return nil
}

// edited by Adrian
// initiate by the primary when it found that it's time to bootstrap the new backup
// since that the current view has not yet changed. so we cannot use `pb.currview.Backup`
// instead, we pass in a backup param
func (pb *PBServer) Bootstrapping(backup string) error {

	args := &BootstrapArgs{pb.database, pb.hashVals}
	var reply BootstrapReply

	ok := false
	for ok == false {
		ok = call(backup, "PBServer.Bootstrapped", args, &reply)
		if ok {
			break
		} else {
			// network failure
			time.Sleep(viewservice.PingInterval)
		}
	}
	return nil
}

// edited by Adrian
// to leverage determinism of the state machine
// the backup got a Get request forwarded by the primary
func (pb *PBServer) ForwardGet(sargs *GetSyncArgs, sreply *GetSyncReply) error {

	pb.rwm.Lock()
	defer pb.rwm.Unlock()

	if sargs.Primary != pb.currview.Primary {
		// the backup first need to check if the primary is still the current primary
		// e.g. split-brain: {s1, s3} -> s1 dies -> {s3, s2} -> s1 revokes
		// -> s1 still receives some requests from client -> so s1 forward to its cache backup, s3
		// -> s3 will tell s1 that "you are no longer the current primary now"
		// -> so finally s1 will reject the client's request
		sreply.Err = "ForwardTest: SENDER IS NOT CURRENT PRIMARY"
		return errors.New("ForwardTest: SENDER IS NOT CURRENT PRIMARY")
	} else {
		// if it is the primary, then we do Get normally
		sreply.Value = pb.database[sargs.Key]
	}
	return nil
}
// edited by Adrian
// to leverage determinism of the state machine
// forward any state necessary for backup to `mimic` the execution
// do exactly the same PutAppend request on the backup
func (pb *PBServer) Forward(sargs *PutAppendSyncArgs, sreply *PutAppendSyncReply) error {

	pb.rwm.Lock()
	defer pb.rwm.Unlock()

	if sargs.Primary != pb.currview.Primary {
		sreply.Err = "ForwardTest: SENDER IS NOT CURRENT PRIMARY"
		return errors.New("ForwardTest: SENDER IS NOT CURRENT PRIMARY")
	} else {
		pb.Update(sargs.Key, sargs.Value, sargs.Op, sargs.HashVal)
	}
	return nil
}

// edited by Adrian
func (pb *PBServer) Update(key string, value string, op string, hashVal int64) {

	// no need to do lock.
	// Update() must be called by Forward() or PutAppend() and they both did acquire the lock
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

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.rwm.Lock()
	defer pb.rwm.Unlock()

	if pb.me != pb.currview.Primary {
		reply.Err = "Get: NOT THE PRIMARY YET"
		// it might be possible that the primary dies and then the backup still not yet
		// realizes that it is now the new primary (the `p3`).

		// e.g., (p1, p3) -> (p3, _)
		// client: already know that p3 is now the new primary
		// p3: according to its cache, it still think (p1, p3) -> still dont think it is the primary
		// so it will return an error the client and tell it to try later
		// -> wait for tick() until it knows (p3, _) and problem solved

		// the backup (at least it thought by itself) should reject a direct client request
		return errors.New("GetTest: NOT THE PRIMARY YET")
	}

	reply.Value = pb.database[args.Key]

	sargs := GetSyncArgs{args.Key,pb.me}
	sreply := GetSyncReply{}

	// if there is no backup currently -> don't do Forward
	ok := pb.currview.Backup == ""

	for ok == false {
		//log.Printf("b get %v, %v, %v", pb.me, pb.currview.Backup, sargs.Key)
		ok = call(pb.currview.Backup, "PBServer.ForwardGet", sargs, &sreply)
		//log.Printf("get %v, %v, %v", pb.me, pb.currview.Backup, sargs.Key)
		if ok == true {
			// everything works well
			break
		} else {
			// case 1. you are no longer the primary
			if sreply.Err == "ForwardTest: SENDER IS NOT CURRENT PRIMARY" {
				reply.Err = sreply.Err
				return errors.New("GetTest: SENDER IS NOT CURRENT PRIMARY") // don't need to update anymore
			}

			time.Sleep(viewservice.PingInterval)
			// case 2. check if the backup was still alive
			// perform exactly the same as tick(). Cannot call it directly as we will acquire lock twice
			newview, _ := pb.vs.Ping(pb.currview.Viewnum)
			pb.checkNewBackup(newview)
			pb.changeView(newview)

			ok = pb.currview.Backup == ""
		}
	}

	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.rwm.Lock()
	defer pb.rwm.Unlock()

	if pb.me != pb.currview.Primary {
		reply.Err = "PutAppend: NOT THE PRIMARY YET"
		return errors.New("PutAppendTest: NOT THE PRIMARY YET")
	}

	// Step 1. Update the primary itself (note: should not update the backup first!)
	pb.Update(args.Key, args.Value, args.Op, args.HashVal)

	sargs := PutAppendSyncArgs{args.Key, args.Value, args.Op, args.HashVal, pb.me}
	sreply := PutAppendSyncReply{}

	// Step 2. Update the backup (if exists)

	// IMPORTANT:
	// only if the primary and the backup is `externally consistent`
	// will the primary respond to the client, i.e., to make this change `externally visible`
	ok := pb.currview.Backup == ""

	for ok == false {
		//log.Printf("b put %v, %v, %v", pb.me, pb.currview.Backup, sargs.Key)
		ok = call(pb.currview.Backup, "PBServer.Forward", sargs, &sreply)
		//log.Printf("put %v, %v, %v", pb.me, pb.currview.Backup, sargs.Key)
		if ok == true {
			// everything works fine
			break
		} else {
			// case 1. you are no longer the primary
			if sreply.Err == "ForwardTest: SENDER IS NOT CURRENT PRIMARY" {
				reply.Err = sreply.Err
				return errors.New("PutAppendTest: SENDER NOT CURRENT PRIMARY") // don't need to update anymore
			}

			time.Sleep(viewservice.PingInterval)
			// case 2. check if the backup was still alive
			// perform exactly the same as tick(). Cannot call it directly as we will acquire lock twice
			newview, _ := pb.vs.Ping(pb.currview.Viewnum)
			pb.checkNewBackup(newview)
			pb.changeView(newview)

			ok = pb.currview.Backup == ""
		}
	}

	return nil
}

// edited by Adrian
// to detect if the backup has changed
func (pb *PBServer) checkNewBackup(newview viewservice.View) {

	// case 1. {s1, _} -> {s1, s2} // s2 is the new backup. s1 is myself.
	// case 2. {s1, s2} -> s2 dies -> {s1, s3} // s3 is the new backup. s1 is myself.
	// note that in case 2, `b` will not be "" in that intermediate state since we called backupByIdleSrv()
	// -> it was already replaced when the primary got notified

	// case 3. {s1, s2} -> {s2, s3} // s3 is the new backup. s2 is me
	// -> therefore we use newview.Primary (s2) to do the bootstrap but not the pb.currview.Primary (s1)
	if newview.Primary == pb.me && pb.currview.Backup != newview.Backup && newview.Backup != "" {
		pb.Bootstrapping(newview.Backup)
	}
}

func (pb* PBServer) changeView(newview viewservice.View) {
	// no need to lock
	// the caller should already acquired a lock
	pb.currview = &newview
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	pb.rwm.Lock()
	defer pb.rwm.Unlock()

	newview, _ := pb.vs.Ping(pb.currview.Viewnum)
	//log.Printf("me=%v, v=%v, p=%v, b=%v", pb.me, newview.Viewnum, newview.Primary, newview.Backup)
	pb.checkNewBackup(newview)
	pb.changeView(newview)
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
