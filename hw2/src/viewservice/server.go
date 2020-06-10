package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.

	// Hint #2: add field(s) to ViewServer to keep track of the current view.
	currview *View
	recentHeard map[string] time.Time

	// Hint #3: keep track of whether the primary for the current view has acknowledged it
	// keep track of whether the primary for the current view has acked the latest view X
	viewBound uint // last value view X of the primary Ping(X)
	idleServers map[string] bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	// Hint #1: you'll want to add field(s) to ViewServer in server.go
	// in order to keep track of the most recent time at which
	// the viewservice has heard a Ping from each server.
	vs.recentHeard[args.Me] = time.Now()

	if vs.currview == nil { // init, Ping(0) from ck1. only do this one time
		vs.viewBound = args.Viewnum // X is now 0
		vs.currview = &View{0, "", ""}
		// ps. as it now received a Ping(0) from primary => can proceed to Viewnum = 1
	}

	if args.Me == vs.currview.Primary {
		// if the incoming Ping(X'): its X' is larger than our view bound X
		// e.g., in the test case #2: Ping(1) from ck1: then 0 < 1
		// received a Ping(1) from the primary ck1 => can later proceed to Viewnum = 2
		if vs.viewBound < args.Viewnum {
			vs.viewBound = args.Viewnum
		}
		// Hint #6: the viewservice needs a way to detect that
		// a primary or backup has failed and re-started.
		// Therefore, we set that when a server re-starts after a crash,
		// it should send one or more Pings with an argument of zero to
		// inform the view service that it crashed.
		if args.Viewnum == 0 { // just got crashed and restarted
			vs.replace(args.Me) // force replace
		}
	} else if args.Me == vs.currview.Backup {
		// same as above.
		if args.Viewnum == 0 { // just got crashed and restarted
			vs.replace(args.Me) // force replace
		}
	} else {
		// an idle server comes in.
		vs.assignRole(args.Me)
	}

	reply.View = *vs.currview
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	if vs.currview != nil {
		reply.View = *vs.currview
	}
	return nil
}

// edited by Adrian
func (vs *ViewServer) backupByIdleSrv() {
	// only when idleServers exists will the backup be filled in
	if len(vs.idleServers) > 0 {
		for key, _ := range vs.idleServers {
			vs.currview.Backup = key // backup will be set
			delete(vs.idleServers, key) // to keep the size of map
			break
		}
	}
}

// edited by Adrian
func (vs *ViewServer) replace(k string) {
	// IMPORTANT!
	// the view service may NOT proceed from view X to view X + 1
	// if it has not received a Ping(X) from the primary of the view X

	// vs.viewBound is the latest view number of which the current primary
	// has already send back an ack to the view service
	// e.g., viewBound = 6 means that the current primary ck_i has sent a
	// Ping(6) to the view service successfully. the View {p=cki, b=_, n=6} is acked

	// if current view's Viewnum in view service = 6, then 6 + 1 > 6
	// so you can do the vs.replace(k) and the Viewnum of vs will be 7
	// however, if current view's Viewnum in the vs = 7, then 6 + 1 > 7 doesn't hold
	// so you CANNOT do the replacement even though many rounds of tick() may have passed

	// X = 6, X+1 = 7:
	// the vs CANNOT proceed from view 7 to view 8 as it has not received a Ping(7)
	// from the primary of the view X. the current viewBound is still 7
	// see testcase: `Viewserver waits for primary to ack view`
	if vs.viewBound + 1 > vs.currview.Viewnum {

		if k == vs.currview.Primary {
			// if k is the current primary -> remove this primary
			vs.currview.Primary = vs.currview.Backup
			vs.currview.Backup = ""
			vs.backupByIdleSrv()
			vs.currview.Viewnum += 1
		} else if k == vs.currview.Backup {
			// if k is the current backup -> remove this backup
			vs.currview.Backup = ""
			vs.backupByIdleSrv()
			vs.currview.Viewnum += 1
		} // if k is neither of both -> we don't do anything
	} else {
		log.Printf("cannot change view: current view not yet acked by primary:\n" +
			"viewBound=%v, vs.currview.Viewnum=%v", vs.viewBound, vs.currview.Viewnum)
	}
}

// edited by Adrian
func (vs *ViewServer) assignRole(me string) {

	// ack rule: same idea as the `replace()` function
	if vs.viewBound + 1 > vs.currview.Viewnum {
		// the current ping is from an arbitrary server (not primary, nor backup)
		// new server has joined! what job should it do? primary? backup? or idle?
		if vs.currview.Primary == "" {
			vs.currview.Primary = me
			vs.currview.Viewnum += 1
		} else if vs.currview.Backup == "" {
			vs.currview.Backup = me
			vs.currview.Viewnum += 1
		} else {
			vs.idleServers[me] = true
			// do not add the viewnum
		}
	} else {
		log.Printf("cannot change view: current view not yet acked by primary:\n " +
			"viewBound=%v, vs.currview.Viewnum=%v", vs.viewBound, vs.currview.Viewnum)
	}
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	// Your code here.

	// Hint #4: your viewservice needs to make periodic decisions,
	// for example to promote the backup if the viewservice has missed
	// DeadPings pings from the primary.
	for k, v := range vs.recentHeard {
		// if current time time.Now() > (recentHeard time + some timeout)
		// then we need to replace this server `k`
		if time.Now().After(v.Add(DeadPings * PingInterval)) {
			vs.replace(k)
		}
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.currview = nil
	vs.recentHeard = make(map[string]time.Time)
	vs.viewBound = 0
	vs.idleServers = make(map[string]bool)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
