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
	currview *View
	recentHeard map[string] time.Time
	// keep track of whether the primary for the current view has acked it
	viewBound uint // last value X of the primary Ping(X)


	idleServers map[string] bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.recentHeard[args.Me] = time.Now()

	// the view service may NOT proceed from view X to view X + 1
	// if it has not received a Ping(X) from the primary of the view X

	if vs.currview == nil { // init, Ping(0) from ck1
		// as it now received a Ping(0) from primary => can proceed to Viewnum = 1
		vs.viewBound = 0 // X is now 0
		vs.currview = &View{0, "", ""}
	}

	if args.Me == vs.currview.Primary {
		// if the incoming Ping(X') its X' is larger than our view bound X
		if vs.viewBound < args.Viewnum {
			// e.g., Ping(1) from ck1: then 0 < 1
			// received a Ping(1) from the primary ck1 => can proceed to Viewnum = 2
			vs.viewBound = args.Viewnum
		}
		if args.Viewnum == 0 { // just got crashed and restarted
			vs.replace(args.Me)
		}

	} else if args.Me == vs.currview.Backup {
		if vs.viewBound < args.Viewnum { // do not change the view
			//log.Printf("%v, %v", vs.viewBound, args.Viewnum)

		} else {

		}
	} else {
		if vs.viewBound < args.Viewnum {
			//log.Printf("%v", vs.viewBound)
		} else {
			if vs.currview.Primary == "" {
				vs.currview.Primary = args.Me
				vs.currview.Viewnum += 1
			} else if vs.currview.Backup == "" {
				vs.currview.Backup = args.Me
				vs.currview.Viewnum += 1
			} else {
				vs.idleServers[args.Me] = true
			}
		}
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

// crafted by Adrian
func (vs *ViewServer) replace(k string) {
	if k == vs.currview.Primary {
		vs.currview.Primary = vs.currview.Backup
		vs.currview.Backup = ""
		if len(vs.idleServers) > 0 {
			for key, _ := range vs.idleServers {
				vs.currview.Backup = key
				delete(vs.idleServers, key)
				break
			}
		}
		vs.currview.Viewnum += 1
	} else if k == vs.currview.Backup {
		vs.currview.Backup = ""
		if len(vs.idleServers) > 0 {
			for key, _ := range vs.idleServers {
				vs.currview.Backup = key
				delete(vs.idleServers, key)
				break
			}
		}
		vs.currview.Viewnum += 1
	}
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	for k, v := range vs.recentHeard {
		if time.Now().After(v.Add(DeadPings * PingInterval)) {

			//log.Printf("%v, %v" , vs.viewBound + 1, vs.currview.Viewnum)
			if vs.viewBound + 1 > vs.currview.Viewnum {
				vs.replace(k)
			}
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
