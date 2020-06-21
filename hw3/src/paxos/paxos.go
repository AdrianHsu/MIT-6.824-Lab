package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"math"
	"net"
)
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"


// added by Adrian
type Instance struct { // key: n_a, value: v_a
	fate        Fate
	n_p         int
	n_a         int
	v_a         interface{}
}

type AcceptedProposal struct {
	n            int
	n_a          int
	v_a          interface{}
}

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	instances  sync.Map
	doneValues sync.Map
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// added by Adrian
// proposer(v)
func (px *Paxos) ProposerPropose(seq int, v interface{}) {

	var N = 1 << px.me
	var vp = v // this v doesn't matter though
	var decided = false

	for !decided {
		var reachMajority = false
		vp = v
		reachMajority, vp = px.ProposerPrepare(N, seq, v)
		if !reachMajority {
			N += 1
			continue
		}

		px.Forget(px.Min())
		//log.Printf("proposer is %v. reach majority for [prepare]. seq is %v, N is %v, value is: %v", px.me, seq, N, vp)

		if px.ProposerAccept(N, seq, vp) == false {
			N += 1
			continue
		}
		//log.Printf("proposer is %v. reach majority for [accept]. seq is %v, N is %v, value is: %v", px.me, seq, N, vp)
		if px.ProposerDecided(N, seq, vp) == false {

		}
		//log.Printf("proposer is %v. reach majority for [decided]. seq is %v, N is %v, value is: %v", px.me, seq, N, vp)
		decided = true
	}
}
// added by Adrian
func (px *Paxos) ProposerPrepare(N int, seq int, v interface{}) (bool, interface{}) {

	var count = 0
	var max_n_a = -1
	var v_prime = v
	for i, peer := range px.peers {
		args := &PrepareArgs{seq, N}
		var reply PrepareReply
		var ok = false
		if i == px.me {
			px.AcceptorPrepare(args, &reply)
			ok = true
		} else {
			ok = call(peer, "Paxos.AcceptorPrepare", args, &reply)
		}
		if ok && reply.Err == "" {
			//log.Printf("me is %v. peer %v prepare_ok: N is %v", px.me, peer, reply.N)
			count += 1
			if reply.N_a > max_n_a {
				max_n_a = reply.N_a
				v_prime = reply.V_a
			}
			px.Update(reply.Z_i, i)
		} else {
			//log.Printf("no reply. me: %v, peer: %v", px.me, peer)
		}
	}
	if count < (len(px.peers) + 1)/ 2 {
		return false, 0
	}
	return true, v_prime
}

// added by Adrian
func (px *Paxos) ProposerAccept(N int, seq int, vp interface{}) bool {
	var count = 0
	for i, peer := range px.peers {
		args :=	&AcceptArgs{seq, N, vp}
		var reply AcceptReply
		var ok = false
		if i == px.me {
			px.AcceptorAccept(args, &reply)
			ok = true
		} else {
			ok = call(peer, "Paxos.AcceptorAccept", args, &reply)
		}
		if ok && reply.Err == "" {
			//log.Printf("me is %v. peer %v accept_ok", px.me, peer)
			count += 1
		}
	}
	if count < (len(px.peers) + 1)/ 2 {
		return false
	}
	return true
}

func (px *Paxos) ProposerDecided(N int, seq int, vp interface{}) bool {
	var count = 0
	for i, peer := range px.peers {
		args :=	&DecidedArgs{seq,N, vp}
		var reply DecidedReply
		var ok = false
		if i == px.me {
			px.AcceptorDecided(args, &reply)
			ok = true
		} else {
			ok = call(peer, "Paxos.AcceptorDecided", args, &reply)
		}
		if ok {
			//log.Printf("me is %v. peer %v decided_ok", px.me, peer)
			count += 1
		} else {
			// So if a peer is dead
			// or unreachable, other peers Min()s will not increase
			// even if all reachable peers call Done.
		}
	}
	if count < (len(px.peers) + 1)/ 2 {
		return false
	}
	return true
}

// added by Adrian
// acceptor's prepare(n) handler
func (px *Paxos) AcceptorPrepare(args *PrepareArgs, reply *PrepareReply) error {

	reply.N = args.N
	ins, _ := px.instances.LoadOrStore(args.Seq, &Instance{fate: Pending, n_p: -1, n_a: -1, v_a: nil})
	inst := ins.(*Instance)
	if args.N > inst.n_p {
		px.instances.Store(args.Seq, &Instance{fate: Pending, n_p: args.N, n_a: inst.n_a, v_a: inst.v_a})
		var doneValue, _ = px.doneValues.Load(px.me)
		reply.Z_i = doneValue.(int)
		reply.N_a = inst.n_a
		reply.V_a = inst.v_a
	} else {
		reply.Err = "1"
	}
	return nil
}

// added by Adrian
func (px *Paxos) AcceptorAccept(args *AcceptArgs, reply *AcceptReply) error {

	reply.N = args.N
	ins, _ := px.instances.LoadOrStore(args.Seq, &Instance{fate: Pending, n_p: -1, n_a: -1, v_a: nil})
	inst := ins.(*Instance)
	if args.N >= inst.n_p {
		px.instances.Store(args.Seq, &Instance{fate: Pending, n_p: args.N, n_a: args.N, v_a: args.V_p})
	} else {
		reply.Err = "2"
	}

	return nil
}

// added by Adrian
func (px *Paxos) AcceptorDecided(args *DecidedArgs, reply *DecidedReply) error {

	//log.Printf("peer %v decided new seq: %v, value: %v", px.me, args.Seq, args.V_p)
	px.instances.Store(args.Seq, &Instance{Decided, args.N, args.N, args.V_p})
	return nil
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.

	go func(seq int, v interface{}) {
		if seq < px.Min() {
			return
		}
		px.ProposerPropose(seq, v)
	}(seq, v)

}

// added by Adrian
func (px *Paxos) Update(z_i int, i int) {
	old, _ := px.doneValues.Load(i)
	if z_i > old.(int) {
		px.doneValues.Store(i, z_i)
	}
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.

	old, _ := px.doneValues.Load(px.me)
	if seq > old.(int) {
		px.doneValues.Store(px.me, seq)
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	var max = -1
	px.instances.Range(func(k, v interface{}) bool { // seq is the key
		if k.(int) > max {
			max = k.(int)
		}
		return true
	})

	return max
}
// added by Adrian
func (px *Paxos) Forget(min int) error {
	sli := []int{}
	px.instances.Range(func(k, v interface{}) bool { // seq is the key
		if k.(int) < min {
			sli = append(sli, k.(int))
		}
		return true
	})

	for k := range sli {
		px.instances.Delete(k)
	}
	return nil
}
//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peer's z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefore cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	var min = math.MaxInt32
	px.doneValues.Range(func(k, v interface{}) bool { // seq is the key
		if v.(int) < min {
			min = v.(int)
		}
		return true
	})

	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	if seq < px.Min() {
		return Forgotten, 0
	}
	ins, ok := px.instances.Load(seq)
	if ok {
		return ins.(*Instance).fate, ins.(*Instance).v_a
	} else {
		return Pending, 0
	}
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.instances = sync.Map{}
	px.doneValues = sync.Map{}

	for peer := range px.peers {
		px.doneValues.Store(peer, -1)
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}
