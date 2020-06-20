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
	"errors"
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
	// acceptor's state
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

	ch := make(chan AcceptedProposal)
	for _, peer := range px.peers {
		go func(peer string, seq int, ch chan AcceptedProposal) {
			args :=	&PrepareArgs{seq}
			var reply PrepareReply
			ok := call(peer, "Paxos.AcceptorPrepare", args, &reply)
			if ok {
				log.Printf("peer %v prepare_ok: seq num is %v", peer, seq)
				ch <- AcceptedProposal{reply.N_a, reply.V_a}
			}
		}(peer, seq, ch)
	}

	var count = 0
	var max_n_a = -1
	var vp = v
	for ap := range ch {
		if ap.n_a > max_n_a {
			log.Printf("%v, %v", ap.n_a, ap.v_a)
			max_n_a = ap.n_a
			vp = ap.v_a
		}
		count += 1
		if count >= (len(px.peers) + 1)/2 { // +1 to deal with odd length
			break
		}
	}
	log.Printf("over majority for promise, value is: %v", vp)

	ch2 := make(chan int)

	for _, peer := range px.peers {
		go func(peer string, seq int, vp interface{}, ch2 chan int) {
			args :=	&AcceptArgs{seq, vp}
			var reply AcceptReply
			ok := call(peer, "Paxos.AcceptorAccept", args, &reply)
			if ok {
				log.Printf("peer %v accept_ok", peer)
				ch2 <- reply.N
			}
		}(peer, seq, vp, ch2)
	}

	count = 0
	for range ch2 {
		count += 1
		if count >= (len(px.peers) + 1)/ 2 {
			break
		}
	}

	for _, peer := range px.peers {
		go func(peer string, seq int, vp interface{}) {
			args :=	&DecidedArgs{seq,vp}
			var reply DecidedReply
			ok := call(peer, "Paxos.AcceptorDecided", args, &reply)
			if ok {
				log.Printf("peer %v decided_ok", peer)
			}
		}(peer, seq, vp)
	}
}
// added by Adrian
// acceptor's prepare(n) handler
func (px *Paxos) AcceptorPrepare(args *PrepareArgs, reply *PrepareReply) error {

	n := args.Seq
	ins, _ := px.instances.LoadOrStore(n, &Instance{fate: Pending, n_p: -1, n_a: -1, v_a: nil})
	inst := ins.(*Instance)
	if n > inst.n_p {
		px.instances.Store(n, &Instance{fate: Pending, n_p: n, n_a: -1, v_a: nil})
		reply.N_a = inst.n_a
		reply.V_a = inst.v_a
	} else {
		text := fmt.Sprintf("PREPARE: rejection %v", n)
		return errors.New(text)
	}
	return nil
}

// added by Adrian
func (px *Paxos) AcceptorAccept(args *AcceptArgs, reply *AcceptReply) error {

	n := args.Seq
	ins, _ := px.instances.Load(n)
	inst := ins.(*Instance) // ins should never be nil
	if n >= inst.n_p {
		reply.N = n
		px.instances.Store(n, &Instance{fate: Pending, n_p: n, n_a: n, v_a: args.V_p})
	} else {
		text := fmt.Sprintf("ACCEPT: rejection %v", n)
		return errors.New(text)
	}

	return nil
}

// added by Adrian
func (px *Paxos) AcceptorDecided(args *DecidedArgs, reply *DecidedReply) error {

	log.Printf("peer %v decided new seq: %v, value: %v", px.me, args.N, args.V_p)

	ins, _ := px.instances.Load(args.N)
	inst := ins.(*Instance)
	px.instances.Store(args.N, &Instance{Decided, inst.n_p, inst.n_a, inst.v_a})
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

	//if seq < px.Min() {
	//	return
	//}
	go func(seq int, v interface{}) {
		px.ProposerPropose(seq, v)
	}(seq, v)

}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
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

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
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
	return 0
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
