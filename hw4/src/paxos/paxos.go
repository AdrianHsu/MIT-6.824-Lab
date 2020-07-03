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
	"time"
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


///// PROPOSER PART //////

// added by Adrian
// main proposer function. The proposer has only 1 function.
func (px *Paxos) ProposerPropose(seq int, v interface{}) {

	var decided = false
	// Hint: The px.me value will be different in each Paxos peer,
	// so you can use px.me to help ensure that proposal numbers are unique.
	var N = 1 << uint(px.me + 20)
	now := time.Now()
	for !decided {
		//log.Printf("propose! proposer is %v. seq is %v, N is %v, v is %v", px.me, seq, N, v)

		// You should call px.isdead() in any loops you have that might run
		// for a while, and break out of the loop if px.isdead() is true.
		if px.isdead() || time.Now().Sub(now).Seconds() > time.Duration(30*time.Second).Seconds() {
			//log.Printf("isdead, killed: %v", px.peers[px.me])
			px.Kill()
			return
		}
		var vp = v // initialization. this v doesn't matter though
		var reachMajority = false
		var highest_n = N

		// ======== Prepare Phase ========
		reachMajority, vp, highest_n = px.ProposerPrepare(N, seq, v)
		if !reachMajority {
			// did not satisfy the majority. re-do the prepare phase
			// case 1. network unreachable -> e.g., 2 out of 5 are replied. but in vein.
			// case 2. those peers have already accepted a higher N_p from other proposers.
			// If it were case 2 -> find out who is the proposer that has committed to those peers
			id := px.CommittedToWhom(highest_n)
			N = highest_n + 1

			// Race condition prevent liveness
			// Solution: back off period chosen based on ordering
			// if the current committed proposer's id is larger than mine
			// -> "alright, I will back off and wait for it!"
			// else we re-do the prepare phase immediately
			if id > px.me {
				// the wait time depends on the order of each peer
				// px.me is srv 0: wait for 2 second
				// px.me is srv 1: wait for 3 second
				// px.me is srv 4: wait for 5 second
				waitTime := 500 * ( px.me + 2 )
				time.Sleep(time.Millisecond * time.Duration(waitTime))
			}

			// re-do the prepare phase
			continue
		}
		// It's OK for your Paxos to piggyback the Done value in the agreement protocol packets;
		// that is, it's OK for peer P1 to only learn P2's latest Done value the next time
		// that P2 sends an agreement message to P1
		// Therefore, my design was that P2 (proposer) will learn P1 (all other acceptors) latest Done value
		// in the prepare phase. Now we can do the memory freeing.
		px.Forget(px.Min())

		//log.Printf("proposer is %v. reach majority for [prepare]. seq is %v, N is %v, h_n is %v, " +
		//	"vp is %v", px.me, seq, N, highest_n, vp)

		// ======== Accept Phase ========

		if px.ProposerAccept(N, seq, vp) == false {
			// if we failed the accept phase -> re-start from the prepare phase
			time.Sleep(time.Millisecond * 100)
			continue
		}
		//log.Printf("proposer is %v. reach majority for [accept]. seq is %v, N is %v, vp is %v", px.me, seq, N, vp)

		// ======== Decided Phase ========

		// try `decided` for a few times. If not work then we give up. -> weird.
		// bc that sometimes the decided will never be able to reach every body
		tryTimes := 0
		for px.ProposerDecided(N, seq, vp) == false && tryTimes < 10 {
			tryTimes += 1
			time.Sleep(time.Millisecond * 100)
		}
		px.ProposerDecided(N, seq, vp)
		//log.Printf("proposer is %v. reach majority for [decided]. seq is %v, N is %v, vp is %v", px.me, seq, N, vp)
		decided = true
	}
}

// added by Adrian
// now that in the prepare phase, this proposal didn't satisfy the majority.
// we want to find out who is the precedent proposer.
func (px *Paxos) CommittedToWhom(N int) int {
	var N0 = uint(N) >> 20
	return int(math.Log2(float64(N0)))
}

// added by Adrian
func (px *Paxos) ProposerPrepare(N int, seq int, v interface{}) (bool, interface{}, int) {

	var count = 0
	var max_n_a = -1
	var v_prime = v
	var highest_n = N
	var wg sync.WaitGroup
	for i, peer := range px.peers {

		wg.Add(1)
		go func(wg *sync.WaitGroup, i int, peer string, N int, seq int,
			v interface{}, count *int, max_n_a *int, v_prime *interface{}, highest_n *int) {
			defer wg.Done()
			args := &PrepareArgs{seq, N}
			var reply PrepareReply
			var ok = false
			if i == px.me {
				// Hint: in order to pass tests assuming unreliable network,
				// your paxos should call the local acceptor through a function call rather than RPC.
				px.AcceptorPrepare(args, &reply)
				ok = true
			} else {
				ok = call(peer, "Paxos.AcceptorPrepare", args, &reply)
			}

			if ok && reply.Err == "" {
				//log.Printf("me is %v. peer %v prepare_ok: N is %v", px.me, peer, reply.N)
				px.mu.Lock()
				*count += 1
				// with an aim to find out the highest n_a and its v_a
				if reply.N_a > *max_n_a {
					//log.Printf("change va: %v, %v, %v", px.me, reply.N_a, reply.V_a)
					*max_n_a = reply.N_a
					*v_prime = reply.V_a
				}
				// piggyback: update the latest Done value of Peer i for my local `doneValues`
				px.Update(reply.Z_i, i)
				px.mu.Unlock()
			} else {
				// log.Printf("prepare failed. me: %v, peer: %v", px.me, peer)
				// case 1: network failure (reply.Err should be nothing)
				// case 2: the acceptor has already accepted someone else's proposal (prepare_reject)
				// if it were case 2, we update the highest N that we've seen now.
				px.mu.Lock()
				if reply.Higher_N > *highest_n {
					*highest_n = reply.Higher_N
				}
				px.mu.Unlock()
			}

		}(&wg, i, peer, N, seq, v, &count, &max_n_a, &v_prime, &highest_n)
	}

	wg.Wait()
	//log.Printf("final va: %v, %v, %v", px.me, max_n_a, v_prime)

	// no majority is satisfied
	if count < (len(px.peers) + 1)/ 2 {
		return false, 0, highest_n
	}
	return true, v_prime, highest_n
}

// added by Adrian
func (px *Paxos) ProposerAccept(N int, seq int, vp interface{}) bool {
	// pretty similar to ProposerPrepare()
	var count = 0

	var wg sync.WaitGroup
	for i, peer := range px.peers {
		wg.Add(1)

		go func(wg *sync.WaitGroup, i int, peer string, N int, seq int, vp interface{}, count *int) {
			defer wg.Done()
			args := &AcceptArgs{seq, N, vp}
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
				px.mu.Lock()
				*count += 1
				px.mu.Unlock()
			} else {
				// case 1. network unreachable
				// case 2. while we are doing our accepting, someone perform prepare() with
				// a higher n_p and thus we got an accept_reject

				// e.g.,
				// Acceptor 1: P1    A1-X
				// Acceptor 2: P1 P2 A1-X (this `A1-X` failed as it has already accepted a higher n_p from P2)
				// Acceptor 3:    P2
			}
		}(&wg, i, peer, N, seq, vp, &count)
	}
	wg.Wait()
	if count < (len(px.peers) + 1)/ 2 {
		return false // no majority
	}
	return true
}

// added by Adrian
func (px *Paxos) ProposerDecided(N int, seq int, vp interface{}) bool {
	var count = 0
	var wg sync.WaitGroup
	for i, peer := range px.peers {
		wg.Add(1)
		go func(wg *sync.WaitGroup, i int, peer string, N int, seq int, vp interface{}, count *int) {
			defer wg.Done()
			args := &DecidedArgs{seq, N, vp}
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
				px.mu.Lock()
				*count += 1
				px.mu.Unlock()
			} else {
				// network unreachable
			}
		}(&wg, i, peer, N, seq, vp, &count)
	}
	wg.Wait()
	// taken from lecture #9 slides. (potential approach)
	// proposer who has proposal accepted by majority of acceptors informs all learners
	if count < len(px.peers) {
		return false
	}
	return true
}

///// ACCEPTOR PART //////

// added by Adrian
// acceptor's prepare(n) handler
func (px *Paxos) AcceptorPrepare(args *PrepareArgs, reply *PrepareReply) error {

	reply.N = args.N
	// if the instance[seq] has been created -> we load it
	// if not -> we create a new one
	ins, _ := px.instances.LoadOrStore(args.Seq, &Instance{fate: Pending, n_p: -1, n_a: -1, v_a: nil})
	inst := ins.(*Instance)

	if args.N > inst.n_p {
		// for storing this instance, we should NOT put the previous fate into it. just put a `Pending`
		// why? because we still want to perform the whole paxos process normally

		px.instances.Store(args.Seq, &Instance{fate: Pending, n_p: args.N, n_a: inst.n_a, v_a: inst.v_a})
		var doneValue, _ = px.doneValues.Load(px.me)
		reply.Z_i = doneValue.(int) // to restore the Done() value
		reply.N_a = inst.n_a
		reply.V_a = inst.v_a
		//reply.Higher_N = args.N // give it a default value
	} else {
		// a proposer needs a way to choose a higher proposal number than any seen so far.
		// It may also be useful for the propose RPC handler to return the highest known proposal number
		// if it rejects an RPC, to help the caller pick a higher one next time.
		reply.Higher_N = inst.n_p
		reply.Err = "1"
	}
	//log.Printf("acceptor prepare: me is %v, seq is %v, n is %v, n_p is %v, v is %v", px.me, args.Seq,
	//	args.N, inst.n_p, inst.v_a)
	return nil
}

// added by Adrian
// acceptor's accept(n, v) handler
func (px *Paxos) AcceptorAccept(args *AcceptArgs, reply *AcceptReply) error {
	reply.N = args.N
	// it is possible that the acceptor hasn't performed the AcceptorPrepare
	// and yet directly jumps into the AcceptorAccept() function
	// e.g.,
	// Acceptor 1 (me):  x A1-X
	// Acceptor 2     : P1 A1-X
	// Acceptor 3     : P1 A1-X
	// says, the acceptor was encountering network failure when proposer 1 got the majority for A2, A3
	// and then later the Proposer 1 will for sure send accept() to all the peers including A1 (me)

	// so what should we do? just give the instance slot a default value
	ins, _ := px.instances.LoadOrStore(args.Seq, &Instance{fate: Pending, n_p: -1, n_a: -1, v_a: nil})

	inst := ins.(*Instance)

	if args.N >= inst.n_p {
		// give it `Decided` here instead of making it Pending
		px.instances.Store(args.Seq, &Instance{fate: Pending, n_p: args.N, n_a: args.N, v_a: args.V_p})
	} else {
		reply.Err = "2"
	}
	//log.Printf("acceptor accept: me is %v, seq is %v, n is %v, n_p is %v, v is %v", px.me, args.Seq,
	//	args.N, inst.n_p, inst.v_a)
	return nil
}

// added by Adrian
// acceptor's decided(v) handler
func (px *Paxos) AcceptorDecided(args *DecidedArgs, reply *DecidedReply) error {

	// to make sure that some acceptors may just be cut off and not yet heard this.
	// so we did exactly the same store() as `AcceptorAccept()` here for these learners to learn
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
		// If Start() is called with a sequence number less than Min(),
		// the Start() call should be ignored.
		if seq < px.Min() {
			return
		}
		px.ProposerPropose(seq, v)
	}(seq, v)

}

// added by Adrian
// helper function for Done()
// update the doneValues for px.me's doneValues[peer i]
// it will put the *largest* one as the done value
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
	px.Update(seq, px.me)
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
// remove those instances whose seq is less than `min`
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
	// find out the smallest done value **locally**,
	// i.e., we check those done values in my cache instead of
	// asking everyone else
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

	// If Status() is called with a sequence number less than Min(),
	// Status() should return Forgotten.
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
