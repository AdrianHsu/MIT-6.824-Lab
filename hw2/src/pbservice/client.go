package pbservice

import (
	"time"
	"viewservice"
)
import "net/rpc"
import "fmt"

import "crypto/rand"
import "math/big"


type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	currPrimary string
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	ck.currPrimary = ""

	return ck
}


//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {


	// Your code here.
	// if the current view has no primary, keep asking vs until a primary shows up
	for ck.currPrimary == "" {
		view, _ := ck.vs.Get()
		ck.currPrimary = view.Primary
	}

	args := &GetArgs{key}
	var reply GetReply
	ok := false
	for ok == false {
		ok := call(ck.currPrimary, "PBServer.Get", args, &reply)
		if ok {
			break
		} else {
			// if the current primary is dead OR it doesn't think itself as the primary
			time.Sleep(viewservice.PingInterval)
			view, _ := ck.vs.Get()
			ck.currPrimary = view.Primary
		}
	}
	return reply.Value
}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	// Your code here.
	// if the current view has no primary, keep asking vs until a primary shows up
	for ck.currPrimary == "" {
		view, _ := ck.vs.Get()
		ck.currPrimary = view.Primary
	}

	// nrand(): make the k/v service can detect duplicates.
	args := &PutAppendArgs{key, value, op, nrand()}
	var reply PutAppendReply

	ok := false
	// clients keep re-trying until they get an answer.
	for ok == false {
		//log.Printf("%v", ck.currPrimary)
		ok := call(ck.currPrimary, "PBServer.PutAppend", args, &reply)
		//log.Printf("%v, %v", ok, ck.currPrimary)

		if ok {
			// everything works fine
			break
		} else {
			// network failed. retry
			// OR if the current primary is dead
			time.Sleep(viewservice.PingInterval)
			view, _ := ck.vs.Get()
			ck.currPrimary = view.Primary
		}
	}
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
