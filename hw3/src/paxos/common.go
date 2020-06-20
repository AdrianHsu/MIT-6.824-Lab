package paxos
// added by Adrian

type Err string


type PrepareArgs struct {
	Seq         int
}

type PrepareReply struct {
	Err         Err
	N           int
	N_a         int
	V_a         interface{}
}

type AcceptArgs struct {
	Seq         int
	V_p         interface{}
}

type AcceptReply struct {
	Err         Err
	N           int
}

type DecidedArgs struct {
	N           int
	V_p          interface{}
}

type DecidedReply struct {
	Err         Err
}