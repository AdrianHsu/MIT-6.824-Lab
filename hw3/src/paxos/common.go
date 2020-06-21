package paxos
// added by Adrian

type Err string


type PrepareArgs struct {
	Seq         int
	N           int
	Z_i         int
	Proposer    int
}

type PrepareReply struct {
	Err         Err
	N           int
	N_a         int
	V_a         interface{}
}

type AcceptArgs struct {
	Seq         int
	N           int
	V_p         interface{} // v prime
}

type AcceptReply struct {
	Err         Err
	N           int
}

type DecidedArgs struct {
	Seq           int
	N             int
	V_p           interface{}
}

type DecidedReply struct {
	Err         Err
}

type ForgetArgs struct {
	Z_i         int
}

type ForgetReply struct {
	Err         Err
}