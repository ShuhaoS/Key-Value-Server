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
// px.Start(Seq int, v interface{}) -- start agreement on new instance
// px.Status(Seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(Seq int) -- ok to forget all instances <= Seq
// px.Max() int -- highest instance Seq known, or -1
// px.Min() int -- instances before this Seq have been forgotten
//

import (
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

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending    // not yet decided.
	Forgotten  // decided but forgotten.
)

type PaxosInstance struct {
	N_p  int
	N_a  int
	V_a  interface{}
	fate Fate
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // Index into peers[]

	// Your data here.
	instances map[int]*PaxosInstance
	dones     []int
	forgotten int
	highest_N map[int]int
}

//Your RPC implementations and definitions
type PrepArgs struct {
	Seq int
	N   int
	//piggyback
	Me   int
	Done int
}

type PrepReply struct {
	Ok  bool
	N_p int
	N_a int
	V_a interface{}
}

type AcceptArgs struct {
	Seq int
	N   int
	V   interface{}
}

type AcceptReply struct {
	Ok bool
	N  int
}

type DecidedArgs struct {
	Seq int
	N   int
	V   interface{}
}

type DecidedReply struct {
}

func (px *Paxos) majorityCount() int {
	return int(len(px.peers)/2 + 1)
}

func (px *Paxos) generateProposeN(seq int) int {
	px.mu.Lock()
	defer px.mu.Unlock()

	N, ok := px.highest_N[seq]
	if ok {
		return N + 20 + px.me
	} else {
		px.highest_N[seq] = 0
		return 0
	}
	//return px.instances[seq].N_p + 50 + px.me
}

func (px *Paxos) Prep(args *PrepArgs, reply *PrepReply) error {
	px.mu.Lock()

	instance, ok := px.instances[args.Seq]
	if ok {
		if args.N > instance.N_p {
			instance.N_p = args.N
			reply.Ok = true
		} else {
			reply.Ok = false
		}
		reply.N_p = instance.N_p
		reply.N_a = instance.N_a
		reply.V_a = instance.V_a
	} else {
		instance := PaxosInstance{
			N_p:  args.N,
			N_a:  -1,
			V_a:  nil,
			fate: Pending,
		}
		px.instances[args.Seq] = &instance
		reply.Ok = true
		reply.N_p = args.N
		reply.N_a = -1
		reply.V_a = nil
	}

	//piggyback the forgetting info
	px.dones[args.Me] = args.Done
	px.mu.Unlock()
	px.Forget(args.Done)

	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	instance, ok := px.instances[args.Seq]
	if !ok {
		return nil
	}
	if args.N >= instance.N_p {
		instance.N_p = args.N
		instance.N_a = args.N
		instance.V_a = args.V
		reply.Ok = true
		reply.N = args.N
	} else {
		reply.Ok = false
		reply.N = instance.N_p
	}

	return nil
}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	instance, ok := px.instances[args.Seq]
	if ok {
		instance.fate = Decided
		instance.V_a = args.V
		instance.N_p = args.N
		instance.N_a = args.N
	} else {
		instance := PaxosInstance{
			fate: Decided,
			V_a:  args.V,
			N_p:  args.N,
			N_a:  args.N,
		}
		px.instances[args.Seq] = &instance
	}

	return nil
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

//
// the application wants paxos to start agreement on
// instance Seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	if seq < px.Min() {
		return
	}

	go func() {
		for {
			status, _ := px.Status(seq)
			if status == Decided {
				return
			}
			n := px.generateProposeN(seq)
			//send prepare rpcs
			prep_count := 0 // for majority checking of prepare
			V_to_send := v  //v', v_a for highest n_a, argument for further accept
			max_n_a := -1   // highest n_a received
			for i, peer := range px.peers {
				//make piggyback info
				px.mu.Lock()
				me := px.me
				done := px.dones[me]
				px.mu.Unlock()
				//make args and reply
				args := PrepArgs{
					Seq: seq,
					N:   n,
					//piggyback info
					Me:   me,
					Done: done,
				}
				reply := PrepReply{}
				//send rpc
				if i == me {
					px.Prep(&args, &reply)
				} else {
					call(peer, "Paxos.Prep", args, &reply)
				}
				if reply.Ok {
					prep_count += 1
					if reply.N_a > max_n_a {
						max_n_a = reply.N_a
						V_to_send = reply.V_a
					}
				} else {
					//store returned highest N for further generating N
					px.mu.Lock()
					if px.highest_N[seq] < reply.N_p {
						px.highest_N[seq] = reply.N_p
					}
					px.mu.Unlock()
				}
			}
			//check majority
			if prep_count < px.majorityCount() {
				continue
			}

			//send accept rpcs
			accept_count := 0 //for majority checking of accept
			for i, peer := range px.peers {
				args := AcceptArgs{
					Seq: seq,
					N:   n,
					V:   V_to_send,
				}
				reply := AcceptReply{}
				ok := true
				if i == px.me {
					px.Accept(&args, &reply)
				} else {
					ok = call(peer, "Paxos.Accept", args, &reply)
				}
				if ok && reply.Ok {
					accept_count++
				} else {
				}
			}
			// check if majority accepts
			if accept_count < px.majorityCount() {
				continue
			}

			// send decided rpc
			for i, peer := range px.peers {
				args := DecidedArgs{
					Seq: seq,
					N:   n,
					V:   V_to_send,
				}
				reply := DecidedReply{}
				if i == px.me {
					px.Decided(&args, &reply)
				} else {
					call(peer, "Paxos.Decided", args, &reply)
				}
			}
			return
		}
	}()
	return
}

//
// the application on this machine is Done with
// all instances <= Seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	//mark the done value
	px.mu.Lock()

	if seq > px.dones[px.me] {
		px.dones[px.me] = seq
	}
	px.mu.Unlock()

	px.Forget(seq)

}

//Add a function to do forgetting, should be called when px.dones are changed
func (px *Paxos) Forget(seq int) {
	min := px.Min()

	px.mu.Lock()
	defer px.mu.Unlock()

	if seq <= px.forgotten {
		return
	} else {
		if min <= seq {
			return
		} else {
			px.forgotten = min-1
			for i, _ := range px.instances {
				if i < min {
					delete(px.instances, i)
					_, ok := px.highest_N[i]
					if ok {
						delete(px.highest_N, i)
					}
				}
			}
			return
		}
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()

	maxSeq := 0
	for seq, _ := range px.instances {
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	return maxSeq
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
// missed -- the other peers therefor cannot forget these
// instances.
//

func (px *Paxos) Min() int {

	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	min := -1

	//get min of dones if exists
	for _, v := range px.dones {
		if v == -1 {
			//if not heard from all peers, no instance can be forgotten
			return 0
		} else {
			if min == -1 {
				min = v
			} else {
				if min > v {
					min = v
				}
			}
		}
	}
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
		return Forgotten, nil
	}

	px.mu.Lock()
	defer px.mu.Unlock()

	instance, ok := px.instances[seq]

	if !ok {
		return Pending, nil
	}
	return instance.fate, instance.V_a

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
	px.instances = make(map[int]*PaxosInstance)
	px.dones = make([]int, len(peers), len(peers))
	px.forgotten = -1
	for i := 0; i < len(px.dones); i++ {
		px.dones[i] = -1
	}
	px.highest_N = make(map[int]int)

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
