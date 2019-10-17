package kvpaxos

import (
	"net"
	"time"
)
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	ID    int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	lock         sync.Mutex
	data         map[string]string
	putAppendIDs map[int64]struct{}
	seq          int
}

func (kv *KVPaxos) applyOp(op Op) {
	if op.Op == "Get" {

	} else {
		_, duplicate := kv.putAppendIDs[op.ID]
		if duplicate {
			return
		}
		kv.putAppendIDs[op.ID] = struct{}{}

		if op.Op == "Put" {
			kv.data[op.Key] = op.Value
		} else { // Append
			oldValue, ok := kv.data[op.Key]
			if !ok {
				oldValue = ""
			}
			kv.data[op.Key] = oldValue + op.Value
		}
	}

	kv.px.Done(kv.seq)
	kv.seq++
}

// create a new Op in Paxos instances
func (kv *KVPaxos) createOp(op Op) {
	for {
		var decidedOp Op
		status, v := kv.px.Status(kv.seq)
		if status == paxos.Decided {
			decidedOp = v.(Op)
		} else {
			kv.px.Start(kv.seq, op)

			// wait to be decided
			to := 10 * time.Millisecond
			for {
				status, v := kv.px.Status(kv.seq)
				if status == paxos.Decided {
					decidedOp = v.(Op)
					break
				}
				time.Sleep(to)
				if to < 10*time.Second {
					to *= 2
				}
			}
		}

		kv.applyOp(decidedOp)
		if decidedOp.ID == op.ID {
			return
		}
	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.lock.Lock()
	defer kv.lock.Unlock()
	kv.createOp(Op{
		Key: args.Key,
		Op:  "Get",
		ID:  args.ID,
	})
	// get value
	value, ok := kv.data[args.Key]
	if !ok {
		reply.Err = ErrNoKey
		return nil
	}
	reply.Err = OK
	reply.Value = value
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.lock.Lock()
	defer kv.lock.Unlock()
	_, duplicate := kv.putAppendIDs[args.ID]
	if !duplicate {
		kv.createOp(Op{
			Key:   args.Key,
			Value: args.Value,
			Op:    args.Op,
			ID:    args.ID,
		})
	}
	reply.Err = OK
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.lock = sync.Mutex{}
	kv.putAppendIDs = make(map[int64]struct{})
	kv.data = make(map[string]string)
	kv.seq = 1

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
