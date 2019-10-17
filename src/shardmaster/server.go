package shardmaster

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
import "math"

import "reflect"

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num
	seq int
}


type Op struct {
	// Your data here.
	Op      string
	GID     int64
	Servers []string
	Shard   int
}

func (sm *ShardMaster) applyOp(op Op) {
	if op.Op == "Join" {
		//checkout current configuration and initialize next
		nextConfig := newConfig()
		if len(sm.configs) == 1 {
			nextConfig.Num = 1
			nextConfig.Groups[op.GID] = op.Servers
			for i, _ := range nextConfig.Shards {
				nextConfig.Shards[i] = op.GID
			}
		} else {
			curConfig := sm.configs[len(sm.configs)-1]
			nextConfig.Num = curConfig.Num + 1
			for k, v := range curConfig.Groups {
				nextConfig.Groups[k] = v
			}
			nextConfig.Groups[op.GID] = op.Servers
			//calculate number of shards per group
			serverNShards := int(math.Ceil(float64(NShards) / float64(len(nextConfig.Groups))))
			//count shards for every group: gid -> shard num array
			shardsCounter := make(map[int64][]int)
			for gid, _ := range nextConfig.Groups {
				shardsCounter[gid] = []int{}
			}
			for i := 0; i < NShards; i++ {
				gid := curConfig.Shards[i]
				shardsCounter[gid] = append(shardsCounter[gid], i)
			}
			//recycle and reassign shards to groups
			recycledShards := []int{}
			for k,v := range shardsCounter {
				if len(v) > serverNShards {
					for i := serverNShards; i < len(v); i++ {
						recycledShards = append(recycledShards, v[i])
					}
					shardsCounter[k] = v[:serverNShards]
				}
			}
			for k,v := range shardsCounter {
				if len(v) < serverNShards {
					for i := len(v); i < serverNShards; i++ {
						if len(recycledShards) == 0 {
							break;
						}
						shardsCounter[k] = append(shardsCounter[k], recycledShards[len(recycledShards)-1])
						recycledShards = recycledShards[:len(recycledShards)-1]
					}
				}
				if len(recycledShards) == 0 {
					break
				}
			}
			//get the new config from shardsCounter
			for gid, v := range shardsCounter {
				for _, shardNum := range v {
					nextConfig.Shards[shardNum] = gid
				}
			}
		}
		sm.configs = append(sm.configs, nextConfig)
	} else if op.Op == "Leave" {
		//checkout current configuration and initialize next
		curConfig := sm.configs[len(sm.configs)-1]
		nextConfig := newConfig()
		nextConfig.Num = curConfig.Num + 1
		for k, v := range curConfig.Groups {
			if k != op.GID {
				nextConfig.Groups[k] = v
			}
		}
		//calculate number of shards per group
		serverNShards := int(math.Ceil(float64(NShards) / float64(len(nextConfig.Groups))))
		//count and recycle shards for every group: gid -> shard num array
		shardsCounter := make(map[int64][]int)
		recycledShards := []int{}
		for gid, _ := range nextConfig.Groups {
			shardsCounter[gid] = []int{}
		}
		for i := 0; i < NShards; i++ {
			gid := curConfig.Shards[i]
			if gid == op.GID {
				recycledShards = append(recycledShards, i)
			} else {
				shardsCounter[gid] = append(shardsCounter[gid], i)
			}
		}
		//reassign shards to groups
		for k,v := range shardsCounter {
			if len(v) < serverNShards {
				for i := len(v); i < serverNShards; i++ {
					if len(recycledShards) == 0 {
						break;
					}
					shardsCounter[k] = append(shardsCounter[k], recycledShards[len(recycledShards)-1])
					recycledShards = recycledShards[:len(recycledShards)-1]
				}
			}
			if len(recycledShards) == 0 {
				break
			}
		}
		//get the new config from shardsCounter
		for gid, v := range shardsCounter {
			for _, shardNum := range v {
				nextConfig.Shards[shardNum] = gid
			}
		}
		sm.configs = append(sm.configs, nextConfig)
	} else if op.Op == "Move" {
		//checkout current configuration and initialize next
		curConfig := sm.configs[len(sm.configs)-1]
		nextConfig := newConfig()
		nextConfig.Num = curConfig.Num + 1
		for k, v := range curConfig.Groups {
				nextConfig.Groups[k] = v
		}
		for shardNum, gid := range curConfig.Shards{
			nextConfig.Shards[shardNum] = gid
		}
		nextConfig.Shards[op.Shard] = op.GID
		sm.configs = append(sm.configs, nextConfig)

	} else {

	}
	sm.px.Done(sm.seq)
	sm.seq++

}

// create a new Op in Paxos instances
func (sm *ShardMaster) createOp(op Op) {
	for {
		var decidedOp Op
		status, v := sm.px.Status(sm.seq)
		if status == paxos.Decided {
			decidedOp = v.(Op)
		} else {
			sm.px.Start(sm.seq, op)

			// wait to be decided
			to := 10 * time.Millisecond
			for {
				status, v := sm.px.Status(sm.seq)
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
		sm.applyOp(decidedOp)
		if reflect.DeepEqual(decidedOp, op) {
			return
		}
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.createOp(Op{
		Op: "Join",
		GID: args.GID,
		Servers:args.Servers,
	})
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.createOp(Op{
		Op:"Leave",
		GID: args.GID,
	})
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.createOp(Op{
		Op:"Move",
		GID: args.GID,
		Shard: args.Shard,
	})
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.createOp(Op{
		Op:"Query",
	})
	curNum := sm.configs[len(sm.configs)-1].Num
	if args.Num == -1 || args.Num > curNum {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[args.Num]
	}
	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

func newConfig() Config {
	return Config{
		Groups: make(map[int64][]string),
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0] = newConfig()

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)
	sm.seq = 1

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
