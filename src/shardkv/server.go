package shardkv

import (
	"errors"
	"net"
)
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	Op     string
	Key    string
	Value  string
	Config shardmaster.Config
	ID     int64

	Data      map[string]string
	ClientOps map[int64]struct{}
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	seq int // Paxos instance number

	config    shardmaster.Config
	data      map[string]string
	clientOps map[int64]struct{}
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) error {

	if args.ConfigNum >= kv.config.Num {
		reply.Err = ErrOldConfig
		return nil
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.createOp(Op{
		Op: "GetShard",
		ID: nrand(),
	})

	reply.Data = make(map[string]string)
	for key, val := range kv.data {
		if key2shard(key) == args.Shard {
			reply.Data[key] = val
		}
	}
	reply.ClientOps = make(map[int64]struct{})
	for id, _ := range kv.clientOps {
		reply.ClientOps[id] = struct{}{}
	}

	reply.Err = OK

	return nil
}

// create Reconfig Op with new config from shardmaster
func (kv *ShardKV) acceptNewConfig(config shardmaster.Config) error {
	// already locked mutex in tick()

	data := make(map[string]string)
	clientOps := make(map[int64]struct{})

	for id, _ := range kv.clientOps {
		clientOps[id] = struct{}{}
	}

	for i := 0; i < shardmaster.NShards; i++ {
		if config.Shards[i] != kv.gid {
			continue
		}
		origGID := kv.config.Shards[i] // GID of the group of the shard in last config
		if origGID != 0 && origGID != kv.gid {
			args := &GetShardArgs{
				ConfigNum: kv.config.Num,
				Shard:     i,
			}
			peers, ok := kv.config.Groups[origGID]
			if !ok {
				return errors.New("Failed to get servers of original group")
			}
			dataReceived := false
			for _, peer := range peers {
				var reply GetShardReply
				ok := call(peer, "ShardKV.GetShard", args, &reply)
				if !ok || reply.Err != OK {
					continue
				}
				for key, val := range reply.Data {
					data[key] = val
				}
				for id, _ := range reply.ClientOps {
					clientOps[id] = struct{}{}
				}
				dataReceived = true
				break
			}
			if !dataReceived {
				return errors.New("Failed to get data from original group")
			}
		}
	}
	//fmt.Printf("ReconfigOp %d %d -> %d: data %d\n",id, kv.config.Num, config.Num, len(data))
	kv.createOp(Op{
		Op:        "Reconfig",
		Config:    config,
		Data:      data,
		ClientOps: clientOps,
		ID:        nrand(),
	})
	return nil
}

func (kv *ShardKV) applyOp(op Op) {
	defer func() {
		kv.px.Done(kv.seq)
		kv.seq++
	}()
	if op.Op == "Reconfig" {
		// outdated config
		if kv.config.Num >= op.Config.Num {
			return
		}
		kv.config = op.Config
		for key, val := range op.Data {
			kv.data[key] = val
		}
		for id, _ := range op.ClientOps {
			kv.clientOps[id] = struct{}{}
		}
	} else if op.Op == "Put" || op.Op == "Append" {
		if kv.config.Shards[key2shard(op.Key)] != kv.gid {
			return
		}
		if op.Op == "Put" {
			kv.data[op.Key] = op.Value
		} else { // Append
			oldValue, ok := kv.data[op.Key]
			if !ok {
				oldValue = ""
			}
			kv.data[op.Key] = oldValue + op.Value
		}
		for id, _ := range op.ClientOps {
			kv.clientOps[id] = struct{}{}
		}
	} else {
		// Get or GetShard, ignored
	}
}

// create a new Op in Paxos instances
func (kv *ShardKV) createOp(op Op) {
	for !kv.isdead() {
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

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// check if key belongs to this shard
	if kv.config.Shards[key2shard(args.Key)] != kv.gid {
		reply.Err = ErrWrongGroup
		return nil
	}
	kv.createOp(Op{
		Key: args.Key,
		Op:  "Get",
		ID:  args.ID,
	})
	//fmt.Printf("Get %s; cur config: %d\n", args.Key, kv.config.Num)
	value, ok := kv.data[args.Key]
	if !ok {
		reply.Err = ErrNoKey
		return nil
	}
	reply.Err = OK
	reply.Value = value
	return nil
}

func (kv *ShardKV) isDuplicate(opID int64) bool {
	_, duplicate := kv.clientOps[opID]
	return duplicate
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.config.Shards[key2shard(args.Key)] != kv.gid {
		reply.Err = ErrWrongGroup
		return nil
	}
	if !kv.isDuplicate(args.ID) {
		kv.clientOps[args.ID] = struct{}{}
		clientOps := make(map[int64]struct{})
		for id, _ := range kv.clientOps {
			clientOps[id] = struct{}{}
		}
		kv.createOp(Op{
			Key:       args.Key,
			Value:     args.Value,
			Op:        args.Op,
			ID:        args.ID,
			ClientOps: clientOps,
		})
	}
	reply.Err = OK
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	config := kv.sm.Query(-1)

	if config.Num <= kv.config.Num {

	} else {
		// reconfig new configs
		for num := kv.config.Num + 1; num < config.Num; num++ {
			config := kv.sm.Query(num)
			err := kv.acceptNewConfig(config)
			if err == nil {
				return
			}
		}
		err := kv.acceptNewConfig(config)
		if err == nil {
			return
		}
	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	kv.data = make(map[string]string)
	kv.clientOps = make(map[int64]struct{})

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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
