package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
	startup bool
	currentView View //currentView is at most one view ahead
	ackedView View
	servertimes map[string]time.Time
	servertimesLock sync.Mutex
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.

	//record time of receiving message
	now := time.Now()
	vs.servertimesLock.Lock()
	vs.servertimes[args.Me] = now
	vs.servertimesLock.Unlock()

	if vs.startup {
		// vs first start up
		vs.startup = false
		vs.ackedView.Primary = args.Me
		vs.ackedView.Viewnum = 0 // ack in start is only 0
		vs.currentView.Primary = args.Me
		vs.currentView.Viewnum = 1
	} else if vs.currentView.Primary == args.Me {
		//if server is primary, the ping should be same as ackedView or currentView
		if vs.currentView.Viewnum == args.Viewnum {
			vs.ackedView = vs.currentView
		} else if vs.ackedView.Viewnum != args.Viewnum {
			//if primary has wrong viewnum then it restarted and should be set dead
			if vs.currentView.Backup == "" {
				log.Fatal("Both Primary server and Backup server are dead")
			} else {
				vs.currentView.Primary = vs.currentView.Backup
				vs.currentView.Backup = ""
				vs.currentView.Viewnum++
			}
		}
	} else if vs.currentView.Backup == ""  {
		//if server is idle server and backup is empty
		if vs.currentView.Viewnum == vs.ackedView.Viewnum {
			vs.currentView.Viewnum++
			vs.currentView.Backup = args.Me
		}

	}
	//primary failure is handled in tick()

	//if server is backup, should only reply currentView
	//if primary and backup are both working, should only reply currentView to idle server
	reply.View = vs.currentView
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View = vs.currentView
	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.

	//If the ACK on current view is not received, further changes on view is stopped
	if vs.currentView.Viewnum != vs.ackedView.Viewnum {
		return
	}
	//Iterate over servers' time to check dead servers
	vs.servertimesLock.Lock()
	for server, server_time := range vs.servertimes {
		now := time.Now()
		if now.Sub(server_time) <= DeadPings * PingInterval {
			continue
		}
		//If the server is dead
		if server == vs.currentView.Backup {
			//If backup is dead, set it to nothing and it will be further handled in Ping
			vs.currentView.Backup = ""
			vs.currentView.Viewnum++
		} else if server == vs.currentView.Primary {
			//If primary is dead, check backup and change backup to primary
			if vs.currentView.Backup == "" {
				log.Printf("Both Primary server and Backup server are dead")
			} else {
				vs.currentView.Primary = vs.currentView.Backup
				vs.currentView.Backup = ""
				vs.currentView.Viewnum++
			}
		}
		//remove dead server from the time map
		delete(vs.servertimes, server)
	}
	vs.servertimesLock.Unlock()
	return
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.startup = true
	vs.currentView = View{Viewnum: 0, Primary: "", Backup:""}
	vs.ackedView = View{Viewnum: 0, Primary: "", Backup:""}
	vs.servertimes = make(map[string]time.Time)


	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
