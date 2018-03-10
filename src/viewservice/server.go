package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import (
	"sync/atomic"
)

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
	currentView View      // Beginning number of view, default viewNum is 0
	isPrimaryAcked bool

	primaryMissingTick uint
	backupMissingTick uint
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// First ping from the client machine or Second ping from client
	if vs.currentView.Primary == "" && vs.currentView.Viewnum == 0 {
		vs.currentView.Viewnum++
		vs.currentView.Primary = args.Me
		vs.currentView.Backup = ""
		vs.primaryMissingTick = 0
		vs.isPrimaryAcked = false

		reply.View = vs.currentView
	}

	// When view is same and not first time, update missingTick
	if vs.currentView.Viewnum == args.Viewnum {
		if vs.currentView.Primary == args.Me {
			// When primary is acked
			vs.isPrimaryAcked = true
			vs.primaryMissingTick = 0
		}

		if vs.currentView.Backup == args.Me {
			vs.backupMissingTick = 0
		}

		reply.View = vs.currentView
	}

	if vs.currentView.Viewnum > args.Viewnum {
		// If there is no backup in current view, promote idle server to backup
		if vs.currentView.Primary != args.Me && vs.currentView.Backup == "" {
			vs.currentView.Viewnum++
			vs.currentView.Backup = args.Me
			vs.primaryMissingTick = 0
		}

		// Backup server got promoted
		if vs.currentView.Primary == args.Me && vs.primaryMissingTick >= DeadPings {
			vs.primaryMissingTick = 0
			vs.isPrimaryAcked = false
		}

		reply.View = vs.currentView
	}

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

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
	vs.mu.Lock()
	defer vs.mu.Unlock()

	vs.primaryMissingTick++
	vs.backupMissingTick++

	if vs.primaryMissingTick >= DeadPings && vs.isPrimaryAcked {
		vs.currentView.Viewnum++
		vs.currentView.Primary = vs.currentView.Backup
		vs.currentView.Backup = ""
	} else if vs.backupMissingTick >= DeadPings && vs.isPrimaryAcked {
		vs.currentView.Viewnum++
		vs.currentView.Backup = ""
	}
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
	vs.currentView.Viewnum = 0
	vs.currentView.Primary = ""
	vs.currentView.Backup = ""
	vs.isPrimaryAcked = false

	vs.primaryMissingTick = 0
	vs.backupMissingTick = 0

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
