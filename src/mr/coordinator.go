package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	Map = iota
	Reduce
	Done
)

type void struct{}

type Phase struct {
	Map, Reduce, Done chan void
}

type Coordinator struct {
	// map
	files      []string
	mapAllocCh chan *JobReply
	mapAck     []chan void
	// reduce
	nReduce       int
	reduceAllocCh chan *JobReply
	reduceAck     []chan void

	phase Phase
}

func (c *Coordinator) FinishJob(args *JobReply, _ *struct{}) error {
	if args.Type == Map {
		c.mapAck[args.ID] <- void{}
	} else if args.Type == Reduce {
		c.reduceAck[args.ID] <- void{}
	}
	return nil
}

func (c *Coordinator) Job(_ *struct{}, reply *JobReply) error {
	select {
	case j := <-c.mapAllocCh:
		// 因为地址不同，不能直接 case reply = <- c.mapAllocCh:
		// 只能修改内容
		reply.ID = j.ID
		reply.Type = j.Type
		reply.MapN = j.MapN
		reply.ReduceN = j.ReduceN
		reply.Filename = j.Filename
	case j := <-c.reduceAllocCh:
		reply.ID = j.ID
		reply.Type = j.Type
		reply.MapN = j.MapN
		reply.ReduceN = j.ReduceN
		reply.Filename = j.Filename
	case <-c.phase.Done:
		go func() { c.phase.Done <- void{} }()
		reply.Type = Done
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	select {
	case <-c.phase.Done:
		go func() { c.phase.Done <- void{} }()
		return true
	default:
	}
	return false
}

func timeout(done chan void, t time.Duration) {
	time.Sleep(t)
	done <- void{}
}

func ensureTaskDone(put chan *JobReply, ack chan void, j *JobReply) {
	for {
		put <- j
		waitCh := make(chan void)
		go timeout(waitCh, 10*time.Second)
		select {
		case <-ack:
			return
		case <-waitCh:
			fmt.Printf("job: %v timeout\n", j)
		}
	}
}

func (c *Coordinator) MapStage() {
	// new goroutine to wait for all map tasks done
	go func() {
		<-c.phase.Map
		var wg sync.WaitGroup
		wg.Add(len(c.files))
		for i, f := range c.files {
			go func(j *JobReply) {
				c.mapAck[j.ID] = make(chan void)
				ensureTaskDone(c.mapAllocCh, c.mapAck[j.ID], j)
				wg.Done()
			}(&JobReply{
				ID:       i,
				Filename: f,
				Type:     Map,
				ReduceN:  c.nReduce,
				MapN:     len(c.files),
			})
		}
		wg.Wait()
		// start reduce stage
		c.phase.Reduce <- void{}
	}()
}

func (c *Coordinator) ReduceStage() {
	go func() {
		<-c.phase.Reduce
		var wg sync.WaitGroup
		wg.Add(c.nReduce)
		for i := 0; i < c.nReduce; i++ {
			go func(j *JobReply) {
				c.reduceAck[j.ID] = make(chan void)
				ensureTaskDone(c.reduceAllocCh, c.reduceAck[j.ID], j)
				wg.Done()
			}(&JobReply{
				ID:       i,
				Filename: "",
				Type:     Reduce,
				ReduceN:  c.nReduce,
				MapN:     len(c.files),
			})
		}
		wg.Wait()
		c.phase.Done <- void{}
	}()
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:   files,
		nReduce: nReduce,
		phase: Phase{
			Map:    make(chan void),
			Reduce: make(chan void),
			Done:   make(chan void),
		},
		mapAllocCh:    make(chan *JobReply),
		reduceAllocCh: make(chan *JobReply),
		mapAck:        make([]chan void, len(files)),
		reduceAck:     make([]chan void, nReduce),
	}
	c.server()
	// allocate map tasks and waiting util they are all done
	c.MapStage()
	c.ReduceStage()
	c.phase.Map <- void{}
	return &c
}
