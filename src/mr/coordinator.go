package mr

import (
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	Map = iota
	Reduce
	Done
	Wait
)

type void struct{}

type Phase struct {
	Map, Reduce, Done, Wait chan void
}

type Coordinator struct {
	// map
	files     []string
	mapAlloc  int
	mapFinish int
	// reduce
	nReduce      int
	reduceAlloc  int
	reduceFinish int

	phase Phase
}

func (c *Coordinator) FinishJob(args *JobReply, _ *struct{}) error {
	if args.Type == Map {
		c.mapFinish += 1
	} else if args.Type == Reduce {
		c.reduceFinish += 1
	}
	return nil
}

func (c *Coordinator) Job(_ *struct{}, reply *JobReply) error {
	select {
	case <-c.phase.Map:
		if c.mapAlloc == len(c.files) {
			if c.mapFinish == len(c.files) {
				go func() { c.phase.Reduce <- void{} }()
			} else {
				go func() { c.phase.Map <- void{} }()
			}
			reply.Type = Wait
			return nil
		}
		reply.ID = c.mapAlloc
		reply.ReduceN = c.nReduce
		reply.Filename = c.files[c.mapAlloc]
		reply.MapN = len(c.files)
		c.mapAlloc += 1
		go func() { c.phase.Map <- void{} }()
		reply.Type = Map
		return nil
	case <-c.phase.Reduce:
		if c.reduceAlloc == c.nReduce {
			if c.reduceFinish == c.nReduce {
				go func() { c.phase.Done <- void{} }()
			} else {
				go func() { c.phase.Reduce <- void{} }()
			}
			reply.Type = Done
			return nil
		}
		reply.ID = c.reduceAlloc
		reply.ReduceN = c.nReduce
		reply.MapN = len(c.files)
		c.reduceAlloc += 1
		go func() { c.phase.Reduce <- void{} }()
		reply.Type = Reduce
		reply.Filename = ""
		return nil
	case <-c.phase.Done:
		go func() { c.phase.Done <- void{} }()
		reply.Type = Done
		return nil
	}
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
			Wait:   make(chan void),
		},
	}
	c.server()
	go func() { c.phase.Map <- void{} }()
	return &c
}
