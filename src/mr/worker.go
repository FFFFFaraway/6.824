package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// ByKey for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		reply := GetJob()
		if reply.Type == Wait {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if reply.Type == Map {
			filename := reply.Filename
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			interFiles := make([]*os.File, reply.ReduceN)
			for i := 0; i < reply.ReduceN; i++ {
				interFiles[i], _ = os.Create("mr-inter-" + strconv.Itoa(reply.ID) + "-" + strconv.Itoa(i))
			}
			for _, kv := range kva {
				i := ihash(kv.Key) % reply.ReduceN
				fmt.Fprintf(interFiles[i], "%v %v\n", kv.Key, kv.Value)
			}
			FinishJob(reply)
			continue
		}
		if reply.Type == Reduce {
			var intermediate []KeyValue
			for i := 0; i < reply.MapN; i++ {
				b, _ := os.ReadFile("mr-inter-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ID))
				content := string(b)
				kva := strings.Split(content, "\n")
				for _, kvs := range kva[:len(kva)-1] {
					kv := strings.Split(kvs, " ")
					k, v := kv[0], kv[1]
					intermediate = append(intermediate, KeyValue{
						Key:   k,
						Value: v,
					})
				}
			}

			oname := "mr-out-" + strconv.Itoa(reply.ID)
			ofile, _ := os.Create(oname)

			sort.Sort(ByKey(intermediate))

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			ofile.Close()
			FinishJob(reply)
			continue
		}
		if reply.Type == Done {
			return
		}
	}
}

func GetJob() *JobReply {
	args := struct{}{}
	reply := JobReply{}

	ok := call("Coordinator.Job", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
	return &reply
}

func FinishJob(r *JobReply) {
	reply := struct{}{}

	ok := call("Coordinator.FinishJob", &r, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
	return
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
