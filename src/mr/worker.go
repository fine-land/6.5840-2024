package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func GenerateUnique() string {
	return strconv.FormatInt(time.Now().UnixNano(), 10)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	//fmt.Printf("[worker]: worker start") debug
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		reply := RPCHandlerReply{}
		MakeRPC(&reply)
		//fmt.Printf("[worker]: get reply ", reply.File, reply.NReduce) debug
		switch reply.TaskType {
		case Map:
			intermediate := []KeyValue{}
			file, err := os.Open(reply.File)
			if err != nil {
				log.Fatalf("[worker]: cannot open file: %v", reply.File)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("[worker]: cannot read %v", reply.File)
			}
			file.Close()
			kva := mapf(reply.File, string(content))
			intermediate = append(intermediate, kva...)

			//fmt.Println("[worker]: map success") debug

			//generate out-X-Y
			filesHandle := make(map[int]*os.File)
			defer func() {
				for _, v := range filesHandle {
					v.Close()
				}
			}()

			uniqueSuffix := GenerateUnique()
			for _, kv := range intermediate {
				partition := ihash(kv.Key) % reply.NReduce
				if _, exists := filesHandle[partition]; !exists {
					filenametmp := "mrt-" + strconv.Itoa(reply.TaskId) + "-" + strconv.Itoa(partition) +
						uniqueSuffix + ".tmp"
					f, err := os.Create(filenametmp)
					if err != nil {
						log.Fatalf("[worker]: create mr-%v-%v error", reply.TaskId, partition)
					}
					filesHandle[partition] = f
				}
				encoder := json.NewEncoder(filesHandle[partition])
				err := encoder.Encode(&kv)
				if err != nil {
					log.Fatalf("[worker]: encode error!")
				}
			}

			for p := range filesHandle {
				filenametmp := "mrt-" + strconv.Itoa(reply.TaskId) + "-" + strconv.Itoa(p) +
					uniqueSuffix + ".tmp"
				filename := "mr-" + strconv.Itoa(reply.TaskId) + "-" + strconv.Itoa(p)
				err := os.Rename(filenametmp, filename)
				if err != nil {
					log.Fatalf("[worker]: os.Rename error")
				}
			}
			MakeFinishRPC(&reply)

		case Reduce:
			//find files like mr-X-Y
			var files []string

			// 遍历当前目录
			err := filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}

				// 检查文件名是否匹配 mr-X-Y 格式
				if !info.IsDir() && strings.HasPrefix(info.Name(), "mr-") {
					parts := strings.Split(info.Name(), "-")
					if len(parts) == 3 {
						y, err := strconv.Atoi(parts[2])
						if err == nil && y == reply.TaskId {
							files = append(files, path)
						}
					}
				}

				return nil
			})

			if err != nil {
				//log.Fatalf("[worker]: filepath error")
				fmt.Printf("Error walking the path %s: %v\n", "./", err)
			}

			kva := []KeyValue{}
			for _, file := range files {
				f, e := os.Open(file)
				if e != nil {
					log.Fatalf("[worker]: Open error")
				}
				dec := json.NewDecoder(f)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				f.Close()
			}
			//fmt.Println("[worker]: read all reduce files") debug
			sort.Sort(ByKey(kva))
			onametmp := "mrt-out" + GenerateUnique() + ".tmp"
			oname := "mr-out-" + strconv.Itoa(reply.TaskId)
			ofile, _ := os.Create(onametmp)
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
				i = j
			}
			os.Rename(onametmp, oname)
			MakeFinishRPC(&reply)
		case AllDone:
			return
		case Wait:
			time.Sleep(1 * time.Second)
		}

	} //end for loop
} //end worker

func MakeFinishRPC(reply *RPCHandlerReply) {
	args := FinishArgs{}
	args.TaskId = reply.TaskId
	args.TaskType = reply.TaskType
	r := FinishReply{}
	ok := call("Coordinator.HandleFinish", &args, &r)
	if ok {
		fmt.Println("[worker]: get reply from Finish ok")
	} else {
		fmt.Println("[worker]: get reply from Finish error")
	}
}

func MakeRPC(reply *RPCHandlerReply) {
	args := RPCHandlerArgs{}
	ok := call("Coordinator.RPCHandler", &args, reply)
	if ok {
		fmt.Println("[Worker]: get reply from coordinator ok")
	} else {
		fmt.Println("[Worker]: get reply from coordinator error")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
