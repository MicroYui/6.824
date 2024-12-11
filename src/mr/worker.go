package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type SchedulePhase uint8

// 定义任务阶段
const (
	MapPhase SchedulePhase = iota
	ReducePhase
	CompletePhase
)

type JobType uint8

// 定义worker任务类型
const (
	MapJob JobType = iota
	ReduceJob
	WaitJob
	CompleteJob
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		response := heartbeat()
		log.Printf("Worker: receive coordinator's heatbeat %v \n", response)
		switch response.JobType {
		case MapJob:
			mapTask(mapf, response)
		case ReduceJob:
			reduceTask(reducef, response)
		case WaitJob:
			time.Sleep(1 * time.Second)
		case CompleteJob:
			return
		default:
			panic(fmt.Sprintf("unexpected jobType %v", response.JobType))
		}
	}

}

func reduceTask(reduceF func(string, []string) string, response *HeartbeatResponse) {
	var kvList []KeyValue
	for i := 0; i < response.NMap; i++ {
		filePath := generateMapResultFileName(i, response.Id)
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("cannot open %v", filePath)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvList = append(kvList, kv)
		}
		err = file.Close()
		if err != nil {
			log.Fatal(err)
		}
	}
	results := make(map[string][]string)
	for _, kv := range kvList {
		results[kv.Key] = append(results[kv.Key], kv.Value)
	}
	// TODO：不需要使用原子的写入，后续进行修改
	var buf bytes.Buffer
	for key, values := range results {
		output := reduceF(key, values)
		_, err := fmt.Fprintf(&buf, "%v %v\n", key, output)
		if err != nil {
			log.Fatal(err)
		}
	}
	err := writeFile(generateReduceResultFileName(response.Id), &buf)
	if err != nil {
		log.Fatal(err)
	}
	report(response.Id, ReducePhase)
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

// worker向master发送心跳
func heartbeat() *HeartbeatResponse {
	response := HeartbeatResponse{}
	call("Coordinator.Heartbeat", &HeartbeatRequest{}, &response)
	return &response
}

// worker执行map任务
func mapTask(mapF func(string, string) []KeyValue, response *HeartbeatResponse) {
	fileName := response.FilePath
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	err = file.Close()
	if err != nil {
		log.Fatalf("cannot close %v", fileName)
	}
	kvList := mapF(fileName, string(content))
	intermediates := make([][]KeyValue, response.NReduce)
	for _, kv := range kvList {
		index := ihash(kv.Key) % response.NReduce
		intermediates[index] = append(intermediates[index], kv)
	}
	var wg sync.WaitGroup
	// TODO: 这里没必要写成并发写入，单个写入也可
	for index, intermediate := range intermediates {
		wg.Add(1)
		go func(index int, intermediate []KeyValue) {
			defer wg.Done()
			intermediateFilePath := generateMapResultFileName(response.Id, index)
			var buf bytes.Buffer
			enc := json.NewEncoder(&buf)
			for _, kv := range intermediate {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot encode json %v", kv.Key)
				}
			}
			err := writeFile(intermediateFilePath, &buf)
			if err != nil {
				log.Fatal(err)
			}
		}(index, intermediate)
	}
	wg.Wait()
	report(response.Id, ReducePhase)
}

func generateMapResultFileName(mapNumber, reduceNumber int) string {
	return fmt.Sprintf("mr-%d-%d", mapNumber, reduceNumber)
}

func generateReduceResultFileName(reduceNumber int) string {
	return fmt.Sprintf("mr-out-%d", reduceNumber)
}

func writeFile(filename string, r io.Reader) (err error) {
	dir, file := filepath.Split(filename)
	if dir == "" {
		dir = "."
	}
	f, err := os.CreateTemp(dir, file)
	if err != nil {
		return fmt.Errorf("cannot create temp file: %v", err)
	}
	defer func() {
		if err != nil {
			_ = os.Remove(f.Name())
		}
	}()
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(f)
	name := f.Name()
	if _, err := io.Copy(f, r); err != nil {
		return fmt.Errorf("cannot write data to tempfile %q: %v", name, err)
	}
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
	} else if err != nil {
		return err
	} else {
		if err := os.Chmod(name, info.Mode()); err != nil {
			return fmt.Errorf("can't set filemode on tempfile %q: %v", name, err)
		}
	}
	if err := os.Rename(name, filename); err != nil {
		return fmt.Errorf("cannot replace %q with tempfile %q: %v", filename, name, err)
	}
	return nil
}

func report(id int, phase SchedulePhase) {
	call("Coordinator.Report", &ReportRequest{Id: id, Phase: phase}, &ReportResponse{})
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
