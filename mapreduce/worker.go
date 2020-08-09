package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"plugin"
)

type Worker struct {
	id         int
	mapNum     int
	reduceNum  int
	task       TaskModel
	mapFunc    func(string, string) []KeyValue
	reduceFunc func(string, []string) string
}


type KeyValue struct {
	Key string
	Value string
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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


func (w *Worker) Register() {
	args := RegisterArgs{}
	reply := RegisterReply{}
	call("Server.RegisterHandler", &args, &reply)
	w.id = reply.WorkerID
	w.reduceNum = reply.ReduceNum
	w.mapNum = reply.MapNum
}

func (w *Worker) ReqTask() {
	args := AssignTaskArgs{}
	reply := AssignTaskReply{}
	call("Server.AssignTaskHandler", &args, &reply)
	w.task = reply.Task
	fmt.Printf("got task %+v\n", w.task)
}

func (w *Worker) doTask() {
	switch w.task.Type {
	case TypeDone:
		os.Exit(0)
	case TypeMap:
		w.doMapTask()
	case TypeReduce:
		w.doReduceTask()
	}
}

func (w *Worker) doMapTask() {
	fileName := w.task.Args
	file, _ := os.Open(fileName)
	content, _ := ioutil.ReadAll(file)
	kvs := w.mapFunc(fileName, string(content))

	intermediate := make([][]KeyValue, w.reduceNum)
	for _, kv := range kvs {
		idx := ihash(kv.Key) % w.reduceNum
		intermediate[idx] = append(intermediate[idx], kv)
	}

	for i := 0; i < w.reduceNum; i++ {
		if len(intermediate[i]) == 0 {
			continue
		}
		tmpName := fmt.Sprintf("mr-%d-%d", w.id, i)
		tmpFile, _ := os.OpenFile(tmpName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		enc := json.NewEncoder(tmpFile)
		for _, kv := range intermediate[i] {
			_ = enc.Encode(&kv)
		}
		_ = tmpFile.Close()
	}
	w.Report(StateFinish)
}

func (w *Worker) doReduceTask() {
	reduceNo := w.task.Args
	values := map[string][]string{}
	for mapIdx := 0; mapIdx < w.mapNum; mapIdx++ {
		fileName := fmt.Sprintf("mr-%d-%s", mapIdx, reduceNo)
		file, err := os.Open(fileName)
		if err != nil {
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err == io.EOF {
				break
			}
			values[kv.Key] = append(values[kv.Key], kv.Value)
		}
		file.Close()
		if err := os.Remove(fileName); true {
			fmt.Println("rm file", fileName, err)
		}
	}
	ofile, _ := os.Create(fmt.Sprintf("mr-out-%s", reduceNo))
	for k, vs := range values {
		output := w.reduceFunc(k, vs)
		fmt.Fprintf(ofile, "%v %v\n", k, output)
	}
	ofile.Close()
	w.Report(StateFinish)
}


func (w *Worker) Report(state TaskState) {
	args := ReportArgs{
		WorkerID: w.id,
		Task:     w.task,
		State:    state,
	}
	reply := ReportReply{}
	call("Server.ReportHandler", &args, &reply)
}


func MakeWorkder(pluginName string) *Worker {
	mapf, reducef := loadPlugin(pluginName)
	w := &Worker{
		mapFunc: mapf,
		reduceFunc: reducef,
	}
	return w
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)
	return mapf, reducef
}

// start worker
func (w *Worker) Start() {
	w.Register()
	for {
		w.ReqTask()
		w.doTask()
	}
}

