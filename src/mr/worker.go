package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

	// 注册Worker
	registerReply := RegisterWorkerReply{}
	ok := call("Coordinator.Register", &RegisterWorker{}, &registerReply)
	if !ok {
		log.Fatalln("register Worker failed")
	}

	w := worker{
		id:      registerReply.WorkId,
		mapf:    mapf,
		reducef: reducef,
	}
	w.start()
}

type worker struct {
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

func (w *worker) start() {
	// 请求任务
	for {
		task := w.pullTask()
		if task.Info == AllDone {
			log.Println("所有任务已经结束！")
			return
		}

		log.Printf("Worker-%d接收到%d任务", w.id, task.Task.TaskType)

		switch task.Task.TaskType {
		case MapTask:
			// 执行map
			kvs := w.doMapTask(task.Task)
			// 写入中间文件
			filePath := w.writeMapIntermediate(task.Task.ID, kvs, task.NReduce)
			// 发送通知
			w.report(task.Task.ID, filePath)
		case ReduceTask:
			// 执行reduce
			kvs := w.doReduceTask(task.Task)
			// 写入中间文件
			filePath := w.writeReduceIntermediate(task.Task.ID, kvs)
			// 发送通知
			w.report(task.Task.ID, filePath)
		default:
			panic("Unknown Task Type")
		}
	}
}

func (w *worker) pullTask() PullTaskReply {
	// RPC请求任务
	pullTask := PullTask{
		WorkID: w.id,
	}
	var reply PullTaskReply
	ok := call("Coordinator.AssignTask", &pullTask, &reply)
	if !ok {
		log.Fatalf("register Worker failed")
	}
	log.Printf("Worker拉取任务成功。【TaskID:%d】【TaskType:%d】【Info(0:normal):%d】", reply.Task.ID, reply.Task.TaskType, reply.Info)
	return reply
}

func (w *worker) report(taskId int, filepath []string) {
	args := ReportTask{
		TaskId:   taskId,
		WorkId:   w.id,
		FilePath: filepath,
	}
	var reply ReportReply
	ok := call("Coordinator.ReceiveReport", &args, &reply)
	if !ok {
		log.Fatalln("send report failed")
	}
}

func (w *worker) doMapTask(task Task) []KeyValue {
	kvs := make([]KeyValue, 0)
	for _, s := range task.SourceFilePath {
		// 打开file
		file, err := os.Open(s)
		if err != nil {
			log.Fatalf("cannot open %v", s)
		}
		// 读取file
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", s)
		}
		file.Close()

		// 提取单词频次
		kva := w.mapf(s, string(content))
		kvs = append(kvs, kva...)
	}
	return kvs
}

// 以mr-X-Y的格式写入中间文件
func (w *worker) writeMapIntermediate(taskId int, kvs []KeyValue, nReduce int) []string {
	if len(kvs) == 0 {
		return []string{}
	}

	bucket := make(map[int][]KeyValue)
	for _, kv := range kvs {
		reduceIdx := ihash(kv.Key) % nReduce
		bucket[reduceIdx] = append(bucket[reduceIdx], kv)
	}

	// 存储至文件中
	ans := make([]string, 0, nReduce)
	dir, _ := os.Getwd()
	for reduceIdx, values := range bucket {
		fname := fmt.Sprintf("mr-%d-%d", taskId, reduceIdx)
		tempFile, err := os.CreateTemp(dir, "tmp")
		if err != nil {
			log.Fatalf("cannot create tempFile %v,err:%s", tempFile.Name(), err.Error())
		}

		encoder := json.NewEncoder(tempFile)
		for _, kv := range values {
			err = encoder.Encode(kv)
			if err != nil {
				log.Fatalf("cannot write %v,err:%s", kv, err.Error())
			}
		}
		ans = append(ans, fname)

		if err = os.Rename(tempFile.Name(), filepath.Join(dir, fname)); err != nil {
			log.Printf("rename tempFile failed , err: %s", err.Error())
			tempFile.Close()
			continue
		}
		log.Printf("create map-out file success, file name: %s", fname)
		tempFile.Close()
	}
	return ans
}

func (w *worker) doReduceTask(task Task) []KeyValue {
	kvs := make([]KeyValue, 0)
	dir, _ := os.Getwd()
	files, err := os.ReadDir(dir)
	if err != nil {
		log.Fatalf("read dir failed,err: %s", err.Error())
	}

	// 定义正则表达式
	reg := fmt.Sprintf(`^mr-(\d+)-%d$`, task.ID)
	re := regexp.MustCompile(reg)

	for _, file := range files {
		if !file.IsDir() && re.MatchString(file.Name()) {
			log.Println("ReduceWorker匹配到文件", file.Name())
			f, err := os.Open(filepath.Join(dir, file.Name()))
			if err != nil {
				log.Fatalf("open file failed,err: %s", err.Error())
			}

			dec := json.NewDecoder(f)
			for {
				var kv KeyValue
				if err = dec.Decode(&kv); err != nil {
					break
				}
				kvs = append(kvs, kv)
			}

		}
	}
	return kvs
}

func (w *worker) writeReduceIntermediate(reduceIdx int, kvs []KeyValue) []string {
	oname := fmt.Sprintf("mr-out-%d", reduceIdx)
	dir, _ := os.Getwd()
	tempFile, err := os.CreateTemp(dir, "tmp")
	if err != nil {
		log.Fatalf("create reduce temp file failed, err: %s", err.Error())
	}
	defer tempFile.Close()
	// 排序
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})

	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := w.reducef(kvs[i].Key, values)

		fmt.Fprintf(tempFile, "%v %v\n", kvs[i].Key, output)

		i = j
	}
	newName := filepath.Join(dir, oname)
	if err = os.Rename(tempFile.Name(), newName); err != nil {
		log.Printf("failed to rename reduce file, err :%s", err.Error())
		return []string{}
	}

	log.Println("create reduce file success", newName)
	return []string{newName}
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
