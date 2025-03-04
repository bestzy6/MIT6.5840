package mr

import (
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// Expire 过期时间
const Expire = 10 * time.Second

type Coordinator struct {
	// Your definitions here.
	done bool //全局的任务完成标志

	filePath []string
	nReduce  int
	nTask    int

	tasks []TaskDetail

	currentStage      TaskType
	doneMapTaskNum    int
	doneReduceTaskNum int

	nextWorkerId int64
	mu           sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func NewCoordinator(filePath []string, nReduce int) *Coordinator {
	// 创建Task
	tasks := make([]TaskDetail, 0, len(filePath))
	for i, p := range filePath {
		tasks = append(tasks, TaskDetail{
			Task: Task{
				ID:             i,
				TaskType:       MapTask,
				SourceFilePath: []string{p},
			},
			WorkerID: 0,
			Status:   Ready,
		})
	}

	ret := &Coordinator{
		filePath:          filePath,
		nReduce:           nReduce,
		nTask:             len(filePath),
		tasks:             tasks,
		currentStage:      MapTask,
		doneMapTaskNum:    0,
		doneReduceTaskNum: 0,
		nextWorkerId:      0,
	}

	go ret.TimeWatcher()

	return ret
}

func (c *Coordinator) TimeWatcher() {
	// 循环监听
	for {
		c.mu.Lock()

		// 程序结束
		if c.done {
			break
		}

		for i := 0; i < len(c.tasks); i++ {
			task := c.tasks[i]
			if task.Status == Completed || task.Status == Ready {
				continue
			}

			if task.Status == Running && time.Now().Sub(task.StartAt) > Expire {
				// 超时
				c.tasks[i].Status = Ready //必须改变c.tasks[i]的状态，而不是task
				log.Printf("任务超时，重置任务【TaskID:%d】为Ready", task.Task.ID)
			}
		}

		c.mu.Unlock()
		time.Sleep(1 * time.Second)
	}

}

// Register 注册Worker
func (c *Coordinator) Register(args *RegisterWorker, reply *RegisterWorkerReply) error {
	if args == nil || reply == nil {
		return errors.New("args or reply is nil")
	}

	id := atomic.AddInt64(&c.nextWorkerId, 1) - 1
	reply.WorkId = int(id)
	log.Printf("Worker已成功注册,派发ID：%d", id)
	return nil
}

// AssignTask Work请求指派任务
func (c *Coordinator) AssignTask(args *PullTask, reply *PullTaskReply) error {
	if args == nil || reply == nil {
		return errors.New("args or reply is nil")
	}
	// 循环扫描，寻找Ready状态的任务
	for {
		// 如果所有任务都结束了，向请求的Worker发送结束命令
		if c.done {
			log.Println("所有任务均已完成，发送AllDone信息")
			reply.Task = Task{}
			reply.Info = AllDone
			reply.NReduce = c.nReduce
			return nil
		}

		c.mu.Lock()
		for i := 0; i < len(c.tasks); i++ {
			task := c.tasks[i]

			if task.Status == Ready {
				reply.Task = Task{
					ID:             task.Task.ID,
					TaskType:       task.Task.TaskType,
					SourceFilePath: task.Task.SourceFilePath,
				}
				reply.Info = Normal
				reply.NReduce = c.nReduce

				// 修改task
				c.tasks[i].StartAt = time.Now()
				c.tasks[i].WorkerID = args.WorkID
				c.tasks[i].Status = Running

				c.mu.Unlock()
				log.Printf("Coordinator 已经成功派发【%d】类型任务至Worker-%d，TaskId:%d", reply.Task.TaskType, args.WorkID, reply.Task.ID)
				return nil
			}
		}
		c.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}

}

// ReceiveReport 接收Work的任务完成反馈
func (c *Coordinator) ReceiveReport(args *ReportTask, reply *ReportReply) error {

	// 遍历查找任务
	for i := range c.tasks {
		if c.tasks[i].Task.ID == args.TaskId && c.tasks[i].WorkerID == args.WorkId {
			c.mu.Lock()

			if c.tasks[i].Status == Running {
				log.Println("Coordinator已成功收到任务完成信息", "任务ID:", args.TaskId)
				c.tasks[i].Status = Completed
				c.tasks[i].Task.SourceFilePath = args.FilePath

				switch c.currentStage {
				case MapTask:
					c.doneMapTaskNum++
				case ReduceTask:
					c.doneReduceTaskNum++
				}

				c.mu.Unlock()
				break
			}

			c.mu.Unlock()
		}
	}

	reply.Result = true
	c.listenAndSwitchStage()
	return nil
}

func (c *Coordinator) listenAndSwitchStage() {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.currentStage {
	case MapTask:
		if c.doneMapTaskNum != c.nTask {
			break
		}

		log.Println("Map阶段完成，升级为Reduce阶段")
		//将任务重置，升级当前阶段为ReduceStage
		c.currentStage = ReduceTask
		// 重置任务
		c.tasks = c.tasks[:0]
		for i := 0; i < c.nReduce; i++ {
			c.tasks = append(c.tasks, TaskDetail{
				Task: Task{
					ID:             i,
					TaskType:       ReduceTask,
					SourceFilePath: []string{},
				},
				Status: Ready,
			})
		}
	case ReduceTask:
		if c.doneReduceTaskNum != c.nReduce {
			break
		}
		log.Println("Reduce阶段已完成，工作即将结束")
		//	升级当前阶段，合并Reduce产物
		c.currentStage = CoordinatorTask
		c.done = true
	default:
		return
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	ret = c.done

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//c := Coordinator{}

	// Your code here.
	c := NewCoordinator(files, nReduce)
	c.server()
	return c
}
