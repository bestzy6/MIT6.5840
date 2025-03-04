package mr

import "time"

// Status 任务状态
type Status int

const (
	Ready     Status = iota // 就绪
	Running                 // 运行
	Completed               // 已完成
)

// =================================

// TaskType 任务类型
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	CoordinatorTask
)

// =================================

// Info Coordinator的通知消息
type Info int

const (
	Normal  Info = iota //正常
	AllDone             //已结束
)

type Task struct {
	ID       int
	TaskType TaskType

	SourceFilePath []string
}

type TaskDetail struct {
	Task     Task
	WorkerID int
	Status   Status
	StartAt  time.Time // 时间戳
}
