package raft

import "log"

type LogEntry struct {
	Term    int         //创建该Log时的任期
	Command interface{} //需要执行的命令
}

// GetLogSlice [i,j)
func (rf *Raft) GetLogSlice(left, right int) []LogEntry {
	if left > right {
		panic("left > right")
	}
	if left < 1 || right > rf.LastLogIndex()+1 {
		panic("数组越界")
	}

	newLog := make([]LogEntry, right-left)
	copy(newLog, rf.log[left-1:right-1])
	return newLog
}

func (rf *Raft) LastLogIndex() int {
	idx := len(rf.log)
	return idx
}

func (rf *Raft) LastLogTerm() int {
	entry := rf.GetLog(rf.LastLogIndex())
	return entry.Term
}

func (rf *Raft) GetLog(index int) LogEntry {
	idx := index
	if idx < 0 {
		log.Panicf("idx:%d < 0", idx)
	}

	if idx > len(rf.log) {
		return LogEntry{
			Term:    0,
			Command: nil,
		}
	}
	return rf.log[idx-1]
}
