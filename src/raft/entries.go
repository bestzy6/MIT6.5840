package raft

import (
	"log"
	"math"
)

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

	left, right = left-rf.LastIncludedIndex, right-rf.LastIncludedIndex

	newLog := make([]LogEntry, right-left)
	copy(newLog, rf.log[left-1:right-1])
	return newLog
}

func (rf *Raft) LastLogIndex() int {
	idx := len(rf.log) + rf.LastIncludedIndex
	return idx
}

func (rf *Raft) LastLogTerm() int {
	entry := rf.GetLog(rf.LastLogIndex())
	return entry.Term
}

func (rf *Raft) GetLog(index int) LogEntry {
	idx := index - rf.LastIncludedIndex
	if idx < 0 {
		log.Panicf("idx:%d < 0", idx)
	}
	if idx == 0 {
		return LogEntry{
			Term:    rf.LastIncludedTerm,
			Command: nil,
		}
	}

	if idx > len(rf.log) {
		return LogEntry{
			Term:    0,
			Command: nil,
		}
	}
	return rf.log[idx-1]
}

func (rf *Raft) TermRange(term int) (minIdx, maxIdx int) {
	if term == 0 {
		return 0, 0
	}
	minIdx, maxIdx = math.MaxInt, -1
	for i := rf.LastIncludedIndex + 1; i <= rf.LastLogIndex(); i++ {
		if rf.GetLog(i).Term == term {
			minIdx = min(minIdx, i)
			maxIdx = max(maxIdx, i)
		}
	}
	if maxIdx == -1 {
		minIdx = -1
	}
	return minIdx, maxIdx
}
