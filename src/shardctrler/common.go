package shardctrler

import (
	"fmt"
	"log"
	"sort"
	"sync"
)

//
// Shard controller: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK          = "OK"
	WrongLeader = "WrongLeader"
	TimeOut     = "TimeOut"
)

const (
	Query = "Query"
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings

	ClerkId int64
	ReqId   int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int

	ClerkId int64
	ReqId   int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int

	ClerkId int64
	ReqId   int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number

	ClerkId int64
	ReqId   int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type AppliedMsg struct {
	ReqId int64
	Val   Config
}

type AppliedLog struct {
	Applied map[int64]AppliedMsg
}

func (a *AppliedLog) Put(clerkId, reqId int64, val Config) {
	if a.Applied == nil {
		a.Applied = make(map[int64]AppliedMsg)
	}
	if a.Applied[clerkId].ReqId >= reqId {
		return
	}
	a.Applied[clerkId] = AppliedMsg{
		ReqId: reqId,
		Val:   val,
	}
}

func (a *AppliedLog) Get(clerkId, reqId int64) (Config, bool) {
	if a.Applied == nil {
		return Config{}, false
	}
	val, ok := a.Applied[clerkId]
	if ok && val.ReqId == reqId {
		return val.Val, true
	}
	return Config{}, false
}

type NotifyMsg struct {
	Val Config
	Err Err
}

type NotifyCh struct {
	m map[int64]map[int64]chan NotifyMsg // map[clerkId][reqId] -> chan
}

func (n *NotifyCh) Get(clerkId, reqId int64) chan NotifyMsg {
	if n.m == nil {
		return nil
	}
	if _, ok := n.m[clerkId]; ok {
		if ch, ok := n.m[clerkId][reqId]; ok {
			return ch
		}
	}
	return nil
}

func (n *NotifyCh) Add(clerkId, reqId int64) chan NotifyMsg {
	if n.m == nil {
		n.m = make(map[int64]map[int64]chan NotifyMsg)
	}
	if _, ok := n.m[clerkId]; !ok {
		n.m[clerkId] = make(map[int64]chan NotifyMsg)
	}
	ch := make(chan NotifyMsg, 1)
	n.m[clerkId][reqId] = ch
	return ch
}

func (n *NotifyCh) Delete(clerkId, reqId int64) {
	if n.m == nil {
		return
	}
	if _, ok := n.m[clerkId]; ok {
		if _, ok := n.m[clerkId][reqId]; ok {
			delete(n.m[clerkId], reqId)
		}
	}
}

type Set struct {
	mu sync.RWMutex
	m  map[int]struct{}
}

func NewSet() *Set {
	return &Set{
		m: make(map[int]struct{}),
	}
}

func (s *Set) Add(element ...int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ele := range element {
		s.m[ele] = struct{}{}
	}
}

func (s *Set) Del(element ...int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ele := range element {
		delete(s.m, ele)
	}
}

func (s *Set) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.m)
}

func (s *Set) Contain(element int) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.m[element]
	return ok
}

func (s *Set) SortedSlice() []int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	slice := make([]int, 0, len(s.m))
	for k := range s.m {
		slice = append(slice, k)
	}
	sort.Ints(slice)
	return slice
}

func (s *Set) String() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fmt.Sprint(s.m)
}
