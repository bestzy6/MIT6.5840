package shardkv

import "log"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeOut"
	ErrWrongConfigNum
	ErrUpdateConfig = "ErrUpdateConfig"
)

const (
	OpTypeGet    = "GET"
	OpTypePut    = "PUT"
	OpTypeAppend = "APPEND"

	OpTypeConfig      = "CONFIG"
	OpTypeAddShard    = "ADD_SHARD"
	OpTypeRemoveShard = "REMOVE_SHARD"
)

type ShardStatus uint8

const (
	ShardStatusServing   ShardStatus = iota // 正在提供服务
	ShardStatusSending               = iota // 正在发送
	ShardStatusReceiving             = iota // 正在接收
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkID int64
	ReqID   int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkID int64
	ReqID   int64
}

type GetReply struct {
	Err   Err
	Value string
}

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KV struct {
	Key   string
	Value string
}
