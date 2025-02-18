package kvraft

import (
	"6.5840/labrpc"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkId    int64
	nextReqId  int64
	lastLeader int64
}

func (ck *Clerk) nextId() int64 {
	return atomic.AddInt64(&ck.nextReqId, 1)
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkId = nrand()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key:     key,
		ClerkID: ck.clerkId,
		ReqID:   ck.nextId(),
	}
	var reply GetReply

	serverId := int(atomic.LoadInt64(&ck.lastLeader))
	for {
		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)
		if !ok {
			// 该Server可能已经宕机，尝试下一个
			serverId = (serverId + 1) % len(ck.servers)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		switch reply.Err {
		case OK:
			DPrintf("[Client] Get key From Server%d:[%s] success, val:[%s]", serverId, key, reply.Value)
			atomic.SwapInt64(&ck.lastLeader, int64(serverId))
			return reply.Value
		case ErrNoKey:
			return ""
		case ErrWrongLeader:
			DPrintf("[Client] Server %d is not Leader", serverId)
			serverId = (serverId + 1) % len(ck.servers)
		case ErrTimeout:
			DPrintf("[Client] Server %d is Timeout", serverId)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:     key,
		Value:   value,
		ClerkID: ck.clerkId,
		ReqID:   ck.nextId(),
	}
	var reply PutAppendReply

	serverId := int(atomic.LoadInt64(&ck.lastLeader))

	for {
		DPrintf("[Client] Ready to %s key:[%s] value:[%s] to S%d", op, key, value, serverId)
		ok := ck.servers[serverId].Call("KVServer."+op, &args, &reply)
		if !ok {
			// 该Server可能已经宕机，尝试下一个
			serverId = (serverId + 1) % len(ck.servers)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		switch reply.Err {
		case OK:
			DPrintf("[Client] %s key:[%s] To S%d success", op, key, serverId)
			atomic.SwapInt64(&ck.lastLeader, int64(serverId))
			return
		case ErrWrongLeader:
			DPrintf("[Client] %s ,But Server %d is not Leader", op, serverId)
			serverId = (serverId + 1) % len(ck.servers)
		case ErrTimeout:
			DPrintf("[Client] %s ,Server %d is Timeout", op, serverId)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
