package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type cacheData struct {
	reqId int64
	value string
}

type Cache map[int64]cacheData

func (c Cache) Get(key int64, reqId int64) (string, bool) {
	if data, ok := c[key]; ok {
		if data.reqId == reqId {
			return data.value, true
		} else {
			return "", false
		}
	}
	return "", false
}

func (c Cache) Set(key int64, reqId int64, value string) {
	c[key] = cacheData{
		reqId: reqId,
		value: value,
	}
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data  map[string]string
	cache Cache
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	key := args.Key
	clerkId := args.ClerkID
	reqId := args.ReqID
	DPrintf("Client[%d] is doing GET , ReqId:[%d], key:[%s]", clerkId, reqId, key)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, ok := kv.data[key]
	if !ok {
		reply.Value = ""
	} else {
		reply.Value = val
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	key := args.Key
	val := args.Value
	clerkId := args.ClerkID
	reqId := args.ReqID

	DPrintf("Client[%d] is doing Put , ReqId:[%d], key:[%s], val:[%s]", clerkId, reqId, key, val)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 获取旧值
	v, ok := kv.data[key]
	if ok {
		reply.Value = v
	} else {
		reply.Value = ""
	}

	kv.data[key] = val
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	key := args.Key
	val := args.Value
	clerkId := args.ClerkID
	reqId := args.ReqID

	DPrintf("Client[%d] is doing Append , ReqId:[%d], key:[%s], val:[%s]", clerkId, reqId, key, val)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 获取缓存key
	if cache, ok := kv.cache.Get(clerkId, reqId); ok {
		DPrintf("hit cache, cache key: %d", clerkId)
		reply.Value = cache
		return
	}

	// 获取旧值
	v, ok := kv.data[key]
	if ok {
		reply.Value = v
	} else {
		reply.Value = ""
	}
	kv.data[key] = reply.Value + val

	//	添加缓存
	kv.cache.Set(clerkId, reqId, reply.Value)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.cache = make(Cache)

	return kv
}
