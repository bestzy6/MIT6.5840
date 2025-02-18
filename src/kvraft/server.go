package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type    string
	Key     string
	Value   string
	ClerkID int64
	ReqId   int64
}

type NotifyMsg struct {
	Val string
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

type AppliedMsg struct {
	ReqId int64
	Val   string
}

type AppliedLog struct {
	Applied map[int64]AppliedMsg
}

func (a *AppliedLog) Put(clerkId, reqId int64, val string) {
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

func (a *AppliedLog) Get(clerkId, reqId int64) (string, bool) {
	if a.Applied == nil {
		return "", false
	}
	val, ok := a.Applied[clerkId]
	if ok && val.ReqId == reqId {
		return val.Val, true
	}
	return "", false
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db       *Database
	notifyCh *NotifyCh
	applied  *AppliedLog
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type:    OpTypeGet,
		Key:     args.Key,
		Value:   "",
		ClerkID: args.ClerkID,
		ReqId:   args.ReqID,
	}
	val, err := kv.opHandler(op)
	if err != OK {
		DPrintf("[Server%d] Raft Operate Get [%s] Failed,Err: %s", kv.me, args.Key, err)
		reply.Err = err
		return
	}
	DPrintf("[Server%d] Raft Operate Get [%s] Success, current Val: [%s]", kv.me, args.Key, val)
	reply.Value = val
	reply.Err = OK
	return
}

func (kv *KVServer) putAppend(opType string, args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Type:    opType,
		Key:     args.Key,
		Value:   args.Value,
		ClerkID: args.ClerkID,
		ReqId:   args.ReqID,
	}
	val, err := kv.opHandler(op)
	if err != OK {
		DPrintf("[Server%d] Raft Operate %s [%s] Failed,Err: %s", kv.me, opType, args.Key, err)
		reply.Err = err
		return
	}
	DPrintf("[Server%d] Raft Operate %s [%s] Success, current val:[%s]", kv.me, opType, args.Key, val)
	reply.Err = OK
	return
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.putAppend(OpTypePut, args, reply)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.putAppend(OpTypeAppend, args, reply)
}

func (kv *KVServer) opHandler(op Op) (string, Err) {
	// Your code here.

	// 初步去重
	kv.mu.Lock()
	val, ok := kv.applied.Get(op.ClerkID, op.ReqId)
	if ok {
		kv.mu.Unlock()
		return val, OK
	}
	ch := kv.notifyCh.Add(op.ClerkID, op.ReqId)
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.notifyCh.Delete(op.ClerkID, op.ReqId)
	}()

	// 提交到Raft
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return "", ErrWrongLeader
	}
	DPrintf("[Server%d] Raft Start handle [%s],  key:[%s] value:[%s]", kv.me, op.Type, op.Key, op.Value)

	timer := time.NewTimer(1000 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		DPrintf("[Server%d] handle [%s] Timeout, key:[%s] value:[%s]", kv.me, op.Type, op.Key, op.Value)
		return "", ErrTimeout
	case msg := <-ch:
		if msg.Err != OK {
			return "", msg.Err
		}
		DPrintf("[Server%d] handle [%s] Success, key:[%s] value:[%s]", kv.me, op.Type, op.Key, op.Value)
		return msg.Val, OK
	}
}

func (kv *KVServer) handleOperation() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			op := msg.Command.(Op)
			DPrintf("[Server%d] Raft Apply [%s], key:[%s] value:[%s]", kv.me, op.Type, op.Key, op.Value)

			var (
				val string
				err Err
			)
			kv.mu.Lock()
			// 去重
			if _, ok := kv.applied.Get(op.ClerkID, op.ReqId); ok {
				kv.mu.Unlock()
				DPrintf("[Server%d] Already Applied [%s], key:[%s]", kv.me, op.Type, op.Key)
				continue
			}
			switch op.Type {
			case OpTypeGet:
				val, err = kv.db.Get(op.Key)
			case OpTypePut:
				val, err = kv.db.Put(op.Key, op.Value)
			case OpTypeAppend:
				val, err = kv.db.Append(op.Key, op.Value)
			}
			ch := kv.notifyCh.Get(op.ClerkID, op.ReqId)
			if err == OK {
				kv.applied.Put(op.ClerkID, op.ReqId, val)
			}
			kv.mu.Unlock()

			DPrintf("[Server%d] Get NotifyCh Done ,Op:[%s], key:[%s], ChIsNil:[%v]", kv.me, op.Type, op.Key, ch == nil)
			if ch != nil {
				ch <- NotifyMsg{
					Val: val,
					Err: err,
				}
				DPrintf("[Server%d] Notify Clerk [%d] Req [%d] Done,Op:[%s], key:[%s] value:[%s]",
					kv.me, op.ClerkID, op.ReqId, op.Type, op.Key, op.Value)
				close(ch)
			}
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	DPrintf("[Server%d] Be Killed", kv.me)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = NewDatabase(kv.me)
	kv.notifyCh = &NotifyCh{}
	kv.applied = &AppliedLog{}
	go kv.handleOperation()

	return kv
}
