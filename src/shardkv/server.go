package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"bytes"
	"sync/atomic"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type       string
	Key        string // OpTypeGet, OpTypePut, OpTypeAppend
	Value      string // OpTypeGet, OpTypePut, OpTypeAppend
	ClerkID    int64
	ReqId      int64
	Config     shardctrler.Config // OpTypeConfig
	Shard      int                // OpTypeAddShard, OpTypeRemoveShard
	ShardData  []KV               // OpTypeAddShard
	ConfigNum  int                // OpTypeAddShard, OpTypeRemoveShard
	AppliedLog AppliedLog         // OpTypeAddShard
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead        int32
	db          *Database
	notifyCh    *NotifyCh
	applied     *AppliedLog
	lastApplied int
	persister   *raft.Persister

	mck          *shardctrler.Clerk
	config       shardctrler.Config
	preConfig    shardctrler.Config
	ShardsStatus [shardctrler.NShards]ShardStatus

	ClerkId   int64
	NextReqId int64
}

func (kv *ShardKV) nextId() int64 {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.NextReqId++
	return kv.NextReqId
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
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
		DPrintf("[Server%d-%d] Raft Operate Get [%s] Failed,Err: %s", kv.gid, kv.me, args.Key, err)
		reply.Err = err
		return
	}
	DPrintf("[Server%d-%d] Raft Operate Get [%s] Success, current Val: [%s]", kv.gid, kv.me, args.Key, val)
	reply.Value = val
	reply.Err = OK
	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// Your code here.
	op := Op{
		Key:     args.Key,
		Value:   args.Value,
		ClerkID: args.ClerkID,
		ReqId:   args.ReqID,
	}
	switch args.Op {
	case "Put":
		op.Type = OpTypePut
	case "Append":
		op.Type = OpTypeAppend
	}
	val, err := kv.opHandler(op)
	if err != OK {
		DPrintf("[Server%d-%d] Raft Operate %s [%s] Failed,Err: %s", kv.gid, kv.me, op.Type, args.Key, err)
		reply.Err = err
		return
	}
	DPrintf("[Server%d-%d] Raft Operate %s [%s] Success, current val:[%s]", kv.gid, kv.me, op.Type, args.Key, val)
	reply.Err = OK
	return
}

func (kv *ShardKV) opHandler(op Op) (string, Err) {
	// Your code here.
	kv.mu.Lock()
	if op.Key != "" {
		shard := key2shard(op.Key)
		// 判断Shard是否正确
		if kv.config.Num == 0 {
			kv.mu.Unlock()
			return "", ErrWrongLeader
		}
		if kv.config.Shards[shard] != kv.gid || kv.ShardsStatus[shard] != ShardStatusServing {
			DPrintf("[Server%d-%d] Handle Key[%s] Shard[%v] Failed, config: %v", kv.gid, kv.me, op.Key, shard, kv.config)
			kv.mu.Unlock()
			return "", ErrWrongGroup
		}
	}
	// 去重
	val, ok := kv.applied.Get(op.ClerkID, op.ReqId)
	if ok {
		kv.mu.Unlock()
		DPrintf("[Server%d-%d] Already Applied [%s], key:[%s], Val:[%s]", kv.gid, kv.me, op.Type, op.Key, val)
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
	DPrintf("[Server%d-%d] Raft Start handle [%s],  key:[%s] value:[%s]", kv.gid, kv.me, op.Type, op.Key, op.Value)

	timer := time.NewTimer(1000 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		DPrintf("[Server%d-%d] handle [%s] Timeout, key:[%s] value:[%s]", kv.gid, kv.me, op.Type, op.Key, op.Value)
		return "", ErrTimeout
	case msg := <-ch:
		if msg.Err != OK {
			return "", msg.Err
		}
		DPrintf("[Server%d-%d] handle [%s] Success, key:[%s] value:[%s]", kv.gid, kv.me, op.Type, op.Key, op.Value)
		return msg.Val, OK
	}
}

func (kv *ShardKV) handleOperation() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			op := msg.Command.(Op)
			DPrintf("[Server%d-%d] Raft Apply [%s], key:[%s] value:[%s]", kv.gid, kv.me, op.Type, op.Key, op.Value)

			var (
				val string
				err Err
			)
			kv.mu.Lock()
			if msg.CommandIndex < kv.lastApplied {
				kv.mu.Unlock()
				DPrintf("[Server%d-%d] Raft Apply Index[%d] < LastApplied[%d], Skip", kv.gid, kv.me, msg.CommandIndex, kv.lastApplied)
				continue
			}
			if op.Key != "" {
				shard := key2shard(op.Key)
				// 判断Shard是否正确
				if kv.config.Num == 0 {
					kv.mu.Unlock()
					continue
				}
				if kv.config.Shards[shard] != kv.gid {
					DPrintf("[Server%d-%d] Handle Key[%s] Shard[%v] Failed, config: %v", kv.gid, kv.me, op.Key, shard, kv.config)
					kv.mu.Unlock()
					continue
				}
			}
			// 去重
			if _, ok := kv.applied.Get(op.ClerkID, op.ReqId); ok {
				kv.mu.Unlock()
				DPrintf("[Server%d-%d] Already Applied [%s], key:[%s]", kv.gid, kv.me, op.Type, op.Key)
				continue
			}
			switch op.Type {
			case OpTypeGet:
				val, err = kv.db.Get(op.Key)
			case OpTypePut:
				val, err = kv.db.Put(op.Key, op.Value)
			case OpTypeAppend:
				val, err = kv.db.Append(op.Key, op.Value)
			case OpTypeConfig:
				err = kv.UpdateConfig(op.Config)
			case OpTypeAddShard:
				err = kv.AddShard(op.Shard, op.ShardData, op.ConfigNum, op.AppliedLog)
			case OpTypeRemoveShard:
				err = kv.RemoveShard(op.Shard, op.ConfigNum)
			}
			ch := kv.notifyCh.Get(op.ClerkID, op.ReqId)
			if err == OK {
				kv.applied.Put(op.ClerkID, op.ReqId, val)
			}
			kv.lastApplied = msg.CommandIndex
			kv.mu.Unlock()

			if ch != nil {
				DPrintf("[Server%d-%d] Get NotifyCh Done ,Op:[%s], key:[%s], ClerkId:[%d], Req:[%d]",
					kv.gid, kv.me, op.Type, op.Key, op.ClerkID, op.ReqId)
				ch <- NotifyMsg{
					Val: val,
					Err: err,
				}
				DPrintf("[Server%d-%d] Notify Clerk [%d] Req [%d] Done,Op:[%s], key:[%s] value:[%s]",
					kv.gid, kv.me, op.ClerkID, op.ReqId, op.Type, op.Key, op.Value)
				close(ch)
			}

			// 判断是否需要生成快照
			size := kv.persister.RaftStateSize()

			if kv.maxraftstate != -1 && size >= int(float64(kv.maxraftstate)*0.9) {
				DPrintf("[Server%d-%d] RaftStateSize:[%d] >= MaxRaftState:[%d], Start GenSnapShot", kv.gid, kv.me, size, kv.maxraftstate)
				kv.mu.Lock()
				data := kv.GenSnapShot()
				kv.mu.Unlock()
				DPrintf("[Server%d-%d] Do Raft SnapShot", kv.gid, kv.me)
				go kv.rf.Snapshot(msg.CommandIndex, data)
			}
		} else if msg.SnapshotValid {
			DPrintf("[Server%d-%d] Raft Apply Snapshot, Index:[%d]", kv.gid, kv.me, msg.SnapshotIndex)
			kv.mu.Lock()
			if msg.SnapshotIndex >= kv.lastApplied {
				kv.LoadSnapShot(msg.Snapshot)
				kv.lastApplied = msg.SnapshotIndex
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) AddShard(shard int, shardData []KV, cfgNum int, appliedLog AppliedLog) Err {
	if cfgNum != kv.config.Num {
		return ErrWrongConfigNum
	}
	// 写入数据库
	for i := range shardData {
		kv.db.Put(shardData[i].Key, shardData[i].Value)
	}
	kv.applied.Merge(appliedLog)
	DPrintf("[Server%d-%d] Put data Success, Shard:[%d]", kv.gid, kv.me, shard)
	// 更新状态
	kv.ShardsStatus[shard] = ShardStatusServing
	DPrintf("[Server%d-%d] Update Status Success, Shard:[%d], Status:[%v]", kv.gid, kv.me, shard, kv.ShardsStatus)
	return OK
}

func (kv *ShardKV) RemoveShard(shard int, cfgNum int) Err {
	if cfgNum != kv.config.Num {
		return ErrWrongConfigNum
	}
	// 删除数据库
	kv.db.DeleteByShard(shard)
	DPrintf("[Server%d-%d] Delete data Success, Shard:[%d]", kv.gid, kv.me, shard)
	// 更新状态
	kv.ShardsStatus[shard] = ShardStatusServing
	DPrintf("[Server%d-%d] Update Status Success, Shard:[%d], Status:[%v]", kv.gid, kv.me, shard, kv.ShardsStatus)
	return OK
}

func (kv *ShardKV) DiffShards(new shardctrler.Config) []int {
	var shards []int
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.config.Shards[i] != 0 && kv.config.Shards[i] != new.Shards[i] {
			shards = append(shards, i)
		}
	}
	return shards
}

// 如果所有Shard都处于Serving状态，则允许更新
func (kv *ShardKV) allowToUpdateConfig() bool {
	for _, status := range kv.ShardsStatus {
		if status != ShardStatusServing {
			return false
		}
	}
	return true
}

func (kv *ShardKV) UpdateConfig(newCfg shardctrler.Config) Err {
	if kv.config.Num >= newCfg.Num {
		DPrintf("[Server%d-%d] Config Num[%d] >= New Config Num[%d], Do not need to Update", kv.gid, kv.me, kv.config.Num, newCfg.Num)
		return OK
	}
	if newCfg.Num-kv.config.Num != 1 {
		return ErrWrongConfigNum
	}
	if newCfg.Num == 1 {
		DPrintf("[Server%d-%d] Config is NIL, Apply Config, Update :[%v]", kv.gid, kv.me, newCfg)
		kv.config = newCfg
		return OK
	}

	// 判断是否更新
	if !kv.allowToUpdateConfig() {
		DPrintf("[Server%d-%d] Can't Update Config, Shards Not Serving, Shard Status: %v", kv.gid, kv.me, kv.ShardsStatus)
		return ErrUpdateConfig
	}

	diffShards := kv.DiffShards(newCfg)
	// 如果Shards不存在差别
	if len(diffShards) == 0 {
		kv.preConfig = kv.config
		kv.config = newCfg
		DPrintf("[Server%d-%d] No Diff, Apply Config, Update Num:[%d]", kv.gid, kv.me, newCfg.Num)
		return OK
	}

	// 已确保"差异Shard"处于服务状态，可以更新配置
	for _, shard := range diffShards {
		// 待发送的Shard
		if kv.config.Shards[shard] == kv.gid && newCfg.Shards[shard] != kv.gid {
			kv.ShardsStatus[shard] = ShardStatusSending
		}
		// 待接受的Shard
		if kv.config.Shards[shard] != kv.gid && newCfg.Shards[shard] == kv.gid {
			kv.ShardsStatus[shard] = ShardStatusReceiving
		}
	}
	kv.preConfig = kv.config
	kv.config = newCfg
	DPrintf("[Server%d-%d] Update Config Success:[%v], Current Status:[%v]", kv.gid, kv.me, newCfg, kv.ShardsStatus)
	return OK
}

func (kv *ShardKV) GenSnapShot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kv.db); err != nil {
		DPrintf("[Server%d-%d] GenSnapShot Encode DB Failed, Err: %v", kv.gid, kv.me, err)
		return nil
	}
	if err := e.Encode(kv.applied); err != nil {
		DPrintf("[Server%d-%d] GenSnapShot Encode Applied Failed, Err: %v", kv.gid, kv.me, err)
		return nil
	}
	if err := e.Encode(kv.lastApplied); err != nil {
		DPrintf("[Server%d-%d] GenSnapShot Encode LastApplied Failed, Err: %v", kv.gid, kv.me, err)
		return nil
	}
	if err := e.Encode(kv.config); err != nil {
		DPrintf("[Server%d-%d] GenSnapShot Encode Config Failed, Err: %v", kv.gid, kv.me, err)
		return nil
	}
	if err := e.Encode(kv.preConfig); err != nil {
		DPrintf("[Server%d-%d] GenSnapShot Encode PreConfig Failed, Err: %v", kv.gid, kv.me, err)
		return nil
	}
	if err := e.Encode(kv.ShardsStatus); err != nil {
		DPrintf("[Server%d-%d] GenSnapShot Encode ShardsStatus Failed, Err: %v", kv.gid, kv.me, err)
		return nil
	}
	if err := e.Encode(kv.ClerkId); err != nil {
		DPrintf("[Server%d-%d] GenSnapShot Encode ClerkId Failed, Err: %v", kv.gid, kv.me, err)
		return nil
	}
	if err := e.Encode(kv.NextReqId); err != nil {
		DPrintf("[Server%d-%d] GenSnapShot Encode NextReqId Failed, Err: %v", kv.gid, kv.me, err)
		return nil
	}

	data := w.Bytes()
	return data
}

func (kv *ShardKV) LoadSnapShot(snapShot []byte) {
	if len(snapShot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapShot)
	d := labgob.NewDecoder(r)

	var (
		db          Database
		applied     AppliedLog
		lastApplied int
		cfg         shardctrler.Config
		preCfg      shardctrler.Config
		status      [shardctrler.NShards]ShardStatus
		clerkId     int64
		nextReqId   int64
	)

	if err := d.Decode(&db); err != nil {
		DPrintf("[Server%d-%d] LoadSnapShot Decode DB Failed, Err: %v", kv.gid, kv.me, err)
		return
	}
	if err := d.Decode(&applied); err != nil {
		DPrintf("[Server%d-%d] LoadSnapShot Decode Applied Failed, Err: %v", kv.gid, kv.me, err)
		return
	}
	if err := d.Decode(&lastApplied); err != nil {
		DPrintf("[Server%d-%d] LoadSnapShot Decode LastApplied Failed, Err: %v", kv.gid, kv.me, err)
		return
	}
	if err := d.Decode(&cfg); err != nil {
		DPrintf("[Server%d-%d] LoadSnapShot Decode Config Failed, Err: %v", kv.gid, kv.me, err)
		return
	}
	if err := d.Decode(&preCfg); err != nil {
		DPrintf("[Server%d-%d] LoadSnapShot Decode PreConfig Failed, Err: %v", kv.gid, kv.me, err)
		return
	}
	if err := d.Decode(&status); err != nil {
		DPrintf("[Server%d-%d] LoadSnapShot Decode ShardsStatus Failed, Err: %v", kv.gid, kv.me, err)
		return
	}
	if err := d.Decode(&clerkId); err != nil {
		DPrintf("[Server%d-%d] LoadSnapShot Decode ClerkId Failed, Err: %v", kv.gid, kv.me, err)
		return
	}
	if err := d.Decode(&nextReqId); err != nil {
		DPrintf("[Server%d-%d] LoadSnapShot Decode NextReqId Failed, Err: %v", kv.gid, kv.me, err)
		return
	}

	kv.db = &db
	kv.applied = &applied
	kv.lastApplied = lastApplied
	kv.config = cfg
	kv.preConfig = preCfg
	kv.ShardsStatus = status
	kv.ClerkId = clerkId
	kv.NextReqId = nextReqId
}

func (kv *ShardKV) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

func (kv *ShardKV) PollConfig() {
	for !kv.killed() {
		time.Sleep(100 * time.Millisecond)
		if kv.isLeader() {
			update := false
			kv.mu.Lock()
			nextCfg := kv.config.Num + 1
			kv.mu.Unlock()
			cfg := kv.mck.Query(nextCfg)
			kv.mu.Lock()
			if cfg.Num > kv.config.Num {
				update = true
			}
			kv.mu.Unlock()

			if update {
				_, err := kv.opHandler(Op{
					Type:    OpTypeConfig,
					ClerkID: kv.ClerkId,
					ReqId:   kv.nextId(),
					Config:  cfg,
				})
				if err != OK {
					DPrintf("[Server%d-%d] Push Config Op Failed, Err: %v", kv.gid, kv.me, err)
				}
			}
		}
	}
}

func (kv *ShardKV) ShardWatcher() {
	for !kv.killed() {
		time.Sleep(50 * time.Millisecond)
		if !kv.isLeader() {
			continue
		}

		kv.mu.Lock()
		if kv.config.Num == 0 {
			kv.mu.Unlock()
			continue
		}

		for shard, status := range kv.ShardsStatus {
			if status == ShardStatusSending {
				DPrintf("[Server%d-%d] ShardWatcher is Preparing to Send Shard", kv.gid, kv.me)
				kv.NextReqId++
				go kv.SendShard(shard, kv.config.Num, kv.applied.Copy(), kv.NextReqId)
			}
		}
		kv.mu.Unlock()
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
	DPrintf("[Server%d-%d] Killed", kv.gid, kv.me)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.ClerkId = nrand()
	kv.db = NewDatabase(kv.gid, kv.me)
	kv.notifyCh = &NotifyCh{}
	kv.applied = &AppliedLog{}
	kv.persister = persister
	kv.LoadSnapShot(persister.ReadSnapshot())

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.handleOperation()
	go kv.PollConfig()
	go kv.ShardWatcher()
	DPrintf("[Server%d-%d] Start Success", kv.gid, kv.me)
	return kv
}
