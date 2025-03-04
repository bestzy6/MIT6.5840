package shardctrler

import (
	"6.5840/raft"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs  []Config // indexed by config num
	notifyCh *NotifyCh
	applied  *AppliedLog
}

type Op struct {
	// Your data here.
	Type    string
	Servers map[int][]string //Join
	GIDs    []int            //Leave
	Shard   int              //Move
	GID     int              //Move
	Num     int              //Query
	ClerkId int64
	ReqId   int64
}

func (sc *ShardCtrler) opHandler(op Op) (Config, Err) {
	sc.mu.Lock()
	cfg, ok := sc.applied.Get(op.ClerkId, op.ReqId)
	if ok {
		sc.mu.Unlock()
		return cfg, OK
	}
	ch := sc.notifyCh.Add(op.ClerkId, op.ReqId)
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		sc.notifyCh.Delete(op.ClerkId, op.ReqId)
	}()

	// 提交到Raft
	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		return Config{}, WrongLeader
	}

	timer := time.NewTimer(1000 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return Config{}, TimeOut
	case msg := <-ch:
		if msg.Err != OK {
			return Config{}, msg.Err
		}
		return msg.Val, OK
	}
}

func (sc *ShardCtrler) handleOperation() {
	for msg := range sc.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)

			sc.mu.Lock()
			// 去重
			if _, ok := sc.applied.Get(op.ClerkId, op.ReqId); ok {
				sc.mu.Unlock()
				continue
			}
			cfg, err := sc.opExecutor(op)
			if err == OK {
				sc.applied.Put(op.ClerkId, op.ReqId, cfg)
			}
			ch := sc.notifyCh.Get(op.ClerkId, op.ReqId)
			sc.mu.Unlock()

			if ch != nil {
				ch <- NotifyMsg{Val: cfg, Err: err}
				DPrintf("[Server%d] Raft Apply [%s] Success", sc.me, op.Type)
				close(ch)
			}
		}
	}
}

func (sc *ShardCtrler) opExecutor(op Op) (Config, Err) {
	var (
		cfg Config
		err Err
	)
	switch op.Type {
	case Join:
		cfg, err = sc.joinExecutor(op)
		sc.configs = append(sc.configs, cfg)
		return cfg, err
	case Leave:
		cfg, err = sc.leaveExecutor(op)
		sc.configs = append(sc.configs, cfg)
		return cfg, err
	case Move:
		cfg, err = sc.moveExecutor(op)
		sc.configs = append(sc.configs, cfg)
		return cfg, err
	case Query:
		cfg, err = sc.queryExecutor(op)
		return cfg, err
	}
	return Config{}, OK
}

// 添加新的副本组，添加之后，尽可能均匀的分配Shard
func (sc *ShardCtrler) joinExecutor(op Op) (Config, Err) {
	var (
		servers = op.Servers //GID -> 服务名称
		cfgNum  = len(sc.configs)
		lastCfg = sc.configs[cfgNum-1]
	)
	// 深拷贝，创建新的Groups
	DPrintf("[Server%d] %v Join, Origin Shards:%v", sc.me, servers, lastCfg.Shards)
	newGroups := make(map[int][]string, len(lastCfg.Groups))
	for gid, srvs := range lastCfg.Groups {
		var (
			newServices = make([]string, 0, len(srvs))         // 服务名称
			ServicesMap = make(map[string]struct{}, len(srvs)) // 服务名称Set，用于去重
		)
		for _, srv := range srvs {
			newServices = append(newServices, srv)
			ServicesMap[srv] = struct{}{}
		}
		if tmpServices, ok := servers[gid]; ok {
			for _, tmpService := range tmpServices {
				// 判断是否重复
				if _, exist := ServicesMap[tmpService]; exist {
					continue
				}
				newServices = append(newServices, tmpService)
			}
			// 删除GID，表示已经添加
			delete(servers, gid)
		}
		newGroups[gid] = newServices
	}
	// 如果servers中的GID在原组中不存在，则servers长度不为0
	for gid, srvs := range servers {
		newGroups[gid] = srvs
	}
	DPrintf("[Server%d] %v Join newGroups: %v", sc.me, servers, newGroups)

	shards := NewBalancer(sc.me).ReBalanceShards(lastCfg.Shards, newGroups)
	DPrintf("[Server%d] %v Join balanced Shards: %v", sc.me, servers, shards)
	return Config{
		Num:    cfgNum,
		Shards: shards,
		Groups: newGroups,
	}, OK
}

// 应创建一个不包含这些组的新配置，并将这些组的分片分配给剩余的组。
// 新配置应尽可能均匀地在组之间分配分片，并应尽可能少地移动分片以实现该目标。
func (sc *ShardCtrler) leaveExecutor(op Op) (Config, Err) {
	var (
		gidMaps = make(map[int]struct{}, len(op.GIDs))
		cfgNum  = len(sc.configs)
		lastCfg = sc.configs[cfgNum-1]
	)
	for _, gid := range op.GIDs {
		gidMaps[gid] = struct{}{}
	}

	// 深拷贝，创建新的Groups
	DPrintf("[Server%d] %v Leave, Origin Shards:%v", sc.me, op.GIDs, lastCfg.Shards)
	newGroups := make(map[int][]string, len(lastCfg.Groups))
	for gid, srvs := range lastCfg.Groups {
		if _, ok := gidMaps[gid]; ok {
			continue
		}
		newSrvs := make([]string, len(srvs))
		copy(newSrvs, srvs)
		newGroups[gid] = newSrvs
	}
	DPrintf("[Server%d] %v Leave newGroups: %v", sc.me, op.GIDs, newGroups)

	shards := NewBalancer(sc.me).ReBalanceShards(lastCfg.Shards, newGroups)
	DPrintf("[Server%d] %v Leave balanced Shards: %v", sc.me, op.GIDs, shards)
	return Config{
		Num:    cfgNum,
		Shards: shards,
		Groups: newGroups,
	}, OK
}

func (sc *ShardCtrler) moveExecutor(op Op) (Config, Err) {
	var (
		shard  = op.Shard
		gid    = op.GID
		cfgNum = len(sc.configs)
		cfg    = sc.configs[cfgNum-1]
	)

	// 深拷贝
	newGroups := make(map[int][]string, len(cfg.Groups))
	for id, srvs := range cfg.Groups {
		newSrvs := make([]string, len(srvs))
		copy(newSrvs, srvs)
		newGroups[id] = newSrvs
	}

	var newShards [NShards]int
	for i := range cfg.Shards {
		if i == shard {
			newShards[i] = gid
			continue
		}
		newShards[i] = cfg.Shards[i]
	}

	newConfig := Config{
		Num:    cfgNum,
		Shards: newShards,
		Groups: newGroups,
	}
	return newConfig, OK
}

func (sc *ShardCtrler) queryExecutor(op Op) (Config, Err) {
	cfg := sc.configs[len(sc.configs)-1]
	num := op.Num
	if num < 0 || num >= len(sc.configs) {
		return cfg, OK
	}
	return sc.configs[num], OK
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		Type:    Join,
		Servers: args.Servers,
		ClerkId: args.ClerkId,
		ReqId:   args.ReqId,
	}
	_, err := sc.opHandler(op)
	if err == WrongLeader {
		reply.Err = WrongLeader
		reply.WrongLeader = true
		return
	}
	reply.Err = err
	return
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		Type:    Leave,
		GIDs:    args.GIDs,
		ClerkId: args.ClerkId,
		ReqId:   args.ReqId,
	}
	_, err := sc.opHandler(op)
	if err == WrongLeader {
		reply.Err = WrongLeader
		reply.WrongLeader = true
		return
	}
	reply.Err = err
	return
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		Type:    Move,
		Shard:   args.Shard,
		GID:     args.GID,
		ClerkId: args.ClerkId,
		ReqId:   args.ReqId,
	}
	_, err := sc.opHandler(op)
	if err == WrongLeader {
		reply.Err = WrongLeader
		reply.WrongLeader = true
		return
	}
	reply.Err = err
	return
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		Type:    Query,
		Num:     args.Num,
		ClerkId: args.ClerkId,
		ReqId:   args.ReqId,
	}
	cfg, err := sc.opHandler(op)
	if err == WrongLeader {
		reply.Err = WrongLeader
		reply.WrongLeader = true
		return
	}
	if err != OK {
		reply.Err = err
		return
	}
	reply.Err = OK
	reply.Config = cfg
	return
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.notifyCh = &NotifyCh{}
	sc.applied = &AppliedLog{}
	go sc.handleOperation()

	return sc
}
