package shardkv

type MigrateShardArgs struct {
	Shard     int
	ConfigNum int
	KVs       []KV
	Applied   AppliedLog

	ClerkId int64
	ReqId   int64
}

type MigrateShardReply struct {
	ConfigNum int
	Err       Err
}

func (kv *ShardKV) MigrateShard(args *MigrateShardArgs, reply *MigrateShardReply) {
	if !kv.isLeader() {
		DPrintf("[Server%d-%d] MigrateShard Shard:[%d] not Leader", kv.gid, kv.me, args.Shard)
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	reply.ConfigNum = kv.config.Num

	if kv.config.Num < args.ConfigNum {
		DPrintf("[Server%d-%d] MigrateShard Shard:[%d] Current ConfigNum:[%d] < ConfigNum:[%d]",
			kv.gid, kv.me, args.Shard, kv.config.Num, args.ConfigNum)
		reply.Err = ErrWrongConfigNum
		kv.mu.Unlock()
		return
	}
	if kv.config.Num > args.ConfigNum {
		DPrintf("[Server%d-%d] MigrateShard Shard:[%d] Current ConfigNum:[%d] > ConfigNum:[%d]",
			kv.gid, kv.me, args.Shard, kv.config.Num, args.ConfigNum)
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	if kv.ShardsStatus[args.Shard] == ShardStatusServing {
		DPrintf("[Server%d-%d] MigrateShard Shard:[%d] Already Serving", kv.gid, kv.me, args.Shard)
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Type:       OpTypeAddShard,
		ClerkID:    args.ClerkId,
		ReqId:      args.ReqId,
		Shard:      args.Shard,
		ShardData:  args.KVs,
		ConfigNum:  args.ConfigNum,
		AppliedLog: args.Applied,
	}
	_, err := kv.opHandler(op)
	if err == OK {
		DPrintf("[Server%d-%d] MigrateShard Shard:[%d] Success", kv.gid, kv.me, args.Shard)
	}
	reply.Err = err
}

func (kv *ShardKV) SendShard(shard, configNum int, applied AppliedLog, reqId int64) {
	if !kv.isLeader() {
		DPrintf("[Server%d-%d] SendShard %d not Leader", kv.gid, kv.me, shard)
		return
	}

	var reply MigrateShardReply
	args := MigrateShardArgs{
		Shard:     shard,
		ConfigNum: configNum,
		Applied:   applied,
		KVs:       nil,
		ClerkId:   kv.ClerkId,
		ReqId:     reqId,
	}

	kv.mu.Lock()
	args.KVs = kv.db.GetByShard(shard)
	gid := kv.config.Shards[shard]
	servers, ok := kv.config.Groups[gid]
	if !ok {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	for i := range servers {
		srv := kv.make_end(servers[i])
		DPrintf("[Server%d-%d] SendShard %d To %s", kv.gid, kv.me, shard, servers[i])
		ok = srv.Call("ShardKV.MigrateShard", &args, &reply)
		if !ok {
			DPrintf("[Server%d-%d] SendShard %d RPC failed", kv.gid, kv.me, shard)
			continue
		}
		switch reply.Err {
		case OK:
			DPrintf("[Server%d-%d] SendShard %d To %s Success", kv.gid, kv.me, shard, servers[i])
			op := Op{
				Type:      OpTypeRemoveShard,
				ClerkID:   args.ClerkId,
				ReqId:     args.ReqId,
				Shard:     shard,
				ConfigNum: configNum,
			}
			_, err := kv.opHandler(op)
			if err != OK {
				DPrintf("[Server%d-%d] handle Op failed, Err: %s", kv.gid, kv.me, err)
			} else {
				DPrintf("[Server%d-%d] SendShard %d Success", kv.gid, kv.me, shard)
			}
		case ErrWrongLeader:
			continue
		default:
			DPrintf("[Server%d-%d] SendShard %d failed,Err:%s", kv.gid, kv.me, shard, reply.Err)
		}
	}
}
