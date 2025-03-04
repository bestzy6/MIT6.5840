package shardctrler

import (
	"sort"
)

type Balancer struct {
	me int
}

func NewBalancer(me int) *Balancer {
	return &Balancer{
		me: me,
	}
}

func (b *Balancer) ReBalanceShards(oldShards [NShards]int, groups map[int][]string) [NShards]int {
	if len(groups) == 0 {
		return [NShards]int{}
	}
	// 初始情况下，不存在GID的Shard应该归属于0
	// oldShards可能被分配至已经不存在的Group
	grpShardMap, freeShards := b.groupShardMap(oldShards, groups)
	sortedGid := b.sortGroupByShard(grpShardMap)
	sleepGid := b.getSleepGid(sortedGid)
	DPrintf("[Server%d] ReBalanceShards sortedGid:%v ; sleepGid:%v ; freeShards: %v", b.me, sortedGid, sleepGid, freeShards)

	// 计算Shard平均数量，平均值最小为1
	avgShardNum := NShards / (len(sortedGid) - sleepGid.Len())
	modShardNum := NShards % (len(sortedGid) - sleepGid.Len())

	// 将超过平均值的Shard放置在freeShards中
	for i, gid := range sortedGid {
		shards := grpShardMap[gid]
		// 如果GID在sleepGid中，则将Shard放置在freeShards中
		if sleepGid.Contain(gid) {
			freeShards.Add(shards...)
			continue
		}

		// 计算当前GID应该拥有的Shard数量
		var pos int
		if i < modShardNum {
			pos = avgShardNum + 1
		} else {
			pos = avgShardNum
		}

		// 如果Shard数量大于pos值，则将多余的Shard放置在freeShards中
		if len(shards) > pos {
			freeShards.Add(shards[pos:]...)
			grpShardMap[gid] = shards[:pos]
		}
	}
	// 保证每次分配Shard时取出的Shard是有序的，以此确保副本的一致性
	unAsgnShards := freeShards.SortedSlice()

	// 重新分配Shards
	for i, gid := range sortedGid {
		if sleepGid.Contain(gid) {
			continue
		}
		shards := grpShardMap[gid]

		var pos int
		if i < modShardNum {
			pos = avgShardNum + 1
		} else {
			pos = avgShardNum
		}

		// 如果Shard数量小于平均值，则从unAsgnShards中取出Shard进行分配
		for len(shards) < pos {
			if len(unAsgnShards) == 0 {
				break
			}
			shards = append(shards, unAsgnShards[0])
			unAsgnShards = unAsgnShards[1:]
		}
		grpShardMap[gid] = shards

	}
	DPrintf("[Server%d] ReBalanceShards grpShardMap: %v", b.me, grpShardMap)
	shards := b.genNShards(grpShardMap)
	return shards
}

// 生成key为GroupID，value为ShardID的Map，如果ShardID不存在对应的Group，则放置在freeShards中
func (b *Balancer) groupShardMap(oldShards [NShards]int, groups map[int][]string) (grpShardMap map[int][]int, freeShards *Set) {
	grpShardMap = make(map[int][]int, len(groups))
	for gid := range groups {
		grpShardMap[gid] = make([]int, 0, NShards)
	}
	freeShards = NewSet()
	for i, gid := range oldShards {
		// 如果groups中不存在对应的gid或者gid为0
		// 则放置在freeShards中
		if _, ok := groups[gid]; !ok || gid == 0 {
			freeShards.Add(i)
			continue
		}
		grpShardMap[gid] = append(grpShardMap[gid], i)
	}
	return
}

// 按照Shard数量倒序排列GroupID，注意需要稳定排序！
func (b *Balancer) sortGroupByShard(grpShardMap map[int][]int) (sortedGid []int) {
	grpShardNum := make(map[int]int, len(grpShardMap))
	sortedGid = make([]int, 0, len(grpShardMap))
	for gid, shards := range grpShardMap {
		grpShardNum[gid] = len(shards)
		sortedGid = append(sortedGid, gid)
	}
	// 按照Shard数量以及GID排序，能够保证排序结果的稳定性
	sort.Slice(sortedGid, func(i, j int) bool {
		if grpShardNum[sortedGid[i]] == grpShardNum[sortedGid[j]] {
			return sortedGid[i] < sortedGid[j]
		}
		return grpShardNum[sortedGid[i]] > grpShardNum[sortedGid[j]]
	})
	return
}

func (b *Balancer) getSleepGid(sortedGid []int) *Set {
	sleepGid := NewSet()
	if len(sortedGid) <= NShards {
		return sleepGid
	}
	for i := len(sortedGid) - 1; i >= NShards; i-- {
		sleepGid.Add(sortedGid[i])
	}
	return sleepGid
}

func (b *Balancer) genNShards(grpShardMap map[int][]int) [NShards]int {
	shards := [NShards]int{}
	for gid, shardIds := range grpShardMap {
		for _, shardId := range shardIds {
			shards[shardId] = gid
		}
	}
	return shards
}
