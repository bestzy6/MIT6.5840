package shardkv

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

func (a *AppliedLog) Merge(other AppliedLog) {
	if a.Applied == nil {
		a.Applied = make(map[int64]AppliedMsg)
	}
	for k, v := range other.Applied {
		if a.Applied[k].ReqId < v.ReqId {
			a.Applied[k] = v
		}
	}
}

func (a *AppliedLog) Copy() AppliedLog {
	if a.Applied == nil {
		return AppliedLog{}
	}
	applied := make(map[int64]AppliedMsg, len(a.Applied))
	for k, v := range a.Applied {
		applied[k] = v
	}
	return AppliedLog{Applied: applied}
}
