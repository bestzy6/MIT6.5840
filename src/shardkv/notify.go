package shardkv

type NotifyMsg struct {
	Val string
	Err Err
}

type NotifyCh struct {
	m map[int64]map[int64]chan NotifyMsg // map[ClerkId][reqId] -> chan
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
