package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state         State
	heartBeatTime time.Time
	electionTime  time.Time

	// 以下字段论文原文
	log         []LogEntry
	currentTerm int   // 当前任期
	votedFor    int   // 投票给的候选者ID
	commitIndex int   // 已经提交的最高日志条目索引
	lastApplied int   // 已经应用到状态机的最高日志条目索引
	nextIndex   []int // 对每个服务器，要发送的下一跳日志条目的索引
	matchIndex  []int // 对每个服务器，已知被复制的最高日志条目的索引
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if rf.state == StateLeader {
		isleader = true
	}

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // 候选者的任期号
	CandidateId  int // 候选者ID
	LastLogIndex int // 候选者最新日志的索引
	LastLogTerm  int // 候选者最新日志的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // 当前任期
	VoteGranted bool // 是否投票
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DebugPrintf(dVote, rf.me, "收到候选人S%d选举请求", args.CandidateId)
	if args.Term < rf.currentTerm {
		DebugPrintf(dVote, rf.me, "拒绝候选者S%d，任期小于自身", args.CandidateId)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = StateFollower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if rf.LastLogIndex() != 0 && rf.LastLogTerm() >= args.LastLogTerm &&
			!(rf.LastLogTerm() == args.LastLogTerm && rf.LastLogIndex() <= args.LastLogIndex) {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			DebugPrintf(dVote, rf.me, "候选者S%d日志更旧，拒绝选举", args.CandidateId)
			return
		}
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateId
		DebugPrintf(dVote, rf.me, "选举候选人 S%d", args.CandidateId)
		// 重置选举时间
		rf.resetElectionTime()
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DebugPrintf(dVote, rf.me, "已选择S%d 拒绝候选人S%d", rf.votedFor, args.CandidateId)
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries RPC 发送心跳或者日志提交
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DebugPrintf(dClient, rf.me, "收到 S%d AppendEntries", args.LeaderId)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果收到的任期小于自身，则拒绝Leader
	if args.Term < rf.currentTerm {
		DebugPrintf(dClient, rf.me, "收到的任期号Term%d小于自身任期号Term%d", args.Term, rf.currentTerm)
		DebugPrintf(dTerm, rf.me, "当前任期号：%d", rf.currentTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.state == StateCandidate && rf.currentTerm == args.Term {
		rf.state = StateFollower
		DebugPrintf(dInfo, rf.me, "由候选者降级为追随者")
	}

	// 收到的任期大于自身，则追随该Leader
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		// 如果是Leader或候选人，则降级为Follower
		if rf.state == StateLeader || rf.state == StateCandidate {
			rf.state = StateFollower
			DebugPrintf(dClient, rf.me, "收到的新的任期号Term%d，降级为Follower", rf.currentTerm)
		}
	}

	// 检查Leader的日志是否与追随者相同
	if args.PrevLogIndex > 0 && args.PrevLogIndex > rf.LastLogIndex() {
		reply.Success = false
		reply.Term = rf.currentTerm
		DebugPrintf(dInfo, rf.me, "日志Index不一致，收到PrevLogIndex:%d,rf.LastLogIndex=%d", args.PrevLogIndex, rf.LastLogIndex())
		return
	}
	// 如果日志在PrevLogIndex中不包含Term与PrevLogTerm匹配的Entry，则回复false
	if rf.LastLogIndex() > 0 && args.PrevLogIndex > 0 && args.PrevLogTerm != rf.GetLog(args.PrevLogIndex).Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		DebugPrintf(dInfo, rf.me, "日志Term不一致，收到的PrevLogTerm: %d,MyPrevLogTerm: %d", args.PrevLogTerm, rf.GetLog(args.PrevLogIndex).Term)
		return
	}
	for i, entry := range args.Entries {
		// 第一个判断条件防止并发过程中有旧的数据被重新收到
		// 第二个判断条件是截断未提交的过期数据
		if rf.LastLogIndex() < args.PrevLogIndex+i+1 || rf.GetLog(args.PrevLogIndex+i+1).Term != entry.Term {
			rf.log = append(rf.GetLogSlice(1, args.PrevLogIndex+i+1), args.Entries[i:]...)
			break
		}
	}
	DebugPrintf(dInfo, rf.me, "Exist Entries: %v", rf.log)

	// 5. 如果leaderCommit > commitIndex，则commitIndex设置为min(leaderCommit,最新Entry的索引)
	if args.LeaderCommit > rf.commitIndex {
		DebugPrintf(dInfo, rf.me, "收到Leader CommitIdx为%d", args.LeaderCommit)
		// 不能超过Leader的Commit。如果Peer的Log比较滞后，args.PrevLogIndex+len(args.Entries)能快速更新commitIdx
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		DebugPrintf(dCommit, rf.me, "更新CommitIdx为%d", rf.commitIndex)
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	rf.resetElectionTime()
}

func (rf *Raft) appendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) broadcastEntries() {
	DebugPrintf(dLeader, rf.me, "开始广播AppendEntries")

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		var (
			prevLogIndex int
			prevLogTerm  int
			entries      []LogEntry
		)
		prevLogIndex = rf.nextIndex[i] - 1
		if rf.nextIndex[i]-1 != 0 {
			prevLogTerm = rf.GetLog(prevLogIndex).Term
		}

		// 剩余log未提交，则附加Entries
		if rf.LastLogIndex() >= rf.nextIndex[i] {
			entries = rf.GetLogSlice(rf.nextIndex[i], rf.LastLogIndex()+1)
			DebugPrintf(dInfo, rf.me, "For S%d, Append Entiries: %d, nextIdx:%d", i, len(entries), rf.nextIndex[i])
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: rf.commitIndex,
			Entries:      entries,
		}

		go rf.sendEntry2Server(i, args)
	}

	rf.resetHeartBeatTime()
	rf.resetElectionTime()
}

func (rf *Raft) sendEntry2Server(server int, args AppendEntriesArgs) {
	var reply AppendEntriesReply
	if !rf.appendEntries(server, &args, &reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		if rf.state == StateLeader {
			DebugPrintf(dInfo, rf.me, "Term:%d > CurrentTerm%d,降级为Follower", reply.Term, rf.currentTerm)
			rf.state = StateFollower
		}
		rf.currentTerm = reply.Term
		rf.resetElectionTime()
		return
	}

	if rf.state != StateLeader {
		DebugPrintf(dWarn, rf.me, "非Leader，拒绝响应 S%d:%d", server, reply.Term)
		return
	}

	if reply.Term < rf.currentTerm {
		DebugPrintf(dWarn, rf.me, "响应的任期号%d更小，拒绝接受", reply.Term, rf.currentTerm)
		return
	}

	if reply.Success {
		DebugPrintf(dLeader, rf.me, "S%d成功AppendEntry", server)
		//更新peer的nextIdx和matchIdx
		newNext := args.PrevLogIndex + len(args.Entries) + 1
		newMatch := args.PrevLogIndex + len(args.Entries)
		//计算当前commitIdx，需要保证幂等性
		rf.nextIndex[server] = max(newNext, rf.nextIndex[server])
		rf.matchIndex[server] = max(newMatch, rf.matchIndex[server])
		DebugPrintf(dClient, rf.me, "S%d的NextIdx修改为%d，MatchIdx修改为%d", server, rf.nextIndex[server], rf.matchIndex[server])
		//寻找commitIdx
		DebugPrintf(dLog2, rf.me, "LastLogIndex=%d,commitIndex=%d", rf.LastLogIndex(), rf.commitIndex)
		for n := rf.LastLogIndex(); n > rf.commitIndex && rf.GetLog(n).Term == rf.currentTerm; n-- {
			cnt := 1
			for peer, matchIdx := range rf.matchIndex {
				if peer == rf.me {
					continue
				}
				// 如果匹配的索引大于n
				if matchIdx >= n {
					cnt++
				}
			}
			if cnt > rf.halfPeerNum() {
				// n是数组下标，n+1为索引
				rf.commitIndex = n
				DebugPrintf(dCommit, rf.me, "获取半数Peer同意，更新CommitIdx为%d", n)
				break
			}
		}
	} else {
		DebugPrintf(dLeader, rf.me, "S%d拒绝AppendEntry", server)

		// 减少nextIdx，然后重试
		if rf.nextIndex[server] > 1 {
			rf.nextIndex[server]--
		}

		nextIdx := rf.nextIndex[server]

		var (
			prevLogIndex = nextIdx - 1
			prevLogTerm  = 0
		)
		if prevLogIndex != 0 {
			prevLogTerm = rf.GetLog(prevLogIndex).Term
		}

		entries := rf.GetLogSlice(nextIdx, rf.LastLogIndex()+1)

		DebugPrintf(dLog2, rf.me, "减少S%d nextId，nextId:%d", server, rf.nextIndex[server])
		newArg := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		go rf.sendEntry2Server(server, newArg)
	}
}

// 开始选举
func (rf *Raft) raiseElection() {
	rf.votedFor = rf.me
	rf.state = StateCandidate
	rf.currentTerm++

	// 升级为Candidate之后，需要更新MatchIndex、NextIndex
	for k := range rf.nextIndex {
		//初始时，nextIndex为最后日志索引 + 1，如果索引由1开始，那么log数组的长度即最后日志索引
		rf.nextIndex[k] = rf.LastLogIndex() + 1
		//初始时，matchIndex均为0
		rf.matchIndex[k] = 0
	}

	DebugPrintf(dTerm, rf.me, "当前任期号：%d", rf.currentTerm)
	DebugPrintf(dVote, rf.me, "发起选举")
	// 构建rpc请求与响应

	var (
		lastLogIndex int
		lastLogTerm  int
	)
	lastLogIndex = rf.LastLogIndex()
	if lastLogIndex != 0 {
		lastLogTerm = rf.LastLogTerm()
	}

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	// 并行发送投票信息
	go rf.broadcastElection(args)

	//重置选举时间
	rf.resetElectionTime()
}

func (rf *Raft) broadcastElection(args RequestVoteArgs) {
	ticket := 1
	var once sync.Once
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(server int) {
			var reply RequestVoteReply
			if !rf.sendRequestVote(server, &args, &reply) {
				// 发送选取请求失败
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.VoteGranted && reply.Term == rf.currentTerm {
				ticket++

				// 如果票数超过半数，则晋升Leader
				if ticket > (len(rf.peers)-1)>>1 {
					// 确保每次晋升Leader只执行一次
					once.Do(func() {
						DebugPrintf(dLeader, rf.me, "升级为Leader")
						DebugPrintf(dTerm, rf.me, "当前任期号：%d", rf.currentTerm)
						rf.state = StateLeader
						rf.broadcastEntries() // 当选之后立马发送Entries
					})
				}
			}
		}(i)
	}
}

func (rf *Raft) commitMsg(applyCh chan ApplyMsg) {
	for !rf.killed() {
		msg := make([]ApplyMsg, 0, rf.commitIndex-rf.lastApplied+1)

		rf.mu.Lock()

		// lastApplied为索引号，从1开始
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msg = append(msg, ApplyMsg{
				CommandValid: true,
				Command:      rf.GetLog(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			})
		}

		rf.mu.Unlock()

		for _, v := range msg {
			DebugPrintf(dCommit, rf.me, "提交 {Index:%d , Cmd: %v}至ApplyCh", v.CommandIndex, v.Command)
			applyCh <- v
		}

		ms := 20
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != StateLeader {
		return -1, -1, false
	}

	DebugPrintf(dInfo, rf.me, "收到Tester传来的Command: { %v }", command)
	rf.log = append(rf.log, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})

	term = rf.currentTerm
	index = rf.LastLogIndex() // 日志索引从1开始
	isLeader = true

	// 发送AppendEntry
	rf.broadcastEntries()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	DebugPrintf(dWarn, rf.me, "Killed")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 重置选举时间
func (rf *Raft) resetElectionTime() {
	now := time.Now()
	extra := time.Duration(float64(rand.Int63()%int64(minElectionInterval)) * 0.7)
	rf.electionTime = now.Add(minElectionInterval).Add(extra)
}

// 重置心跳时间
func (rf *Raft) resetHeartBeatTime() {
	now := time.Now()
	rf.heartBeatTime = now.Add(minHeartBeatInterval)
}

func (rf *Raft) halfPeerNum() int {
	return len(rf.peers) >> 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state == StateLeader && time.Now().After(rf.heartBeatTime) {
			rf.broadcastEntries()
		}
		rf.mu.Unlock()
		rf.mu.Lock()
		if time.Now().After(rf.electionTime) {
			rf.raiseElection()
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.log = make([]LogEntry, 0)
	rf.state = StateFollower
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.resetElectionTime()
	rf.resetHeartBeatTime()
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	go rf.commitMsg(applyCh)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
