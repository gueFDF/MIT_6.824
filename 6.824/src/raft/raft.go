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

	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// 心跳包超时时长
var hearttimeout time.Duration = time.Duration(110) * time.Millisecond

type MsgLog struct {
	Index   int         //日志索引
	Term    int         //任期号
	Command interface{} //具体操作
}

// raft server 状态枚举
const (
	Follower  = iota //跟随者
	Candidate        //候选者
	Leader           //leader
)

// 枚举投票失败的原因
const (
	ISLEADER = iota //对方是leader
	LOGOUT          //日志落后
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	//TODO 2A
	//需要持久化
	log         []MsgLog //存放日志信息
	voteFor     int      //在当前获得选票的候选人的 Id
	currentTerm int      //当前任期号

	////不需要持久化
	commitIndex int //已知的最大日志提交索引
	lastApplied int //最后被应用到状态机的日志条目索引值
	////leader里经常改变的
	nextIndext []int //对于每一个服务器，需要发送给他的下一个日志条目的索引值
	matchIndex []int //对于每一个服务器，已经复制给他的日志的最高索引值

	//自己添加的参数
	status int //当前raft server 状态
	//overtime time.Duration //超时时间
	timer *time.Ticker //计时器

	votenum int //获得票的总数

	applyCh chan ApplyMsg
}

// 附加日志RPC
type AppendEntriesArgs struct {
	Term         int      //领导人的任期号
	LeaderId     int      //领导人的ID,以便于跟随者重合向请求
	PrevLogIndex int      //新的日志条目紧随之前的索引值
	PrevLogTerm  int      //prevLogIndex 条目的任期号
	Entries      []MsgLog //准备存储的日志条目（表示心跳时为空；一次性发送多个是为了提高效率）
	LeaderCommit int      //领导人已经提交的日志的索引值
}

type AppendEntriesReply struct {
	Term     int  //当前的任期号，用于领导人更新自己的任期号
	Success  bool //跟随者包含了匹配上 prevLogIndex 和 prevLogTerm 的日志时为真
	Logindex int  //如果日志落后，应该发送的下一条日志的下标
}

// 请求投票RPC
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //候选人的任期号
	Candidateld  int //候选人ID
	LastLogIndex int //候选人的最后日志条目索引
	LastLogTerm  int //候选人最后日志条目的任期号
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //当前任期号
	VoteGranted bool //是否支持选举该候选人
	Voterr      int  //投票失败的原因
}

// 获取当前raft server 的状态
// 询问 Raft 的当前任期，以及它是否认为自己是领导者
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.status == Leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// 附加日志的RPC函数
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 重置超时选举时长
	rf.timer.Reset(getovertimr())
	reply.Term = rf.currentTerm //用于领导人更新任期号

	//1.当前leader已经落后
	if args.Term < rf.currentTerm {
		if args.PrevLogIndex+len(args.Entries)+1 <= len(rf.log) {
			DPrintf("leader%d任期为%d落后于当前节点%d的任期%d", args.LeaderId, args.Term, rf.me, rf.currentTerm)
			reply.Success = false
			return
		}
	}

	//重置ticker
	rf.currentTerm = args.Term
	rf.status = Follower
	rf.voteFor = args.LeaderId
	rf.votenum = 0
	reply.Success = true

	//更新当前server的最大日志提交索引
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), (float64(len(rf.log) - 1))))
		DPrintf("节点%d更新最大日志提交索引%d\n", rf.me, rf.commitIndex)
	}

	//进行日志提交
	rf.logcommit()
	if args.Entries == nil {
		//是心跳包
		reply.Logindex = len(rf.log)
		DPrintf("节点%d收到%d发送的心跳包,leader的任期%d,该节点的任期%d\n", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	currentLogIndex := len(rf.log) - 1

	//防止越界访问
	currentLogTerm := 0
	if currentLogIndex != -1 {
		currentLogTerm = rf.log[currentLogIndex].Term
	}

	//判断该服务器的日志是否落后
	if args.PrevLogIndex != currentLogIndex {
		println(rf.me, " 该follor的日志落后/超前,", args.PrevLogIndex, " ", currentLogIndex)
		reply.Logindex = currentLogIndex + 1
		reply.Success = false
		return
	}

	//判断新旧日志是否冲突(索引值相同，任期号不同)
	if args.PrevLogTerm != currentLogTerm {
		//删除这一条和之后所有的日志
		println(rf.me, " 新旧日志冲突,", args.PrevLogTerm, " ", currentLogTerm)
		rf.log = rf.log[:args.PrevLogIndex]
		reply.Logindex = args.PrevLogIndex
		reply.Success = false
		return
	}

	//日志追加
	rf.log = append(rf.log, args.Entries...)
	DPrintf("节点%d日志追加成功,日志%d内容为%v\n", rf.me, len(rf.log)-1, args.Entries)

	return
}

// 请求投票的RPC函数
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//重置超时时间
	rf.timer.Reset(getovertimr())
	reply.Term = rf.currentTerm

	currentLogIndex := len(rf.log) - 1
	currentLogTerm := 0
	if currentLogIndex >= 0 {
		currentLogTerm = rf.log[currentLogIndex].Term
	}
	//判断候选人日志是否落后于当前节点
	if args.LastLogIndex < currentLogIndex || args.LastLogTerm < currentLogTerm {
		reply.VoteGranted = false
		reply.Voterr = LOGOUT
		DPrintf("候选人%d日志%d落后当前节点日志%d,当前节点%d拒绝投票\n", args.Candidateld, args.LastLogIndex, currentLogIndex, rf.me)
		return
	}
	//判断候选人的term是否落后于当前raft Server的term
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		DPrintf("候选人%d任期%d落后,当前节点%d拒绝投票\n", args.Candidateld, args.Term, rf.me)
		return
	}
	//如果候选人任期大于该节点当前任期，更新任期，并放弃选举
	if args.Term > rf.currentTerm {
		rf.status = Follower
		rf.currentTerm = args.Term
		rf.voteFor = -1 //确保任期小于候选人的server手上都有票
	}

	//最后一步查看自己是否有选票
	if rf.voteFor != -1 && rf.voteFor != args.Candidateld {
		reply.VoteGranted = false //没有选票了
		return
	}
	//支持选票
	reply.VoteGranted = true
	rf.voteFor = args.Candidateld
	DPrintf("节点%d将票投给节点%d\n", rf.me, args.Candidateld)
	return
}

func (rf *Raft) sendRequestAppend(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)

	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Success == false {

	}
	//当前leader已经落后，已经不再适合当选leader
	if reply.Term > rf.currentTerm {
		DPrintf("当前节点%d任期%d落后于节点%d的任期%d\n", rf.me, rf.currentTerm, server, reply.Term)
		rf.status = Follower          //成为Follower
		rf.currentTerm = reply.Term   //更新任期
		rf.voteFor = -1               //重置选票
		rf.timer.Reset(getovertimr()) //重置超时时间
		rf.votenum = 0
	}
	//更新其他副本节点信息
	rf.nextIndext[server] = reply.Logindex

	return ok
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
	//远程调用失败，直至调用成功
	for !ok {
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != Candidate { //已经不是候选者，在其他协程被改变
		return false
	}
	//获得选票
	if reply.VoteGranted == true {
		rf.votenum++
	} else { //没有获得选票
		//当前节点落后了或日志落后，放弃选票
		if reply.Term > rf.currentTerm || reply.Voterr == LOGOUT ||
			reply.Voterr == ISLEADER {
			rf.status = Follower          //成为Follower
			rf.currentTerm = reply.Term   //更新任期
			rf.voteFor = -1               //重置选票
			rf.timer.Reset(getovertimr()) //重置超时时间
			rf.votenum = -999
		}
	}
	return ok
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
// 启动处理，将命令追加到日志副本当中，如果不是leader就返回false
// 第一个返回只是日志索引
// 第二个返回值是当前任期号
// 第三个返回值是否为leader
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//存放返回信息
	index := 0
	term := rf.currentTerm
	isLeader := (rf.status == Leader)

	//如果不是领导人就立刻返回
	if !isLeader {
		return index, term, isLeader
	}

	//封装日志信息
	log := MsgLog{
		Index:   rf.commitIndex,
		Term:    rf.currentTerm,
		Command: command,
	}

	//先将日志写入自己
	rf.log = append(rf.log, log)
	index = len(rf.log)
	rf.matchIndex[rf.me]++
	lognum := 0
	DPrintf("节点%d日志追加成功,最后一条日志的日志号为%d,日志内容如下:%v\n", rf.me, rf.commitIndex, command)
	//将日志发送给其他节点
	for i := 0; i < len(rf.peers); i++ {
		//排除自己
		if i == rf.me {
			continue
		}
		//附加日志RPC的请求参数
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndext[i] - 1,
			PrevLogTerm:  0,
			Entries:      rf.log[rf.nextIndext[i]:],
			LeaderCommit: rf.commitIndex,
		}

		//这样做是为了防止当log为nil时，地址的非法访问
		if args.PrevLogIndex >= 0 {
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		}
		reply := &AppendEntriesReply{}

		DPrintf("节点%d将日志发送给节点%d,index为%d,日志内容：%v\n", rf.me, i, rf.nextIndext[i], args.Entries)
		go rf.sendLog(i, args, reply, &lognum)

	}

	rf.timer.Reset(hearttimeout)
	return index, term, isLeader
}

func (rf *Raft) sendLog(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, lognum *int) {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//远程调用失败，直至调用成功
	for !ok {
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Success == false {
		if rf.currentTerm < reply.Term {
			rf.status = Follower          //成为Follower
			rf.currentTerm = reply.Term   //更新任期
			rf.voteFor = -1               //重置选票
			rf.timer.Reset(getovertimr()) //重置超时时间
			rf.votenum = 0
			return
		}

		//发送的日志落后
		fmt.Println(rf.me, "  ", server, "follower日志落后,希望收到日志的Index:", reply.Logindex)
		rf.nextIndext[server] = reply.Logindex
		rf.matchIndex[server] = reply.Logindex - 1
		if reply.Logindex==len(rf.log) {
			//和自己日志已经同步无需继续发送
			return
		}
		args.Entries = rf.log[reply.Logindex:]
		args.PrevLogIndex = reply.Logindex - 1
		if args.PrevLogIndex >= 0 {
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		}
		reply = &AppendEntriesReply{}
		args.Term = rf.currentTerm
		//重新发送
		go rf.sendLog(server, args, reply, lognum)

		return
	} else {
		*lognum++
		if *lognum >= len(rf.peers)/2 {
			rf.commitIndex += 1
			*lognum = -9999
		}
		rf.nextIndext[server] += len(args.Entries)
		rf.matchIndex[server] = rf.nextIndext[server] - 1
		//检测是否需要进行日志提交

		//进行日志提交
		rf.logcommit()

	}
	return

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
}

// 判断是否crash
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

// 如果一段时间未收到心跳，开始选举
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.timer.C:
			switch rf.status { //超时，开始选举
			case Follower:
				rf.mu.Lock()
				rf.status = Candidate //成为候选人
				rf.mu.Unlock()
				fallthrough
			case Candidate:
				rf.startelect() //开始选举
			case Leader:
				rf.broadcastHeart() //发送心跳包
			}
		default: //执行一些默认操作，eg:检查选票
			rf.mu.Lock()
			if rf.votenum > len(rf.peers)/2 {
				//超过半数支持，成为领导者
				rf.votenum = -999
				rf.status = Leader
				DPrintf("节点%d成为leader,当前任期%d", rf.me, rf.currentTerm)
				rf.broadcastHeart() //选举成功立刻发起心跳
			}
			rf.mu.Unlock()
		}
	}
	rf.timer.Stop()
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
//
// 创建一个raft服务器实例
// peers是一个raft服务器数组，所有的raft服务器都在这个数组当中
// me是自己在peers中的位置下标
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.currentTerm = 0
	rf.applyCh = applyCh
	rf.log = make([]MsgLog, 0)
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndext = make([]int, len(peers))
	//初始化投票ID
	rf.voteFor = -1
	//初始状态设置为跟随者
	rf.status = Follower

	//初始化时间种子
	rand.Seed(time.Now().UnixNano())
	//初始化超时时间，采用随机
	rf.timer = time.NewTicker(getovertimr())
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func getovertimr() time.Duration {
	return time.Duration(rand.Intn(150)+350) * time.Millisecond
}

// 日志提交
func (rf *Raft) logcommit() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++

		//封装ApplyMsg
		logmsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied + 1,
		}
		//提交
		rf.applyCh <- logmsg
		DPrintf("节点%d进行日志提交,日志序号为%d\n", rf.me, rf.lastApplied+1)
	}
}

// 发送心跳包
func (rf *Raft) broadcastHeart() {
	rf.timer.Reset(hearttimeout) //重置超时时间
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: len(rf.log) - 1,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}
	//发送心跳包，除了自己
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		reply := &AppendEntriesReply{}
		go rf.sendRequestAppend(i, args, reply)
	}
	//DPrintf("leader%d发送心跳包,当前任期%d\n", rf.me, rf.currentTerm)

}

// 发送投票选举
func (rf *Raft) startelect() {
	rf.mu.Lock()
	rf.voteFor = rf.me //将票投给自己
	rf.currentTerm++   //自增当前任期号
	rf.votenum = 1
	rf.mu.Unlock()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		Candidateld:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  0,
	}
	//此处是为了防止越界访问
	if args.LastLogIndex >= 0 {
		args.LastLogTerm = rf.log[args.LastLogIndex].Term
	}

	rf.timer.Reset(getovertimr()) //重置超时时间
	//发起请求投票RPC
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		reply := &RequestVoteReply{}
		go rf.sendRequestVote(i, args, reply) //发起投票请求
	}
	DPrintf("节点%d发起选举,当前任期%d\n", rf.me, rf.currentTerm)
}
