//领导选举

package raft

import (
	"fmt"
	"math/rand"
	"time"
)

// 重置选举计时器
func (rf *Raft) resetElectionTimerLocked() {
	rf.electionStart = time.Now()                                                   //记录当前时间
	randRange := int64(electionTimeoutMax - electionTimeoutMin)                     //计算随机化范围
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63()%randRange) //设置随机的选举超时时间
	//这里使用随机时间间隔的原因是，防止由于“等距”检查，造成同样检查间隔时，一同发起选举，可能造成选举时票数相同
}

// 进行超时检查，选举Timer是否已经超时，Candidate只有超时后才会真正发起选举
func (rf *Raft) isElectionTimeoutLocked() bool {
	return time.Since(rf.electionStart) > rf.electionTimeout
}

// check whether my last log is more up to date than the candidate's last log
// 检查日志一致性，检查当前节点的日志是否比请求的候选人的日志更更新
// 判断日志更新的比较为：1、Term高者更新 2、Term同，索引大者更新
func (rf *Raft) isMoreUpToDateLocked(candidateIndex, candidateTerm int) bool {
	lastIndex, lastTerm := rf.log.last()

	LOG(rf.me, rf.currentTerm, DVote, "Compare last log, Me: [%d]T%d, Candidate: [%d]T%d", lastIndex, lastTerm, candidateIndex, candidateTerm)
	if lastTerm != candidateTerm {
		return lastTerm > candidateTerm
	} // 如果当前节点日志更新，拒绝投票
	return lastIndex > candidateIndex // 当前节点任期一样，索引更新，拒绝投票
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (PartA, PartB).
	Term        int
	CandidateId int

	LastLogIndex int
	LastLogTerm  int
}

func (args *RequestVoteArgs) String() string {
	return fmt.Sprintf("Candidate-%d, T%d, Last: [%d]T%d", args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (PartA).
	Term        int
	VoteGranted bool
}

func (reply *RequestVoteReply) String() string {
	return fmt.Sprintf("T%d, VoteGranted: %v", reply.Term, reply.VoteGranted)
}

// example RequestVote RPC handler.定义该Peer收到peer的要票请求的处理逻辑
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (PartA, PartB).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, VoteAsked, Args=%v", args.CandidateId, args.String())

	reply.Term = rf.currentTerm // 同步对齐term
	reply.VoteGranted = false   // 默认拒绝投票请求
	// align the term，检查任期号
	if args.Term < rf.currentTerm { //请求的任期号小于当前peer任期号，无视请求
		LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Reject voted, Higher term, T%d>T%d", args.CandidateId, rf.currentTerm, args.Term)
		return
	}
	if args.Term > rf.currentTerm { //否则，状态变为Follower,并更新任期号
		rf.becomeFollowerLocked(args.Term)
	}

	// check for votedFor,检查是否已经投票。
	// 如果当前节点已经为其他节点投票，则拒绝投票
	if rf.votedFor != -1 {
		LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Reject voted, Already voted to S%d", args.CandidateId, rf.votedFor)
		return
	}

	// check if candidate's last log is more up to date
	// 检查日志一致性，检查本Peer比Candidate更新，则拒绝投票给Candidate
	if rf.isMoreUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Reject voted, Candidate less up-to-date", args.CandidateId)
		return
	}

	reply.VoteGranted = true       // 通过上述规则，则授予投票
	rf.votedFor = args.CandidateId // 设置投票目标为传入的候选人ID
	rf.persistLocked()             // 持久化状态,将投票信息和任期号保存在磁盘中
	rf.resetElectionTimerLocked()  // 重置选举计时器， 只有投票给对方后，才能重置选举time
	LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Vote granted", args.CandidateId)
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

// 构造参数，回调RequestVote()
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 单轮选举、成为Candidata后向所有peer发起一次要票过程，不能在加锁的同时进行同步地发送RPC
func (rf *Raft) startElection(term int) {
	votes := 0
	// 嵌套askVoteFormPeer函数，通过RPC收集其他peer的投票
	askVoteFromPeer := func(peer int, args *RequestVoteArgs) {
		// send RPC
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(peer, args, reply)

		// handle the reponse
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Ask vote, Lost or error", peer)
			return
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, AskVote Reply=%v", peer, reply.String())

		// align term
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term) //如果返回的任期大于当前的任期，状态转为follow
			return
		}

		// check the context 检查peer当前状态是否发生变化
		if rf.contextLostLocked(Candidate, term) {
			LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Lost context, abort RequestVoteReply", peer)
			return
		}

		// count the votes
		if reply.VoteGranted {
			votes++                      //投票加1
			if votes > len(rf.peers)/2 { //若票数大于一半，角色变为leader
				rf.becomeLeaderLocked()
				go rf.replicationTicker(term) //异步执行
			}
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.contextLostLocked(Candidate, term) {
		LOG(rf.me, rf.currentTerm, DVote, "Lost Candidate[T%d] to %s[T%d], abort RequestVote", rf.role, term, rf.currentTerm)
		return
	}

	lastIdx, lastTerm := rf.log.last()
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			votes++
			continue
		}

		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastIdx,
			LastLogTerm:  lastTerm,
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, AskVote, Args=%v", peer, args.String())

		go askVoteFromPeer(peer, args)
	}
}

// 异步发起选举（同步会造成主Loop检查延迟）
func (rf *Raft) electionTicker() {
	for !rf.killed() {

		// Your code here (PartA)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.role != Leader && rf.isElectionTimeoutLocked() {
			rf.becomeCandidateLocked()
			go rf.startElection(rf.currentTerm) // 异步发起选举
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		//每次发送票据之间随机暂停一段时间，避免多个节点同时触发选举检查，减少竞争条件
	}
}
