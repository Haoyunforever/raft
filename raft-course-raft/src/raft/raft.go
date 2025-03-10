package raft

import (
	"course/labrpc"
	"sync"
	"sync/atomic"
	"time"
)

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

const (
	electionTimeoutMin time.Duration = 250 * time.Millisecond
	electionTimeoutMax time.Duration = 400 * time.Millisecond

	replicateInterval time.Duration = 70 * time.Millisecond
)

const (
	InvalidTerm  int = 0
	InvalidIndex int = 0
)

type Role string

// 针对peer角色，定义角色类型和常量
const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part PartD you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For PartD:
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

	// Your data here (PartA, PartB, PartC).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role        Role
	currentTerm int
	votedFor    int // -1 means vote for none

	// log in the Peer's local
	log *RaftLog

	// only used in Leader
	// every peer's view
	nextIndex  []int // 日志同步成功时的匹配点记录
	matchIndex []int // 日志同步成功后的匹配点记录

	// fields for apply loop
	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg
	snapPending bool
	applyCond   *sync.Cond

	electionStart   time.Time
	electionTimeout time.Duration // random
}

// 下面实现三个状态转换函数：becomeFollowerLocked、becomeCandidateLocked、becomeLeaderLocked。每个函数带上locked后缀、表面需要在加锁的临界曲中调用
func (rf *Raft) becomeFollowerLocked(term int) {
	if term < rf.currentTerm { //如果任期小于当前任期
		LOG(rf.me, rf.currentTerm, DError, "Can't become Follower, lower term: T%d", term)
		return
	}
	//节点只接受比当前任期多和相等的任期号
	LOG(rf.me, rf.currentTerm, DLog, "%s->Follower, For T%v->T%v", rf.role, rf.currentTerm, term)
	rf.role = Follower
	shouldPersit := rf.currentTerm != term //判断是否需要持久化状态
	if term > rf.currentTerm {
		rf.votedFor = -1 //表示在新任期中尚未投票给任何节点
	}
	rf.currentTerm = term //更新当前的任期为传入的term
	if shouldPersit {
		rf.persistLocked() //如果需要持久化状态，则将当前状态持久化到磁盘，保证节点奔溃后能够恢复到正确的一致性状态
	}
}

func (rf *Raft) becomeCandidateLocked() {
	// leader不会自动转变为Candidate，其他节点会通过超时机制自动发起选举
	if rf.role == Leader {
		LOG(rf.me, rf.currentTerm, DError, "Leader can't become Candidate")
		return
	}

	LOG(rf.me, rf.currentTerm, DVote, "%s->Candidate, For T%d", rf.role, rf.currentTerm+1)
	//表面从当前节点变为Candidate，说明新的任期号为前一个任期号+1
	rf.resetElectionTimerLocked() //重置选举计时器，当每个节点的计时器超时且未收到Leader的心跳时，会自动转变为Candidate并发起选举
	rf.currentTerm++              //任期+1
	rf.role = Candidate           //角色转换
	rf.votedFor = rf.me           //节点在成为Candidate后会立即投给自己
	rf.persistLocked()            //持久化状态
}

func (rf *Raft) becomeLeaderLocked() {
	if rf.role != Candidate {
		LOG(rf.me, rf.currentTerm, DError, "Only Candidate can become Leader")
		return
	}
	LOG(rf.me, rf.currentTerm, DLeader, "Become Leader in T%d", rf.currentTerm)
	rf.role = Leader
	// 初始化日志复制状态
	for peer := 0; peer < len(rf.peers); peer++ {
		rf.nextIndex[peer] = rf.log.size() //遍历所有其他节点，发送给下一个需要发送给该节点的条目索引，初始化为当前日志大小，从最新的日志条目开始复制
		rf.matchIndex[peer] = 0            //该节点成功复制的最后一个日志条目索引，初始化为0
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (PartA).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return 0, 0, false
	}
	rf.log.append(LogEntry{
		CommandValid: true,
		Command:      command,
		Term:         rf.currentTerm,
	})
	LOG(rf.me, rf.currentTerm, DLeader, "Leader accept log [%d]T%d", rf.log.size()-1, rf.currentTerm)
	rf.persistLocked()

	return rf.log.size() - 1, rf.currentTerm, true
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

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 检查任期内，角色状态没有发生变化，放心推进状态机
func (rf *Raft) contextLostLocked(role Role, term int) bool {
	return !(rf.currentTerm == term && rf.role == role)
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

	// Your initialization code here (PartA, PartB, PartC).
	rf.role = Follower
	rf.currentTerm = 1
	rf.votedFor = -1

	// a dummy entry to aovid lots of corner checks
	rf.log = NewLog(InvalidIndex, InvalidTerm, nil, nil)

	// initialize the leader's view slice
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize the fields used for apply
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.snapPending = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.applicationTicker()

	return rf
}
