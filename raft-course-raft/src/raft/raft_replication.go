//日志复制。
//当选为Leader后，向所有Peer发送心跳/复制日志的RPC，并处理RPC返回值

package raft

import (
	"fmt"
	"sort"
	"time"
)

// 构造日志结构体
type LogEntry struct {
	Term         int         // the log entry's term 通过任期号来区分日志条目属于哪个Leader
	CommandValid bool        // if it should be applied 表示是否需要将该命令应用到状态机
	Command      interface{} // the command should be applied to the state machine 表示需要应用到状态机的命令
}

// RPC请求参数
type AppendEntriesArgs struct {
	Term     int //表示 Leader 的当前任期号。用于同步对齐和日志一致性检查
	LeaderId int //表示 Leader 的 ID。Follower 可以通过这个字段识别当前的 Leader

	// used to probe the match point
	PrevLogIndex int //表示上一个已同步的日志索引
	PrevLogTerm  int //表示上一个日志条目所属的任期
	//Leader 在发送 AppendEntries RPC 时，会通过这两个字段来确保 Follower 的日志与 Leader 一致
	Entries []LogEntry //表示需要复制到 Follower 的日志条目,心跳消息中，可能为空

	// used to update the follower's commitIndex
	LeaderCommit int
	//表示 Leader 的当前提交索引（commitIndex），Follower 会根据这个字段更新自己的 commitIndex，并将日志条目应用到状态机
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Prev:[%d]T%d, (%d, %d], CommitIdx: %d",
		args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm,
		args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), args.LeaderCommit)
}

// RPC响应结构体
type AppendEntriesReply struct {
	Term    int  //表示 Follower 当前的任期号
	Success bool //表示 Follower 是否成功处理了 AppendEntries 请求

	ConfilictIndex int //如果 Success 为 false，表示 Follower 的日志与 Leader 的日志存在冲突。 ConflictIndex 表示冲突开始的日志索引位置。 Leader 可以根据这个索引调整日志复制的起点，解决冲突
	ConfilictTerm  int //表示冲突日志条目所属的任期号
}

func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("T%d, Sucess: %v, ConflictTerm: [%d]T%d", reply.Term, reply.Success, reply.ConfilictIndex, reply.ConfilictTerm)
}

// Peer's callback
// 心跳接收方收到心跳后，只要Leader的term不小于自己，对其认可投票，变为Follower，并重置选举时钟，承诺一段时间内不发起选举
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Appended, Args=%v", args.LeaderId, args.String())

	reply.Term = rf.currentTerm
	reply.Success = false

	// align the term
	// Leader的任期Term小于当前任期，则无视请求
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Higher term, T%d<T%d", args.LeaderId, args.Term, rf.currentTerm)
		return
	}
	// term任期大于当前任期，则变为Follower
	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}
	// 并重置自己的选举时钟，并承诺一段时间内不发起选举
	defer func() {
		rf.resetElectionTimerLocked()
		if !reply.Success {
			LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Follower Conflict: [%d]T%d", args.LeaderId, reply.ConfilictIndex, reply.ConfilictTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Follower Log=%v", args.LeaderId, rf.log.String())
		}
	}()

	// return failure if prevLog not matched
	// 大于当前日志长度，错误返回。记录发生冲突的索引和任期号
	if args.PrevLogIndex >= rf.log.size() {
		reply.ConfilictTerm = InvalidTerm
		reply.ConfilictIndex = rf.log.size()
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower log too short, Len:%d < Prev:%d", args.LeaderId, rf.log.size(), args.PrevLogIndex)
		return
	}
	// 小于当前日志最后一个条目，返回发生冲突的所有和任期号
	if args.PrevLogIndex < rf.log.snapLastIdx {
		reply.ConfilictTerm = rf.log.snapLastTerm
		reply.ConfilictIndex = rf.log.snapLastIdx
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower log truncated in %d", args.LeaderId, rf.log.snapLastIdx)
		return
	}
	// 检查当前日志中传入索引的任期号是否和传入任期号相同，记录发生冲突的第一个索引和任期号
	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.ConfilictTerm = rf.log.at(args.PrevLogIndex).Term
		reply.ConfilictIndex = rf.log.firstFor(reply.ConfilictTerm)
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Prev log not match, [%d]: T%d != T%d", args.LeaderId, args.PrevLogIndex, rf.log.at(args.PrevLogIndex).Term, args.PrevLogTerm)
		return
	}

	// append the leader log entries to local
	// 没有发生错误，则在日志中添加索引和日志信息
	rf.log.appendFrom(args.PrevLogIndex, args.Entries)
	rf.persistLocked()
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLog2, "Follower accept logs: (%d, %d]", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	// hanle LeaderCommit
	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Follower update the commit index %d->%d", rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit
		rf.applyCond.Signal()
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 计算已匹配的
func (rf *Raft) getMajorityIndexLocked() int {
	tmpIndexes := make([]int, len(rf.peers))
	copy(tmpIndexes, rf.matchIndex)        //matchIndex为每个Follower匹配成功的日志索引
	sort.Ints(sort.IntSlice(tmpIndexes))   //进行排序。从小到大排列
	majorityIdx := (len(rf.peers) - 1) / 2 //计算多数派的起始索引
	LOG(rf.me, rf.currentTerm, DDebug, "Match index after sort: %v, majority[%d]=%d", tmpIndexes, majorityIdx, tmpIndexes[majorityIdx])
	return tmpIndexes[majorityIdx] // 返回当前多数派匹配的日志条目索引
}

// only valid in the given `term`
// Leader 会给除自己外的所有其他Peer发送心跳/复制日志
func (rf *Raft) startReplication(term int) bool {
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost or crashed", peer)
			return
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, Reply=%v", peer, reply.String())

		// align the term
		// 当请求Peer的任期大于Leader的任期，Leader节点转换为Follow节点
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// check context lost
		if rf.contextLostLocked(Leader, term) {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Context Lost, T%d:Leader->T%d:%s", peer, term, rf.currentTerm, rf.role)
			return
		}

		// hanle the reply
		// probe the lower index if the prevLog not matched
		// 进行日志一致性处理， 复制失败，根据Follower的反馈来调整Leader的nextIndex，解决日志冲突
		if !reply.Success {
			prevIndex := rf.nextIndex[peer] //记录当前Peer日志下一条目的索引
			if reply.ConfilictTerm == InvalidTerm {
				rf.nextIndex[peer] = reply.ConfilictIndex //保存发送冲突的日志索引
			} else {
				firstIndex := rf.log.firstFor(reply.ConfilictTerm) //找到第一个发生冲突的日志条目的索引
				if firstIndex != InvalidIndex {
					rf.nextIndex[peer] = firstIndex //找到，使用该索引
				} else {
					rf.nextIndex[peer] = reply.ConfilictIndex // 否则，使用follower返回的冲突索引
				}
			}
			// avoid unordered reply
			// avoid the late reply move the nextIndex forward again
			// 避免nextIndex被错误的向前移动，当前的nextIndex值大于调整后的preIndex，直接恢复成preIndex，可能发生在Follower的响应延迟到达
			if rf.nextIndex[peer] > prevIndex {
				rf.nextIndex[peer] = prevIndex
			}

			// 计算下一次发生日志的前一个索引和任期号
			nextPrevIndex := rf.nextIndex[peer] - 1
			nextPrevTerm := InvalidTerm
			// 大于快照的最后索引，获取对应日志条目任期号
			if nextPrevIndex >= rf.log.snapLastIdx {
				nextPrevTerm = rf.log.at(nextPrevIndex).Term
			}
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Not matched at Prev=[%d]T%d, Try next Prev=[%d]T%d",
				peer, args.PrevLogIndex, args.PrevLogTerm, nextPrevIndex, nextPrevTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Leader log=%v", peer, rf.log.String())
			return
		}

		// update match/next index if log appended successfully
		// 处理日志成功复制的情况
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries) // important 表示对应Peer已成功匹配的日志索引
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1                // Leader下一次发送日志条目的起始索引

		// update the commitIndex
		// 获取当前已匹配的多数派日志索引，在每次更新 `rf.matchIndex` 后，依据此全局匹配点视图，我们可以算出多数 Peer 的匹配点，
		// 进而更新 Leader 的 `CommitIndex`。我们可以使用排序后找中位数的方法来计算
		majorityMatched := rf.getMajorityIndexLocked()
		// 判断多数派日志索引是否大于commitIndex，并且该索引的日志条目任期是否属于当前任期，则更新CommitIndex和通知状态机应用新的日志条目
		if majorityMatched > rf.commitIndex && rf.log.at(majorityMatched).Term == rf.currentTerm {
			LOG(rf.me, rf.currentTerm, DApply, "Leader update the commit index %d->%d", rf.commitIndex, majorityMatched)
			rf.commitIndex = majorityMatched
			// 如果commitIndex发生变化，则唤醒apply工作流，提醒可以apply新的日志到本地了。
			rf.applyCond.Signal()
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "Lost Leader[%d] to %s[T%d]", term, rf.role, rf.currentTerm)
		return false
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			rf.matchIndex[peer] = rf.log.size() - 1
			rf.nextIndex[peer] = rf.log.size()
			continue
		}

		prevIdx := rf.nextIndex[peer] - 1
		if prevIdx < rf.log.snapLastIdx {
			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.log.snapLastIdx,
				LastIncludedTerm:  rf.log.snapLastTerm,
				Snapshot:          rf.log.snapshot,
			}
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, SendSnap, Args=%v", peer, args.String())
			go rf.installToPeer(peer, term, args)
			continue
		}

		prevTerm := rf.log.at(prevIdx).Term
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log.tail(prevIdx + 1),
			LeaderCommit: rf.commitIndex,
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, Args=%v", peer, args.String())
		go replicateToPeer(peer, args)
	}

	return true
}

// could only replcate in the given term
// 心跳循环，相同间隔发送心跳/复制日志RPC
func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {
		ok := rf.startReplication(term) //检查当前Peer是否为当前Term的Leader
		if !ok {
			break
		}

		time.Sleep(replicateInterval)
	}
}
