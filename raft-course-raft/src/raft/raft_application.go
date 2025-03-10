package raft

// Apply 工作流在实现的时候，最重要的就是在给 applyCh 发送 ApplyMsg 时，不要在加锁的情况下进行。
// 因为我们并不知道这个操作会耗时多久（即应用层多久会取走数据），因此不能让其在 apply 的时候持有锁

// 周期性检查是否有新的日志条目或者快照需要应用到状态机
func (rf *Raft) applicationTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.applyCond.Wait() //等待条件变量被触发，在CommitIndex更新时触发，表示有新的日志条目或快照需要应用
		entries := make([]LogEntry, 0)
		snapPendingApply := rf.snapPending // 检查是否有快照需要应用
		// 如果没有快照需要应用，处理日志条目
		if !snapPendingApply {
			//直接跳到快照的最后索引
			if rf.lastApplied < rf.log.snapLastIdx {
				rf.lastApplied = rf.log.snapLastIdx
			}

			// make sure that the rf.log have all the entries
			// 计算应用的日志条目范围，不超过日志的最后索引
			start := rf.lastApplied + 1
			end := rf.commitIndex
			if end >= rf.log.size() {
				end = rf.log.size() - 1
			}
			// 遍历日志，将需要应用的日志条目追加到entries
			for i := start; i <= end; i++ {
				entries = append(entries, rf.log.at(i))
			}
		}
		//在处理日志条目或快照之前，释放锁，避免阻塞其他线程
		rf.mu.Unlock()
		//如果没有快照需要应用，将日志条目发送到状态机的通道
		if !snapPendingApply {
			for i, entry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: entry.CommandValid,
					Command:      entry.Command,
					CommandIndex: rf.lastApplied + 1 + i, // must be cautious
				}
			}
		} else {
			// 应用快照
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.log.snapshot,
				SnapshotIndex: rf.log.snapLastIdx,
				SnapshotTerm:  rf.log.snapLastTerm,
			}
		}

		rf.mu.Lock()
		// 没有快照需要应用，更新lastApplied，记录已经应用的日志条目范围
		if !snapPendingApply {
			LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d, %d]", rf.lastApplied+1, rf.lastApplied+len(entries))
			rf.lastApplied += len(entries)
		} else {
			//有快照需要应用，更新lastApplied 为快照的最后索引
			LOG(rf.me, rf.currentTerm, DApply, "Apply snapshot for [0, %d]", 0, rf.log.snapLastIdx)
			rf.lastApplied = rf.log.snapLastIdx
			if rf.commitIndex < rf.lastApplied {
				rf.commitIndex = rf.lastApplied
			}
			//表面快照已经应用完成
			rf.snapPending = false
		}
		rf.mu.Unlock()
	}
}
