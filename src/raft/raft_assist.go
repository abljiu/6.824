package raft

import (
	"math/rand"
	"sync/atomic"
	"time"
)

func (rf *Raft) changeState(newState State) {
	if newState < 0 || newState > 3 {
		panic("unknown state")
	}
	rf.state = newState
	switch newState {
	case Follower:
	case Candidate:
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.resetElectionTimer()
	case Leader:
		//leader只有两个特殊的数据结构：nextIndex,matchIndex
		_, lastLogIndex := rf.getLastLogTermAndIndex()
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = lastLogIndex + 1
			rf.matchIndex[i] = lastLogIndex
		}
		rf.resetElectionTimer()
	default:
		panic("unknown state")
	}

}

// 标记一个节点为关闭状态
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

// 检查一个节点是否关闭
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 获得一个随机选举超时时间
func (rf *Raft) getElectionTimeout() time.Duration {
	t := ElectionTimeout + time.Duration(rand.Int63())%ElectionTimeout
	return t
}

// 刷新随机选举超时时间
func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(rf.getElectionTimeout())
}

// 刷新心跳时间
func (rf *Raft) resetAppendEntriesTimer(peerId int) {
	rf.appendEntriesTimers[peerId].Stop()
	rf.appendEntriesTimers[peerId].Reset(HeartBeatInterval)
}

// 设置一个节点心跳时间为0
func (rf *Raft) resetAppendEntriesTimerZero(peerId int) {
	rf.appendEntriesTimers[peerId].Stop()
	rf.appendEntriesTimers[peerId].Reset(0)
}

// 设置全部节点的心跳时间为0
func (rf *Raft) resetAllAppendEntriesTimerZero() {
	for _, timer := range rf.appendEntriesTimers {
		timer.Stop()
		timer.Reset(0)
	}
}

// 返回当前状态机的最后一条日志的任期和索引
func (rf *Raft) getLastLogTermAndIndex() (int, int) {
	return rf.logs[len(rf.logs)-1].Term, rf.lastSnapshotIndex + len(rf.logs) - 1
}

// 获取当前节点应该的存储位置的索引
func (rf *Raft) getStoreIndex(index int) int {
	StoreIndex := index - rf.lastSnapshotIndex
	if StoreIndex < 0 {
		return -1
	}
	return StoreIndex
}

// 判断当前raft的日志记录是否超过发送过来的日志记录
func (rf *Raft) isOutOfArgsAppendEntries(args *AppendEntriesArgs) bool {
	argsLastLogIndex := args.PrevLogIndex + len(args.Entries)
	lastLogTerm, lastLogIndex := rf.getLastLogTermAndIndex()
	if lastLogTerm == args.Term && argsLastLogIndex < lastLogIndex {
		return true
	}
	return false
}
