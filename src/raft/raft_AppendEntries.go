package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Term         int        //自己当前的任期号
	LeaderId     int        //leader(自己)的id
	PrevLogIndex int        //前一个日志的日志号
	PrevLogTerm  int        //前一个日志的任期号
	Entries      []LogEntry //当前日志体
	LeaderCommit int        //leader已经提交的日志号
}

type AppendEntriesReply struct {
	Term         int  //自己当前的任期号
	Success      bool //如果follower包括前一个日志，则返回true
	NextLogTerm  int  //下一条日志的任期
	NextLogIndex int  //下一条日志的索引
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("%v receive a appendEntries: %+v", rf.me, args)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm = args.Term
	rf.changeState(Follower)
	rf.resetElectionTimer()
	// reply.Success = true

	_, lastLogIndex := rf.getLastLogTermAndIndex()
	//先判断两边，再判断刚好从快照开始，再判断中间的情况
	if args.PrevLogIndex < rf.lastSnapshotIndex {
		//1.要插入的前一个index小于快照index，几乎不会发生
		reply.Success = false
		reply.NextLogIndex = rf.lastSnapshotIndex + 1
	} else if args.PrevLogIndex > lastLogIndex {
		//2. 要插入的前一个index大于最后一个log的index，说明中间还有log
		reply.Success = false
		reply.NextLogIndex = lastLogIndex + 1
	} else if args.PrevLogIndex == rf.lastSnapshotIndex {
		//3. 要插入的前一个index刚好等于快照的index，说明可以全覆盖，但要判断是否是全覆盖
		if rf.isOutOfArgsAppendEntries(args) {
			reply.Success = false
			reply.NextLogIndex = 0 //=0代表着插入会导致乱序
		} else {
			reply.Success = true
			rf.logs = append(rf.logs[:1], args.Entries...)
			_, currentLogIndex := rf.getLastLogTermAndIndex()
			reply.NextLogIndex = currentLogIndex + 1
		}
	} else if args.PrevLogTerm == rf.logs[rf.getStoreIndex(args.PrevLogIndex)].Term {
		//4. 中间的情况：索引处的两个term相同
		if rf.isOutOfArgsAppendEntries(args) {
			reply.Success = false
			reply.NextLogIndex = 0
		} else {
			reply.Success = true
			rf.logs = append(rf.logs[:rf.getStoreIndex(args.PrevLogIndex)+1], args.Entries...)
			_, currentLogIndex := rf.getLastLogTermAndIndex()
			reply.NextLogIndex = currentLogIndex + 1
		}
	} else {
		//5. 中间的情况：索引处的两个term不相同，跳过一个term
		term := rf.logs[rf.getStoreIndex(args.PrevLogIndex)].Term
		index := args.PrevLogIndex
		for index > rf.commitIndex && index > rf.lastSnapshotIndex && rf.logs[rf.getStoreIndex(index)].Term == term {
			index--
		}
		reply.Success = false
		reply.NextLogIndex = index + 1
	}

	asdasdasd

	rf.persist()
	DPrintf("%v role: %v, get appendentries finish,args = %v,reply = %+v", rf.me, rf.state, *args, *reply)
	rf.mu.Unlock()
}

// 发送添加请求
func (rf *Raft) sendAppendEntries(peerId int) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	if rf.state != Leader {
		rf.resetAppendEntriesTimer(peerId)
		rf.mu.Unlock()
		return
	}
	DPrintf("%v send append entries to peer %v", rf.me, peerId)

	prevLogIndex, prevLogTerm, logEntries := rf.getAppendLogs(peerId)
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      logEntries,
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	rf.resetAppendEntriesTimer(peerId)
	rf.mu.Unlock()

	//发送rpc
	rpcTimer := time.NewTimer(RPCTimeout)
	defer rpcTimer.Stop()

	ch := make(chan bool, 1)
	go func() {
		//尝试10次
		for i := 0; i < 10 && !rf.killed(); i++ {
			ok := rf.peers[peerId].Call("Raft.AppendEntries", &args, &reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
				continue
			} else {
				ch <- ok
				return
			}
		}
	}()

	select {
	case <-rpcTimer.C:
		DPrintf("%v role: %v, send append entries to peer %v TIME OUT!!!", rf.me, rf.state, peerId)
	case <-ch:
	}

	DPrintf("%v role: %v, send append entries to peer finish,%v,args = %+v,reply = %+v", rf.me, rf.state, peerId, args, reply)

	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.changeState(Follower)
		rf.currentTerm = reply.Term
		rf.resetElectionTimer()
		rf.persist()
		rf.mu.Unlock()
		return
	}

	if rf.state != Leader || rf.currentTerm != args.Term {
		rf.mu.Unlock()
		return
	}

	//响应成功
	if reply.Success {
		//全部接收
		if reply.NextLogIndex > rf.nextIndex[peerId] {
			rf.nextIndex[peerId] = reply.NextLogIndex
			rf.matchIndex[peerId] = reply.NextLogIndex - 1
		}
		//如果发送的日志是自己的任期的日志
		if len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Term == rf.currentTerm {
			rf.tryCommitLog()
		}
	}
	rf.mu.Unlock()
}

// 获取要发送给对应节点的日志信息
func (rf *Raft) getAppendLogs(peerId int) (prevLogIndex int, prevLogTerm int, logEntries []LogEntry) {
	nextIndex := rf.nextIndex[peerId]
	lastLogTerm, lastLogIndex := rf.getLastLogTermAndIndex()
	if nextIndex <= rf.lastSnapshotIndex || nextIndex > lastLogIndex {
		prevLogTerm = lastLogTerm
		prevLogIndex = lastLogIndex
		return
	}
	//深拷贝确保原来的原来的日志被删除了依旧能发送rpc
	logEntries = make([]LogEntry, lastLogIndex-nextIndex+1)
	copy(logEntries, rf.logs[nextIndex-rf.lastSnapshotIndex:])
	prevLogIndex = nextIndex - 1
	if prevLogIndex == rf.lastSnapshotIndex {
		prevLogTerm = rf.lastSnapshotTerm
	} else {
		prevLogTerm = rf.logs[prevLogIndex-rf.lastSnapshotIndex].Term
	}

	return
}

// 尝试提交日志
func (rf *Raft) tryCommitLog() {
	_, lastLogIndex := rf.getLastLogTermAndIndex()
	hasCommit := false

	for i := rf.commitIndex + 1; i <= lastLogIndex; i++ {
		count := 0
		for _, m := range rf.matchIndex {
			if m >= i {
				//该节点已经复制成功
				count += 1
				//提交数大于一半
				if count > len(rf.peers)/2 {
					rf.commitIndex = i
					hasCommit = true
					DPrintf("%v state: %v,commit index %v", rf.me, rf.state, i)
					break
				}
			}
		}
		if rf.commitIndex != i {
			break
		}
	}
	if hasCommit {
		rf.notifyApplyCh <- struct{}{}
	}
}
