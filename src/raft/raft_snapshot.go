package raft

import "time"

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}
type InstallSnapshotReply struct {
	Term int
}

// 该服务表示它已经创建了一个快照
// 直到并包括index的所有信息。这意味着
// 服务不再需要日志通过（并包括）
// 该索引。 Raft 现在应该尽可能地修剪其日志。
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
}

// 向指定节点发送快照
func (rf *Raft) sendInstallSnapshotToPeer(server int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.lastSnapshotTerm,
		Data:              rf.persister.snapshot,
	}
	rf.mu.Unlock()

	//调用超时
	timer := time.NewTicker(RPCTimeout)
	defer timer.Stop()
	DPrintf("%v state: %v,send snapshot to %v,args = %+v", rf.me, rf.state, server, args)

	//发送RPC
	for {
		timer.Stop()
		timer.Reset(RPCTimeout)

		ch := make(chan bool, 1)
		reply := &InstallSnapshotReply{}
		go func() {
			ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
			if !ok {
				time.Sleep(time.Microsecond * 10)
			}
			ch <- ok
		}()

		select {
		case <-rf.stopCh:
			return
		case <-timer.C:
			DPrintf("%v state:%v,send snapshot to %v TIME OUT!!!", rf.me, rf.state, server)
			continue
		case ok := <-ch:
			if !ok {
				continue
			}
		}

		//处理reply
		rf.mu.Lock()
		defer rf.mu.Lock()
		if rf.state != Leader || args.Term != rf.currentTerm {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.changeState(Follower)
			rf.currentTerm = reply.Term
			rf.resetElectionTimer()
			rf.persist()
			return
		}
		//判断对端的index
		if args.LastIncludedIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = args.LastIncludedIndex
		}

		if args.LastIncludedIndex+1 > rf.nextIndex[server] {
			rf.nextIndex[server] = args.LastIncludedIndex + 1
		}
		return
	}
}

// 读取快照
func (rf *Raft) CondInstallSnapshot(Term int, Index int, snapshot []byte) {

}
