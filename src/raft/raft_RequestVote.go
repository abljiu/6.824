package raft

import "time"

type RequestVoteArgs struct {
	Term         int //自己当前的任期号
	CandidateId  int //自己的id
	LastLogIndex int //自己最后一个日志号
	LastLogTerm  int //自己最后一个日志号的任期
	// Your data here (2A, 2B).
}
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //自己当前的任期号
	VoteGranted bool //自己会不会投给这个Candidate
}

// 示例 RequestVote RPC 处理程序。
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//默认失败，返回
	lastLogTerm, lastLogIndex := rf.getLastLogTermAndIndex()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.currentTerm > args.Term {
	//如果自己的任期大直接返回
		return
	} else if rf.currentTerm == args.Term {
		if rf.state == Leader {
			return
		}

		//如果当前节点已经投票给了该候选人，则同意投票
		if args.CandidateId == rf.votedFor {
			reply.Term = args.Term
			reply.VoteGranted = true
			return
		}
		//如果当前节点已经投票给了其他人，则拒绝投票
		if rf.votedFor != -1 && args.CandidateId != rf.votedFor {
			return
		}
	}

	//如果请求中的任期大于当前节点的任期，则更新当前节点的任期并转换为follower
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.changeState(Follower)
		rf.votedFor = -1
		reply.Term = rf.currentTerm
	}

	//判断谁的日志更新
	if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		return
	}

	// 同意投票
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.changeState(Follower)
	rf.resetElectionTimer()
	rf.persist()
	DPrintf("%v, state: %v,voteFor: %v", rf.me, rf.state, rf.votedFor)
}

// 发送选举请求
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	if server < 0 || server > len(rf.peers) || server == rf.me {
		panic("server invalid in sendRequestVote!")
	}

	rpcTimer := time.NewTimer(RPCTimeout)
	defer rpcTimer.Stop()

	ch := make(chan bool, 1)
	go func() {
		for i := 0; i < 10 && !rf.killed(); i++ {
			ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
			if !ok {
				continue
			} else {
				ch <- ok
				return
			}
		}
	}()

	select {
	case <-rpcTimer.C:
		DPrintf("%v state: %v, send request vote to peer %v TIME OUT!!!", rf.me, rf.state, server)
		return
	case <-ch:
		return
	}

}

// 开始选举
func (rf *Raft) startElection() {
	rf.mu.Lock()
	//刷新选举时间
	rf.resetElectionTimer()
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}

	rf.changeState(Candidate)
	DPrintf("%v state %v,start election,term: %v", rf.me, rf.state, rf.currentTerm)

	//获取自己节点的信息
	lastLogTerm, lastLogIndex := rf.getLastLogTermAndIndex()
	args := RequestVoteArgs{
		CandidateId:  rf.me,
		Term:         rf.currentTerm,
		LastLogTerm:  lastLogTerm,
		LastLogIndex: lastLogIndex,
	}
	rf.persist()
	rf.mu.Unlock()

	allCount := len(rf.peers)
	grantedCount := 1
	resCount := 1
	grantedChan := make(chan bool, len(rf.peers)-1)
	for i := 0; i < allCount; i++ {
		if i == rf.me {
			continue
		}
		//对其他节点发送rpc
		go func(grantedChan chan bool, index int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(index, &args, &reply)
			grantedChan <- reply.VoteGranted
			if reply.Term > args.Term {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					//放弃选举
					rf.currentTerm = reply.Term
					rf.changeState(Follower)
					rf.votedFor = -1
					rf.resetElectionTimer()
					rf.persist()
				}
				rf.mu.Unlock()
			}
		}(grantedChan, i)

	}

	for rf.state == Candidate {
		//统计投票
		flag := <-grantedChan
		resCount++
		if flag {
			grantedCount++
		}
		DPrintf("vote: %v, allCount: %v, resCount: %v, grantedCount: %v", flag, allCount, resCount, grantedCount)

		if grantedCount > allCount/2 {
			//竞选成功
			rf.mu.Lock()
			DPrintf("before try change to leader,count:%d, args:%+v, currentTerm: %v, argsTerm: %v", grantedCount, args, rf.currentTerm, args.Term)
			if rf.state == Candidate && rf.currentTerm == args.Term {
				rf.changeState(Leader)
			}
			if rf.state == Leader {
				//刷新append倒计时
				rf.resetAllAppendEntriesTimerZero()
			}
			rf.persist()
			rf.mu.Unlock()
			DPrintf("%v current state: %v", rf.me, rf.state)
		} else if resCount == allCount || resCount-grantedCount > allCount/2 {
			DPrintf("grant fail! grantedCount <= len/2:count:%d", grantedCount)
			return
		}
	}

}
