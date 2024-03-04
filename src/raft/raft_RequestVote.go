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

	//判断日志是否完整，如果不完整，则拒绝投票
	if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		return
	}

	// 同意投票
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.changeState(Follower)
	rf.resetElectionTimer()

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
