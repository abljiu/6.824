package raft

// rf = Make(...)
// 创建一个新的 Raft 服务器。
// rf.Start(命令接口{}) (索引, 术语, isleader)
// 开始对新日志条目达成一致
// rf.GetState()（术语，isLeader）
// 向 Raft 询问其当前任期，以及它是否认为自己是领导者
// 应用消息
// 每次有新条目提交到日志时，每个 Raft 对等点
// 应该向服务（或测试人员）发送ApplyMsg
// 在同一服务器中。
//
import (
	//	"bytes"
	// "flag"
	"go/doc/comment"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

type State int

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)
const (
	ElectionTimeout   = time.Millisecond * 300 // 选举超时时间/心跳超时时间
	HeartBeatInterval = time.Millisecond * 150 // leader 发送心跳
	ApplyInterval     = time.Millisecond * 100 // apply log
	RPCTimeout        = time.Millisecond * 100
	MaxLockTime       = time.Millisecond * 10 // debug
)

// 当每个 Raft 对等体意识到连续的日志条目
// 提交后，对等方应向服务发送一条 ApplyMsg（或
// tester) 在同一服务器上，通过 applyCh 传递给 Make()。放
// CommandValid 为 true 表示 ApplyMsg 包含新的
// 提交的日志条目。
//
// 在 2D 部分中，您需要发送其他类型的消息（例如，
// snapshots) 在 applyCh 上，但将 CommandValid 设置为 false
// 其他用途。
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

// 实现单个 Raft 对等点的 Go 对象。
type Raft struct {
	mu        sync.Mutex          // 锁定以保护对此对等点状态的共享访问
	peers     []*labrpc.ClientEnd // 所有peer的RPC端点
	persister *Persister          // 保存该对等点的持久状态的对象
	me        int                 // 该节点在节点[]中的索引
	dead      int32               // 由 Kill() 设置

	// 这里是您的数据（2A、2B、2C）。
	// 查看论文的图 2 了解内容的描述
	// Raft 服务器必须维护的状态。

	state       State      //当前的状态
	currentTerm int        //当前的任期号
	votedFor    int        //投票给了哪个节点
	logs        []LogEntry //复制到的日志队列
	commitIndex int        //已经提交的最大日志索引
	lastApplied int        //最新应用的日志索引
	nextIndex   []int      //leader用来记录每一个follower下一个复制日志条目的索引
	matchIndex  []int      //leader用来记录每一个follower已经复制的日志条目的最高索引

	electionTimer       *time.Timer   //自己的选举计时器
	appendEntriesTimers []*time.Timer //leader向follower发送心跳的的计时器
	applyTimer          *time.Timer   //
	stopCh              chan struct{} //控制后台线程推出的chan
	applyCh             chan ApplyMsg //提交应用的日志的chan
	notifyApplyCh       chan struct{} //

	lastSnapshotIndex int // 快照中最后一条日志的index，是真正的index，不是存储在logs中的index
	lastSnapshotTerm  int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// 返回当前Term以及是否是这个服务器
// 相信它是领导者。
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	flag := false
	if rf.state == Leader {
		flag = true
	}
	return rf.currentTerm, flag
}

// 将 Raft 的持久状态保存到稳定存储中，
// 崩溃并重新启动后可以在其中检索它。
// 请参阅论文的图 2，了解什么应该持久的描述。
// 在实现快照之前，您应该传递 nil 作为
// persister.Save() 的第二个参数。
// 实现快照后，传递当前快照
// （如果还没有快照则为零）。
func (rf *Raft) persist() {
	// 这里是你的代码 (2C)。
	// 例子：
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// 恢复之前持久化的状态。
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // 没有任何状态的引导程序？
		return
	}
	// 这里是你的代码 (2C)。
	// 例子：
	// r := bytes.NewBuffer(数据)
	// d := labgob.NewDecoder(r)
	// 变量xxx
	// 变量 yyy
	// if d.Decode(&xxx) != nil ||
	//
	//	d.Decode(&yyy) != nil {
	//	  error...
	//	}else{
	//
	// rf.xxx = xxx
	// rf.yyy = yyy
	// }
}

// 该服务表示它已经创建了一个快照
// 直到并包括index的所有信息。这意味着
// 服务不再需要日志通过（并包括）
// 该索引。 Raft 现在应该尽可能地修剪其日志。
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// labrpc包模拟有损网络，其中服务器
// 可能无法访问，并且其中请求和回复可能会丢失。
// Call() 发送请求并等待回复。如果收到回复
// 在超时间隔内，Call() 返回 true；否则
// Call() 返回 false。因此 Call() 可能会暂时不会返回。
// 错误返回可能是由死服务器、活服务器引起的
// 无法到达、请求丢失或回复丢失。
//
// Call() 保证返回（可能在延迟后）*除非*如果
// 服务器端的处理函数不返回。于是就有了
// 无需在 Call() 周围实现您自己的超时。
//
// 查看 ../labrpc/labrpc.go 中的注释以获取更多详细信息。
//
// 如果您在使 RPC 工作时遇到困难，请检查您是否已
// 将通过 RPC 传递的结构体中的所有字段名称大写，并且
// 调用者使用 & 传递回复结构的地址，而不是
// 结构体本身。

// 使用 Raft 的服务（例如 k/v 服务器）想要启动
// 就附加到 Raft 日志的下一个命令达成一致。如果这
// 服务器不是领导者，返回 false。否则启动
// 同意并立即返回。不能保证这
// 命令将永远被提交到 Raft 日志，因为领导者
// 可能会失败或输掉选举。即使 Raft 实例已被杀死，
// 这个函数应该优雅地返回。
//
// 第一个返回值是命令将出现的索引
// 如果它曾经被提交过。第二个返回值是当任期。如果第三个返回值为 true服务器认为是领导人。

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return index, term, isLeader
	}

	rf.logs = append(rf.logs, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})
	_, lastIndex := rf.getLastLogTermAndIndex()
	index = lastIndex
	rf.matchIndex[rf.me] = lastIndex
	rf.nextIndex[rf.me] = lastIndex + 1

	term = rf.currentTerm
	isLeader = true

	rf.resetAllAppendEntriesTimerZero()

	return index, term, isLeader
}

// 开始选举
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.resetElectionTimer()
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}

	rf.changeState(Candidate)
	DPrintf("%v role %v,start election,term: %v", rf.me, rf.state, rf.currentTerm)

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
		//对每一个其他节点都要发送rpc
		go func(gch chan bool, index int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(index, &args, &reply)
			gch <- reply.VoteGranted
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
				rf.resetAllAppendEntriesTimerZero()
			}
			rf.persist()
			rf.mu.Unlock()
			DPrintf("%v current role: %v", rf.me, rf.state)
		} else if resCount == allCount || resCount-grantedCount > allCount/2 {
			DPrintf("grant fail! grantedCount <= len/2:count:%d", grantedCount)
			return
		}
	}

}

func (rf *Raft) startApplyLogs() {
	defer rf.applyTimer.Reset(ApplyInterval)

	rf.mu.Lock()
	var msgs []ApplyMsg
	if rf.lastApplied < rf.lastSnapshotIndex {
		//此时要安装快照，命令在接收到快照时就发布过了，等待处理
		msgs = make([]ApplyMsg, 0)
	} else if rf.commitIndex <= rf.lastApplied {
		//snapShot 没有更新 commitindex
		msgs = make([]ApplyMsg, 0)
	} else {
		//获取所有提交但是没有应用的日志
		msgs = make([]ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.getStoreIndex(i)].Command,
				CommandIndex: i,
			})
		}
		rf.mu.Unlock()
		for _, msg := range msgs {
			rf.applyCh <- msg
			rf.mu.Lock()
			rf.lastApplied = msg.CommandIndex
			rf.mu.Unlock()
		}

	}
}

// 创建计时器
func (rf *Raft) ticker() {
	go func() {
		for {
			select {
			case <-rf.stopCh:
				return
			case <-rf.applyTimer.C:
				rf.notifyApplyCh <- struct{}{}
			case <-rf.notifyApplyCh: //当有日志记录提交了，要进行应用
				rf.startApplyLogs()
			}
		}
	}()

	//开启选举定时
	go func() {
		for !rf.killed() {
			select {
			case <-rf.stopCh:
				return
			case <-rf.electionTimer.C:
				rf.startElection()
			}
		}
	}()

	//leader定时发送日志或者心跳
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(cur int) {
			for !rf.killed() {
				select {
				case <-rf.stopCh:
					return
				case <-rf.appendEntriesTimers[cur].C:
					rf.sendAppendEntries(cur)
				}
			}
		}(i)
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 1)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.electionTimer = time.NewTimer(rf.getElectionTimeout())
	rf.appendEntriesTimers = make([]*time.Timer, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.appendEntriesTimers[i] = time.NewTimer(HeartBeatInterval)
	}
	rf.stopCh = make(chan struct{})
	// 从崩溃前持续的状态进行初始化
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
