package raft

//
// 这是 raft 必须公开的 API 的概述
// 服务（或测试器）。请参阅下面的评论
// 每个函数的更多细节。
//
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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
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

}

// 返回当前Term以及是否是这个服务器
// 相信它是领导者。
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
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

// RequestVote RPC 参数结构示例。
// 字段名称必须以大写字母开头！
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
}

// RequestVote RPC 回复结构示例。
// 字段名称必须以大写字母开头！
type RequestVoteReply struct {
	// Your data here (2A).
}

// 示例 RequestVote RPC 处理程序。
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
}

// 将 RequestVote RPC 发送到服务器的示例代码。
// server 是rf.peers[]中目标服务器的索引。
// 需要 args 中的 RPC 参数。
// 用 RPC 回复填写 *reply，所以调用者应该
// 传递&回复。
// 传递给 Call() 的参数和回复的类型必须是
// 与声明的参数类型相同
// 处理函数（包括它们是否是指针）。
//
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 使用 Raft 的服务（例如 k/v 服务器）想要启动
// 就附加到 Raft 日志的下一个命令达成一致。如果这
// 服务器不是领导者，返回 false。否则启动
// 同意并立即返回。不能保证这
// 命令将永远被提交到 Raft 日志，因为领导者
// 可能会失败或输掉选举。即使 Raft 实例已被杀死，
// 这个函数应该优雅地返回。
//
// 第一个返回值是命令将出现的索引
// 如果它曾经被提交过。第二个返回值是当前值
// 学期。如果第三个返回值为 true服务器认为是领导人。

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// 测试器在每次测试后不会停止 Raft 创建的 goroutine，
// 但它确实调用了 Kill() 方法。你的代码可以使用killed()来
// 检查 Kill() 是否被调用。使用atomic避免了
// 需要锁。
//
// 问题是长时间运行的 goroutine 使用内存并且可能会消耗内存
// 占用 CPU 时间，可能会导致后面的测试失败并生成
// 令人困惑的调试输出。任何具有长时间运行循环的 goroutine
// 应该调用killed()来检查是否应该停止。
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// 服务或测试人员想要创建 Raft 服务器。港口
// 所有 Raft 服务器（包括这台）都在 peers[] 中。这
// 服务器的端口是peers[me]。所有服务器的 Peers[] 数组
// 具有相同的顺序。 persister 是该服务器的一个地方
// 保存其持久状态，并且最初也保存最多的
// 最近保存的状态（如果有）。 applyCh 是一个通道，
// 测试人员或服务期望 Raft 发送 ApplyMsg 消息。
// Make() 必须快速返回，因此它应该启动 goroutine
// 对于任何长时间运行的工作。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// 从崩溃前持续的状态进行初始化
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
