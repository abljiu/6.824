package raft

//
// 支持Raft和kvraft保存持久化
// Raft 状态（日志 &c）和 k/v 服务器快照。
//
// 我们将使用原始的 persister.go 来测试您的代码以进行评分。
// 因此，虽然您可以修改此代码来帮助您调试，但请
// 在提交之前用原始文件进行测试。
//

import "sync"

type Persister struct {
	mu        sync.Mutex
	raftstate []byte//当前raft节点的状态
	snapshot  []byte//当前raft的快照
}

func MakePersister() *Persister {
	return &Persister{}
}

func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// 将 Raft 状态和 K/V 快照保存为单个原子操作，
// 帮助避免它们不同步。
func (ps *Persister) Save(raftstate []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(raftstate)
	ps.snapshot = clone(snapshot)
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}
