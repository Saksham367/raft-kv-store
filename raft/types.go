package raft

// Entry is one slot in the replicated log. Command is opaque to Raft: the
// service above encodes its own operations into bytes.
type Entry struct {
	Index   int
	Term    int
	Command []byte
}

// ApplyMsg is delivered on the apply channel, in log order, once an entry is
// committed (CommandValid) or when the leader installs a snapshot on this
// peer (SnapshotValid).
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex int
	CommandTerm  int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex int
	SnapshotTerm  int
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

// AppendEntriesReply carries the "fast backup" hints from the extended Raft
// paper (section 5.3) so a leader can skip a whole conflicting term per round
// trip instead of decrementing nextIndex one entry at a time.
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int // term of the conflicting entry, or -1 if the log is too short
	ConflictIndex int // first index the follower holds for ConflictTerm (or its log length)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// Transport sends RPCs to peers. Calls return false when the peer could not
// be reached (dropped message, partition, crash, timeout). Implementations
// must be safe for concurrent use.
type Transport interface {
	RequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool
	AppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool
	InstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool
}

// Handler is the server side of the RPCs; *Raft implements it.
type Handler interface {
	HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply)
	HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply)
	HandleInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply)
}
