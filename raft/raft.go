// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout,
// "In Search of an Understandable Consensus Algorithm", extended version):
// leader election, log replication, persistence, and log compaction through
// snapshots with InstallSnapshot.
package raft

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type role int

const (
	follower role = iota
	candidate
	leader
)

func (r role) String() string {
	return [...]string{"follower", "candidate", "leader"}[r]
}

// Config controls timing. Election timeouts are randomized in
// [ElectionTimeoutMin, ElectionTimeoutMax) to avoid split votes.
type Config struct {
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
}

func DefaultConfig() Config {
	return Config{
		HeartbeatInterval:  50 * time.Millisecond,
		ElectionTimeoutMin: 300 * time.Millisecond,
		ElectionTimeoutMax: 600 * time.Millisecond,
	}
}

type Raft struct {
	mu        sync.Mutex
	me        int
	peers     int // cluster size, peer ids are 0..peers-1
	transport Transport
	persister Persister
	applyCh   chan<- ApplyMsg
	cfg       Config
	dead      int32

	// Persistent state (Figure 2), saved before answering any RPC.
	currentTerm int
	votedFor    int
	log         raftLog
	snapshot    []byte // latest snapshot, covering log.firstIndex()

	// Volatile state.
	role          role
	leaderID      int
	commitIndex   int
	lastApplied   int
	electionReset time.Time
	electionWait  time.Duration

	// Leader-only state.
	nextIndex  []int
	matchIndex []int

	applyCond    *sync.Cond
	flushCond    *sync.Cond // wakes the leader's group-commit flusher
	flushPending bool

	// Persistence ordering: every encoded state gets a version under mu; a
	// write is skipped if a newer version already reached disk. This lets the
	// flusher fsync outside mu without ever overwriting newer state.
	persistMu       sync.Mutex
	stateVersion    uint64
	savedVersion    uint64
	replicateCond   []*sync.Cond // wakes per-peer replicator goroutines
	replicatePend   []bool       // a send to this peer has been requested
	pendingSnapshot *ApplyMsg    // snapshot waiting to be delivered to the service
}

// New creates a Raft peer and starts its background goroutines. State saved
// in persister (if any) is restored, so New is also how a crashed peer
// restarts.
func New(peers, me int, transport Transport, persister Persister, applyCh chan<- ApplyMsg, cfg Config) *Raft {
	rf := &Raft{
		me:        me,
		peers:     peers,
		transport: transport,
		persister: persister,
		applyCh:   applyCh,
		cfg:       cfg,
		votedFor:  -1,
		leaderID:  -1,
		log:       newLog(),
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.flushCond = sync.NewCond(&rf.mu)
	rf.nextIndex = make([]int, peers)
	rf.matchIndex = make([]int, peers)
	rf.replicateCond = make([]*sync.Cond, peers)
	rf.replicatePend = make([]bool, peers)

	rf.restore(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	// Entries covered by the snapshot are already reflected in the service's
	// state once it loads the snapshot, so start applying after it.
	rf.commitIndex = rf.log.firstIndex()
	rf.lastApplied = rf.log.firstIndex()
	rf.resetElectionTimer()

	for p := 0; p < peers; p++ {
		if p == me {
			continue
		}
		rf.replicateCond[p] = sync.NewCond(&rf.mu)
		go rf.replicator(p)
	}
	go rf.ticker()
	go rf.applier()
	go rf.flusher()
	return rf
}

// GetState returns the current term and whether this peer believes it is leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == leader
}

// Leader returns the id of the last known leader, or -1.
func (rf *Raft) Leader() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.leaderID
}

// Start proposes a command. It returns immediately; the command is committed
// only if it later appears on applyCh at the returned index.
func (rf *Raft) Start(command []byte) (index, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != leader || rf.killed() {
		return -1, rf.currentTerm, false
	}
	e := Entry{Index: rf.log.lastIndex() + 1, Term: rf.currentTerm, Command: command}
	rf.log.append(e)
	// Group commit: the flusher makes this (and any entries appended
	// meanwhile) durable with one fsync, in parallel with replication. The
	// leader only counts itself toward a majority for entries it has flushed
	// (Raft paper section 10.2.1).
	rf.flushPending = true
	rf.flushCond.Signal()
	rf.broadcast()
	return e.Index, e.Term, true
}

// Snapshot tells Raft that the service has a snapshot covering every entry up
// to and including index, so those entries can be discarded.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.log.firstIndex() || index > rf.lastApplied {
		return
	}
	rf.log.compact(index, rf.log.term(index))
	rf.snapshot = clone(snapshot)
	rf.persist(rf.snapshot)
}

// Kill stops background goroutines. The peer stops responding to RPCs.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	rf.applyCond.Broadcast()
	rf.flushCond.Broadcast()
	for _, c := range rf.replicateCond {
		if c != nil {
			c.Broadcast()
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool { return atomic.LoadInt32(&rf.dead) == 1 }

// ---------------------------------------------------------------- persistence

type persistentState struct {
	CurrentTerm int
	VotedFor    int
	Entries     []Entry
}

func (rf *Raft) encodeState() []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(persistentState{rf.currentTerm, rf.votedFor, rf.log.entries}); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// persist must be called with mu held, before replying to any RPC that
// changed currentTerm, votedFor or the log.
func (rf *Raft) persist(snapshot []byte) {
	rf.stateVersion++
	rf.save(rf.encodeState(), snapshot, rf.stateVersion)
}

// save writes state unless a newer version is already durable.
func (rf *Raft) save(state, snapshot []byte, version uint64) {
	rf.persistMu.Lock()
	defer rf.persistMu.Unlock()
	if version <= rf.savedVersion {
		return
	}
	if err := rf.persister.Save(state, snapshot); err != nil {
		panic("raft: persist failed: " + err.Error())
	}
	rf.savedVersion = version
}

// flusher is the leader's group-commit loop: it encodes the current state
// under mu, fsyncs it without holding mu, then marks the flushed prefix as
// durable on the leader and re-evaluates the commit index.
func (rf *Raft) flusher() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		for !rf.flushPending && !rf.killed() {
			rf.flushCond.Wait()
		}
		if rf.killed() {
			return
		}
		rf.flushPending = false
		rf.stateVersion++
		version, state := rf.stateVersion, rf.encodeState()
		term, last := rf.currentTerm, rf.log.lastIndex()
		rf.mu.Unlock()
		rf.save(state, nil, version)
		rf.mu.Lock()
		if rf.role == leader && rf.currentTerm == term && last > rf.matchIndex[rf.me] {
			rf.matchIndex[rf.me] = last
			rf.advanceCommit()
		}
	}
}

func (rf *Raft) restore(data []byte) {
	if len(data) == 0 {
		return
	}
	var st persistentState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&st); err != nil {
		panic("raft: corrupt persisted state: " + err.Error())
	}
	rf.currentTerm, rf.votedFor = st.CurrentTerm, st.VotedFor
	rf.log.entries = st.Entries
}

// ---------------------------------------------------------------- timers

func (rf *Raft) resetElectionTimer() {
	span := int64(rf.cfg.ElectionTimeoutMax - rf.cfg.ElectionTimeoutMin)
	rf.electionWait = rf.cfg.ElectionTimeoutMin + time.Duration(rand.Int63n(span))
	rf.electionReset = time.Now()
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		if rf.role == leader {
			rf.mu.Unlock()
			continue
		}
		if time.Since(rf.electionReset) >= rf.electionWait {
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) heartbeatLoop(term int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		rf.broadcast()
		rf.mu.Unlock()
		time.Sleep(rf.cfg.HeartbeatInterval)
	}
}

// ---------------------------------------------------------------- elections

// becomeFollower adopts a newer term. Caller persists.
func (rf *Raft) becomeFollower(term int) {
	rf.role = follower
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
	}
}

func (rf *Raft) startElection() {
	rf.role = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.leaderID = -1
	rf.persist(nil)
	rf.resetElectionTimer()

	term := rf.currentTerm
	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  rf.me,
		LastLogIndex: rf.log.lastIndex(),
		LastLogTerm:  rf.log.lastTerm(),
	}
	votes := 1
	if rf.peers == 1 {
		rf.becomeLeader()
		return
	}
	for p := 0; p < rf.peers; p++ {
		if p == rf.me {
			continue
		}
		go func(p int) {
			var reply RequestVoteReply
			if !rf.transport.RequestVote(p, &args, &reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				rf.persist(nil)
				return
			}
			if rf.role != candidate || rf.currentTerm != term || !reply.VoteGranted {
				return
			}
			votes++
			if votes > rf.peers/2 {
				rf.becomeLeader()
			}
		}(p)
	}
}

func (rf *Raft) becomeLeader() {
	rf.role = leader
	rf.leaderID = rf.me
	for p := range rf.nextIndex {
		rf.nextIndex[p] = rf.log.lastIndex() + 1
		rf.matchIndex[p] = 0
	}
	rf.matchIndex[rf.me] = rf.log.lastIndex()
	if rf.flushPending {
		rf.matchIndex[rf.me] = 0 // unflushed entries: the flusher will set this
	}
	go rf.heartbeatLoop(rf.currentTerm)
}

func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.persist(nil)
		return
	}
	// Election restriction (5.4.1): only vote for candidates whose log is at
	// least as up to date as ours, so a leader always holds every committed entry.
	upToDate := args.LastLogTerm > rf.log.lastTerm() ||
		(args.LastLogTerm == rf.log.lastTerm() && args.LastLogIndex >= rf.log.lastIndex())
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && upToDate {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
		rf.resetElectionTimer()
	}
	rf.persist(nil)
}

// ---------------------------------------------------------------- replication

// broadcast wakes every replicator. Caller holds mu.
func (rf *Raft) broadcast() {
	for p, c := range rf.replicateCond {
		if p != rf.me && c != nil {
			rf.replicatePend[p] = true
			c.Signal()
		}
	}
}

// replicator is a long-lived goroutine per peer. It sleeps until signalled
// (new entry or heartbeat tick) and then sends one AppendEntries or
// InstallSnapshot. One in-flight RPC per peer keeps ordering simple.
func (rf *Raft) replicator(peer int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		for !rf.replicatePend[peer] && !rf.killed() {
			rf.replicateCond[peer].Wait()
		}
		rf.replicatePend[peer] = false
		// Keep sending while the peer is behind and answering, so a lagging
		// follower catches up without waiting for the next heartbeat.
		for !rf.killed() && rf.role == leader && rf.sendTo(peer) {
		}
	}
}

// sendTo performs one replication round trip with peer. Called with mu held;
// releases it during the RPC. Returns true if another round should follow
// immediately (the peer replied and is still behind).
func (rf *Raft) sendTo(peer int) bool {
	term := rf.currentTerm
	next := rf.nextIndex[peer]

	if next <= rf.log.firstIndex() {
		args := InstallSnapshotArgs{
			Term:              term,
			LeaderID:          rf.me,
			LastIncludedIndex: rf.log.firstIndex(),
			LastIncludedTerm:  rf.log.term(rf.log.firstIndex()),
			Data:              rf.snapshot,
		}
		rf.mu.Unlock()
		var reply InstallSnapshotReply
		ok := rf.transport.InstallSnapshot(peer, &args, &reply)
		rf.mu.Lock()
		if !ok || rf.currentTerm != term || rf.role != leader {
			return false
		}
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			rf.persist(nil)
			return false
		}
		rf.matchIndex[peer] = max(rf.matchIndex[peer], args.LastIncludedIndex)
		rf.nextIndex[peer] = max(rf.nextIndex[peer], args.LastIncludedIndex+1)
		rf.advanceCommit()
		return rf.nextIndex[peer] <= rf.log.lastIndex()
	}

	prev := next - 1
	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     rf.me,
		PrevLogIndex: prev,
		PrevLogTerm:  rf.log.term(prev),
		Entries:      rf.log.from(next),
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	var reply AppendEntriesReply
	ok := rf.transport.AppendEntries(peer, &args, &reply)
	rf.mu.Lock()
	if !ok || rf.currentTerm != term || rf.role != leader {
		return false
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		rf.persist(nil)
		return false
	}
	if reply.Success {
		match := args.PrevLogIndex + len(args.Entries)
		if match > rf.matchIndex[peer] {
			rf.matchIndex[peer] = match
		}
		rf.nextIndex[peer] = max(rf.nextIndex[peer], match+1)
		rf.advanceCommit()
		return rf.nextIndex[peer] <= rf.log.lastIndex()
	}
	// Fast backup using the follower's conflict hints.
	if reply.ConflictTerm == -1 {
		rf.nextIndex[peer] = reply.ConflictIndex
	} else if last := rf.log.lastIndexOfTerm(reply.ConflictTerm); last != -1 {
		rf.nextIndex[peer] = last + 1
	} else {
		rf.nextIndex[peer] = reply.ConflictIndex
	}
	if rf.nextIndex[peer] < 1 {
		rf.nextIndex[peer] = 1
	}
	return true // retry promptly with the earlier index
}

// advanceCommit moves commitIndex to the highest index replicated on a
// majority, but only for entries from the current term (Figure 8 rule).
func (rf *Raft) advanceCommit() {
	for n := rf.log.lastIndex(); n > rf.commitIndex && n > rf.log.firstIndex(); n-- {
		if rf.log.term(n) != rf.currentTerm {
			break // earlier entries are older terms too
		}
		count := 0
		for p := 0; p < rf.peers; p++ {
			if rf.matchIndex[p] >= n {
				count++
			}
		}
		if count > rf.peers/2 {
			rf.commitIndex = n
			rf.applyCond.Broadcast()
			rf.broadcast() // let followers learn the new commit index quickly
			return
		}
	}
}

func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.persist(nil)
		return
	}
	rf.role = follower
	rf.leaderID = args.LeaderID
	rf.resetElectionTimer()

	// Our snapshot already covers part of the request: those entries are
	// committed, so treat the snapshot point as the agreed prefix.
	if args.PrevLogIndex < rf.log.firstIndex() {
		skip := rf.log.firstIndex() - args.PrevLogIndex
		if skip >= len(args.Entries) {
			reply.Success = true
			rf.persist(nil)
			return
		}
		args.Entries = args.Entries[skip:]
		args.PrevLogIndex = rf.log.firstIndex()
		args.PrevLogTerm = rf.log.term(rf.log.firstIndex())
	}

	if args.PrevLogIndex > rf.log.lastIndex() {
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.log.lastIndex() + 1
		rf.persist(nil)
		return
	}
	if t := rf.log.term(args.PrevLogIndex); t != args.PrevLogTerm {
		reply.ConflictTerm = t
		reply.ConflictIndex = rf.log.firstIndexOfTerm(t, args.PrevLogIndex)
		rf.persist(nil)
		return
	}

	// Append new entries, truncating only on a real conflict so a stale,
	// reordered RPC can never erase entries we already accepted.
	for i, e := range args.Entries {
		if e.Index > rf.log.lastIndex() {
			rf.log.append(args.Entries[i:]...)
			break
		}
		if rf.log.term(e.Index) != e.Term {
			rf.log.truncateAfter(e.Index - 1)
			rf.log.append(args.Entries[i:]...)
			break
		}
	}
	rf.persist(nil)

	lastNew := args.PrevLogIndex + len(args.Entries)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, lastNew)
		rf.applyCond.Broadcast()
	}
	reply.Success = true
}

// ---------------------------------------------------------------- snapshots

func (rf *Raft) HandleInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.persist(nil)
		return
	}
	rf.role = follower
	rf.leaderID = args.LeaderID
	rf.resetElectionTimer()

	if args.LastIncludedIndex <= rf.commitIndex {
		rf.persist(nil)
		return // we already have (and will apply) everything it covers
	}
	rf.log.compact(args.LastIncludedIndex, args.LastIncludedTerm)
	rf.snapshot = clone(args.Data)
	rf.persist(rf.snapshot)
	rf.commitIndex = args.LastIncludedIndex
	rf.pendingSnapshot = &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      clone(args.Data),
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	rf.applyCond.Broadcast()
}

// ---------------------------------------------------------------- applying

// applier is the only goroutine that writes to applyCh, so the service sees
// snapshots and commands strictly in order.
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if rf.pendingSnapshot != nil {
			msg := *rf.pendingSnapshot
			rf.pendingSnapshot = nil
			if msg.SnapshotIndex > rf.lastApplied {
				rf.lastApplied = msg.SnapshotIndex
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
			continue
		}
		if rf.lastApplied < rf.log.firstIndex() {
			rf.lastApplied = rf.log.firstIndex()
		}
		if rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
			continue
		}
		start := rf.lastApplied + 1
		end := min(rf.commitIndex, rf.log.lastIndex())
		batch := make([]ApplyMsg, 0, end-start+1)
		for i := start; i <= end; i++ {
			e := rf.log.at(i)
			batch = append(batch, ApplyMsg{CommandValid: true, Command: e.Command, CommandIndex: e.Index, CommandTerm: e.Term})
		}
		rf.lastApplied = end
		rf.mu.Unlock()
		for _, m := range batch {
			rf.applyCh <- m
		}
		rf.mu.Lock()
	}
}

// Stats is a point-in-time view used by the HTTP status endpoint.
type Stats struct {
	ID          int    `json:"id"`
	Role        string `json:"role"`
	Term        int    `json:"term"`
	Leader      int    `json:"leader"`
	CommitIndex int    `json:"commit_index"`
	LastApplied int    `json:"last_applied"`
	LogFirst    int    `json:"log_first_index"`
	LogLast     int    `json:"log_last_index"`
}

func (rf *Raft) Stats() Stats {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return Stats{rf.me, rf.role.String(), rf.currentTerm, rf.leaderID, rf.commitIndex,
		rf.lastApplied, rf.log.firstIndex(), rf.log.lastIndex()}
}
