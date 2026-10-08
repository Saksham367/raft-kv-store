// Package kv is a linearizable, fault-tolerant key-value store replicated
// with Raft. Every operation, reads included, goes through the Raft log, and
// client retries are deduplicated by (ClientID, Seq) so each write takes
// effect exactly once even when a leader fails mid-request.
package kv

import (
	"bytes"
	"encoding/gob"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Saksham367/raft-kv-store/raft"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

// Op kinds.
const (
	OpGet    = "Get"
	OpPut    = "Put"
	OpAppend = "Append"
	OpDelete = "Delete"
)

type Op struct {
	Kind     string `json:"kind"`
	Key      string `json:"key"`
	Value    string `json:"value,omitempty"`
	ClientID int64  `json:"client_id"`
	Seq      int64  `json:"seq"`
}

type Result struct {
	Err      string `json:"err"`
	Value    string `json:"value,omitempty"`
	LeaderID int    `json:"leader_id"` // hint for clients on ErrWrongLeader
	term     int
}

type clientRecord struct {
	Seq int64
	Err string
}

type Server struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	persister    raft.Persister
	maxRaftState int // snapshot when persisted Raft state exceeds this (-1 = never)
	dead         int32
	opTimeout    time.Duration

	data        map[string]string
	lastClient  map[int64]clientRecord // dedup table
	lastApplied int
	waiters     map[int]chan Result
}

func NewServer(peers, me int, t raft.Transport, p raft.Persister, maxRaftState int, cfg raft.Config) *Server {
	s := &Server{
		me:           me,
		persister:    p,
		maxRaftState: maxRaftState,
		applyCh:      make(chan raft.ApplyMsg, 256),
		data:         map[string]string{},
		lastClient:   map[int64]clientRecord{},
		waiters:      map[int]chan Result{},
		opTimeout:    800 * time.Millisecond,
	}
	if snap := p.ReadSnapshot(); len(snap) > 0 {
		s.restoreSnapshot(snap)
	}
	s.rf = raft.New(peers, me, t, p, s.applyCh, cfg)
	go s.applyLoop()
	return s
}

func (s *Server) Raft() *raft.Raft { return s.rf }

func (s *Server) Kill() {
	atomic.StoreInt32(&s.dead, 1)
	s.rf.Kill()
}

func (s *Server) killed() bool { return atomic.LoadInt32(&s.dead) == 1 }

// Do submits op to Raft and waits until it is applied (or fails).
func (s *Server) Do(op Op) Result {
	if s.killed() {
		return Result{Err: ErrWrongLeader, LeaderID: -1}
	}
	cmd, err := encode(op)
	if err != nil {
		return Result{Err: err.Error()}
	}

	s.mu.Lock()
	// Fast path: a retried write that was already applied.
	if op.Kind != OpGet && op.ClientID != 0 {
		if rec, ok := s.lastClient[op.ClientID]; ok && op.Seq <= rec.Seq {
			s.mu.Unlock()
			if _, isLeader := s.rf.GetState(); isLeader {
				return Result{Err: rec.Err}
			}
			return Result{Err: ErrWrongLeader, LeaderID: s.rf.Leader()}
		}
	}
	index, term, isLeader := s.rf.Start(cmd)
	if !isLeader {
		s.mu.Unlock()
		return Result{Err: ErrWrongLeader, LeaderID: s.rf.Leader()}
	}
	ch := make(chan Result, 1)
	s.waiters[index] = ch
	s.mu.Unlock()

	timer := time.NewTimer(s.opTimeout)
	defer timer.Stop()
	select {
	case res := <-ch:
		// A different entry landed at our index: leadership was lost and the
		// op was overwritten. The client will retry elsewhere.
		if res.term != term {
			return Result{Err: ErrWrongLeader, LeaderID: s.rf.Leader()}
		}
		return res
	case <-timer.C:
		s.mu.Lock()
		if s.waiters[index] == ch {
			delete(s.waiters, index)
		}
		s.mu.Unlock()
		return Result{Err: ErrTimeout, LeaderID: s.rf.Leader()}
	}
}

func (s *Server) applyLoop() {
	for msg := range s.applyCh {
		if s.killed() {
			continue
		}
		s.mu.Lock()
		switch {
		case msg.SnapshotValid:
			if msg.SnapshotIndex > s.lastApplied {
				s.restoreSnapshot(msg.Snapshot)
				s.lastApplied = msg.SnapshotIndex
			}
		case msg.CommandValid:
			if msg.CommandIndex <= s.lastApplied {
				break
			}
			s.lastApplied = msg.CommandIndex
			op, err := decode(msg.Command)
			res := Result{Err: OK}
			if err != nil {
				res.Err = err.Error()
			} else {
				res = s.apply(op)
			}
			res.term = msg.CommandTerm
			if ch, ok := s.waiters[msg.CommandIndex]; ok {
				ch <- res
				delete(s.waiters, msg.CommandIndex)
			}
			if s.maxRaftState > 0 && s.persister.RaftStateSize() >= s.maxRaftState {
				s.rf.Snapshot(msg.CommandIndex, s.encodeSnapshot())
			}
		}
		s.mu.Unlock()
	}
}

// apply executes op against the state machine. Caller holds mu.
func (s *Server) apply(op Op) Result {
	if op.Kind == OpGet {
		v, ok := s.data[op.Key]
		if !ok {
			return Result{Err: ErrNoKey}
		}
		return Result{Err: OK, Value: v}
	}
	// ClientID 0 means "no retry protection" (single-shot HTTP requests).
	if op.ClientID != 0 {
		if rec, ok := s.lastClient[op.ClientID]; ok && op.Seq <= rec.Seq {
			return Result{Err: rec.Err} // duplicate: already applied once
		}
	}
	res := Result{Err: OK}
	switch op.Kind {
	case OpPut:
		s.data[op.Key] = op.Value
	case OpAppend:
		s.data[op.Key] += op.Value
	case OpDelete:
		if _, ok := s.data[op.Key]; ok {
			delete(s.data, op.Key)
		} else {
			res.Err = ErrNoKey
		}
	default:
		res.Err = "unknown op " + op.Kind
	}
	if op.ClientID != 0 {
		s.lastClient[op.ClientID] = clientRecord{Seq: op.Seq, Err: res.Err}
	}
	return res
}

type snapshotState struct {
	Data       map[string]string
	LastClient map[int64]clientRecord
}

func (s *Server) encodeSnapshot() []byte {
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(snapshotState{s.data, s.lastClient}); err != nil {
		panic(err)
	}
	return b.Bytes()
}

func (s *Server) restoreSnapshot(snap []byte) {
	var st snapshotState
	if err := gob.NewDecoder(bytes.NewReader(snap)).Decode(&st); err != nil {
		panic("kv: corrupt snapshot: " + err.Error())
	}
	s.data, s.lastClient = st.Data, st.LastClient
	if s.data == nil {
		s.data = map[string]string{}
	}
	if s.lastClient == nil {
		s.lastClient = map[int64]clientRecord{}
	}
}

// Len returns the number of keys (for status endpoints and tests).
func (s *Server) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.data)
}

func encode(op Op) ([]byte, error) {
	var b bytes.Buffer
	err := gob.NewEncoder(&b).Encode(op)
	return b.Bytes(), err
}

func decode(b []byte) (Op, error) {
	var op Op
	err := gob.NewDecoder(bytes.NewReader(b)).Decode(&op)
	return op, err
}
