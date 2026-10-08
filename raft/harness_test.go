package raft_test

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/Saksham367/raft-kv-store/raft"
	"github.com/Saksham367/raft-kv-store/transport"
)

// cluster is a test harness: n Raft peers over a simulated network, each
// with a tiny state machine that records the applied log. Every applied
// entry is cross-checked against all other peers, which is the core Raft
// safety property (State Machine Safety).
type cluster struct {
	t             *testing.T
	n             int
	net           *transport.MemNetwork
	mu            sync.Mutex
	rafts         []*raft.Raft
	persisters    []*raft.MemPersister
	gen           []int
	logs          []map[int]string // per server: index -> command
	lastApplied   []int
	connected     []bool
	maxIndex      int
	applyErr      string
	snapshotEvery int
	start         time.Time
}

func newCluster(t *testing.T, n int, reliable bool, snapshotEvery int) *cluster {
	c := &cluster{
		t: t, n: n, net: transport.NewMemNetwork(), snapshotEvery: snapshotEvery,
		rafts: make([]*raft.Raft, n), persisters: make([]*raft.MemPersister, n),
		gen: make([]int, n), logs: make([]map[int]string, n), lastApplied: make([]int, n),
		connected: make([]bool, n), start: time.Now(),
	}
	c.net.SetReliable(reliable)
	for i := 0; i < n; i++ {
		c.persisters[i] = raft.NewMemPersister()
		c.start1(i)
	}
	t.Cleanup(c.cleanup)
	return c
}

func (c *cluster) cleanup() {
	for i := 0; i < c.n; i++ {
		if c.rafts[i] != nil {
			c.rafts[i].Kill()
		}
	}
	if c.applyErr != "" {
		c.t.Fatal(c.applyErr)
	}
}

type snapState struct {
	LastIndex int
	Log       map[int]string
}

func encodeSnap(last int, log map[int]string) []byte {
	var b bytes.Buffer
	cp := map[int]string{}
	for k, v := range log {
		if k <= last {
			cp[k] = v
		}
	}
	if err := gob.NewEncoder(&b).Encode(snapState{last, cp}); err != nil {
		panic(err)
	}
	return b.Bytes()
}

func decodeSnap(data []byte) snapState {
	var s snapState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&s); err != nil {
		panic(err)
	}
	return s
}

// crash1 kills server i, keeping a copy of its persisted state.
func (c *cluster) crash1(i int) {
	c.net.Remove(i)
	c.mu.Lock()
	rf := c.rafts[i]
	c.rafts[i] = nil
	c.gen[i]++
	c.persisters[i] = c.persisters[i].Copy()
	c.mu.Unlock()
	if rf != nil {
		rf.Kill()
	}
}

// start1 (re)starts server i from its persister, like a reboot.
func (c *cluster) start1(i int) {
	c.crash1(i)
	c.mu.Lock()
	c.logs[i] = map[int]string{}
	c.lastApplied[i] = 0
	if snap := c.persisters[i].ReadSnapshot(); len(snap) > 0 {
		s := decodeSnap(snap)
		c.logs[i] = s.Log
		c.lastApplied[i] = s.LastIndex
	}
	gen := c.gen[i]
	c.mu.Unlock()

	ch := make(chan raft.ApplyMsg, 64)
	cfg := raft.Config{HeartbeatInterval: 50 * time.Millisecond, ElectionTimeoutMin: 250 * time.Millisecond, ElectionTimeoutMax: 500 * time.Millisecond}
	rf := raft.New(c.n, i, c.net.Endpoint(i), c.persisters[i], ch, cfg)
	c.mu.Lock()
	c.rafts[i] = rf
	c.connected[i] = true
	c.mu.Unlock()
	c.net.Register(i, rf)
	go c.applier(i, gen, rf, ch)
}

func (c *cluster) applier(i, gen int, rf *raft.Raft, ch chan raft.ApplyMsg) {
	for m := range ch {
		c.mu.Lock()
		if c.gen[i] != gen {
			c.mu.Unlock()
			continue
		}
		if m.SnapshotValid {
			s := decodeSnap(m.Snapshot)
			if s.LastIndex > c.lastApplied[i] {
				c.logs[i] = s.Log
				c.lastApplied[i] = s.LastIndex
			}
			c.mu.Unlock()
			continue
		}
		if !m.CommandValid {
			c.mu.Unlock()
			continue
		}
		cmd := string(m.Command)
		if m.CommandIndex != c.lastApplied[i]+1 && c.applyErr == "" {
			c.applyErr = fmt.Sprintf("server %d applied index %d out of order (last %d)", i, m.CommandIndex, c.lastApplied[i])
		}
		for j := 0; j < c.n; j++ {
			if old, ok := c.logs[j][m.CommandIndex]; ok && old != cmd && c.applyErr == "" {
				c.applyErr = fmt.Sprintf("commit index=%d server=%d %q != server=%d %q", m.CommandIndex, i, cmd, j, old)
			}
		}
		c.logs[i][m.CommandIndex] = cmd
		c.lastApplied[i] = m.CommandIndex
		if m.CommandIndex > c.maxIndex {
			c.maxIndex = m.CommandIndex
		}
		snapNow := c.snapshotEvery > 0 && m.CommandIndex%c.snapshotEvery == 0
		var data []byte
		if snapNow {
			data = encodeSnap(m.CommandIndex, c.logs[i])
		}
		c.mu.Unlock()
		if snapNow {
			rf.Snapshot(m.CommandIndex, data)
		}
	}
}

func (c *cluster) disconnect(i int) {
	c.mu.Lock()
	c.connected[i] = false
	c.mu.Unlock()
	c.net.Disconnect(i)
}

func (c *cluster) connect(i int) {
	c.mu.Lock()
	c.connected[i] = true
	c.mu.Unlock()
	c.net.Connect(i)
}

func (c *cluster) raftAt(i int) *raft.Raft {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rafts[i]
}

func (c *cluster) isConnected(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected[i] && c.rafts[i] != nil
}

func (c *cluster) failIfApplyErr() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.applyErr != "" {
		c.t.Fatal(c.applyErr)
	}
}

// checkOneLeader waits for exactly one leader in the newest term among
// connected servers and returns its id.
func (c *cluster) checkOneLeader() int {
	for iter := 0; iter < 10; iter++ {
		time.Sleep(time.Duration(450+rand.Intn(100)) * time.Millisecond)
		leaders := map[int][]int{}
		for i := 0; i < c.n; i++ {
			if c.isConnected(i) {
				if term, isLeader := c.raftAt(i).GetState(); isLeader {
					leaders[term] = append(leaders[term], i)
				}
			}
		}
		last := -1
		for term, ls := range leaders {
			if len(ls) > 1 {
				c.t.Fatalf("term %d has %d leaders", term, len(ls))
			}
			if term > last {
				last = term
			}
		}
		if len(leaders) != 0 {
			return leaders[last][0]
		}
	}
	c.t.Fatal("expected one leader, got none")
	return -1
}

func (c *cluster) checkTerms() int {
	term := -1
	for i := 0; i < c.n; i++ {
		if c.isConnected(i) {
			t, _ := c.raftAt(i).GetState()
			if term == -1 {
				term = t
			} else if term != t {
				c.t.Fatal("servers disagree on term")
			}
		}
	}
	return term
}

func (c *cluster) checkNoLeader() {
	for i := 0; i < c.n; i++ {
		if c.isConnected(i) {
			if _, isLeader := c.raftAt(i).GetState(); isLeader {
				c.t.Fatalf("expected no leader among connected servers, but %d claims to be leader", i)
			}
		}
	}
}

// nCommitted returns how many servers applied index, and the command.
func (c *cluster) nCommitted(index int) (int, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.applyErr != "" {
		c.t.Fatal(c.applyErr)
	}
	count, cmd := 0, ""
	for i := 0; i < c.n; i++ {
		if v, ok := c.logs[i][index]; ok {
			if count > 0 && v != cmd {
				c.t.Fatalf("committed values do not match: index %d, %q vs %q", index, cmd, v)
			}
			count++
			cmd = v
		}
	}
	return count, cmd
}

// one submits cmd, retrying across servers and leader changes, until at
// least expected servers have committed it. Returns its log index.
func (c *cluster) one(cmd string, expected int, retry bool) int {
	t0 := time.Now()
	starts := 0
	for time.Since(t0) < 10*time.Second {
		index := -1
		for k := 0; k < c.n; k++ {
			starts = (starts + 1) % c.n
			if c.isConnected(starts) {
				if idx, _, ok := c.raftAt(starts).Start([]byte(cmd)); ok {
					index = idx
					break
				}
			}
		}
		if index != -1 {
			t1 := time.Now()
			for time.Since(t1) < 2*time.Second {
				if nd, got := c.nCommitted(index); nd > 0 && nd >= expected && got == cmd {
					return index
				}
				time.Sleep(20 * time.Millisecond)
			}
			if !retry {
				c.t.Fatalf("one(%q) failed to reach agreement", cmd)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	c.t.Fatalf("one(%q) failed to reach agreement within 10s", cmd)
	return -1
}

// wait blocks until n servers have committed index.
func (c *cluster) wait(index, n int, startTerm int) string {
	to := 10 * time.Millisecond
	for iters := 0; iters < 30; iters++ {
		if nd, _ := c.nCommitted(index); nd >= n {
			break
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
		if startTerm > -1 {
			for i := 0; i < c.n; i++ {
				r := c.raftAt(i)
				if r == nil {
					continue
				}
				if t, _ := r.GetState(); t > startTerm {
					return "" // someone moved on; can't expect progress
				}
			}
		}
	}
	nd, cmd := c.nCommitted(index)
	if nd < n {
		c.t.Fatalf("only %d decided for index %d; wanted %d", nd, index, n)
	}
	return cmd
}
