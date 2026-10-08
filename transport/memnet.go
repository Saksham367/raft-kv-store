// Package transport provides Raft transports: an in-memory network with
// fault injection (used by tests and benchmarks) and HTTP/JSON for real
// multi-process clusters.
package transport

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Saksham367/raft-kv-store/raft"
)

// MemNetwork simulates a cluster network. Tests can disconnect servers
// (partitions), make the network unreliable (drops and random delays),
// enable long reordering delays, and swap in a restarted server.
type MemNetwork struct {
	mu          sync.Mutex
	handlers    map[int]raft.Handler
	connected   map[int]bool
	reliable    bool
	longReorder bool
	rpcCount    atomic.Int64
	bytes       atomic.Int64
}

func NewMemNetwork() *MemNetwork {
	return &MemNetwork{handlers: map[int]raft.Handler{}, connected: map[int]bool{}, reliable: true}
}

// Register installs (or replaces, after a restart) the handler for id.
func (n *MemNetwork) Register(id int, h raft.Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
	n.connected[id] = true
}

// Remove unplugs a crashed server: RPCs to it fail.
func (n *MemNetwork) Remove(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.handlers, id)
}

func (n *MemNetwork) Connect(id int)    { n.setConnected(id, true) }
func (n *MemNetwork) Disconnect(id int) { n.setConnected(id, false) }

func (n *MemNetwork) setConnected(id int, v bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.connected[id] = v
}

func (n *MemNetwork) SetReliable(v bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.reliable = v
}

func (n *MemNetwork) SetLongReordering(v bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.longReorder = v
}

func (n *MemNetwork) RPCCount() int64 { return n.rpcCount.Load() }

// Endpoint returns the Transport used by server `from`.
func (n *MemNetwork) Endpoint(from int) raft.Transport { return &memEndpoint{net: n, from: from} }

// deliver runs fn against the destination handler, applying the fault model.
// It returns false if the message (or its reply) was lost.
func (n *MemNetwork) deliver(from, to int, fn func(h raft.Handler)) bool {
	n.rpcCount.Add(1)
	n.mu.Lock()
	h := n.handlers[to]
	ok := n.connected[from] && n.connected[to] && h != nil
	reliable, longReorder := n.reliable, n.longReorder
	n.mu.Unlock()

	if !ok {
		// A partitioned send times out rather than failing instantly.
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		return false
	}
	if !reliable {
		time.Sleep(time.Duration(rand.Intn(27)) * time.Millisecond)
		if rand.Intn(10) == 0 { // drop request 10% of the time
			return false
		}
	}
	fn(h)

	n.mu.Lock()
	stillUp := n.connected[from] && n.connected[to] && n.handlers[to] == h
	n.mu.Unlock()
	if !stillUp {
		return false
	}
	if !reliable && rand.Intn(10) == 0 { // drop reply 10% of the time
		return false
	}
	if longReorder && rand.Intn(900) < 600 {
		time.Sleep(time.Duration(200+rand.Intn(1+rand.Intn(2000))) * time.Millisecond)
	}
	return true
}

type memEndpoint struct {
	net  *MemNetwork
	from int
}

// Each RPC hands the handler fresh copies so the caller and callee never
// share memory, as with a real network.
func (e *memEndpoint) RequestVote(to int, args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) bool {
	a := *args
	var r raft.RequestVoteReply
	ok := e.net.deliver(e.from, to, func(h raft.Handler) { h.HandleRequestVote(&a, &r) })
	if ok {
		*reply = r
	}
	return ok
}

func (e *memEndpoint) AppendEntries(to int, args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) bool {
	a := *args
	a.Entries = append([]raft.Entry(nil), args.Entries...)
	var r raft.AppendEntriesReply
	ok := e.net.deliver(e.from, to, func(h raft.Handler) { h.HandleAppendEntries(&a, &r) })
	if ok {
		*reply = r
	}
	return ok
}

func (e *memEndpoint) InstallSnapshot(to int, args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) bool {
	a := *args
	a.Data = append([]byte(nil), args.Data...)
	var r raft.InstallSnapshotReply
	ok := e.net.deliver(e.from, to, func(h raft.Handler) { h.HandleInstallSnapshot(&a, &r) })
	if ok {
		*reply = r
	}
	return ok
}
