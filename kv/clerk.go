package kv

import (
	"crypto/rand"
	"encoding/binary"
	"sync/atomic"
	"time"
)

// Endpoint is how a clerk reaches one server: in-process for tests, HTTP in
// a real deployment.
type Endpoint interface {
	Do(op Op) Result
}

// Clerk is a client. It remembers the last known leader, tags every request
// with (ClientID, Seq) and retries until some leader applies it. Operations
// from one Clerk must not be issued concurrently.
type Clerk struct {
	servers  []Endpoint
	leader   int
	clientID int64
	seq      atomic.Int64
}

func NewClerk(servers []Endpoint) *Clerk {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return &Clerk{servers: servers, clientID: int64(binary.LittleEndian.Uint64(b[:])>>1) | 1}
}

func (c *Clerk) do(kind, key, value string) Result {
	op := Op{Kind: kind, Key: key, Value: value, ClientID: c.clientID, Seq: c.seq.Add(1)}
	tried := 0
	for {
		res := c.servers[c.leader].Do(op)
		if res.Err == OK || res.Err == ErrNoKey {
			return res
		}
		if res.LeaderID >= 0 && res.LeaderID < len(c.servers) && res.LeaderID != c.leader {
			c.leader = res.LeaderID
		} else {
			c.leader = (c.leader + 1) % len(c.servers)
		}
		tried++
		if tried%len(c.servers) == 0 {
			time.Sleep(50 * time.Millisecond) // probably mid-election
		}
	}
}

// Get returns the value for key, or "" if absent.
func (c *Clerk) Get(key string) string { return c.do(OpGet, key, "").Value }

// Lookup returns the value and whether the key exists.
func (c *Clerk) Lookup(key string) (string, bool) {
	r := c.do(OpGet, key, "")
	return r.Value, r.Err == OK
}

func (c *Clerk) Put(key, value string)    { c.do(OpPut, key, value) }
func (c *Clerk) Append(key, value string) { c.do(OpAppend, key, value) }

// Delete removes key and reports whether it existed.
func (c *Clerk) Delete(key string) bool { return c.do(OpDelete, key, "").Err == OK }
