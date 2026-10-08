package kv_test

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Saksham367/raft-kv-store/kv"
	"github.com/Saksham367/raft-kv-store/raft"
	"github.com/Saksham367/raft-kv-store/transport"
)

type kvCluster struct {
	t          *testing.T
	n          int
	maxState   int
	net        *transport.MemNetwork
	mu         sync.Mutex
	servers    []*kv.Server
	persisters []*raft.MemPersister
}

var testCfg = raft.Config{HeartbeatInterval: 50 * time.Millisecond, ElectionTimeoutMin: 250 * time.Millisecond, ElectionTimeoutMax: 500 * time.Millisecond}

func newKVCluster(t *testing.T, n int, reliable bool, maxState int) *kvCluster {
	c := &kvCluster{t: t, n: n, maxState: maxState, net: transport.NewMemNetwork(),
		servers: make([]*kv.Server, n), persisters: make([]*raft.MemPersister, n)}
	c.net.SetReliable(reliable)
	for i := 0; i < n; i++ {
		c.persisters[i] = raft.NewMemPersister()
		c.start(i)
	}
	t.Cleanup(func() {
		for i := 0; i < n; i++ {
			c.crash(i)
		}
	})
	return c
}

func (c *kvCluster) crash(i int) {
	c.net.Remove(i)
	c.mu.Lock()
	s := c.servers[i]
	c.servers[i] = nil
	c.persisters[i] = c.persisters[i].Copy()
	c.mu.Unlock()
	if s != nil {
		s.Kill()
	}
}

func (c *kvCluster) start(i int) {
	c.crash(i)
	s := kv.NewServer(c.n, i, c.net.Endpoint(i), c.persisters[i], c.maxState, testCfg)
	c.mu.Lock()
	c.servers[i] = s
	c.mu.Unlock()
	c.net.Register(i, s.Raft())
}

// endpoint resolves server i at call time, so clerks keep working across
// crashes and restarts.
type endpoint struct {
	c *kvCluster
	i int
}

func (e endpoint) Do(op kv.Op) kv.Result {
	e.c.mu.Lock()
	s := e.c.servers[e.i]
	e.c.mu.Unlock()
	if s == nil {
		time.Sleep(10 * time.Millisecond)
		return kv.Result{Err: kv.ErrWrongLeader, LeaderID: -1}
	}
	return s.Do(op)
}

func (c *kvCluster) clerk() *kv.Clerk {
	eps := make([]kv.Endpoint, c.n)
	for i := range eps {
		eps[i] = endpoint{c, i}
	}
	return kv.NewClerk(eps)
}

func TestBasicOps(t *testing.T) {
	c := newKVCluster(t, 3, true, -1)
	ck := c.clerk()
	ck.Put("a", "1")
	ck.Append("a", "2")
	if got := ck.Get("a"); got != "12" {
		t.Fatalf("Get(a) = %q, want 12", got)
	}
	if _, ok := ck.Lookup("missing"); ok {
		t.Fatal("missing key reported present")
	}
	if !ck.Delete("a") || ck.Delete("a") {
		t.Fatal("Delete semantics wrong")
	}
	if _, ok := ck.Lookup("a"); ok {
		t.Fatal("deleted key still present")
	}
}

// runWorkload has nClients clerks each append unique tokens to their own key
// while chaos() injects faults. Afterwards every acknowledged append must
// appear exactly once, in order: no lost writes, no duplicates from retries.
func runWorkload(t *testing.T, c *kvCluster, nClients int, dur time.Duration, chaos func(stop <-chan struct{})) int64 {
	var done atomic.Bool
	var ops atomic.Int64
	acked := make([][]string, nClients)
	var wg sync.WaitGroup
	for cl := 0; cl < nClients; cl++ {
		wg.Add(1)
		go func(cl int) {
			defer wg.Done()
			ck := c.clerk()
			key := fmt.Sprintf("client-%d", cl)
			ck.Put(key, "")
			for j := 0; !done.Load(); j++ {
				tok := fmt.Sprintf("[%d.%d]", cl, j)
				if rand.Intn(4) == 0 {
					v := ck.Get(key) // reads must reflect every prior acked write
					if want := strings.Join(acked[cl], ""); v != want {
						t.Errorf("client %d read %q, want %q", cl, v, want)
						return
					}
				} else {
					ck.Append(key, tok)
					acked[cl] = append(acked[cl], tok)
				}
				ops.Add(1)
			}
		}(cl)
	}
	stop := make(chan struct{})
	chaosDone := make(chan struct{})
	go func() { chaos(stop); close(chaosDone) }()
	time.Sleep(dur)
	close(stop)
	<-chaosDone
	done.Store(true)
	wg.Wait()

	// Heal everything and verify final state from a fresh clerk.
	for i := 0; i < c.n; i++ {
		c.net.Connect(i)
	}
	ck := c.clerk()
	for cl := 0; cl < nClients; cl++ {
		got := ck.Get(fmt.Sprintf("client-%d", cl))
		if want := strings.Join(acked[cl], ""); got != want {
			t.Fatalf("client %d final value mismatch:\n got  %q\n want %q", cl, got, want)
		}
	}
	return ops.Load()
}

func TestLinearizableUnderPartitions(t *testing.T) {
	c := newKVCluster(t, 5, true, -1)
	ops := runWorkload(t, c, 5, 8*time.Second, func(stop <-chan struct{}) {
		for {
			select {
			case <-stop:
				return
			case <-time.After(700 * time.Millisecond):
			}
			for i := 0; i < c.n; i++ {
				c.net.Connect(i)
			}
			// Random minority/majority split.
			perm := rand.Perm(c.n)
			for _, i := range perm[:rand.Intn(3)] {
				c.net.Disconnect(i)
			}
		}
	})
	t.Logf("%d operations completed under partitions", ops)
}

func TestLinearizableUnderCrashesUnreliableWithSnapshots(t *testing.T) {
	c := newKVCluster(t, 5, false, 2000)
	ops := runWorkload(t, c, 5, 10*time.Second, func(stop <-chan struct{}) {
		for {
			select {
			case <-stop:
				return
			case <-time.After(1 * time.Second):
			}
			victims := rand.Perm(c.n)[:1+rand.Intn(2)] // crash 1-2 of 5
			for _, v := range victims {
				c.crash(v)
			}
			time.Sleep(500 * time.Millisecond)
			for _, v := range victims {
				c.start(v)
			}
		}
	})
	for i := 0; i < c.n; i++ {
		if sz := c.persisters[i].RaftStateSize(); sz > 8*2000 {
			t.Fatalf("server %d raft state is %d bytes; snapshots are not bounding the log", i, sz)
		}
	}
	t.Logf("%d operations completed under crashes, drops and restarts", ops)
}
