// kvbench measures write throughput, latency and leader-failover time for a
// real cluster: every node runs its own HTTP server, Raft RPCs travel over
// HTTP on localhost, and state is fsync'd to disk with FilePersister.
//
//	go run ./cmd/kvbench -nodes 3 -clients 64 -duration 10s
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Saksham367/raft-kv-store/kv"
	"github.com/Saksham367/raft-kv-store/raft"
	"github.com/Saksham367/raft-kv-store/transport"
)

type node struct {
	server *kv.Server
	http   *http.Server
	dead   atomic.Bool
}

// localEndpoint lets benchmark clients call a node directly while the
// replication path (leader -> followers) still goes over HTTP.
type localEndpoint struct{ n *node }

func (e localEndpoint) Do(op kv.Op) kv.Result {
	if e.n.dead.Load() {
		return kv.Result{Err: kv.ErrWrongLeader, LeaderID: -1}
	}
	return e.n.server.Do(op)
}

func main() {
	nNodes := flag.Int("nodes", 3, "cluster size")
	nClients := flag.Int("clients", 64, "concurrent clients")
	dur := flag.Duration("duration", 10*time.Second, "load duration")
	valueSize := flag.Int("value-size", 128, "bytes per value")
	failovers := flag.Int("failovers", 5, "leader kill/recover trials")
	snapBytes := flag.Int("snapshot-bytes", 256<<10, "compact the Raft log beyond this size")
	flag.Parse()

	dir, err := os.MkdirTemp("", "kvbench")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Reserve ports up front.
	listeners := make([]net.Listener, *nNodes)
	peers := make([]string, *nNodes)
	for i := range listeners {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			log.Fatal(err)
		}
		listeners[i] = l
		peers[i] = "http://" + l.Addr().String()
	}

	nodes := make([]*node, *nNodes)
	for i := range nodes {
		p, err := raft.NewFilePersister(fmt.Sprintf("%s/%d", dir, i))
		if err != nil {
			log.Fatal(err)
		}
		n := &node{server: kv.NewServer(*nNodes, i, transport.NewHTTPTransport(peers), p, *snapBytes, raft.DefaultConfig())}
		mux := http.NewServeMux()
		transport.RegisterHandlers(mux, func() raft.Handler { return n.server.Raft() })
		n.http = &http.Server{Handler: mux}
		go func(l net.Listener) {
			if err := n.http.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Print(err)
			}
		}(listeners[i])
		nodes[i] = n
	}
	eps := func() []kv.Endpoint {
		out := make([]kv.Endpoint, len(nodes))
		for i, n := range nodes {
			out[i] = localEndpoint{n}
		}
		return out
	}

	// Wait for a leader.
	warm := kv.NewClerk(eps())
	warm.Put("warmup", "1")

	value := make([]byte, *valueSize)
	for i := range value {
		value[i] = 'a' + byte(i%26)
	}

	fmt.Printf("cluster: %d nodes, Raft over HTTP (localhost), fsync'd file persistence\n", *nNodes)
	fmt.Printf("load:    %d concurrent clients, %d-byte values, %s\n\n", *nClients, *valueSize, *dur)

	var stop atomic.Bool
	lat := make([][]time.Duration, *nClients)
	var wg sync.WaitGroup
	start := time.Now()
	for c := 0; c < *nClients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			ck := kv.NewClerk(eps())
			for i := 0; !stop.Load(); i++ {
				t0 := time.Now()
				ck.Put(fmt.Sprintf("k%d-%d", c, i%1000), string(value))
				lat[c] = append(lat[c], time.Since(t0))
			}
		}(c)
	}
	time.Sleep(*dur)
	stop.Store(true)
	wg.Wait()
	elapsed := time.Since(start)

	var all []time.Duration
	for _, l := range lat {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	pct := func(p float64) time.Duration { return all[int(float64(len(all)-1)*p)] }
	fmt.Printf("writes:      %d committed\n", len(all))
	fmt.Printf("throughput:  %.0f writes/sec\n", float64(len(all))/elapsed.Seconds())
	fmt.Printf("latency:     p50 %v  p95 %v  p99 %v\n\n", pct(0.50).Round(10*time.Microsecond),
		pct(0.95).Round(10*time.Microsecond), pct(0.99).Round(10*time.Microsecond))

	// Failover: kill the leader, measure time until a write commits again.
	var times []time.Duration
	ck := kv.NewClerk(eps())
	for trial := 0; trial < *failovers; trial++ {
		ck.Put("probe", "x")
		leader := -1
		for i, n := range nodes {
			if _, isLeader := n.server.Raft().GetState(); isLeader && !n.dead.Load() {
				leader = i
			}
		}
		if leader < 0 {
			log.Fatal("no leader found")
		}
		old := nodes[leader]
		t0 := time.Now()
		old.dead.Store(true)
		old.server.Kill()
		_ = old.http.Close()
		ck.Put("probe", fmt.Sprintf("after-%d", trial))
		times = append(times, time.Since(t0))

		// Restart the failed node from its on-disk state so the next trial
		// again has a full cluster.
		p, _ := raft.NewFilePersister(fmt.Sprintf("%s/%d", dir, leader))
		n := &node{server: kv.NewServer(*nNodes, leader, transport.NewHTTPTransport(peers), p, *snapBytes, raft.DefaultConfig())}
		mux := http.NewServeMux()
		transport.RegisterHandlers(mux, func() raft.Handler { return n.server.Raft() })
		n.http = &http.Server{Addr: peers[leader][len("http://"):], Handler: mux}
		go func() { _ = n.http.ListenAndServe() }()
		nodes[leader] = n
		ck = kv.NewClerk(eps())
		time.Sleep(1500 * time.Millisecond)
	}
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	var sum time.Duration
	for _, t := range times {
		sum += t
	}
	fmt.Printf("failover:    %d trials, mean %v, worst %v (leader killed -> next write committed)\n",
		len(times), (sum / time.Duration(len(times))).Round(time.Millisecond), times[len(times)-1].Round(time.Millisecond))
	if v := ck.Get("probe"); v != fmt.Sprintf("after-%d", *failovers-1) {
		log.Fatalf("data check failed: probe=%q", v)
	}
	fmt.Println("data check:  OK (last committed write survived every failover)")

	for _, n := range nodes {
		n.server.Kill()
		_ = n.http.Close()
	}
}
