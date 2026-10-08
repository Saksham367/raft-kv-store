// kvnode runs one replica of the key-value store.
//
//	kvnode -id 0 -peers http://127.0.0.1:9001,http://127.0.0.1:9002,http://127.0.0.1:9003 -data ./data/0
//
// Client API (any node; non-leaders answer 307 with the leader's address):
//
//	GET    /kv/{key}
//	PUT    /kv/{key}         body = value
//	POST   /kv/{key}/append  body = suffix
//	DELETE /kv/{key}
//	GET    /status
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Saksham367/raft-kv-store/kv"
	"github.com/Saksham367/raft-kv-store/raft"
	"github.com/Saksham367/raft-kv-store/transport"
)

func main() {
	id := flag.Int("id", 0, "this node's index in -peers")
	peersFlag := flag.String("peers", "", "comma-separated peer base URLs, in id order")
	dataDir := flag.String("data", "", "directory for Raft state and snapshots (default ./data/<id>)")
	maxState := flag.Int("snapshot-bytes", 1<<20, "snapshot when the Raft log exceeds this many bytes")
	flag.Parse()

	peers := strings.Split(*peersFlag, ",")
	if *peersFlag == "" || *id < 0 || *id >= len(peers) {
		log.Fatal("need -peers and a valid -id")
	}
	if *dataDir == "" {
		*dataDir = "./data/" + strconv.Itoa(*id)
	}
	persister, err := raft.NewFilePersister(*dataDir)
	if err != nil {
		log.Fatal(err)
	}

	var server atomic.Pointer[kv.Server]
	mux := http.NewServeMux()
	transport.RegisterHandlers(mux, func() raft.Handler { return server.Load().Raft() })
	server.Store(kv.NewServer(len(peers), *id, transport.NewHTTPTransport(peers), persister, *maxState, raft.DefaultConfig()))
	srv := server.Load()

	// Clients may send X-Client-ID and X-Seq to make retries idempotent
	// (exactly-once). Without them a request is executed at most once per call.
	run := func(w http.ResponseWriter, r *http.Request, op kv.Op) {
		if cid, err := strconv.ParseInt(r.Header.Get("X-Client-ID"), 10, 64); err == nil && cid > 0 {
			if seq, err := strconv.ParseInt(r.Header.Get("X-Seq"), 10, 64); err == nil && seq > 0 {
				op.ClientID, op.Seq = cid, seq
			}
		}
		res := srv.Do(op)
		switch res.Err {
		case kv.OK:
			if op.Kind == kv.OpGet {
				_, _ = io.WriteString(w, res.Value)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		case kv.ErrNoKey:
			http.Error(w, "key not found", http.StatusNotFound)
		case kv.ErrWrongLeader:
			if res.LeaderID >= 0 && res.LeaderID < len(peers) {
				w.Header().Set("Location", peers[res.LeaderID]+r.URL.RequestURI())
				w.Header().Set("X-Raft-Leader", peers[res.LeaderID])
				http.Error(w, "not leader", http.StatusTemporaryRedirect)
				return
			}
			http.Error(w, "no leader elected", http.StatusServiceUnavailable)
		default:
			http.Error(w, res.Err, http.StatusServiceUnavailable)
		}
	}
	key := func(r *http.Request) (string, bool) {
		k, err := url.PathUnescape(r.PathValue("key"))
		return k, err == nil && k != ""
	}
	body := func(r *http.Request) (string, error) {
		b, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		return string(b), err
	}

	mux.HandleFunc("GET /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		if k, ok := key(r); ok {
			run(w, r, kv.Op{Kind: kv.OpGet, Key: k})
		} else {
			http.Error(w, "bad key", http.StatusBadRequest)
		}
	})
	mux.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		k, ok := key(r)
		v, err := body(r)
		if !ok || err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		run(w, r, kv.Op{Kind: kv.OpPut, Key: k, Value: v})
	})
	mux.HandleFunc("POST /kv/{key}/append", func(w http.ResponseWriter, r *http.Request) {
		k, ok := key(r)
		v, err := body(r)
		if !ok || err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		run(w, r, kv.Op{Kind: kv.OpAppend, Key: k, Value: v})
	})
	mux.HandleFunc("DELETE /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		if k, ok := key(r); ok {
			run(w, r, kv.Op{Kind: kv.OpDelete, Key: k})
		} else {
			http.Error(w, "bad key", http.StatusBadRequest)
		}
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"raft": srv.Raft().Stats(), "keys": srv.Len()})
	})

	u, err := url.Parse(peers[*id])
	if err != nil {
		log.Fatal(err)
	}
	httpSrv := &http.Server{Addr: u.Host, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	log.Printf("node %d listening on %s (data: %s)", *id, u.Host, *dataDir)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	srv.Kill()
	_ = httpSrv.Close()
}
