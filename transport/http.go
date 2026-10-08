package transport

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"github.com/Saksham367/raft-kv-store/raft"
)

// HTTPTransport sends Raft RPCs as JSON over HTTP to peer addresses.
type HTTPTransport struct {
	peers  []string // base URLs, e.g. http://127.0.0.1:9001
	client *http.Client
}

func NewHTTPTransport(peers []string) *HTTPTransport {
	return &HTTPTransport{
		peers: peers,
		client: &http.Client{
			Timeout: 500 * time.Millisecond,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 64,
				IdleConnTimeout:     30 * time.Second,
			},
		},
	}
}

func (t *HTTPTransport) call(peer int, path string, args, reply any) bool {
	body, err := json.Marshal(args)
	if err != nil {
		return false
	}
	resp, err := t.client.Post(t.peers[peer]+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return json.NewDecoder(resp.Body).Decode(reply) == nil
}

func (t *HTTPTransport) RequestVote(p int, a *raft.RequestVoteArgs, r *raft.RequestVoteReply) bool {
	return t.call(p, "/raft/vote", a, r)
}

func (t *HTTPTransport) AppendEntries(p int, a *raft.AppendEntriesArgs, r *raft.AppendEntriesReply) bool {
	return t.call(p, "/raft/append", a, r)
}

func (t *HTTPTransport) InstallSnapshot(p int, a *raft.InstallSnapshotArgs, r *raft.InstallSnapshotReply) bool {
	return t.call(p, "/raft/snapshot", a, r)
}

// RegisterHandlers mounts the server side of the RPCs on mux.
func RegisterHandlers(mux *http.ServeMux, h func() raft.Handler) {
	mux.HandleFunc("POST /raft/vote", func(w http.ResponseWriter, r *http.Request) {
		var a raft.RequestVoteArgs
		var rep raft.RequestVoteReply
		serve(w, r, &a, &rep, func() { h().HandleRequestVote(&a, &rep) })
	})
	mux.HandleFunc("POST /raft/append", func(w http.ResponseWriter, r *http.Request) {
		var a raft.AppendEntriesArgs
		var rep raft.AppendEntriesReply
		serve(w, r, &a, &rep, func() { h().HandleAppendEntries(&a, &rep) })
	})
	mux.HandleFunc("POST /raft/snapshot", func(w http.ResponseWriter, r *http.Request) {
		var a raft.InstallSnapshotArgs
		var rep raft.InstallSnapshotReply
		serve(w, r, &a, &rep, func() { h().HandleInstallSnapshot(&a, &rep) })
	})
}

func serve(w http.ResponseWriter, r *http.Request, args, reply any, fn func()) {
	if err := json.NewDecoder(r.Body).Decode(args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	fn()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(reply)
}
