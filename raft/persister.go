package raft

import (
	"os"
	"path/filepath"
	"sync"
)

// Persister stores Raft's durable state and the latest snapshot. Save must
// write both atomically with respect to a crash.
type Persister interface {
	Save(raftState, snapshot []byte) error
	ReadRaftState() []byte
	ReadSnapshot() []byte
	RaftStateSize() int
}

// MemPersister keeps state in memory. Tests "crash" a server by copying its
// persister and starting a new Raft from the copy.
type MemPersister struct {
	mu        sync.Mutex
	raftState []byte
	snapshot  []byte
}

func NewMemPersister() *MemPersister { return &MemPersister{} }

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

func (p *MemPersister) Copy() *MemPersister {
	p.mu.Lock()
	defer p.mu.Unlock()
	return &MemPersister{raftState: clone(p.raftState), snapshot: clone(p.snapshot)}
}

func (p *MemPersister) Save(raftState, snapshot []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.raftState = clone(raftState)
	if snapshot != nil {
		p.snapshot = clone(snapshot)
	}
	return nil
}

func (p *MemPersister) ReadRaftState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return clone(p.raftState)
}

func (p *MemPersister) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return clone(p.snapshot)
}

func (p *MemPersister) RaftStateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.raftState)
}

// FilePersister writes state to disk with write-to-temp, fsync, rename, so a
// crash mid-write leaves the previous version intact.
type FilePersister struct {
	mu        sync.Mutex
	dir       string
	stateSize int
}

func NewFilePersister(dir string) (*FilePersister, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	p := &FilePersister{dir: dir}
	p.stateSize = len(p.read("raftstate"))
	return p, nil
}

func (p *FilePersister) path(name string) string { return filepath.Join(p.dir, name) }

func (p *FilePersister) writeAtomic(name string, data []byte) error {
	tmp := p.path(name + ".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, p.path(name))
}

func (p *FilePersister) read(name string) []byte {
	b, err := os.ReadFile(p.path(name))
	if err != nil {
		return nil
	}
	return b
}

func (p *FilePersister) Save(raftState, snapshot []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Snapshot first: a state file that references a snapshot must never be
	// visible before the snapshot itself.
	if snapshot != nil {
		if err := p.writeAtomic("snapshot", snapshot); err != nil {
			return err
		}
	}
	if err := p.writeAtomic("raftstate", raftState); err != nil {
		return err
	}
	p.stateSize = len(raftState)
	return nil
}

func (p *FilePersister) ReadRaftState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.read("raftstate")
}

func (p *FilePersister) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.read("snapshot")
}

func (p *FilePersister) RaftStateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stateSize
}
