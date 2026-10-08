package raft

// raftLog stores entries after the latest snapshot. entries[0] is a sentinel
// whose Index/Term are the snapshot's last included index/term, so index
// arithmetic works the same before and after compaction.
type raftLog struct {
	entries []Entry
}

func newLog() raftLog {
	return raftLog{entries: []Entry{{Index: 0, Term: 0}}}
}

func (l *raftLog) firstIndex() int { return l.entries[0].Index } // snapshot index
func (l *raftLog) lastIndex() int  { return l.entries[len(l.entries)-1].Index }
func (l *raftLog) lastTerm() int   { return l.entries[len(l.entries)-1].Term }

// contains reports whether index is inside [firstIndex, lastIndex].
func (l *raftLog) contains(index int) bool {
	return index >= l.firstIndex() && index <= l.lastIndex()
}

func (l *raftLog) at(index int) Entry { return l.entries[index-l.firstIndex()] }
func (l *raftLog) term(index int) int { return l.at(index).Term }

// from returns a copy of entries with Index >= index.
func (l *raftLog) from(index int) []Entry {
	if index > l.lastIndex() {
		return nil
	}
	src := l.entries[index-l.firstIndex():]
	out := make([]Entry, len(src))
	copy(out, src)
	return out
}

// truncateAfter drops every entry with Index > index.
func (l *raftLog) truncateAfter(index int) {
	l.entries = l.entries[:index-l.firstIndex()+1]
}

func (l *raftLog) append(es ...Entry) { l.entries = append(l.entries, es...) }

// compact discards entries up to and including index, which becomes the new
// sentinel with the given term. Entries after index are kept if present.
func (l *raftLog) compact(index, term int) {
	var rest []Entry
	if index < l.lastIndex() && index >= l.firstIndex() && l.term(index) == term {
		rest = l.from(index + 1)
	}
	entries := make([]Entry, 0, len(rest)+1)
	entries = append(entries, Entry{Index: index, Term: term})
	entries = append(entries, rest...)
	l.entries = entries
}

// firstIndexOfTerm returns the smallest index in the log holding term,
// searching backwards from 'from'.
func (l *raftLog) firstIndexOfTerm(term, from int) int {
	i := from
	for i > l.firstIndex() && l.term(i-1) == term {
		i--
	}
	return i
}

// lastIndexOfTerm returns the largest index holding term, or -1.
func (l *raftLog) lastIndexOfTerm(term int) int {
	for i := l.lastIndex(); i > l.firstIndex(); i-- {
		t := l.term(i)
		if t == term {
			return i
		}
		if t < term {
			break
		}
	}
	return -1
}
