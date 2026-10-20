package raft

// raftLog is the in-memory view of the log after the last snapshot.
// entries[0] holds index snapIndex+1. All indexes are absolute.
type raftLog struct {
	snapIndex uint64
	snapTerm  uint64
	entries   []Entry
}

func (l *raftLog) lastIndex() uint64 { return l.snapIndex + uint64(len(l.entries)) }

func (l *raftLog) lastTerm() uint64 {
	if len(l.entries) == 0 {
		return l.snapTerm
	}
	return l.entries[len(l.entries)-1].Term
}

// term returns the term of entry i. ok is false if i is compacted or beyond the log.
func (l *raftLog) term(i uint64) (t uint64, ok bool) {
	switch {
	case i == l.snapIndex:
		return l.snapTerm, true
	case i < l.snapIndex || i > l.lastIndex():
		return 0, false
	default:
		return l.entries[i-l.snapIndex-1].Term, true
	}
}

// slice returns a copy of entries in [lo, hi). lo must be > snapIndex.
func (l *raftLog) slice(lo, hi uint64) []Entry {
	if lo >= hi {
		return nil
	}
	return append([]Entry(nil), l.entries[lo-l.snapIndex-1:hi-l.snapIndex-1]...)
}

// truncateFrom removes entries with index >= i.
func (l *raftLog) truncateFrom(i uint64) {
	if i <= l.snapIndex {
		l.entries = nil
		return
	}
	if off := i - l.snapIndex - 1; off < uint64(len(l.entries)) {
		l.entries = l.entries[:off]
	}
}

func (l *raftLog) append(es ...Entry) { l.entries = append(l.entries, es...) }

// compact discards entries up to and including index, which becomes the new snapshot point.
func (l *raftLog) compact(index, term uint64) {
	if index <= l.snapIndex {
		return
	}
	if index >= l.lastIndex() {
		l.entries = nil
	} else {
		l.entries = append([]Entry(nil), l.entries[index-l.snapIndex:]...)
	}
	l.snapIndex, l.snapTerm = index, term
}

// firstIndexOfTerm returns the first retained index whose term equals t, scanning back from i.
func (l *raftLog) firstIndexOfTerm(t, i uint64) uint64 {
	for i > l.snapIndex+1 {
		if pt, _ := l.term(i - 1); pt != t {
			break
		}
		i--
	}
	return i
}

// lastIndexOfTerm returns the last index whose term equals t.
func (l *raftLog) lastIndexOfTerm(t uint64) (uint64, bool) {
	for i := l.lastIndex(); i > l.snapIndex; i-- {
		et, _ := l.term(i)
		if et == t {
			return i, true
		}
		if et < t {
			break
		}
	}
	return 0, false
}
