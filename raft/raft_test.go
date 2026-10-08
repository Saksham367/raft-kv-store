package raft_test

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

// ---------------------------------------------------------------- elections

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	c.checkOneLeader()
	time.Sleep(50 * time.Millisecond)
	term1 := c.checkTerms()
	time.Sleep(1 * time.Second)
	if term2 := c.checkTerms(); term1 != term2 {
		t.Logf("warning: term changed with no failures (%d -> %d)", term1, term2)
	}
	c.checkOneLeader()
}

func TestReElection(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	l1 := c.checkOneLeader()

	c.disconnect(l1) // leader fails: a new one must be elected
	c.checkOneLeader()

	c.connect(l1) // old leader rejoins: must not disturb the new leader
	l2 := c.checkOneLeader()

	c.disconnect(l2) // no quorum: no leader may be elected
	c.disconnect((l2 + 1) % 3)
	time.Sleep(1 * time.Second)
	c.checkNoLeader()

	c.connect((l2 + 1) % 3) // quorum restored
	c.checkOneLeader()
	c.connect(l2)
	c.checkOneLeader()
}

func TestManyElections(t *testing.T) {
	c := newCluster(t, 7, true, 0)
	c.checkOneLeader()
	for i := 0; i < 10; i++ {
		a, b, d := rand.Intn(7), rand.Intn(7), rand.Intn(7)
		c.disconnect(a)
		c.disconnect(b)
		c.disconnect(d)
		c.checkOneLeader() // at least 4 of 7 remain connected
		c.connect(a)
		c.connect(b)
		c.connect(d)
	}
	c.checkOneLeader()
}

// ---------------------------------------------------------------- replication

func TestBasicAgree(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	for index := 1; index <= 3; index++ {
		if nd, _ := c.nCommitted(index); nd > 0 {
			t.Fatal("entries committed before Start()")
		}
		if got := c.one(strconv.Itoa(index*100), 3, false); got != index {
			t.Fatalf("got index %d, want %d", got, index)
		}
	}
}

func TestFollowerFailure(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	c.one("101", 3, false)
	l := c.checkOneLeader()
	c.disconnect((l + 1) % 3)
	c.one("102", 2, false) // two servers still form a majority
	time.Sleep(600 * time.Millisecond)
	c.one("103", 2, false)

	l2 := c.checkOneLeader()
	c.disconnect((l2 + 1) % 3)
	c.disconnect((l2 + 2) % 3)
	index, _, ok := c.raftAt(l2).Start([]byte("104"))
	if !ok {
		t.Fatal("leader rejected Start()")
	}
	time.Sleep(1200 * time.Millisecond)
	if n, _ := c.nCommitted(index); n > 0 {
		t.Fatalf("%d committed without a majority", n)
	}
}

func TestLeaderFailure(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	c.one("101", 3, false)
	l := c.checkOneLeader()
	c.disconnect(l)
	c.one("102", 2, false)
	time.Sleep(600 * time.Millisecond)
	c.one("103", 2, false)
}

func TestFailNoAgree(t *testing.T) {
	c := newCluster(t, 5, true, 0)
	c.one("10", 5, false)

	l := c.checkOneLeader()
	c.disconnect((l + 1) % 5)
	c.disconnect((l + 2) % 5)
	c.disconnect((l + 3) % 5)
	index, _, ok := c.raftAt(l).Start([]byte("20"))
	if !ok {
		t.Fatal("leader rejected Start()")
	}
	if index != 2 {
		t.Fatalf("expected index 2, got %d", index)
	}
	time.Sleep(2 * time.Second)
	if n, _ := c.nCommitted(index); n > 0 {
		t.Fatalf("%d committed but no majority", n)
	}

	c.connect((l + 1) % 5)
	c.connect((l + 2) % 5)
	c.connect((l + 3) % 5)
	l2 := c.checkOneLeader()
	index2, _, ok2 := c.raftAt(l2).Start([]byte("30"))
	if !ok2 {
		t.Fatal("leader2 rejected Start()")
	}
	if index2 < 2 || index2 > 3 {
		t.Fatalf("unexpected index %d", index2)
	}
	c.one("1000", 5, true)
}

func TestRejoin(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	c.one("101", 3, true)

	l1 := c.checkOneLeader() // leader partitioned, accepts entries it cannot commit
	c.disconnect(l1)
	c.raftAt(l1).Start([]byte("102"))
	c.raftAt(l1).Start([]byte("103"))
	c.raftAt(l1).Start([]byte("104"))

	c.one("103", 2, true) // new leader commits at index 2

	l2 := c.checkOneLeader()
	c.disconnect(l2)
	c.connect(l1) // old leader must discard its uncommitted entries

	c.one("104", 2, true)
	c.connect(l2)
	c.one("105", 3, true)
}

// TestBackup forces long divergent logs so the leader must back up nextIndex
// across many conflicting entries quickly.
func TestBackup(t *testing.T) {
	c := newCluster(t, 5, true, 0)
	c.one(strconv.Itoa(rand.Int()), 5, true)

	l1 := c.checkOneLeader()
	c.disconnect((l1 + 2) % 5)
	c.disconnect((l1 + 3) % 5)
	c.disconnect((l1 + 4) % 5)
	for i := 0; i < 50; i++ { // uncommittable entries on the minority side
		c.raftAt(l1).Start([]byte(strconv.Itoa(rand.Int())))
	}
	time.Sleep(500 * time.Millisecond)
	c.disconnect(l1)
	c.disconnect((l1 + 1) % 5)

	c.connect((l1 + 2) % 5)
	c.connect((l1 + 3) % 5)
	c.connect((l1 + 4) % 5)
	for i := 0; i < 50; i++ {
		c.one(strconv.Itoa(rand.Int()), 3, true)
	}

	l2 := c.checkOneLeader()
	other := (l1 + 2) % 5
	if l2 == other {
		other = (l2 + 1) % 5
	}
	c.disconnect(other)
	for i := 0; i < 50; i++ {
		c.raftAt(l2).Start([]byte(strconv.Itoa(rand.Int())))
	}
	time.Sleep(500 * time.Millisecond)

	for i := 0; i < 5; i++ {
		c.disconnect(i)
	}
	c.connect(l1)
	c.connect((l1 + 1) % 5)
	c.connect(other)
	for i := 0; i < 50; i++ {
		c.one(strconv.Itoa(rand.Int()), 3, true)
	}

	for i := 0; i < 5; i++ {
		c.connect(i)
	}
	c.one(strconv.Itoa(rand.Int()), 5, true)
}

// ---------------------------------------------------------------- persistence

func TestPersistCrashRestart(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	c.one("11", 3, true)

	for i := 0; i < 3; i++ { // whole cluster reboots
		c.start1(i)
	}
	c.one("12", 3, true)

	l1 := c.checkOneLeader()
	c.crash1(l1)
	c.start1(l1)
	c.one("13", 3, true)

	l2 := c.checkOneLeader()
	c.crash1(l2)
	c.one("14", 2, true)
	c.start1(l2)
	c.wait(4, 3, -1) // the restarted server catches up from its persisted log

	i3 := (c.checkOneLeader() + 1) % 3
	c.crash1(i3)
	c.one("15", 2, true)
	c.start1(i3)
	c.one("16", 3, true)
}

// TestFigure8Unreliable reproduces the scenario from Figure 8 of the paper
// under random crashes, drops, delays and reordering: an entry from an old
// term must never be counted as committed by replica count alone.
func TestFigure8Unreliable(t *testing.T) {
	c := newCluster(t, 5, false, 0)
	c.one(strconv.Itoa(rand.Intn(10000)), 1, true)

	nup := 5
	for iters := 0; iters < 200; iters++ {
		if iters == 100 {
			c.net.SetLongReordering(true)
		}
		leader := -1
		for i := 0; i < 5; i++ {
			if c.isConnected(i) {
				if _, _, ok := c.raftAt(i).Start([]byte(strconv.Itoa(rand.Intn(10000)))); ok {
					leader = i
				}
			}
		}
		if rand.Intn(1000) < 100 {
			time.Sleep(time.Duration(rand.Intn(250)) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(rand.Intn(13)) * time.Millisecond)
		}
		if leader != -1 && rand.Intn(1000) < 500 {
			c.disconnect(leader)
			nup--
		}
		if nup < 3 {
			s := rand.Intn(5)
			if !c.isConnected(s) {
				c.connect(s)
				nup++
			}
		}
	}
	for i := 0; i < 5; i++ {
		c.connect(i)
	}
	c.net.SetLongReordering(false)
	c.one(strconv.Itoa(rand.Intn(10000)), 5, true)
	c.failIfApplyErr()
}

// ---------------------------------------------------------------- snapshots

// TestSnapshotInstall leaves a follower far behind while the others compact
// their logs; it can only catch up through InstallSnapshot.
func TestSnapshotInstall(t *testing.T) {
	for _, reliable := range []bool{true, false} {
		t.Run(fmt.Sprintf("reliable=%v", reliable), func(t *testing.T) {
			c := newCluster(t, 3, reliable, 10)
			c.one(strconv.Itoa(rand.Int()), 3, true)
			for round := 0; round < 6; round++ {
				victim := (c.checkOneLeader() + 1) % 3
				if round%2 == 0 {
					c.disconnect(victim)
				} else {
					c.crash1(victim)
				}
				for i := 0; i < 25; i++ { // > snapshotEvery, so the gap is compacted away
					c.one(strconv.Itoa(rand.Int()), 2, true)
				}
				if round%2 == 0 {
					c.connect(victim)
				} else {
					c.start1(victim)
				}
				c.one(strconv.Itoa(rand.Int()), 3, true)
			}
		})
	}
}

func TestSnapshotAllCrash(t *testing.T) {
	c := newCluster(t, 3, true, 10)
	c.one("x", 3, true)
	for iter := 0; iter < 5; iter++ {
		for i := 0; i < 15; i++ {
			c.one(strconv.Itoa(rand.Int()), 3, true)
		}
		index1 := c.one(strconv.Itoa(rand.Int()), 3, true)
		for i := 0; i < 3; i++ {
			c.crash1(i)
		}
		for i := 0; i < 3; i++ {
			c.start1(i)
		}
		index2 := c.one(strconv.Itoa(rand.Int()), 3, true)
		if index2 < index1+1 {
			t.Fatalf("index decreased from %d to %d after restart", index1, index2)
		}
	}
}
