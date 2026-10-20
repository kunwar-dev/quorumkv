package raft_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	c.checkOneLeader()

	term1 := c.checkTerms()
	if term1 < 1 {
		t.Fatalf("expected term >= 1, got %d", term1)
	}
	// With no failures, the leader should keep its term.
	time.Sleep(2 * electionTimeout * 3)
	if term2 := c.checkTerms(); term1 != term2 {
		t.Logf("warning: term changed from %d to %d without failures", term1, term2)
	}
	c.checkOneLeader()
}

func TestReElection(t *testing.T) {
	c := newCluster(t, 3, true, 0)

	leader1 := c.checkOneLeader()
	c.disconnect(leader1)
	c.checkOneLeader()

	// The old leader rejoining must not disturb the new one.
	c.connect(leader1)
	leader2 := c.checkOneLeader()

	// Without a quorum, no leader can be elected.
	c.disconnect(leader2)
	c.disconnect((leader2 + 1) % 3)
	time.Sleep(4 * electionTimeout)
	c.checkNoLeader()

	c.connect((leader2 + 1) % 3)
	c.checkOneLeader()
	c.connect(leader2)
	c.checkOneLeader()
}

func TestManyElections(t *testing.T) {
	const n = 7
	c := newCluster(t, n, true, 0)
	c.checkOneLeader()
	for range 10 {
		a, b, d := rand.Intn(n), rand.Intn(n), rand.Intn(n)
		c.disconnect(a)
		c.disconnect(b)
		c.disconnect(d)
		// Either the remaining majority elects a leader, or there is no majority.
		c.checkOneLeader()
		c.connect(a)
		c.connect(b)
		c.connect(d)
	}
	c.checkOneLeader()
}

func TestBasicAgreement(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	for i := 1; i <= 5; i++ {
		if n, _ := c.nCommitted(uint64(i) + 1); n > 0 {
			t.Fatal("entries committed before Propose")
		}
		c.one(cmd(fmt.Sprint(i)), 3, false)
	}
}

func TestFollowerFailure(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	c.one(cmd("a"), 3, false)

	leader := c.checkOneLeader()
	c.disconnect((leader + 1) % 3)
	// The remaining two still form a majority.
	c.one(cmd("b"), 2, false)
	c.one(cmd("c"), 2, false)

	c.connect((leader + 1) % 3)
	// The rejoined follower catches up.
	c.one(cmd("d"), 3, true)
}

func TestNoAgreementWithoutQuorum(t *testing.T) {
	c := newCluster(t, 5, true, 0)
	c.one(cmd("a"), 5, false)

	leader := c.checkOneLeader()
	for k := 1; k <= 3; k++ {
		c.disconnect((leader + k) % 5)
	}
	index, _, err := c.node(leader).Propose(cmd("b"))
	if err != nil {
		t.Fatalf("leader rejected Propose: %v", err)
	}
	time.Sleep(2 * electionTimeout * 3)
	if n, _ := c.nCommitted(index); n > 0 {
		t.Fatalf("%d nodes committed without a majority", n)
	}

	for k := 1; k <= 3; k++ {
		c.connect((leader + k) % 5)
	}
	// The uncommitted entry may be overwritten; the cluster must make progress.
	c.one(cmd("c"), 5, true)
}

func TestRejoinOfPartitionedLeader(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	c.one(cmd("a"), 3, true)

	leader1 := c.checkOneLeader()
	c.disconnect(leader1)
	// The isolated leader accepts entries that can never commit.
	for _, s := range []string{"x1", "x2", "x3"} {
		_, _, _ = c.node(leader1).Propose(cmd(s))
	}

	c.one(cmd("b"), 2, true)

	leader2 := c.checkOneLeader()
	c.disconnect(leader2)
	c.connect(leader1)
	c.one(cmd("c"), 2, true)

	c.connect(leader2)
	c.one(cmd("d"), 3, true)
}

// TestFastBackup forces long divergent logs and checks the leader repairs
// followers efficiently using conflict hints.
func TestFastBackup(t *testing.T) {
	c := newCluster(t, 5, true, 0)
	c.one(cmd("seed"), 5, true)

	leader1 := c.checkOneLeader()
	c.disconnect((leader1 + 2) % 5)
	c.disconnect((leader1 + 3) % 5)
	c.disconnect((leader1 + 4) % 5)
	for i := range 50 {
		_, _, _ = c.node(leader1).Propose(cmd(fmt.Sprintf("lost-%d", i)))
	}
	time.Sleep(electionTimeout)
	c.disconnect(leader1)
	c.disconnect((leader1 + 1) % 5)

	c.connect((leader1 + 2) % 5)
	c.connect((leader1 + 3) % 5)
	c.connect((leader1 + 4) % 5)
	for i := range 50 {
		c.one(cmd(fmt.Sprintf("kept-%d", i)), 3, true)
	}

	leader2 := c.checkOneLeader()
	other := (leader1 + 2) % 5
	if leader2 == other {
		other = (leader2 + 1) % 5
	}
	c.disconnect(other)
	for i := range 50 {
		_, _, _ = c.node(leader2).Propose(cmd(fmt.Sprintf("lost2-%d", i)))
	}
	time.Sleep(electionTimeout)

	for i := range 5 {
		c.disconnect(i)
	}
	c.connect(leader1)
	c.connect((leader1 + 1) % 5)
	c.connect(other)
	for i := range 50 {
		c.one(cmd(fmt.Sprintf("kept2-%d", i)), 3, true)
	}

	for i := range 5 {
		c.connect(i)
	}
	c.one(cmd("final"), 5, true)

	before := c.net.RPCCount()
	c.one(cmd("probe"), 5, true)
	if used := c.net.RPCCount() - before; used > 60 {
		t.Fatalf("too many RPCs (%d) for one agreement after repair", used)
	}
}

func TestPersistenceAcrossFullRestart(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	c.one(cmd("11"), 3, true)

	for i := range 3 {
		c.restart(i)
	}
	c.one(cmd("12"), 3, true)

	leader := c.checkOneLeader()
	c.restart(leader)
	c.one(cmd("13"), 3, true)

	leader = c.checkOneLeader()
	c.crash(leader)
	c.one(cmd("14"), 2, true)
	c.start(leader)

	idx := c.one(cmd("15"), 3, true)
	c.wait(idx, 3)
}

// TestFigure8Unreliable reproduces the scenario from Figure 8 of the Raft
// paper on a lossy, reordering network with repeated leader crashes.
func TestFigure8Unreliable(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running")
	}
	const n = 5
	c := newCluster(t, n, false, 0)
	c.one(cmd("start"), 1, true)

	up := n
	for iter := range 300 {
		leader := -1
		for i := range n {
			if c.isConnected(i) {
				if _, _, err := c.node(i).Propose(cmd(fmt.Sprintf("v%d", iter))); err == nil {
					leader = i
				}
			}
		}
		if rand.Intn(1000) < 100 {
			time.Sleep(time.Duration(rand.Int63n(int64(electionTimeout * 3))))
		} else {
			time.Sleep(time.Duration(rand.Int63n(13)) * time.Millisecond)
		}
		if leader != -1 && rand.Intn(1000) < int(electionTimeout/time.Millisecond)/2 {
			c.crash(leader)
			up--
		}
		if up < 3 {
			s := rand.Intn(n)
			if !c.isConnected(s) {
				c.start(s)
				up++
			}
		}
	}
	for i := range n {
		if !c.isConnected(i) {
			c.start(i)
		}
	}
	c.net.SetReliable(true)
	c.one(cmd("end"), n, true)
}

func TestUnreliableAgreement(t *testing.T) {
	c := newCluster(t, 5, false, 0)
	done := make(chan struct{})
	for iter := range 20 {
		for j := range 4 {
			go func() {
				c.one(cmd(fmt.Sprintf("%d-%d", iter, j)), 1, true)
				done <- struct{}{}
			}()
		}
		c.one(cmd(fmt.Sprint(iter)), 1, true)
	}
	for range 20 * 4 {
		<-done
	}
	c.net.SetReliable(true)
	c.one(cmd("final"), 5, true)
}

func TestSnapshotInstallToLaggingFollower(t *testing.T) {
	c := newCluster(t, 3, true, 10)
	c.one(cmd("seed"), 3, true)

	leader := c.checkOneLeader()
	victim := (leader + 1) % 3
	c.disconnect(victim)
	// Far more entries than the snapshot interval, so the leader compacts
	// past what the victim has and must ship a snapshot.
	for i := range 60 {
		c.one(cmd(fmt.Sprintf("s%d", i)), 2, true)
	}
	if st := c.node(leader).Status(); st.SnapshotIndex == 0 {
		t.Fatal("leader never compacted its log")
	}

	c.connect(victim)
	idx := c.one(cmd("after"), 3, true)
	c.wait(idx, 3)
	if st := c.node(victim).Status(); st.SnapshotIndex == 0 {
		t.Fatal("victim caught up without installing a snapshot")
	}
}

func TestSnapshotSurvivesRestart(t *testing.T) {
	c := newCluster(t, 3, true, 10)
	for i := range 35 {
		c.one(cmd(fmt.Sprintf("r%d", i)), 3, true)
	}
	for i := range 3 {
		c.restart(i)
	}
	idx := c.one(cmd("post-restart"), 3, true)
	c.wait(idx, 3)
	for i := range 3 {
		if st := c.node(i).Status(); st.SnapshotIndex == 0 {
			t.Fatalf("node %d lost its snapshot across restart", i)
		}
	}
}

func TestSingleNodeCluster(t *testing.T) {
	c := newCluster(t, 1, true, 0)
	if leader := c.checkOneLeader(); leader != 0 {
		t.Fatalf("expected node 0 to lead, got %d", leader)
	}
	for i := range 10 {
		c.one(cmd(fmt.Sprint(i)), 1, false)
	}
}
