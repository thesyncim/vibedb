package raftservice_test

import "testing"

func TestMultiGroupRF3NetworkIsolationPreservesHealthyQuorum(t *testing.T) {
	for leader := 0; leader < multiGroupRF3Voters; leader++ {
		network := &multiGroupRF3Network{}
		for member := 0; member < multiGroupRF3Voters; member++ {
			if network.nodeIsolated(member) {
				t.Fatalf("fresh network member %d is isolated", member)
			}
		}
		network.isolate(leader)
		for member := 0; member < multiGroupRF3Voters; member++ {
			if got, want := network.nodeIsolated(member), member == leader; got != want {
				t.Fatalf("isolated leader %d: member %d isolated=%t, want %t", leader, member, got, want)
			}
		}
		network.heal(leader)
		for member := 0; member < multiGroupRF3Voters; member++ {
			if network.nodeIsolated(member) {
				t.Fatalf("healed leader %d: member %d remains isolated", leader, member)
			}
		}
	}
}

func TestMultiGroupRF3NetworkIsolationRequiresBothDirectionsForEveryPeer(t *testing.T) {
	for node := 0; node < multiGroupRF3Voters; node++ {
		for peer := 0; peer < multiGroupRF3Voters; peer++ {
			if peer == node {
				continue
			}
			for direction := 0; direction < 2; direction++ {
				network := &multiGroupRF3Network{}
				network.isolate(node)
				if direction == 0 {
					network.blocked[node][peer] = false
				} else {
					network.blocked[peer][node] = false
				}
				if network.nodeIsolated(node) {
					t.Fatalf("node %d peer %d direction %d: one reachable direction treated as complete isolation", node, peer, direction)
				}
			}
		}
		network := &multiGroupRF3Network{}
		network.blocked[node][node] = true
		if network.nodeIsolated(node) {
			t.Fatalf("self block isolates member %d", node)
		}
		network.isolate(node)
		if !network.nodeIsolated(node) {
			t.Fatalf("all remote peers blocked but member %d not isolated", node)
		}
	}
}
