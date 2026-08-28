//go:build darwin || linux

package rf3testfixture

import (
	"errors"
	"net"
	"sync"
)

const (
	ProcessClusterMembers   = 4
	ProcessClusterListeners = ProcessClusterMembers * 4
)

// ProcessCluster owns the complete 3-voter plus cold-target address cut used
// by external RF3 tests. All sixteen sockets remain bound while credentials,
// catalogs, and process manifests are written. ReleaseListeners closes the
// complete cut under one lock immediately before processes are started, so a
// parallel test cannot steal only part of a topology and leave a misleading
// half-live cluster behind.
type ProcessCluster struct {
	mu        sync.Mutex
	reserved  *ReservedAddresses
	released  bool
	listeners [ProcessClusterMembers]ProcessListeners
}

func ReserveProcessCluster() (*ProcessCluster, error) {
	reserved, err := ReserveLoopbackAddresses(ProcessClusterListeners)
	if err != nil {
		return nil, err
	}
	cluster := &ProcessCluster{reserved: reserved}
	for member := range ProcessClusterMembers {
		base := member * 4
		cluster.listeners[member] = ProcessListeners{
			Peer: reserved.Addresses[base], Native: reserved.Addresses[base+1],
			Snapshot: reserved.Addresses[base+2], Control: reserved.Addresses[base+3],
		}
	}
	return cluster, nil
}

func (cluster *ProcessCluster) Member(index int) (ProcessListeners, bool) {
	if cluster == nil || index < 0 || index >= ProcessClusterMembers {
		return ProcessListeners{}, false
	}
	cluster.mu.Lock()
	defer cluster.mu.Unlock()
	return cluster.listeners[index], !cluster.released
}

func (cluster *ProcessCluster) Members() [ProcessClusterMembers]ProcessListeners {
	if cluster == nil {
		return [ProcessClusterMembers]ProcessListeners{}
	}
	cluster.mu.Lock()
	defer cluster.mu.Unlock()
	return cluster.listeners
}

// ReleaseListeners transfers address ownership to the process launch phase.
// It is exact and idempotent: the reservation is detached before any socket is
// closed, and a retry cannot close a listener subsequently opened by a child.
func (cluster *ProcessCluster) ReleaseListeners() error {
	if cluster == nil {
		return errors.New("rf3 process fixture: invalid process cluster")
	}
	cluster.mu.Lock()
	if cluster.released {
		cluster.mu.Unlock()
		return nil
	}
	reserved := cluster.reserved
	cluster.reserved, cluster.released = nil, true
	cluster.mu.Unlock()
	if reserved == nil {
		return errors.New("rf3 process fixture: missing process cluster reservation")
	}
	return reserved.Close()
}

// Close releases an unlaunched reservation. After ReleaseListeners it is a
// no-op and cannot interfere with child-owned sockets.
func (cluster *ProcessCluster) Close() error { return cluster.ReleaseListeners() }

func processClusterAddressAvailable(address string) bool {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return false
	}
	_ = listener.Close()
	return true
}
