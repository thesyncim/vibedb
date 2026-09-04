package main

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestReserveDevPortsHoldsRequestedPGEndpointsDuringEveryAllocation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		physical int
		hosts    []string
	}{
		{name: "single", physical: 3, hosts: []string{"127.0.0.1", "", ""}},
		{name: "three", physical: 3, hosts: []string{"127.0.0.1", "127.0.0.1", "127.0.0.1"}},
		{name: "six", physical: 6, hosts: []string{"127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1"}},
		{name: "address_families", physical: 3, hosts: []string{"127.0.0.1", "::1", "::1"}},
		{name: "loopback_alias", physical: 3, hosts: []string{"127.0.0.1", "127.0.0.2", ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requested := make([]string, len(tc.hosts))
			var initial []net.Listener
			t.Cleanup(func() {
				for _, listener := range initial {
					listener.Close()
				}
			})
			for index, host := range tc.hosts {
				if host == "" {
					continue
				}
				listener, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
				if host != "127.0.0.1" && (errors.Is(err, syscall.EAFNOSUPPORT) || errors.Is(err, syscall.EADDRNOTAVAIL)) {
					t.Skipf("loopback address %q unavailable: %v", host, err)
				}
				if err != nil {
					t.Fatal(err)
				}
				initial = append(initial, listener)
				requested[index] = listener.Addr().String()
			}
			for _, listener := range initial {
				if err := listener.Close(); err != nil {
					t.Fatal(err)
				}
			}
			initial = nil
			allocations := 0
			ports, err := reserveDevPortsUsing(1+tc.physical*6, requested, func(network, address string) (net.Listener, error) {
				if address == "127.0.0.1:0" {
					allocations++
					// Deliberately try every requested binding before each
					// internal allocation; a missing reservation fails on every
					// run instead of depending on an ephemeral-port coincidence.
					for _, pgAddress := range requested {
						if pgAddress == "" {
							continue
						}
						probe, probeErr := net.Listen(network, pgAddress)
						if probeErr == nil {
							probe.Close()
							t.Fatalf("PG endpoint %q unreserved during allocation %d", pgAddress, allocations)
						}
						if !errors.Is(probeErr, syscall.EADDRINUSE) {
							t.Fatalf("PG reservation probe %q: %v", pgAddress, probeErr)
						}
					}
				}
				return net.Listen(network, address)
			})
			if err != nil || len(ports) != 1+tc.physical*6 || allocations != len(ports) {
				t.Fatalf("ports=%v allocations=%d err=%v", ports, allocations, err)
			}
			assertDevPortsCanBindTogether(t, append(requested, ports...))
		})
	}
}

func TestReserveDevPortsClosesAllReservationsOnFailure(t *testing.T) {
	allocationFailure := errors.New("injected listener allocation failure")
	closeFailure := errors.New("injected listener close failure")
	for _, tc := range []struct {
		name     string
		failCall int
	}{
		{name: "requested_listener", failCall: 2},
		{name: "internal_listener", failCall: 4},
		{name: "close"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requested, err := reserveDevPorts(2)
			if err != nil {
				t.Fatal(err)
			}
			var listeners []*devPortCloseProbe
			var addresses []string
			calls := 0
			ports, err := reserveDevPortsUsing(3, requested, func(network, address string) (net.Listener, error) {
				calls++
				if calls == tc.failCall {
					return nil, allocationFailure
				}
				listener, err := net.Listen(network, address)
				if err != nil {
					return nil, err
				}
				probe := &devPortCloseProbe{Listener: listener, failure: closeFailure}
				listeners = append(listeners, probe)
				addresses = append(addresses, listener.Addr().String())
				return probe, nil
			})
			if ports != nil || !errors.Is(err, closeFailure) || tc.failCall != 0 && !errors.Is(err, allocationFailure) {
				t.Fatalf("ports=%v err=%v", ports, err)
			}
			for _, listener := range listeners {
				if listener.closed != 1 {
					t.Fatalf("reservation closed %d times", listener.closed)
				}
			}
			assertDevPortsCanBindTogether(t, addresses)
		})
	}
}

func TestReserveDevPortsRejectsPGAliasesBeforeBinding(t *testing.T) {
	for _, requested := range [][]string{
		{"127.0.0.1:5000", "127.0.0.1:5000"},
		{"127.0.0.1:5000", "[::ffff:127.0.0.1]:5000"},
		{"[::]:5000"},
		{"0.0.0.0:5000"},
	} {
		_, err := reserveDevPortsUsing(1, requested, func(string, string) (net.Listener, error) {
			t.Fatalf("invalid requested endpoints reached socket allocation: %v", requested)
			return nil, nil
		})
		if !errors.Is(err, errDevCluster) {
			t.Fatalf("requested=%v err=%v", requested, err)
		}
	}
}

func TestInitializeDevPhysicalClusterRejectsBusyPGBeforeWritingIdentity(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	// Keep this root short enough for the DDL socket preflight on every OS.
	root, err := os.MkdirTemp("", "dev-pg-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	_, err = initializeDevPhysicalCluster(devClusterOptions{
		root: root, replicas: 3, physicalNodes: 3, pgListen: listener.Addr().String(),
	}, filepath.Join(root, "cluster.vibejson"))
	if !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("busy PG endpoint error=%v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("busy endpoint left durable initialization behind: entries=%v err=%v", entries, err)
	}
}

type devPortCloseProbe struct {
	net.Listener
	failure error
	closed  int
}

func (p *devPortCloseProbe) Close() error {
	p.closed++
	return errors.Join(p.Listener.Close(), p.failure)
}

func assertDevPortsCanBindTogether(t *testing.T, addresses []string) {
	t.Helper()
	var listeners []net.Listener
	defer func() {
		for _, listener := range listeners {
			listener.Close()
		}
	}()
	for _, address := range addresses {
		if address == "" {
			continue
		}
		listener, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatalf("released endpoints cannot bind together at %q: %v", address, err)
		}
		listeners = append(listeners, listener)
	}
}
