package main

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// rf3PeerProxy forwards opaque TLS bytes on one directed peer link. Blocking
// closes both halves of every established connection and refuses new ones;
// native client and health listeners remain live. No TLS identity is changed.
type rf3PeerProxy struct {
	listener net.Listener
	target   string
	mu       sync.Mutex
	blocked  bool
	closed   bool
	active   map[net.Conn]net.Conn
	wg       sync.WaitGroup
}

func newRF3PeerProxy(t testing.TB, target string) *rf3PeerProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &rf3PeerProxy{listener: listener, target: target, active: make(map[net.Conn]net.Conn)}
	p.wg.Add(1)
	go p.accept()
	t.Cleanup(p.close)
	return p
}

func (p *rf3PeerProxy) accept() {
	defer p.wg.Done()
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		if p.closed || p.blocked || len(p.active) >= 8 {
			p.mu.Unlock()
			_ = client.Close()
			continue
		}
		p.active[client] = nil
		p.wg.Add(1)
		p.mu.Unlock()
		go p.forward(client)
	}
}

func (p *rf3PeerProxy) forward(client net.Conn) {
	defer p.wg.Done()
	defer func() {
		_ = client.Close()
		p.mu.Lock()
		delete(p.active, client)
		p.mu.Unlock()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	server, err := (&net.Dialer{}).DialContext(ctx, "tcp", p.target)
	if err != nil {
		return
	}
	defer server.Close()
	p.mu.Lock()
	if p.closed || p.blocked {
		p.mu.Unlock()
		return
	}
	p.active[client] = server
	p.mu.Unlock()
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(client, server)
		_ = client.Close()
		_ = server.Close()
		close(done)
	}()
	_, _ = io.Copy(server, client)
	_ = client.Close()
	_ = server.Close()
	<-done
}

func (p *rf3PeerProxy) setBlocked(blocked bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blocked = blocked
	if blocked {
		for client, server := range p.active {
			_ = client.Close()
			if server != nil {
				_ = server.Close()
			}
		}
	}
}

func (p *rf3PeerProxy) close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	_ = p.listener.Close()
	p.setBlocked(true)
	p.wg.Wait()
}

func TestRF3PeerProxyCutsExistingAndNewLinksButLeavesTargetLive(t *testing.T) {
	server, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	go func() {
		for {
			connection, err := server.Accept()
			if err != nil {
				return
			}
			go func() { defer connection.Close(); _, _ = io.Copy(connection, connection) }()
		}
	}()
	p := newRF3PeerProxy(t, server.Addr().String())
	dial := func(address string) net.Conn {
		t.Helper()
		connection, err := net.DialTimeout("tcp", address, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = connection.Close() })
		_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
		return connection
	}
	echo := func(connection net.Conn) {
		t.Helper()
		if _, err := connection.Write([]byte{0x17, 0x03, 0x03}); err != nil {
			t.Fatal(err)
		}
		var got [3]byte
		if _, err := io.ReadFull(connection, got[:]); err != nil || got != ([3]byte{0x17, 0x03, 0x03}) {
			t.Fatalf("opaque forwarding: %x %v", got, err)
		}
	}
	existing := dial(p.listener.Addr().String())
	echo(existing)
	p.setBlocked(true)
	for _, connection := range []net.Conn{existing, dial(p.listener.Addr().String())} {
		_, _ = connection.Write([]byte{7})
		var got [1]byte
		if n, err := connection.Read(got[:]); n != 0 || err == nil {
			t.Fatalf("blocked link forwarded n=%d err=%v", n, err)
		}
	}
	echo(dial(server.Addr().String()))
	p.setBlocked(false)
	echo(dial(p.listener.Addr().String()))
}
