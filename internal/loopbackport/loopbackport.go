// Package loopbackport allocates loopback TCP ports that are persisted and
// rebound across process restarts.
//
// A kernel-assigned (":0") port comes from the ephemeral range, which the
// kernel also hands out as the source port of outbound connections. While the
// process owning a persisted ephemeral port is down, any client or peer dial
// may claim it and the restart fails with EADDRINUSE. Ports from this package
// are drawn strictly below every common default ephemeral range (Linux 32768,
// macOS/Windows 49152), so only another listener can hold them.
package loopbackport

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"strconv"
	"syscall"
)

const (
	// Low and High bound the persistent port range [Low, High).
	Low  = 20000
	High = 32768

	attempts = 512
)

// ErrExhausted reports that no free port was found in [Low, High).
var ErrExhausted = errors.New("loopbackport: no free persistent loopback port")

// Listen binds 127.0.0.1 on a random free port in [Low, High) using listen.
func Listen(listen func(network, address string) (net.Listener, error)) (net.Listener, error) {
	var last error
	for range attempts {
		port := Low + rand.IntN(High-Low)
		listener, err := listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
		if err == nil {
			return listener, nil
		}
		if !errors.Is(err, syscall.EADDRINUSE) && !errors.Is(err, syscall.EACCES) {
			return nil, err
		}
		last = err
	}
	return nil, fmt.Errorf("%w in [%d,%d): %w", ErrExhausted, Low, High, last)
}

// Reserve returns a persistent loopback address that was free when probed.
// The caller binds it later; it must tolerate losing a race to another
// listener, but never to an outbound connection's source port.
func Reserve() (string, error) {
	listener, err := Listen(net.Listen)
	if err != nil {
		return "", err
	}
	address := listener.Addr().String()
	return address, listener.Close()
}
