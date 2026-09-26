//go:build !windows

package schemainstall

import (
	"net"
	"syscall"
)

func platformControlOpenResetError() error {
	return &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
}

func platformTransientControlOpenFixtures() []controlOpenErrorFixture {
	return []controlOpenErrorFixture{
		{name: "connection refused", err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}},
		{name: "connection aborted", err: &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNABORTED}},
		{name: "host unreachable", err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.EHOSTUNREACH}},
		{name: "network unreachable", err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ENETUNREACH}},
		{name: "connect timeout", err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ETIMEDOUT}},
		{name: "broken pipe", err: &net.OpError{Op: "read", Net: "tcp", Err: syscall.EPIPE}},
	}
}
