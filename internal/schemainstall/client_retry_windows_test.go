//go:build windows

package schemainstall

import (
	"net"
	"syscall"
)

func platformControlOpenResetError() error {
	return &net.OpError{Op: "read", Net: "tcp", Err: syscall.WSAECONNRESET}
}

func platformTransientControlOpenFixtures() []controlOpenErrorFixture {
	return []controlOpenErrorFixture{
		{name: "connection reset", err: &net.OpError{Op: "read", Net: "tcp", Err: syscall.WSAECONNRESET}},
		{name: "deleted connection name", err: &net.OpError{Op: "read", Net: "tcp", Err: syscall.ERROR_NETNAME_DELETED}},
		{name: "connection refused", err: &net.OpError{Op: "dial", Net: "tcp", Err: windowsWSAECONNREFUSED}},
		{name: "host unreachable", err: &net.OpError{Op: "dial", Net: "tcp", Err: windowsWSAEHOSTUNREACH}},
		{name: "network unreachable", err: &net.OpError{Op: "dial", Net: "tcp", Err: windowsWSAENETUNREACH}},
		{name: "network reset", err: &net.OpError{Op: "read", Net: "tcp", Err: windowsWSAENETRESET}},
		{name: "connect timeout", err: &net.OpError{Op: "dial", Net: "tcp", Err: windowsWSAETIMEDOUT}},
		{name: "connection aborted", err: &net.OpError{Op: "read", Net: "tcp", Err: syscall.WSAECONNABORTED}},
		{name: "broken pipe", err: &net.OpError{Op: "read", Net: "tcp", Err: syscall.ERROR_BROKEN_PIPE}},
	}
}
