//go:build windows

package schemainstall

import (
	"errors"
	"syscall"
)

const (
	windowsWSAENETRESET    syscall.Errno = 10052
	windowsWSAETIMEDOUT    syscall.Errno = 10060
	windowsWSAECONNREFUSED syscall.Errno = 10061
	windowsWSAENETUNREACH  syscall.Errno = 10051
	windowsWSAEHOSTUNREACH syscall.Errno = 10065
)

func transientControlOpenSystemCause(err error) bool {
	return errors.Is(err, syscall.WSAECONNRESET) ||
		errors.Is(err, syscall.ERROR_NETNAME_DELETED) ||
		errors.Is(err, syscall.WSAECONNABORTED) ||
		errors.Is(err, windowsWSAENETRESET) ||
		errors.Is(err, windowsWSAETIMEDOUT) ||
		errors.Is(err, windowsWSAECONNREFUSED) ||
		errors.Is(err, windowsWSAENETUNREACH) ||
		errors.Is(err, windowsWSAEHOSTUNREACH) ||
		errors.Is(err, syscall.ERROR_BROKEN_PIPE)
}
