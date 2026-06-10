//go:build windows

package observe

import (
	"errors"
	"syscall"
)

func addrInUse(err error) bool {
	const wsaeAddrInUse syscall.Errno = 10048
	return errors.Is(err, syscall.EADDRINUSE) || errors.Is(err, wsaeAddrInUse)
}
