//go:build !windows

package observe

import (
	"errors"
	"syscall"
)

func addrInUse(err error) bool {
	return errors.Is(err, syscall.EADDRINUSE)
}
