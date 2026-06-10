//go:build !windows

package ops

import (
	"errors"
	"fmt"
	"syscall"
)

type meshSignal syscall.Signal

const (
	signalTerminate meshSignal = meshSignal(syscall.SIGTERM)
	signalKill      meshSignal = meshSignal(syscall.SIGKILL)
)

func signalProcess(pid int, sig meshSignal) error {
	return syscall.Kill(pid, syscall.Signal(sig))
}

func isNoSuchProcess(err error) bool {
	return errors.Is(err, syscall.ESRCH)
}

func (s meshSignal) String() string {
	return fmt.Sprint(syscall.Signal(s))
}
