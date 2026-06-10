//go:build windows

package ops

import (
	"errors"
	"fmt"
	"os"
)

type meshSignal string

const (
	signalTerminate meshSignal = "terminate"
	signalKill      meshSignal = "kill"
)

func signalProcess(pid int, sig meshSignal) error {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	return proc.Kill()
}

func isNoSuchProcess(err error) bool {
	return errors.Is(err, os.ErrProcessDone)
}

func (s meshSignal) String() string {
	return fmt.Sprint(string(s))
}
