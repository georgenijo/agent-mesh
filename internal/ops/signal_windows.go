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

// signalProcess implements Down()'s terminate→kill escalation on Windows.
//
// Both signals deliberately map to TerminateProcess (proc.Kill): no graceful
// SIGTERM equivalent exists for the processes ops manages. Console control
// events (GenerateConsoleCtrlEvent CTRL_BREAK_EVENT) only reach processes
// attached to a console, and every mesh daemon is spawned console-less via
// autostart's detachedSysProcAttr (CREATE_NEW_PROCESS_GROUP |
// DETACHED_PROCESS) — AttachConsole(pid) fails for such a target, so there
// is no console to deliver the event through. Down()'s grace window and
// escalation order are preserved (the kill pass no-ops on already-dead
// pids), and the post-down stale-artifact sweep cleans the socket/pidfile
// residue a hard kill leaves behind. A genuinely graceful Windows shutdown
// would need an IPC shutdown verb on the daemons — a design change, not a
// signal mapping.
func signalProcess(pid int, _ meshSignal) error {
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
