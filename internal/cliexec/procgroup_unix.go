//go:build !windows

package cliexec

import (
	"os/exec"
	"syscall"
)

// setupProcessGroup makes the child the leader of a new process group and
// installs a Cancel that kills the WHOLE group on ctx cancellation/timeout.
//
// Why this exists (#122): the claude CLI spawns its own subprocesses (node, MCP
// servers, the model client). exec.CommandContext's default cancel signals only
// the direct child, so on a timeout those grandchildren orphan and keep running.
// Over a long fleet run they pile up, exhaust the account's concurrent-session
// capacity, and new workers then hang and time out — a self-reinforcing
// collapse. Putting the child in its own group (Setpgid) and signalling the
// negative pid reaps the entire subtree, leaving nothing behind.
func setupProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		// Negative pid targets the process group led by the child. Fall back to
		// the direct child if the group is already gone (ESRCH) so cancellation
		// still terminates a child that out-raced its own setpgid.
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
			return cmd.Process.Kill()
		}
		return nil
	}
}
