//go:build windows

package cliexec

import "os/exec"

// setupProcessGroup is a no-op on Windows. The grandchild-leak fix it provides
// on Unix (#122) relies on POSIX process groups (Setpgid + kill(-pgid)), which
// have no direct Windows equivalent; exec.CommandContext's default child kill is
// retained. The production fleet runs on Linux, where the leak actually bites.
func setupProcessGroup(cmd *exec.Cmd) {}
