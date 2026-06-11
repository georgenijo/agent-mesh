//go:build windows

package ops

import (
	"os/exec"
	"strconv"
	"strings"

	"github.com/georgenijo/agent-mesh/internal/observe"
)

var runtimeAliveByPID = func(pids []int) map[int]bool {
	out := make(map[int]bool, len(pids))
	for _, pid := range pids {
		out[pid] = observe.PIDAlive(pid)
	}
	return out
}

func processCommandLine(pid int) (string, error) {
	script := "(Get-CimInstance Win32_Process -Filter 'ProcessId = " + strconv.Itoa(pid) + "').CommandLine"
	raw, err := exec.Command("powershell", "-NoProfile", "-NonInteractive", "-Command", script).Output()
	return strings.TrimSpace(string(raw)), err
}
