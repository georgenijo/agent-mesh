//go:build windows

package e2e

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/georgenijo/agent-mesh/internal/observe"
)

func killProcess(pid int) error {
	p, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	return p.Kill()
}

func pidDead(pid int) bool {
	return !observe.PIDAlive(pid)
}

func rawMeshPIDs(t *testing.T, dir string) []int {
	t.Helper()
	script := `Get-CimInstance Win32_Process | ForEach-Object { if ($_.CommandLine -like '*--mesh-dir*') { "$($_.ProcessId)` + "`t" + `$($_.CommandLine)" } }`
	raw, err := exec.Command("powershell", "-NoProfile", "-Command", script).Output()
	if err != nil {
		t.Fatalf("powershell process scan: %v", err)
	}
	var pids []int
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if !strings.Contains(line, dir) {
			continue
		}
		pidText, _, ok := strings.Cut(line, "\t")
		if !ok {
			continue
		}
		if pid, err := strconv.Atoi(strings.TrimSpace(pidText)); err == nil {
			pids = append(pids, pid)
		}
	}
	return pids
}
