//go:build !windows

package e2e

import (
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

func killProcess(pid int) error {
	return syscall.Kill(pid, syscall.SIGKILL)
}

func pidDead(pid int) bool {
	return syscall.Kill(pid, 0) != nil
}

// rawMeshPIDs scans raw `ps` output for processes whose argv carries the
// `--mesh-dir <dir>` ownership marker, deliberately independent of mesh code.
func rawMeshPIDs(t *testing.T, dir string) []int {
	t.Helper()
	out, err := exec.Command("ps", "-axo", "pid=,command=").Output()
	if err != nil {
		t.Fatalf("ps: %v", err)
	}
	var pids []int
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		for i := 1; i < len(fields)-1; i++ {
			if fields[i] == "--mesh-dir" && fields[i+1] == dir {
				if pid, err := strconv.Atoi(fields[0]); err == nil {
					pids = append(pids, pid)
				}
				break
			}
		}
	}
	return pids
}
