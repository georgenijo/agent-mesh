//go:build !windows

package ops

import (
	"os/exec"
	"strconv"
)

var runtimeAliveByPID func([]int) map[int]bool

func processCommandLine(pid int) (string, error) {
	raw, err := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "command=").Output()
	return string(raw), err
}
