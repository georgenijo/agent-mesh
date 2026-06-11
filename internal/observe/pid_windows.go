//go:build windows

package observe

import (
	"os"
	"syscall"
	"unsafe"
)

const (
	processQueryLimitedInformation = 0x1000
	stillActive                    = 259
)

func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	handle, err := syscall.OpenProcess(processQueryLimitedInformation, false, uint32(pid))
	if err != nil {
		return false
	}
	defer syscall.CloseHandle(handle) //nolint:errcheck

	var code uint32
	r1, _, _ := syscall.SyscallN(
		syscall.NewLazyDLL("kernel32.dll").NewProc("GetExitCodeProcess").Addr(),
		uintptr(handle),
		uintptr(unsafe.Pointer(&code)),
	)
	if r1 == 0 {
		return false
	}
	return code == stillActive
}

var _ = os.Getpid
