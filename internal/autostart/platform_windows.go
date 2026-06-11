//go:build windows

package autostart

import (
	"os"
	"syscall"
	"unsafe"
)

func meshdNames() []string {
	return []string{"meshd.exe", "meshd"}
}

func lockExclusive(f *os.File) error {
	var ol syscall.Overlapped
	const lockfileExclusiveLock = 0x00000002
	r1, _, err := syscall.SyscallN(
		syscall.NewLazyDLL("kernel32.dll").NewProc("LockFileEx").Addr(),
		uintptr(syscall.Handle(f.Fd())),
		uintptr(lockfileExclusiveLock),
		0,
		1,
		0,
		uintptr(unsafe.Pointer(&ol)),
	)
	if r1 == 0 {
		return err
	}
	return nil
}

func unlockFile(f *os.File) error {
	var ol syscall.Overlapped
	r1, _, err := syscall.SyscallN(
		syscall.NewLazyDLL("kernel32.dll").NewProc("UnlockFileEx").Addr(),
		uintptr(syscall.Handle(f.Fd())),
		0,
		1,
		0,
		uintptr(unsafe.Pointer(&ol)),
	)
	if r1 == 0 {
		return err
	}
	return nil
}

func detachedSysProcAttr() *syscall.SysProcAttr {
	const detachedProcess = 0x00000008
	return &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP | detachedProcess}
}
