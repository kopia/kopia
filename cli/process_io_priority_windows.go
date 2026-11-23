//go:build windows

package cli

import (
	"fmt"
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	processInfoClassIOPriority = 33
	ioPriorityHintVeryLow      = 0
	ioPriorityHintLow          = 1
)

func (c *App) maybeApplyProcessIOPriority() error {
	var (
		ioPriority  uint32
		cpuPriority uint32
	)

	switch c.processIOPriority {
	case "", "normal":
		return nil
	case "low":
		ioPriority = ioPriorityHintLow
		cpuPriority = windows.BELOW_NORMAL_PRIORITY_CLASS
	case "very-low":
		ioPriority = ioPriorityHintVeryLow
		cpuPriority = windows.IDLE_PRIORITY_CLASS
	default:
		return fmt.Errorf("unsupported process priority %q", c.processIOPriority)
	}

	if err := windows.SetPriorityClass(windows.CurrentProcess(), cpuPriority); err != nil {
		return fmt.Errorf("SetPriorityClass failed: %w", err)
	}

	ntdll := windows.NewLazySystemDLL("ntdll.dll")
	ntSetInformationProcess := ntdll.NewProc("NtSetInformationProcess")
	if err := ntSetInformationProcess.Find(); err != nil {
		return fmt.Errorf("NtSetInformationProcess is unavailable: %w", err)
	}

	status, _, _ := ntSetInformationProcess.Call(
		uintptr(windows.CurrentProcess()),
		uintptr(processInfoClassIOPriority),
		uintptr(unsafe.Pointer(&ioPriority)),
		unsafe.Sizeof(ioPriority),
	)
	if status != 0 {
		return fmt.Errorf("NtSetInformationProcess(ProcessIoPriority) failed with NTSTATUS 0x%08x", uint32(status))
	}

	return nil
}
