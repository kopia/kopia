//go:build windows

package winfsp

// Override cgofuse's default include path to use the WinFsp SDK installation.
// Uses the 8.3 short name for "Program Files (x86)" to avoid spaces in the path.

// #cgo windows CFLAGS: -IC:/PROGRA~2/WinFsp/inc/fuse
import "C"
