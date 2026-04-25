//go:build windows && winfsp && cgo

package winfsp

// Default include path for the WinFsp SDK on a stock Windows install.
// cgofuse's upstream default is the Linux/macOS path -I/usr/local/include/winfsp,
// so without an override here a stock `-tags winfsp` build can't find <fuse.h>.
// The 8.3 short name (`PROGRA~2`) avoids the cgo-incompatible space in
// "Program Files (x86)".
//
// If WinFsp is installed elsewhere, override at build time via CGO_CFLAGS:
//   set CGO_CFLAGS=-IC:/path/to/WinFsp/inc/fuse
//   go build -tags winfsp ./...
// Anything in CGO_CFLAGS is appended to the directive below by the cgo tool.

// #cgo windows CFLAGS: -IC:/PROGRA~2/WinFsp/inc/fuse
import "C"
