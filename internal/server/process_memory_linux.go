//go:build linux

package server

import (
	"os"
	"strconv"
	"strings"
	"syscall"
)

// processResidentBytes 返回当前 RSS，而不是 getrusage 的历史峰值。
// /proc/self/statm 第二列以页为单位，Linux 容器内同样可用。
func processResidentBytes() (uint64, bool) {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, false
	}
	f := strings.Fields(string(b))
	if len(f) < 2 {
		return 0, false
	}
	pages, err := strconv.ParseUint(f[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return pages * uint64(syscall.Getpagesize()), true
}
