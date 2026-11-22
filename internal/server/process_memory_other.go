//go:build !linux

package server

// 百万连接验收跑在 Linux；其他平台没有语义一致、零依赖的当前 RSS
// 接口，因此不拿历史峰值冒充当前驻留量。
func processResidentBytes() (uint64, bool) { return 0, false }
