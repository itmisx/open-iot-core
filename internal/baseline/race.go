//go:build race

package baseline

// Race 为真时堆被检测器放大，不能和基线字节比。
const Race = true
