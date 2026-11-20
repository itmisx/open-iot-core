//go:build !race

package baseline

// Race 为假时按 docs/baseline/memory.json 做 10% 比较。
const Race = false
