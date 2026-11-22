// 与被测对象无关的小工具。

package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
)

// ---------- 小工具 ----------

// forEach 以 r.conc 的并发度跑 n 个任务。
func (r *runner) forEach(n int, fn func(i int)) {
	r.forEachConcurrent(n, r.conc, fn)
}

func (r *runner) forEachConcurrent(n, concurrency int, fn func(i int)) {
	idx := make(chan int, concurrency)
	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range idx {
				fn(i)
			}
		}()
	}
	for i := 0; i < n; i++ {
		idx <- i
	}
	close(idx)
	wg.Wait()
}

// names 生成设备名。clientId 必须等于设备名：insecure-none 把 clientId
// 当作设备名，策略也按设备名取。
func (r *runner) names(kind string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s-%s%06d", r.prefix, kind, r.clientOffset+i)
	}
	return out
}

func splitAddrs(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func pct(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * q)
	return sorted[i]
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "mqttbench: "+format+"\n", args...)
	os.Exit(1)
}
