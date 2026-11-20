package metrics

import (
	"math"
	"strings"
	"testing"
)

func encode(f func(*Encoder)) string {
	var sb strings.Builder
	e := NewEncoder(&sb)
	f(e)
	if err := e.Flush(); err != nil {
		panic(err)
	}
	return sb.String()
}

func TestHelpAndTypeLines(t *testing.T) {
	got := encode(func(e *Encoder) {
		e.Help("openiot_x", "一句说明", TypeCounter)
		e.CounterUint("openiot_x", 7)
	})
	want := "# HELP openiot_x 一句说明\n# TYPE openiot_x counter\nopeniot_x 7\n"
	if got != want {
		t.Errorf("得到:\n%q\n想要:\n%q", got, want)
	}
}

func TestLabels(t *testing.T) {
	got := encode(func(e *Encoder) {
		e.Gauge("m", 1, Label{"a", "1"}, Label{"b", "2"})
	})
	if got != `m{a="1",b="2"} 1`+"\n" {
		t.Errorf("标签编码错误: %q", got)
	}
}

// 标签值里的特殊字符必须转义。
//
// 不转义的后果不是显示难看，而是整个响应被解析坏：原因文案里一个
// 双引号就能让 Prometheus 丢掉这次抓取的全部样本。
func TestLabelValueEscaping(t *testing.T) {
	got := encode(func(e *Encoder) {
		e.Gauge("m", 1, Label{"reason", `他说"不行"` + "\n" + `路径 C:\tmp`})
	})
	// 直接断言转义后的形式，比数引号个数清楚
	want := `m{reason="他说\"不行\"\n路径 C:\\tmp"} 1` + "\n"
	if got != want {
		t.Errorf("转义错误:\n得到 %q\n想要 %q", got, want)
	}
	// 转义之后仍须是单独一行
	if n := strings.Count(strings.TrimRight(got, "\n"), "\n"); n != 0 {
		t.Errorf("一个样本被写成了多行: %q", got)
	}
}

// 整数不写小数点：指标端点被按秒抓取，响应体大小是实际成本。
func TestIntegersHaveNoDecimalPoint(t *testing.T) {
	got := encode(func(e *Encoder) { e.Gauge("m", 42) })
	if got != "m 42\n" {
		t.Errorf("整数应写成 42，得到 %q", got)
	}
	got = encode(func(e *Encoder) { e.Gauge("m", 1.5) })
	if got != "m 1.5\n" {
		t.Errorf("小数应保留，得到 %q", got)
	}
}

func TestSpecialFloats(t *testing.T) {
	for _, c := range []struct {
		v    float64
		want string
	}{
		{math.NaN(), "m NaN\n"},
		{math.Inf(1), "m +Inf\n"},
		{math.Inf(-1), "m -Inf\n"},
	} {
		if got := encode(func(e *Encoder) { e.Gauge("m", c.v) }); got != c.want {
			t.Errorf("%v 编码为 %q，想要 %q", c.v, got, c.want)
		}
	}
}

// 直方图的桶必须是累积的（le 语义），且 +Inf 桶等于总数。
func TestHistogramBucketsAreCumulative(t *testing.T) {
	h := NewHistogram(1, 10, 100)
	for _, v := range []float64{0.5, 5, 5, 50, 5000} {
		h.Observe(v)
	}
	got := encode(func(e *Encoder) { e.Histogram("h", h.Snapshot()) })
	for _, want := range []string{
		`h_bucket{le="1"} 1`,    // 0.5
		`h_bucket{le="10"} 3`,   // + 5,5
		`h_bucket{le="100"} 4`,  // + 50
		`h_bucket{le="+Inf"} 5`, // + 5000，超出最大桶
		"h_count 5",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("缺少 %q:\n%s", want, got)
		}
	}
}

// 超出最大桶的观测值只进 +Inf 与总数，不得被丢弃。
//
// 丢弃它会让"全部请求都很慢"显示成"没有请求"。
func TestHistogramKeepsOverflowInCount(t *testing.T) {
	h := NewHistogram(1)
	h.Observe(1000)
	s := h.Snapshot()
	if s.Count != 1 {
		t.Errorf("超出最大桶的观测未计入总数: count=%d", s.Count)
	}
	if s.Counts[0] != 0 {
		t.Errorf("超出最大桶的观测错误地计入了桶: %v", s.Counts)
	}
	if s.Sum != 1000 {
		t.Errorf("Sum = %v，想要 1000", s.Sum)
	}
}

// Sum 用整数微秒累加，长时间累加不得丢精度。
//
// 浮点累加在观测次数很大之后会把小样本直接吃掉，使 _sum 偏小，
// 而平均耗时正是由 _sum/_count 算出来的。
func TestHistogramSumPrecision(t *testing.T) {
	h := NewHistogram(LatencyBuckets()...)
	const n = 200000
	for i := 0; i < n; i++ {
		h.Observe(1.234)
	}
	want := 1.234 * n
	got := h.Snapshot().Sum
	if diff := math.Abs(got - want); diff > want*1e-9 {
		t.Errorf("Sum = %v，想要约 %v（差 %v）", got, want, diff)
	}
}

func TestHistogramIgnoresNaN(t *testing.T) {
	h := NewHistogram(1)
	h.Observe(math.NaN())
	if c := h.Snapshot().Count; c != 0 {
		t.Errorf("NaN 不应被计入: count=%d", c)
	}
}
