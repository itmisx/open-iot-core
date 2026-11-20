// Package metrics 实现 Prometheus 文本暴露格式的编码。
//
// 为什么自己写而不用 prometheus/client_golang：后者引入 32 个模块，
// 而本项目当前全部依赖是 12 个——会让依赖图翻近三倍，并带进 protobuf、
// oauth2、jwt、httprouter、kingpin 等一个 broker 完全不需要的东西。
// 文本暴露格式本身是稳定的公开格式（version 0.0.4），编码它不到两百行。
// 与自写 config（对比 viper 的 26 个模块）、选 bbolt（对比 Pebble 的
// 26 个）是同一个取舍。
//
// 本包只做编码，不做注册表。指标的存储放在数据本来所在的地方：
// 事件总线持有自己的精确计数器，各包有自己的 Stats()。这样就不存在
// 第二套埋点，PRD OBS-001「单一事件源，禁止为各消费者分别埋点」
// 得以成立。
package metrics

import (
	"io"
	"math"
	"strconv"
)

// Label 是一个标签键值对。
type Label struct {
	Name  string
	Value string
}

// Type 是指标类型。
type Type string

const (
	TypeCounter   Type = "counter"
	TypeGauge     Type = "gauge"
	TypeHistogram Type = "histogram"
)

// Encoder 把指标写成 Prometheus 文本格式。
//
// 错误被记住而不是逐次返回：暴露端点一旦写坏就整体无效，让每个
// Gauge 调用都返回 error 只会让采集代码被错误处理淹没。调用方在
// 最后检查一次 Err。
type Encoder struct {
	w   io.Writer
	buf []byte
	err error
}

// NewEncoder 创建编码器。
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: w, buf: make([]byte, 0, 4096)}
}

// Err 返回首个写入错误。
func (e *Encoder) Err() error { return e.err }

// Flush 把缓冲写出。
func (e *Encoder) Flush() error {
	if e.err == nil && len(e.buf) > 0 {
		_, e.err = e.w.Write(e.buf)
		e.buf = e.buf[:0]
	}
	return e.err
}

// Help 写一条指标的 HELP 与 TYPE 行。
//
// 每个指标名只能声明一次，且必须在该名字的全部样本之前——这是格式
// 要求，顺序错了 Prometheus 会拒绝整个响应。
func (e *Encoder) Help(name, help string, t Type) {
	e.lit("# HELP ")
	e.lit(name)
	e.lit(" ")
	e.text(help)
	e.lit("\n# TYPE ")
	e.lit(name)
	e.lit(" ")
	e.lit(string(t))
	e.lit("\n")
}

// Gauge 与 Counter 写一个样本。二者编码相同，分开只为调用处可读。
func (e *Encoder) Gauge(name string, v float64, labels ...Label) {
	e.sample(name, "", v, labels, nil)
}

func (e *Encoder) Counter(name string, v float64, labels ...Label) {
	e.sample(name, "", v, labels, nil)
}

// GaugeInt 是整数值的便捷形式，避免调用处到处写 float64(...)。
func (e *Encoder) GaugeInt(name string, v int64, labels ...Label) {
	e.sample(name, "", float64(v), labels, nil)
}

func (e *Encoder) CounterUint(name string, v uint64, labels ...Label) {
	e.sample(name, "", float64(v), labels, nil)
}

// Histogram 写一个直方图的全部样本：各 le 桶、_sum、_count。
//
// 桶值须是累积的（le 语义），HistogramSnapshot 已保证这一点。
func (e *Encoder) Histogram(name string, h Snapshot, labels ...Label) {
	cum := uint64(0)
	for i, b := range h.Bounds {
		cum += h.Counts[i]
		e.sample(name, "_bucket", float64(cum), labels,
			&Label{Name: "le", Value: formatBound(b)})
	}
	// +Inf 桶等于总数，格式要求必须出现。
	e.sample(name, "_bucket", float64(h.Count), labels,
		&Label{Name: "le", Value: "+Inf"})
	e.sample(name, "_sum", h.Sum, labels, nil)
	e.sample(name, "_count", float64(h.Count), labels, nil)
}

// sample 写一行样本。extra 是追加在 labels 之后的标签（直方图的 le）。
func (e *Encoder) sample(name, suffix string, v float64, labels []Label, extra *Label) {
	if e.err != nil {
		return
	}
	e.lit(name)
	e.lit(suffix)
	if len(labels) > 0 || extra != nil {
		e.lit("{")
		for i, l := range labels {
			if i > 0 {
				e.lit(",")
			}
			e.label(l)
		}
		if extra != nil {
			if len(labels) > 0 {
				e.lit(",")
			}
			e.label(*extra)
		}
		e.lit("}")
	}
	e.lit(" ")
	e.number(v)
	e.lit("\n")
	// 分块写出，避免整个响应堆在内存里。
	if len(e.buf) > 16<<10 {
		_ = e.Flush()
	}
}

func (e *Encoder) label(l Label) {
	e.lit(l.Name)
	e.lit(`="`)
	e.text(l.Value)
	e.lit(`"`)
}

// number 按格式要求写数值。
//
// 整数不带小数点：Prometheus 接受两种写法，但不带小数点的响应体更小，
// 而指标端点会被按秒级抓取。
func (e *Encoder) number(v float64) {
	switch {
	case math.IsNaN(v):
		e.lit("NaN")
	case math.IsInf(v, 1):
		e.lit("+Inf")
	case math.IsInf(v, -1):
		e.lit("-Inf")
	case v == math.Trunc(v) && math.Abs(v) < 1e15:
		e.buf = strconv.AppendInt(e.buf, int64(v), 10)
	default:
		e.buf = strconv.AppendFloat(e.buf, v, 'g', -1, 64)
	}
}

// lit 追加一段不需要转义的字面量。
func (e *Encoder) lit(s string) { e.buf = append(e.buf, s...) }

// text 追加需要转义的文本。
//
// 反斜杠、双引号、换行必须转义，否则一个带引号的原因文案就能把
// 整个响应解析坏。制表符与回车按格式规定原样允许，但换行不行。
func (e *Encoder) text(s string) {
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '\\':
			e.lit(`\\`)
		case '"':
			e.lit(`\"`)
		case '\n':
			e.lit(`\n`)
		default:
			e.buf = append(e.buf, c)
		}
	}
}

// formatBound 格式化桶上界。
func formatBound(b float64) string {
	if b == math.Trunc(b) && math.Abs(b) < 1e15 {
		return strconv.FormatInt(int64(b), 10)
	}
	return strconv.FormatFloat(b, 'g', -1, 64)
}
