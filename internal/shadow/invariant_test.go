package shadow

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

// assertMetaMirrorsState 断言元数据树与状态树同形。
//
// 这条不变量来自一个真实缺陷（见 merge.go 中关于 replaced 的说明）：
// 标量的元数据与对象的元数据在 Go 里都是 map[string]any，一旦复用错，
// 产出的元数据既不是叶子也不是镜像。那种文档不会让任何操作报错，
// 它只是对设备撒谎——所以必须由不变量来守，而不是靠逐个用例。
func assertMetaMirrorsState(t *testing.T, path string, state, meta any) {
	t.Helper()
	switch sv := state.(type) {
	case map[string]any:
		mv, ok := meta.(map[string]any)
		if !ok {
			t.Errorf("%s: 状态是对象但元数据是 %T", path, meta)
			return
		}
		for k, child := range sv {
			m, ok := mv[k]
			if !ok {
				t.Errorf("%s.%s: 状态有该键但元数据缺失", path, k)
				continue
			}
			assertMetaMirrorsState(t, path+"."+k, child, m)
		}
		for k := range mv {
			if _, ok := sv[k]; !ok {
				t.Errorf("%s.%s: 元数据有该键但状态缺失（删除时未同步清理）", path, k)
			}
		}
	case []any:
		mv, ok := meta.([]any)
		if !ok {
			t.Errorf("%s: 状态是数组但元数据是 %T", path, meta)
			return
		}
		if len(sv) != len(mv) {
			t.Errorf("%s: 数组长度 %d 但元数据长度 %d", path, len(sv), len(mv))
			return
		}
		for i := range sv {
			assertMetaMirrorsState(t, fmt.Sprintf("%s[%d]", path, i), sv[i], mv[i])
		}
	default:
		mv, ok := meta.(map[string]any)
		if !ok {
			t.Errorf("%s: 标量的元数据应为对象，实际 %T", path, meta)
			return
		}
		if len(mv) != 1 {
			t.Errorf("%s: 标量的元数据应只含 timestamp，实际 %v", path, mv)
			return
		}
		ts, ok := mv[metaTimestampKey]
		if !ok {
			t.Errorf("%s: 标量的元数据缺少 timestamp：%v", path, mv)
			return
		}
		switch ts.(type) {
		case int64, json.Number, float64:
		default:
			t.Errorf("%s: timestamp 应为数字，实际 %T", path, ts)
		}
	}
}

// checkDoc 对一份文档的两个段都做不变量检查。
func checkDoc(t *testing.T, d *Document) {
	t.Helper()
	if d == nil {
		return
	}
	if d.Desired != nil || d.DesiredMeta != nil {
		assertMetaMirrorsState(t, "desired", anyOrEmpty(d.Desired), anyOrEmpty(d.DesiredMeta))
	}
	if d.Reported != nil || d.ReportedMeta != nil {
		assertMetaMirrorsState(t, "reported", anyOrEmpty(d.Reported), anyOrEmpty(d.ReportedMeta))
	}
}

func anyOrEmpty(o Object) any {
	if o == nil {
		return map[string]any{}
	}
	return map[string]any(o)
}

// TestMetaMirrorsStateAcrossRandomSequences 用随机操作序列压不变量。
//
// 固定种子使失败可复现；不用 Date/随机时钟，时间戳由序号推进。
func TestMetaMirrorsStateAcrossRandomSequences(t *testing.T) {
	patches := []string{
		`{"a":1}`,
		`{"a":null}`,
		`{"a":{"b":1}}`,
		`{"a":{"b":null}}`,
		`{"a":{"b":{"c":1}}}`,
		`{"a":[1,2,3]}`,
		`{"a":[{"x":1}]}`,
		`{"a":"s"}`,
		`{"a":true}`,
		`{"b":{"c":1},"a":2}`,
		`{"b":null,"a":null}`,
		`{"a":{}}`,
		`{"timestamp":1}`,
		`{"a":{"timestamp":1}}`,
	}
	rng := rand.New(rand.NewSource(20261026))
	for seq := 0; seq < 400; seq++ {
		var doc *Document
		var applied []string
		n := 2 + rng.Intn(6)
		for step := 0; step < n; step++ {
			pj := patches[rng.Intn(len(patches))]
			section := "reported"
			if rng.Intn(2) == 0 {
				section = "desired"
			}
			applied = append(applied, section+"="+pj)

			patch, err := Decode([]byte(`{"` + section + `":` + pj + `}`))
			if err != nil {
				t.Fatalf("测试补丁不合法: %v", err)
			}
			next, _, aerr := ApplyCommand(doc, Command{
				Kind: CmdUpdate, Thing: "d", Patch: patch,
				NowSec: int64(1_700_000_000 + seq*100 + step),
			})
			if aerr != nil {
				t.Fatalf("序列 %v 失败: %v", applied, aerr)
			}
			doc = next
		}
		t.Run(fmt.Sprint(seq), func(t *testing.T) {
			if t.Failed() {
				t.Logf("序列: %s", strings.Join(applied, " -> "))
			}
			checkDoc(t, doc)
			if t.Failed() {
				b, _ := json.Marshal(doc)
				t.Logf("序列: %s\n文档: %s", strings.Join(applied, " -> "), b)
			}
		})
	}
}

// FuzzMergeKeepsInvariants 用任意 JSON 压合并逻辑。
//
// 两条断言：不得 panic，且结果须满足元数据镜像不变量。
// 合并逻辑是递归的，而输入是用户完全可控的任意 JSON——
// 这正是 fuzz 的用武之地。
func FuzzMergeKeepsInvariants(f *testing.F) {
	f.Add([]byte(`{"state":{"reported":{"a":1}}}`), []byte(`{"state":{"reported":{"a":null}}}`))
	f.Add([]byte(`{"state":{"desired":{"a":{"b":1}}}}`), []byte(`{"state":{"desired":{"a":2}}}`))
	f.Add([]byte(`{"state":{"reported":{"a":[1,2]}}}`), []byte(`{"state":{"reported":{"a":{"x":1}}}}`))
	f.Add([]byte(`{"state":{"reported":{"timestamp":1}}}`),
		[]byte(`{"state":{"reported":{"timestamp":{"timestamp":2}}}}`))

	f.Fuzz(func(t *testing.T, a, b []byte) {
		var doc *Document
		for i, raw := range [][]byte{a, b} {
			obj, err := Decode(raw)
			if err != nil {
				continue
			}
			state, serr := extractState(obj)
			if serr != nil {
				continue
			}
			if Depth(state) > MaxDepth {
				continue
			}
			next, _, aerr := ApplyCommand(doc, Command{
				Kind: CmdUpdate, Thing: "d", Patch: state,
				NowSec: int64(1_700_000_000 + i),
			})
			if aerr != nil {
				t.Fatalf("合法输入不应失败: %v", aerr)
			}
			doc = next
		}
		checkDoc(t, doc)
	})
}

// FuzzDecodeNeverPanics 压 JSON 解析与深度计算。
func FuzzDecodeNeverPanics(f *testing.F) {
	f.Add([]byte(`{"a":1}`))
	f.Add([]byte(`{"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h":{"i":1}}}}}}}}}`))
	f.Fuzz(func(t *testing.T, b []byte) {
		obj, err := Decode(b)
		if err != nil {
			return
		}
		_ = Depth(obj)
		_ = Clone(obj)
		_ = Equal(obj, obj)
		if _, err := Encode(obj); err != nil {
			// 能解出来的就应能编回去
			t.Fatalf("可解析的 JSON 不应无法编码: %v", err)
		}
	})
}

// FuzzParseRequestNeverPanics 压主题解析。
func FuzzParseRequestNeverPanics(f *testing.F) {
	f.Add("$aws/things/d1/shadow/update")
	f.Add("$aws/things/d1/shadow/name/cfg/get")
	f.Add("$aws/things//shadow/name//delete")
	f.Fuzz(func(t *testing.T, topic string) {
		r, ok := ParseRequest(topic)
		if !ok {
			return
		}
		// 解析成功则各响应主题都应可构造，且仍以请求前缀开头
		base := r.Base()
		for _, s := range []string{
			r.AcceptedTopic(), r.RejectedTopic(), r.DeltaTopic(), r.DocumentsTopic(),
		} {
			if !strings.HasPrefix(s, base) {
				t.Fatalf("响应主题 %q 不以 %q 开头", s, base)
			}
		}
	})
}
