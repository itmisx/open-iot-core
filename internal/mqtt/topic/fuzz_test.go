package topic

import (
	"bytes"
	"sort"
	"testing"
)

// FuzzValidateFilter 保证过滤器校验对任意输入都不 panic，
// 且通过校验的过滤器都能被订阅树接受。
func FuzzValidateFilter(f *testing.F) {
	for _, s := range []string{
		"a", "a/b", "#", "+", "a/#", "+/+", "$aws/things/+/shadow/get",
		"$share/g/a/#", "a//b", "/", "a#", "#/a", "$share", "$share/g",
	} {
		f.Add([]byte(s))
	}
	l := DefaultLimits()
	f.Fuzz(func(t *testing.T, data []byte) {
		if err := ValidateFilter(data, l); err != nil {
			return
		}
		// 通过校验的过滤器必须能进订阅树（共享订阅除外，它交给 SharedGroups）
		tr := New(l)
		err := tr.Subscribe(data, Subscription{Handle: 1})
		if err != nil && !IsShared(data) {
			t.Fatalf("过滤器 %q 通过校验却无法订阅: %v", data, err)
		}
		if err == nil && tr.Len() != 1 {
			t.Fatalf("订阅 %q 后订阅数 = %d", data, tr.Len())
		}
	})
}

// FuzzValidateName 保证主题名校验不 panic，且合法主题名不含通配符。
func FuzzValidateName(f *testing.F) {
	for _, s := range []string{"a", "a/b", "$aws/x", "", "a/+", "中文"} {
		f.Add([]byte(s))
	}
	l := DefaultLimits()
	f.Fuzz(func(t *testing.T, data []byte) {
		if err := ValidateName(data, l); err != nil {
			return
		}
		if HasWildcard(data) {
			t.Fatalf("主题名 %q 通过校验却含通配符", data)
		}
		if len(data) == 0 {
			t.Fatal("空主题名不应通过校验")
		}
		// 合法主题名必须可分类且不 panic
		_, _ = Classify(data)
	})
}

// FuzzMatch 保证匹配函数对任意输入都不 panic。
func FuzzMatch(f *testing.F) {
	f.Add([]byte("a/#"), []byte("a/b"))
	f.Add([]byte("#"), []byte("$aws/x"))
	f.Add([]byte("+/+"), []byte("/finance"))
	for _, strict := range []bool{false, true} {
		l := DefaultLimits()
		l.AWSStrict = strict
		_ = l
	}
	f.Fuzz(func(t *testing.T, filter, topic []byte) {
		for _, strict := range []bool{false, true} {
			l := DefaultLimits()
			l.AWSStrict = strict
			_ = Match(filter, topic, l)
		}
	})
}

// FuzzTrieAgreesWithPairwise 是本包最强的一条 fuzz：
// 以 fuzz 生成的过滤器集合与主题，交叉校验订阅树与逐对匹配的结果一致。
// 两套实现的任何分歧都会在这里暴露。
func FuzzTrieAgreesWithPairwise(f *testing.F) {
	f.Add([]byte("a/#\n#\n+/b\na/b"), []byte("a/b"))
	f.Add([]byte("$aws/#\n#"), []byte("$aws/x"))
	f.Add([]byte("a/+/c\na/b/c"), []byte("a/b/c"))
	f.Add([]byte("sport/#"), []byte("sport"))

	f.Fuzz(func(t *testing.T, filterBlob, topic []byte) {
		// 过滤器以换行分隔，最多取 16 条，避免单次用例过大
		parts := bytes.Split(filterBlob, []byte("\n"))
		if len(parts) > 16 {
			parts = parts[:16]
		}
		for _, strict := range []bool{false, true} {
			l := DefaultLimits()
			l.AWSStrict = strict
			tr := New(l)

			var kept [][]byte
			for _, p := range parts {
				if ValidateFilter(p, l) != nil || IsShared(p) {
					continue
				}
				// 同一过滤器重复出现时只保留一次，否则 handle 会被替换
				dup := false
				for _, k := range kept {
					if bytes.Equal(k, p) {
						dup = true
						break
					}
				}
				if dup {
					continue
				}
				if err := tr.Subscribe(p, Subscription{Handle: Handle(len(kept))}); err != nil {
					t.Fatalf("过滤器 %q 通过校验却无法订阅: %v", p, err)
				}
				kept = append(kept, p)
			}
			if len(kept) == 0 {
				continue
			}
			if tr.Len() != len(kept) {
				t.Fatalf("订阅数 = %d，期望 %d", tr.Len(), len(kept))
			}

			var want []Handle
			for i, p := range kept {
				if Match(p, topic, l) {
					want = append(want, Handle(i))
				}
			}
			got := collect(tr, string(topic))
			sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
			if len(got) != len(want) {
				t.Fatalf("strict=%v 主题 %q：树命中 %v，逐对匹配 %v（过滤器 %q）",
					strict, topic, got, want, kept)
			}
			for i := range got {
				if got[i] != want[i] {
					t.Fatalf("strict=%v 主题 %q：树命中 %v，逐对匹配 %v（过滤器 %q）",
						strict, topic, got, want, kept)
				}
			}

			// 退订全部后树必须回到只有根节点，验证节点回收无泄漏
			for i := range kept {
				tr.RemoveHandle(Handle(i))
			}
			if tr.Len() != 0 || tr.NodeCount() != 1 {
				t.Fatalf("全部退订后 Len=%d NodeCount=%d，期望 0 与 1（过滤器 %q）",
					tr.Len(), tr.NodeCount(), kept)
			}
		}
	})
}

// FuzzClassify 保证分类函数不 panic，且返回的 scope 是入参的子串。
func FuzzClassify(f *testing.F) {
	f.Add([]byte("$aws/things/dev/shadow/update"))
	f.Add([]byte("$aws/things/"))
	f.Add([]byte("a/b"))
	f.Fuzz(func(t *testing.T, data []byte) {
		c, scope := Classify(data)
		if c == ClassDeviceScoped {
			if len(scope) == 0 {
				t.Fatalf("%q 判为设备域主题却没有 scope", data)
			}
			if !bytes.Contains(data, scope) {
				t.Fatalf("%q 的 scope %q 不是其子串", data, scope)
			}
		} else if scope != nil {
			t.Fatalf("%q 判为 %s 却返回了 scope %q", data, c, scope)
		}
	})
}
