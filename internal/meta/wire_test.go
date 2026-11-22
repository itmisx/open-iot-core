package meta

import (
	"reflect"
	"strings"
	"testing"
)

// TestCommandWireRoundTripCoversEveryField 往返一条「每个字段都非零」的命令。
//
// 用反射逐字段检查而不是手写断言：线格式是另一个节点看到的全部内容，
// Command 上加了字段而 wireCmd 忘了带，表现是那个字段在别的节点上
// 恒为零值——没有任何报错，而两个节点的视图从此不一样（CLU-064）。
func TestCommandWireRoundTripCoversEveryField(t *testing.T) {
	full := Command{
		ID:                "cmd-1",
		Kind:              CmdSetCert,
		RequestedRevision: 7,
		Cert: &CertCmd{
			Fingerprint: fp(3), Status: CertRevoked, PolicySet: 9,
			Thing: "lamp-1", CA: "ca-1", NotAfterUnixMs: 1_700_000_000_000,
			Reason: "密钥泄露",
		},
		Thing:     &ThingCmd{Name: "lamp-1", PolicySet: 9, Attrs: []string{"sh", "a"}},
		Things:    []ThingCmd{{Name: "lamp-2", PolicySet: 9, Attrs: []string{"sz"}}},
		Policy:    &PolicyCmd{ID: 9, Name: "p1", Doc: []byte(`{"Version":"2012-10-17"}`)},
		AttrSlots: []string{"site", "line"},
		Records: []RecordOp{
			{Key: []byte("tlamp-1"), Value: []byte(`{"name":"lamp-1"}`)},
			{Key: []byte("cdeadbeef"), Delete: true},
		},
		Shadow: []byte(`{"v":1,"kind":1,"thing":"lamp-1"}`),
	}

	// 先确认这条命令真的每个字段都非零——否则这个测试会在
	// 「新字段没被覆盖」时假通过。
	rv := reflect.ValueOf(full)
	for i := 0; i < rv.NumField(); i++ {
		f := rv.Type().Field(i)
		if rv.Field(i).IsZero() {
			t.Fatalf("本测试的样例命令里 %s 还是零值，补上它再断言往返", f.Name)
		}
	}

	raw, err := EncodeCommand(full)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeCommand(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(full, got) {
		t.Fatalf("往返不等：\n原 = %+v\n回 = %+v", full, got)
	}

	// 指纹要以十六进制出现在日志里：这条日志是要被人读的（备份、排查），
	// [32]byte 默认会编成 32 个数字的数组。
	if !strings.Contains(string(raw), fp(3).String()) {
		t.Errorf("日志条目里没有可读的指纹: %s", raw)
	}
}

// TestDecodeCommandRejectsUnknownVersion 读到不认识的格式版本要报错。
//
// 尽力解析的后果是：缺字段的命令被应用，这个节点的视图与别人的不一样，
// 而没有任何报错（CLU-066）。
func TestDecodeCommandRejectsUnknownVersion(t *testing.T) {
	if _, err := DecodeCommand([]byte(`{"v":99,"kind":0}`)); err == nil {
		t.Error("更高的格式版本应当被拒")
	}
	if _, err := DecodeCommand([]byte(`{"kind":0}`)); err == nil {
		t.Error("缺版本号（v=0）应当被拒")
	}
	if _, err := DecodeCommand([]byte(`{not json`)); err == nil {
		t.Error("解不开的条目应当被拒")
	}
	if _, err := DecodeCommand([]byte(`{"v":1,"kind":1,"cert":{"fp":"xx"}}`)); err == nil {
		t.Error("指纹解不开应当被拒")
	}
}

// TestColdRecordsRideTheSameCommand 冷层记录与热视图改动在同一条命令里。
//
// 这是集群下 follower 能读到记录的唯一原因：复制层只搬日志条目，
// 不认识注册表。记录不进命令，follower 上就只有热视图——鉴权能过，
// 而 GetThing、证书正文、策略正文全是空的（CLU-065）。
func TestColdRecordsRideTheSameCommand(t *testing.T) {
	cold := &fakeCold{vals: map[string][]byte{}}
	s := NewLocalStore(LocalOptions{Cold: cold, Compile: stubCompile})
	defer func() { _ = s.Close() }()

	rev, err := s.Apply(ctx(), Command{
		ID: "t1", Kind: CmdSetThing,
		Thing:   &ThingCmd{Name: "lamp-1"},
		Records: []RecordOp{{Key: []byte("tlamp-1"), Value: []byte(`{"name":"lamp-1"}`)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if rev != 1 {
		t.Fatalf("revision = %d", rev)
	}
	// 热视图与冷层都要有
	if _, ok := s.View().Thing("lamp-1"); !ok {
		t.Error("热视图里没有这台设备")
	}
	if got := cold.vals["tlamp-1"]; string(got) != `{"name":"lamp-1"}` {
		t.Errorf("冷层记录 = %q", got)
	}

	// 删除同样一起走
	if _, err := s.Apply(ctx(), Command{
		ID: "t2", Kind: CmdDeleteThing,
		Thing:   &ThingCmd{Name: "lamp-1"},
		Records: []RecordOp{{Key: []byte("tlamp-1"), Delete: true}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok := cold.vals["tlamp-1"]; ok {
		t.Error("冷层记录没被删掉")
	}
}

// TestMissingDependenciesAreRefused 漏了装配要报错，不得静默跳过。
//
// 跳过的后果各自是：策略集进了视图却没有匹配器（什么都不放行），
// 或者记录只落在热视图里而冷层永远缺一条——两者都没有任何报错。
func TestMissingDependenciesAreRefused(t *testing.T) {
	noCold := NewLocalStore(LocalOptions{Compile: stubCompile})
	defer func() { _ = noCold.Close() }()
	if _, err := noCold.Apply(ctx(), Command{
		Kind: CmdNop, Records: []RecordOp{{Key: []byte("k"), Value: []byte("v")}},
	}); err == nil {
		t.Error("没有冷层写入口时，带记录的命令应当被拒")
	}

	noCompiler := NewLocalStore(LocalOptions{Cold: &fakeCold{vals: map[string][]byte{}}})
	defer func() { _ = noCompiler.Close() }()
	if _, err := noCompiler.Apply(ctx(), Command{
		Kind: CmdSetPolicySet, Policy: &PolicyCmd{ID: 1, Name: "p", Doc: []byte("x")},
	}); err == nil {
		t.Error("没有编译器时，SetPolicySet 应当被拒")
	}
}

// TestPolicyCompiledAtApply 策略在 apply 阶段编译，编译失败要报出来。
//
// 跨节点传的是文档（编译产物序列化不了），所以每个节点各自编译。
// 失败说明两边的编译器对不上——那是真分叉，必须显式失败。
func TestPolicyCompiledAtApply(t *testing.T) {
	cold := &fakeCold{vals: map[string][]byte{}}
	s := NewLocalStore(LocalOptions{
		Cold: cold,
		Compile: func(name string, doc []byte) (any, error) {
			if string(doc) == "坏文档" {
				return nil, errBadDoc
			}
			return "编译自:" + string(doc), nil
		},
	})
	defer func() { _ = s.Close() }()

	if _, err := s.Apply(ctx(), Command{
		ID: "p1", Kind: CmdSetPolicySet,
		Policy: &PolicyCmd{ID: 1, Name: "p", Doc: []byte("好文档")},
	}); err != nil {
		t.Fatal(err)
	}
	ps, ok := s.View().PolicySet(1)
	if !ok || ps.Compiled != "编译自:好文档" {
		t.Fatalf("策略集 = %+v ok=%v", ps, ok)
	}

	if _, err := s.Apply(ctx(), Command{
		ID: "p2", Kind: CmdSetPolicySet,
		Policy: &PolicyCmd{ID: 2, Name: "bad", Doc: []byte("坏文档")},
	}); err == nil {
		t.Error("编译失败应当报出来，而不是把一个没有匹配器的策略集装进视图")
	}
	if _, ok := s.View().PolicySet(2); ok {
		t.Error("编译失败的策略集不该出现在视图里")
	}
}
