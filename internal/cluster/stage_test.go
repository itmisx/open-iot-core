package cluster

import (
	"errors"
	"testing"
)

func TestTermFenceRejectsStaleEpoch(t *testing.T) {
	a := NewTermFence(true)
	b := NewTermFence(false)
	if err := a.SetTerm(1); err != nil {
		t.Fatal(err)
	}
	if err := b.SetTerm(1); err != nil {
		t.Fatal(err)
	}
	ea, err := a.Claim("dev", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Claim("dev", "node-b"); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("跟随者发放 = %v，期望 ErrNotLeader", err)
	}
	a.SetLeader(false)
	b.SetLeader(true)
	if err := b.SetTerm(2); err != nil {
		t.Fatal(err)
	}
	eb, err := b.Claim("dev", "node-b")
	if err != nil {
		t.Fatal(err)
	}
	if eb <= ea || eb.Term() != 2 {
		t.Fatalf("新 epoch = %d，旧 = %d", eb, ea)
	}
	if _, err := a.Claim("dev", "node-a"); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("旧 leader 仍能发放: %v", err)
	}
	if err := b.Release("dev", ea); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("旧 epoch 释放 = %v，期望 ErrStaleEpoch", err)
	}
	node, got, ok := b.Lookup("dev")
	if !ok || node != "node-b" || got != eb {
		t.Fatalf("归属 = %s %d %v", node, got, ok)
	}
}

func TestShardAndVersionStayMonotonic(t *testing.T) {
	if ShardOf("lamp", 64) != ShardOf("lamp", 64) {
		t.Fatal("同一键的分区不稳定")
	}
	if ShardOf("lamp", 0) != 0 {
		t.Fatal("分区数为 0 时应落在 0")
	}
	v, err := MonotonicVersion(2, 3)
	if err != nil || v != 3 {
		t.Fatalf("版本 = %d %v", v, err)
	}
	if _, err := MonotonicVersion(3, 3); err == nil {
		t.Fatal("版本没有前进时应当拒绝")
	}
}

func TestMeshQueueRejectsWhenFull(t *testing.T) {
	m := NewMesh(2)
	frame := func(p string) Frame { return Frame{Topic: "floor/temp", Payload: []byte(p)} }
	if err := m.Forward("b", []Frame{frame("1")}); err != nil {
		t.Fatal(err)
	}
	if err := m.Forward("b", []Frame{frame("2")}); err != nil {
		t.Fatal(err)
	}
	if err := m.Forward("b", []Frame{frame("3")}); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("= %v，期望 ErrBackpressure", err)
	}
	if m.Pending("b") != 2 {
		t.Fatalf("队列长度 = %d，满了以后不该再涨", m.Pending("b"))
	}
}
