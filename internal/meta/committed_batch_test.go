package meta

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"
	"testing"
)

type testMetaTx struct{ vals map[string][]byte }

func (t *testMetaTx) Get(key []byte) ([]byte, error) {
	v, ok := t.vals[string(key)]
	if !ok {
		return nil, errors.New("not found")
	}
	return append([]byte(nil), v...), nil
}
func (t *testMetaTx) Set(key, value []byte) error {
	t.vals[string(key)] = append([]byte(nil), value...)
	return nil
}
func (t *testMetaTx) Delete(key []byte) error { delete(t.vals, string(key)); return nil }
func (t *testMetaTx) Scan(prefix []byte, fn func([]byte, []byte) bool) error {
	keys := make([]string, 0, len(t.vals))
	for key := range t.vals {
		if bytes.HasPrefix([]byte(key), prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		if !fn([]byte(key), t.vals[key]) {
			break
		}
	}
	return nil
}

func TestApplyCommittedBatchUsesOneTransaction(t *testing.T) {
	vals := map[string][]byte{}
	txCalls := 0
	postCalls := 0
	revKey := []byte("revision")
	s := NewLocalStore(LocalOptions{
		Cold:        &fakeCold{vals: vals},
		RevisionKey: revKey,
		InTransaction: func(fn func(Transaction) error) error {
			txCalls++
			return fn(&testMetaTx{vals: vals})
		},
		ApplyShadowTx: func(_ string, _ []byte, tx Transaction) (func(), error) {
			if err := tx.Set([]byte("shadow"), []byte("updated")); err != nil {
				return nil, err
			}
			return func() { postCalls++ }, nil
		},
	})
	defer s.Close()

	err := s.ApplyCommittedBatch([]CommittedEntry{
		{Revision: 7, Command: Command{ID: "a", Kind: CmdNop,
			Records: []RecordOp{{Key: []byte("a"), Value: []byte("1")}}}},
		{Revision: 9, Command: Command{ID: "b", Kind: CmdNop, Shadow: []byte("cmd"),
			Records: []RecordOp{{Key: []byte("b"), Value: []byte("2")}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if txCalls != 1 {
		t.Fatalf("事务次数 = %d，期望 1", txCalls)
	}
	if postCalls != 1 {
		t.Fatalf("提交后回调次数 = %d，期望 1", postCalls)
	}
	if string(vals["a"]) != "1" || string(vals["b"]) != "2" || string(vals["shadow"]) != "updated" {
		t.Fatalf("批事务内容不完整: %#v", vals)
	}
	if got := binary.BigEndian.Uint64(vals[string(revKey)]); got != 9 {
		t.Fatalf("持久化 revision = %d，期望 9", got)
	}
	if s.Revision() != 9 || s.View().Revision() != 9 {
		t.Fatalf("状态机 revision=%d view=%d", s.Revision(), s.View().Revision())
	}
}

func TestApplyCommittedBatchDoesNotPublishBeforeCommit(t *testing.T) {
	boom := errors.New("commit failed")
	postCalls := 0
	s := NewLocalStore(LocalOptions{
		Cold: &fakeCold{vals: map[string][]byte{}},
		InTransaction: func(fn func(Transaction) error) error {
			if err := fn(&testMetaTx{vals: map[string][]byte{}}); err != nil {
				return err
			}
			return boom
		},
		ApplyShadowTx: func(_ string, _ []byte, _ Transaction) (func(), error) {
			return func() { postCalls++ }, nil
		},
	})
	defer s.Close()
	err := s.ApplyCommittedBatch([]CommittedEntry{{Revision: 1,
		Command: Command{ID: "x", Kind: CmdNop, Shadow: []byte("cmd")}}})
	if !errors.Is(err, boom) {
		t.Fatalf("错误 = %v，期望 %v", err, boom)
	}
	if postCalls != 0 {
		t.Fatalf("回滚事务执行了 %d 次提交后回调", postCalls)
	}
	select {
	case ch := <-s.Changes():
		t.Fatalf("回滚事务发布了变更: %+v", ch)
	default:
	}
}
