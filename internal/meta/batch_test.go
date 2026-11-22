package meta

import (
	"context"
	"errors"
	"testing"
)

func TestSetThingsUsesOneColdBatch(t *testing.T) {
	cold := &fakeCold{vals: map[string][]byte{}}
	calls := 0
	s := NewLocalStore(LocalOptions{
		Cold: cold,
		ApplyRecords: func(records []RecordOp) error {
			calls++
			if len(records) != 2 {
				t.Fatalf("批记录数 = %d", len(records))
			}
			for _, op := range records {
				cold.vals[string(op.Key)] = append([]byte(nil), op.Value...)
			}
			return nil
		},
	})
	defer s.Close()

	rev, err := s.Apply(context.Background(), Command{
		Kind:   CmdSetThings,
		Things: []ThingCmd{{Name: "a"}, {Name: "b"}},
		Records: []RecordOp{
			{Key: []byte("ta"), Value: []byte("a")},
			{Key: []byte("tb"), Value: []byte("b")},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if rev != 1 || calls != 1 {
		t.Fatalf("revision=%d batch calls=%d", rev, calls)
	}
	for _, name := range []string{"a", "b"} {
		if _, ok := s.View().Thing(name); !ok {
			t.Errorf("热视图缺少 %s", name)
		}
	}
}

func TestSetThingsValidation(t *testing.T) {
	tests := []Command{
		{Kind: CmdSetThings},
		{Kind: CmdSetThings, Things: []ThingCmd{{Name: ""}}},
		{Kind: CmdSetThings, Things: []ThingCmd{{Name: "a"}, {Name: "a"}}},
		{Kind: CmdSetThings, Things: make([]ThingCmd, MaxThingsPerCommand+1)},
		{Kind: CmdSetThings, Things: []ThingCmd{{Name: "a"}}, Records: []RecordOp{{
			Key: []byte("ta"), Value: make([]byte, MaxSetThingsCommandBytes),
		}}},
	}
	for i := range tests {
		if err := tests[i].Validate(); err == nil {
			t.Errorf("第 %d 个非法批次未被拒绝", i)
		}
	}

	want := errors.New("write failed")
	s := NewLocalStore(LocalOptions{
		Cold:         &fakeCold{vals: map[string][]byte{}},
		ApplyRecords: func([]RecordOp) error { return want },
	})
	defer s.Close()
	_, err := s.Apply(context.Background(), Command{
		Kind: CmdSetThings, Things: []ThingCmd{{Name: "a"}},
		Records: []RecordOp{{Key: []byte("ta"), Value: []byte("a")}},
	})
	if !errors.Is(err, want) {
		t.Fatalf("批写错误未保留: %v", err)
	}
}
