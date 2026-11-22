package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/itmisx/open-iot-core/internal/shadow"
)

func encodedShadowCmd(t *testing.T, cmd shadow.Command) []byte {
	t.Helper()
	raw, err := shadow.EncodeCommand(cmd)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestApplyShadowBatchIsOrderedAndCommitsAppliedIndex(t *testing.T) {
	kv := NewMemKV()
	s := NewShadowStore(kv)
	owns := func(thing string) bool { return thing == "lamp" }
	ents := []ShadowEntry{
		{Index: 7, Data: encodedShadowCmd(t, shadow.Command{
			Kind: shadow.CmdUpdate, Thing: "lamp", NowSec: 10,
			Patch: shadow.Object{"desired": shadow.Object{"power": "on"}},
		})},
		{Index: 8, Data: encodedShadowCmd(t, shadow.Command{
			Kind: shadow.CmdUpdate, Thing: "lamp", NowSec: 11, ExpectVersion: 1,
			Patch: shadow.Object{"reported": shadow.Object{"power": "on"}},
		})},
	}
	if err := s.ApplyShadowBatch(2, ents, owns); err != nil {
		t.Fatal(err)
	}
	if ents[0].Err != nil || ents[1].Err != nil || ents[1].Result == nil {
		t.Fatalf("batch outcomes = %#v", ents)
	}
	doc, ok := s.Get("lamp", "")
	if !ok || doc.Version != 2 {
		t.Fatalf("ordered document = %#v, %v", doc, ok)
	}
	if got := s.ShadowAppliedIndex(2); got != 8 {
		t.Fatalf("applied index = %d, want 8", got)
	}
}

func TestApplyShadowBatchKeepsDomainErrorDeterministic(t *testing.T) {
	s := NewShadowStore(NewMemKV())
	ents := []ShadowEntry{{Index: 1, Data: encodedShadowCmd(t, shadow.Command{
		Kind: shadow.CmdUpdate, Thing: "lamp", NowSec: 10, ExpectVersion: 9,
		Patch: shadow.Object{"desired": shadow.Object{"power": "on"}},
	})}}
	if err := s.ApplyShadowBatch(2, ents, func(string) bool { return true }); err != nil {
		t.Fatal(err)
	}
	if ents[0].Err == nil || ents[0].Result != nil {
		t.Fatalf("domain outcome = %#v", ents[0])
	}
	if got := s.ShadowAppliedIndex(2); got != 1 {
		t.Fatalf("conflict entry was not durably consumed: %d", got)
	}
}

func TestShadowShardSnapshotRestoreIsIsolated(t *testing.T) {
	src := NewShadowStore(NewMemKV())
	for _, thing := range []string{"owned-a", "other", "owned-b"} {
		if _, err := src.Apply(context.Background(), shadow.Command{
			Kind: shadow.CmdUpdate, Thing: thing, NowSec: 10,
			Patch: shadow.Object{"desired": shadow.Object{"thing": thing}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	var idx [8]byte
	binary.BigEndian.PutUint64(idx[:], 23)
	if err := src.kv.Set(ShadowAppliedKey(2), idx[:], true); err != nil {
		t.Fatal(err)
	}
	owns := func(thing string) bool { return thing == "owned-a" || thing == "owned-b" }
	var snap bytes.Buffer
	if err := src.SnapshotShadowShard(2, owns, &snap); err != nil {
		t.Fatal(err)
	}

	dst := NewShadowStore(NewMemKV())
	for _, thing := range []string{"owned-stale", "other"} {
		if _, err := dst.Apply(context.Background(), shadow.Command{
			Kind: shadow.CmdUpdate, Thing: thing, NowSec: 1,
			Patch: shadow.Object{"desired": shadow.Object{"stale": true}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	restoreOwns := func(thing string) bool { return owns(thing) || thing == "owned-stale" }
	if err := dst.RestoreShadowShard(2, restoreOwns, bytes.NewReader(snap.Bytes())); err != nil {
		t.Fatal(err)
	}
	if _, ok := dst.Get("owned-stale", ""); ok {
		t.Fatal("stale owned record survived restore")
	}
	if _, ok := dst.Get("other", ""); !ok {
		t.Fatal("restore deleted another shard's record")
	}
	for _, thing := range []string{"owned-a", "owned-b"} {
		if _, ok := dst.Get(thing, ""); !ok {
			t.Fatalf("snapshot omitted %s", thing)
		}
	}
	if got := dst.ShadowAppliedIndex(2); got != 23 {
		t.Fatalf("restored applied index = %d", got)
	}
}

func TestRestoreShadowShardRejectsForeignKey(t *testing.T) {
	var snap bytes.Buffer
	snap.Write(make([]byte, 8))
	if err := writeKV(&snap, shadowKey("foreign", ""), []byte(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}
	s := NewShadowStore(NewMemKV())
	if err := s.RestoreShadowShard(2, func(thing string) bool { return thing == "owned" }, &snap); err == nil {
		t.Fatal("foreign snapshot key accepted")
	}
}

func TestPruneNonLocalKeepsPlacedShadowsAndRunsOnce(t *testing.T) {
	kv, err := OpenSwappable(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()
	s := NewShadowStore(kv)
	for _, thing := range []string{"local-a", "remote-a", "local-b", "remote-b"} {
		if _, err := s.Apply(context.Background(), shadow.Command{
			Kind: shadow.CmdUpdate, Thing: thing, NowSec: 1,
			Patch: shadow.Object{"reported": shadow.Object{"thing": thing}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	keep := func(thing []byte) bool { return bytes.HasPrefix(thing, []byte("local-")) }
	deleted, already, err := s.PruneNonLocal("layout-a", keep)
	if err != nil || already || deleted != 2 {
		t.Fatalf("first prune = deleted %d already %v err %v", deleted, already, err)
	}
	for _, thing := range []string{"local-a", "local-b"} {
		if _, ok := s.Get(thing, ""); !ok {
			t.Fatalf("local shadow %q was pruned", thing)
		}
	}
	for _, thing := range []string{"remote-a", "remote-b"} {
		if _, ok := s.Get(thing, ""); ok {
			t.Fatalf("non-local shadow %q survived", thing)
		}
	}
	// 同布局 marker 不再全表扫描，即使此后人工塞入了一条。
	if err := kv.Set(shadowKey("remote-late", ""), []byte(`{"v":1}`), true); err != nil {
		t.Fatal(err)
	}
	deleted, already, err = s.PruneNonLocal("layout-a", keep)
	if err != nil || !already || deleted != 0 {
		t.Fatalf("second prune = deleted %d already %v err %v", deleted, already, err)
	}
	if _, err := kv.Get(shadowKey("remote-late", "")); err != nil {
		t.Fatalf("marker did not skip repeat scan: %v", err)
	}
}

func TestPruneMarkedFailureRollsBackAndCanRerun(t *testing.T) {
	kv, err := OpenSwappable(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()
	for _, thing := range []string{"a", "b", "c"} {
		if err := kv.Set(shadowKey(thing, ""), []byte(thing), true); err != nil {
			t.Fatal(err)
		}
	}
	marker := Key(PrefixSystem, "prune-crash-test")
	stop := errors.New("simulated crash before marker")
	seen := 0
	if _, _, err := kv.PruneMarked([]byte{byte(PrefixShadow)}, marker,
		func([]byte) (bool, error) {
			seen++
			if seen == 2 {
				return false, stop
			}
			return true, nil
		}); !errors.Is(err, stop) {
		t.Fatalf("prune failure = %v", err)
	}
	for _, thing := range []string{"a", "b", "c"} {
		if _, err := kv.Get(shadowKey(thing, "")); err != nil {
			t.Fatalf("failed transaction deleted %q: %v", thing, err)
		}
	}
	if _, err := kv.Get(marker); !errors.Is(err, ErrNotFound) {
		t.Fatalf("failed transaction wrote marker: %v", err)
	}
	deleted, already, err := kv.PruneMarked([]byte{byte(PrefixShadow)}, marker,
		func([]byte) (bool, error) { return true, nil })
	if err != nil || already || deleted != 3 {
		t.Fatalf("rerun = deleted %d already %v err %v", deleted, already, err)
	}
}
