package store

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/itmisx/open-iot-core/internal/shadow"
)

func BenchmarkShadowPrune200KKeys(b *testing.B) {
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		kv, err := OpenSwappable(Options{Dir: b.TempDir()})
		if err != nil {
			b.Fatal(err)
		}
		if err := kv.InTx(true, func(tx Tx) error {
			for i := uint64(0); i < 200_000; i++ {
				key := Key(PrefixShadow, fmt.Sprintf("thing-%06d", i), "")
				if err := tx.Set(key, []byte{1}); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}
		s := NewShadowStore(kv)
		b.StartTimer()
		deleted, already, err := s.PruneNonLocal("bench", func(thing []byte) bool {
			return thing[len(thing)-1]&1 == 0
		})
		b.StopTimer()
		_ = kv.Close()
		if err != nil || already || deleted != 100_000 {
			b.Fatalf("deleted=%d already=%v err=%v", deleted, already, err)
		}
	}
}

const shadowApplyBatchSize = 64

func benchmarkShadowApply64(b *testing.B, transactional bool) {
	kv, err := Open(Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer kv.Close()
	s := NewShadowStore(kv)
	cmds := make([]shadow.Command, shadowApplyBatchSize)
	for i := range cmds {
		cmds[i] = shadow.Command{
			Kind: shadow.CmdUpdate, Thing: fmt.Sprintf("device-%d", i),
			Patch:  shadow.Object{"reported": shadow.Object{"online": true}},
			NowSec: 1_700_000_000,
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if transactional {
			err = kv.InTx(true, func(tx Tx) error {
				for i := range cmds {
					if _, err := s.ApplyTx(tx, cmds[i]); err != nil {
						return err
					}
				}
				return nil
			})
		} else {
			for i := range cmds {
				if _, err = s.Apply(context.Background(), cmds[i]); err != nil {
					break
				}
			}
		}
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkShadowApply64Individual(b *testing.B) { benchmarkShadowApply64(b, false) }
func BenchmarkShadowApply64Transactional(b *testing.B) {
	benchmarkShadowApply64(b, true)
}

func BenchmarkShadowLaneIndex200K(b *testing.B) {
	things := make([]string, 200_000)
	for i := range things {
		things[i] = fmt.Sprintf("device-%d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s := NewShadowStore(NewMemKV())
		for _, thing := range things {
			_ = s.lane(thing)
		}
	}
}

type mapShadowLaneIndex struct {
	mu    sync.Mutex
	lanes map[string]*sync.Mutex
}

func (s *mapShadowLaneIndex) lane(thing string) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.lanes[thing]
	if m == nil {
		m = &sync.Mutex{}
		s.lanes[thing] = m
	}
	return m
}

func BenchmarkShadowLaneIndex200KMapBaseline(b *testing.B) {
	things := make([]string, 200_000)
	for i := range things {
		things[i] = fmt.Sprintf("device-%d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s := &mapShadowLaneIndex{lanes: make(map[string]*sync.Mutex)}
		for _, thing := range things {
			_ = s.lane(thing)
		}
	}
}

func BenchmarkSessionLaneIndex200K(b *testing.B) {
	things := make([]string, 200_000)
	for i := range things {
		things[i] = fmt.Sprintf("device-%d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s := NewSessionStore(NewMemKV())
		for _, thing := range things {
			_ = s.lane(thing)
		}
	}
}

func BenchmarkSessionLaneIndex200KMapBaseline(b *testing.B) {
	BenchmarkShadowLaneIndex200KMapBaseline(b)
}
