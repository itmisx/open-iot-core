package registry

import (
	"context"
	"fmt"
	"testing"

	"github.com/itmisx/open-iot-core/internal/authz"
	"github.com/itmisx/open-iot-core/internal/meta"
	"github.com/itmisx/open-iot-core/internal/store"
)

func benchmarkThingWrites(b *testing.B, bulk bool) {
	kv, err := store.Open(store.Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	ms := meta.NewLocalStore(meta.LocalOptions{
		Cold: kv,
		ApplyRecords: func(records []meta.RecordOp) error {
			ops := make([]store.Op, len(records))
			for i := range records {
				kind := store.OpSet
				if records[i].Delete {
					kind = store.OpDelete
				}
				ops[i] = store.Op{Kind: kind, Key: records[i].Key, Value: records[i].Value}
			}
			return kv.Batch(ops, true)
		},
		Compile: testCompile(authz.DefaultEnv()),
	})
	r, err := New(Options{
		KV: kv, Meta: ms, Env: authz.DefaultEnv(), Now: func() int64 { return testNowMs },
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = ms.Close()
		_ = kv.Close()
	})

	ctx := context.Background()
	things := make([]Thing, MaxPutThingsBatch)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i := range things {
			things[i] = Thing{Name: fmt.Sprintf("bench-%d-%d", n, i)}
		}
		if bulk {
			if err := r.PutThings(ctx, things); err != nil {
				b.Fatal(err)
			}
			continue
		}
		for i := range things {
			if err := r.PutThing(ctx, things[i]); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.ReportMetric(float64(MaxPutThingsBatch), "devices/op")
	if bulk {
		b.ReportMetric(1, "raft_entries/op")
	} else {
		b.ReportMetric(float64(MaxPutThingsBatch), "raft_entries/op")
	}
}

func BenchmarkPutThings512Individual(b *testing.B) { benchmarkThingWrites(b, false) }
func BenchmarkPutThings512Bulk(b *testing.B)       { benchmarkThingWrites(b, true) }
