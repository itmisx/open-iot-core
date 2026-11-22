package store

import (
	"bytes"
	"fmt"
	"testing"
)

func BenchmarkFilteredMetadataSnapshot200KKeys(b *testing.B) {
	kv, err := OpenSwappable(Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = kv.Close() }()
	const each = 100_000
	for start := 0; start < each; start += 1000 {
		ops := make([]Op, 0, 2000)
		for i := start; i < start+1000; i++ {
			id := fmt.Sprintf("device-%06d", i)
			ops = append(ops,
				Op{Kind: OpSet, Key: Key(PrefixThing, id), Value: []byte("metadata")},
				Op{Kind: OpSet, Key: Key(PrefixSession, id), Value: []byte("session")},
			)
		}
		if err := kv.Batch(ops, false); err != nil {
			b.Fatal(err)
		}
	}
	preserve := func(key []byte) bool { return len(key) > 0 && key[0] == byte(PrefixSession) }
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out bytes.Buffer
		n, err := kv.SnapshotFiltered(&out, preserve)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(n), "snapshot_bytes")
	}
}
