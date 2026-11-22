package broker

import (
	"testing"

	"github.com/itmisx/open-iot-core/internal/meta"
)

const certIndexBenchSize = 200_000

func benchFingerprint(i int) meta.CertFingerprint {
	var fp meta.CertFingerprint
	fp[0] = byte(i)
	fp[1] = byte(i >> 8)
	fp[2] = byte(i >> 16)
	fp[3] = byte(i >> 24)
	return fp
}

func BenchmarkCertIndex200KLegacyNestedMap(b *testing.B) {
	conns := make([]conn, certIndexBenchSize)
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		idx := make(map[meta.CertFingerprint]map[*conn]struct{}, certIndexBenchSize)
		for i := range conns {
			idx[benchFingerprint(i)] = map[*conn]struct{}{&conns[i]: {}}
		}
		if len(idx) != certIndexBenchSize {
			b.Fatal(len(idx))
		}
	}
}

func BenchmarkCertIndex200KInlineOwner(b *testing.B) {
	conns := make([]conn, certIndexBenchSize)
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		idx := make(map[meta.CertFingerprint]certOwners, certIndexBenchSize)
		for i := range conns {
			idx[benchFingerprint(i)] = certOwners{one: &conns[i]}
		}
		if len(idx) != certIndexBenchSize {
			b.Fatal(len(idx))
		}
	}
}
