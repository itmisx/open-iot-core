package broker

import (
	"strconv"
	"testing"

	"github.com/itmisx/open-iot-core/internal/authz"
)

type benchThingAttrs struct{}

func (benchThingAttrs) ThingAttr(_, _ string) (string, bool) { return "v", true }

type legacyThingAttrBinding struct {
	lookup func(string) (string, bool)
}

func BenchmarkThingAttrBindingLegacyClosure(b *testing.B) {
	const slots = 4096
	bindings := make([]legacyThingAttrBinding, slots)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		thing := i
		bindings[i&(slots-1)].lookup = func(string) (string, bool) {
			return strconv.Itoa(thing), true
		}
	}
}

func BenchmarkThingAttrBindingViewReference(b *testing.B) {
	const slots = 4096
	contexts := make([]authz.Context, slots)
	source := benchThingAttrs{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx := &contexts[i&(slots-1)]
		ctx.Thing = "device"
		ctx.ThingAttrs = source
	}
}
