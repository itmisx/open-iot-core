package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/config"
	"github.com/itmisx/open-iot-core/internal/shadow"
	"github.com/itmisx/open-iot-core/internal/store"
)

func TestMetadataSnapshotPreservesShadowPruneMarker(t *testing.T) {
	s := &Server{cfg: config.Config{}}
	s.cfg.Cluster.Replication.ShadowShards = 8
	for _, key := range [][]byte{
		store.Key(store.PrefixShadow, "thing", ""),
		store.Key(store.PrefixSystem, "shadow-applied-1024"),
		store.Key(store.PrefixSystem, "shadow-pruned-v1-layout"),
	} {
		if !s.preserveShardKey(key) {
			t.Fatalf("metadata restore would overwrite shard-owned key %q", key)
		}
	}
}

func TestShadowReadFallsBackToSecondReplicaAndCountsPhysicalRPCs(t *testing.T) {
	s := &Server{}
	nodes := []cluster.NodeID{"node-a", "node-b", "node-c"}
	var called []cluster.NodeID
	want := shadowRPCResponse{Found: true, Document: &shadow.Document{Version: 7}}
	raw, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.tryShadowRead(nodes, []byte(`{"op":"get"}`),
		func(node cluster.NodeID, _ []byte) ([]byte, error) {
			called = append(called, node)
			if node == "node-a" {
				return nil, errors.New("first replica unavailable")
			}
			return raw, nil
		})
	if err != nil || !got.Found || got.Document == nil || got.Document.Version != 7 {
		t.Fatalf("fallback result = %+v, %v", got, err)
	}
	if !reflect.DeepEqual(called, nodes[:2]) {
		t.Fatalf("called replicas = %v", called)
	}
	if n := s.shadowRoute.remoteReadRPCs.Load(); n != 2 {
		t.Fatalf("physical RPCs = %d", n)
	}
	if n := s.shadowRoute.remoteReadFallbacks.Load(); n != 1 {
		t.Fatalf("fallbacks = %d", n)
	}
	if n := s.shadowRoute.remoteReadErrors.Load(); n != 0 {
		t.Fatalf("logical errors = %d", n)
	}
}

func TestShadowReadAllReplicasFailCountsOneLogicalError(t *testing.T) {
	s := &Server{}
	nodes := []cluster.NodeID{"node-a", "node-b", "node-c"}
	_, err := s.tryShadowRead(nodes, nil,
		func(cluster.NodeID, []byte) ([]byte, error) { return nil, errors.New("down") })
	if err == nil {
		t.Fatal("all failed read returned success")
	}
	if n := s.shadowRoute.remoteReadRPCs.Load(); n != 3 {
		t.Fatalf("physical RPCs = %d", n)
	}
	if n := s.shadowRoute.remoteReadFallbacks.Load(); n != 2 {
		t.Fatalf("fallbacks = %d", n)
	}
	if n := s.shadowRoute.remoteReadErrors.Load(); n != 1 {
		t.Fatalf("logical errors = %d", n)
	}
}

func shadowEnvelopeBytes(t *testing.T, id string) []byte {
	t.Helper()
	raw, err := json.Marshal(shadowShardEnvelope{ID: id, Raw: json.RawMessage(`{"v":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestShadowRPCResponsePreservesJSONNumbers(t *testing.T) {
	want := json.Number("9007199254740993")
	raw, err := json.Marshal(shadowRPCResponse{Result: &shadow.Result{
		Version: 3,
		Current: &shadow.Document{Version: 3, Reported: shadow.Object{"counter": want}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeShadowRPCResponse(raw)
	if err != nil {
		t.Fatal(err)
	}
	value, ok := got.Result.Current.Reported["counter"].(json.Number)
	if !ok || value.String() != want.String() {
		t.Fatalf("number = %#v (%T), want %s", got.Result.Current.Reported["counter"], got.Result.Current.Reported["counter"], want)
	}
}

func TestMergeShadowShardProposalsFlattensLeaderBatch(t *testing.T) {
	items := make([][]byte, 5)
	for i := range items {
		items[i] = shadowEnvelopeBytes(t, fmt.Sprintf("id-%d", i))
	}
	merged, widths, consumed, err := mergeShadowShardProposals(items)
	if err != nil {
		t.Fatal(err)
	}
	if consumed != len(items) || len(widths) != len(items) {
		t.Fatalf("consumed=%d widths=%v", consumed, widths)
	}
	for _, width := range widths {
		if width != 1 {
			t.Fatalf("widths=%v", widths)
		}
	}
	envs, err := decodeShadowShardEnvelopes(merged)
	if err != nil {
		t.Fatal(err)
	}
	if len(envs) != len(items) {
		t.Fatalf("merged logical commands=%d", len(envs))
	}
	for i := range envs {
		if envs[i].ID != fmt.Sprintf("id-%d", i) {
			t.Fatalf("order changed at %d: %q", i, envs[i].ID)
		}
	}
}

func TestMergeShadowShardProposalsPreservesSingleWireCommand(t *testing.T) {
	one := shadowEnvelopeBytes(t, "id-1")
	merged, widths, consumed, err := mergeShadowShardProposals([][]byte{one})
	if err != nil {
		t.Fatal(err)
	}
	if consumed != 1 || len(widths) != 1 || widths[0] != 1 || string(merged) != string(one) {
		t.Fatalf("single proposal rewritten: consumed=%d widths=%v raw=%s", consumed, widths, merged)
	}
}

func TestDecodeShadowShardEnvelopeRejectsNestedBatch(t *testing.T) {
	nested, err := json.Marshal(shadowShardEnvelope{Batch: []shadowShardEnvelope{{
		Batch: []shadowShardEnvelope{{ID: "id", Raw: json.RawMessage(`{"v":1}`)}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeShadowShardEnvelopes(nested); err == nil {
		t.Fatal("nested batch accepted")
	}
}

func BenchmarkMergeShadowShardProposals64(b *testing.B) {
	items := make([][]byte, 64)
	for i := range items {
		raw, err := json.Marshal(shadowShardEnvelope{
			ID:  fmt.Sprintf("node-1-sh-%d", i),
			Raw: json.RawMessage(`{"v":1,"kind":1,"thing":"lamp","patch":{"desired":{"power":"on"}},"nowSec":1}`),
		})
		if err != nil {
			b.Fatal(err)
		}
		items[i] = raw
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, consumed, err := mergeShadowShardProposals(items); err != nil || consumed != 64 {
			b.Fatalf("consumed=%d err=%v", consumed, err)
		}
	}
}
