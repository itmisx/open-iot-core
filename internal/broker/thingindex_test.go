package broker

import (
	"fmt"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/authz"
	"github.com/itmisx/open-iot-core/internal/meta"
)

func newThingIndexBroker() *Broker {
	b := &Broker{}
	b.initConnIndexes()
	return b
}

func indexedTestConn(clientID, thing string) *conn {
	return &conn{princ: livePrincipal{
		AuthzCtx: authz.Context{ClientID: clientID, Thing: thing}}}
}

func TestThingIndexTracksRegisterUnregister(t *testing.T) {
	b := newThingIndexBroker()
	a := indexedTestConn("a", "thing-1")
	b2 := indexedTestConn("b", "thing-1")
	empty := indexedTestConn("console", "")

	b.register(a)
	b.register(b2)
	b.register(empty)
	if !b.HoldsThing([]byte("thing-1")) {
		t.Fatal("登记后设备索引未命中")
	}
	if got := b.LocalThings(); !slices.Equal(got, []string{"thing-1"}) {
		t.Fatalf("设备快照 = %v，期望 [thing-1]", got)
	}

	b.unregister(a)
	if !b.HoldsThing([]byte("thing-1")) {
		t.Fatal("同设备仍有连接时不应删除设备索引")
	}
	b.unregister(b2)
	if b.HoldsThing([]byte("thing-1")) {
		t.Fatal("最后一条连接注销后设备索引仍命中")
	}
}

func TestThingIndexMovesOnClientTakeover(t *testing.T) {
	b := newThingIndexBroker()
	old := indexedTestConn("same", "old-thing")
	next := indexedTestConn("same", "new-thing")

	b.register(old)
	b.register(next)
	if b.HoldsThing([]byte("old-thing")) || !b.HoldsThing([]byte("new-thing")) {
		t.Fatalf("接管后索引错误: old=%v new=%v",
			b.HoldsThing([]byte("old-thing")), b.HoldsThing([]byte("new-thing")))
	}

	// 旧连接的清理发生在接管之后，不能删掉新连接对应的索引。
	b.unregister(old)
	if !b.HoldsThing([]byte("new-thing")) {
		t.Fatal("旧连接延迟清理误删了新连接索引")
	}
	b.unregister(next)
	if len(b.LocalThings()) != 0 {
		t.Fatalf("全部注销后仍有设备索引: %v", b.LocalThings())
	}
}

func TestThingChangesCoalesceAndVersion(t *testing.T) {
	b := newThingIndexBroker()
	a := indexedTestConn("a", "thing-1")
	b.register(a)
	b.unregister(a)
	b.register(a)

	version, changes := b.DrainThingChanges()
	if version != 3 {
		t.Fatalf("版本 = %d，期望 3", version)
	}
	if len(changes) != 1 || changes[0].Name != "thing-1" || !changes[0].Present {
		t.Fatalf("合并后的变化 = %+v，期望 thing-1 存在", changes)
	}
	if next, changes := b.DrainThingChanges(); next != version || len(changes) != 0 {
		t.Fatalf("无变化 drain = version %d changes %+v", next, changes)
	}

	snapshotVersion, things := b.ThingSnapshot()
	if snapshotVersion != version || !slices.Equal(things, []string{"thing-1"}) {
		t.Fatalf("快照 = version %d things %v", snapshotVersion, things)
	}
}

func TestThingChangesReleaseLargeDirtyMap(t *testing.T) {
	b := newThingIndexBroker()
	target := &b.things[0]
	// 直接填充一个分片，隔离测试 drain 的内存回收策略；在线 counts 与
	// dirty 的业务一致性由其他 register/unregister 测试覆盖。
	for i := 0; len(target.dirty) < thingDirtyRebuildThreshold; i++ {
		name := fmt.Sprintf("thing-%d", i)
		if indexHash(name) == 0 {
			target.dirty[name] = true
		}
	}
	old := target.dirty
	_, changes := b.DrainThingChanges()
	if len(changes) != thingDirtyRebuildThreshold {
		t.Fatalf("drain 条数 = %d，期望 %d", len(changes), thingDirtyRebuildThreshold)
	}
	// 若仍是 clear(old)，向旧引用写入会污染当前 dirty；换表后不会。
	old["stale-bucket-probe"] = true
	if _, ok := target.dirty["stale-bucket-probe"]; ok {
		t.Fatal("大批次 drain 后仍复用旧 dirty map，哈希桶无法回收")
	}
}

func TestSameThingTakeoverDoesNotAdvanceThingVersion(t *testing.T) {
	b := newThingIndexBroker()
	old := indexedTestConn("same", "thing-1")
	next := indexedTestConn("same", "thing-1")
	b.register(old)
	version, _ := b.DrainThingChanges()
	b.register(next)
	nextVersion, changes := b.DrainThingChanges()
	if nextVersion != version || len(changes) != 0 {
		t.Fatalf("同设备接管产生了虚假变化: %d→%d %+v", version, nextVersion, changes)
	}
}

func TestConnectionIndexShardsAreIndependent(t *testing.T) {
	b := newThingIndexBroker()
	a, c := "client-a", "client-b"
	for indexHash(a) == indexHash(c) {
		c += "x"
	}

	blocked := b.clientShard(a)
	blocked.mu.Lock()
	done := make(chan struct{})
	go func() {
		_ = b.lookup(c)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("锁住一个 client 分片阻塞了另一个分片")
	}
	blocked.mu.Unlock()

	thingA, thingB := "thing-a", "thing-b"
	for indexHash(thingA) == indexHash(thingB) {
		thingB += "x"
	}
	thingShard := b.thingShard(thingA)
	thingShard.mu.Lock()
	done = make(chan struct{})
	go func() {
		_ = b.HoldsThing([]byte(thingB))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("锁住一个 thing 分片阻塞了另一个分片")
	}
	thingShard.mu.Unlock()
}

func TestConcurrentRegisterUnregisterAcrossShards(t *testing.T) {
	b := newThingIndexBroker()
	const n = 1024
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			var fp meta.CertFingerprint
			fp[0] = byte(i)
			c := indexedTestConn(fmt.Sprintf("client-%04d", i), fmt.Sprintf("thing-%04d", i))
			c.princ.HasFP, c.princ.CertFP = true, fp
			b.register(c)
			b.unregister(c)
		}()
	}
	wg.Wait()
	if got := len(b.clientIndexSnapshot()); got != 0 {
		t.Fatalf("并发注销后仍有 %d 条 client 索引", got)
	}
	if _, things := b.ThingSnapshot(); len(things) != 0 {
		t.Fatalf("并发注销后仍有 thing 索引: %v", things)
	}
	for i := range b.certs {
		shard := &b.certs[i]
		shard.mu.RLock()
		left := len(shard.items)
		shard.mu.RUnlock()
		if left != 0 {
			t.Fatalf("cert 分片 %d 仍有 %d 个指纹", i, left)
		}
	}
}

func TestCertIndexInlinesUniqueOwnerAndCollapsesDuplicates(t *testing.T) {
	b := newThingIndexBroker()
	var fp meta.CertFingerprint
	fp[0] = 42
	c1 := indexedTestConn("c1", "")
	c2 := indexedTestConn("c2", "")
	c1.princ.HasFP, c1.princ.CertFP = true, fp
	c2.princ.HasFP, c2.princ.CertFP = true, fp
	b.register(c1)
	shard := b.certShard(fp)
	if got := shard.items[fp]; got.one != c1 || got.many != nil {
		t.Fatalf("唯一连接未内联: %+v", got)
	}
	b.register(c2)
	if got := shard.items[fp]; got.one != nil || len(got.many) != 2 {
		t.Fatalf("同证书双连接未升级: %+v", got)
	}
	b.unregister(c1)
	if got := shard.items[fp]; got.one != c2 || got.many != nil {
		t.Fatalf("双连接删除后未折回内联: %+v", got)
	}
	b.unregister(c2)
	if _, ok := shard.items[fp]; ok {
		t.Fatal("最后一条连接删除后仍保留证书索引")
	}
}

func TestConnectionIndexesCompactAfterLargeChurn(t *testing.T) {
	clientShard := clientIndexShard{items: map[string]*conn{"remaining": {}}}
	clientBefore := reflect.ValueOf(clientShard.items).Pointer()
	clientShard.deleted = connIndexRebuildThreshold
	clientShard.compactIfNeeded()
	if reflect.ValueOf(clientShard.items).Pointer() == clientBefore || clientShard.deleted != 0 {
		t.Fatal("client 索引大量删除后没有换表")
	}

	thingShard := thingIndexShard{counts: map[string]uint32{"remaining": 1}}
	thingBefore := reflect.ValueOf(thingShard.counts).Pointer()
	thingShard.deleted = connIndexRebuildThreshold
	thingShard.compactIfNeeded()
	if reflect.ValueOf(thingShard.counts).Pointer() == thingBefore || thingShard.deleted != 0 {
		t.Fatal("thing 索引大量删除后没有换表")
	}

	var fp meta.CertFingerprint
	certShard := certIndexShard{items: map[meta.CertFingerprint]certOwners{fp: {one: &conn{}}}}
	certBefore := reflect.ValueOf(certShard.items).Pointer()
	certShard.deleted = connIndexRebuildThreshold - 1
	certShard.removedKey()
	if reflect.ValueOf(certShard.items).Pointer() == certBefore || certShard.deleted != 0 {
		t.Fatal("cert 索引大量删除后没有换表")
	}
}
