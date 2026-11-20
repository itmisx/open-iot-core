package session

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/itmisx/open-iot-core/internal/cluster"
)

// TestStateIsPureData 是契约 04 与 PRD ARCH-018、ARCH-053、TST-058 的断言。
//
// State 必须是纯数据：不得出现 net.Conn、chan、func、sync.*、unsafe.Pointer，
// 也不使用指针字段。这是本清单里事后代价最高的一条契约——一旦连接或锁被
// 塞进会话结构体，持久会话落盘、会话迁移、跨节点接管、控制台快照全部无从下手，
// 而会话结构体是全系统引用最密集的类型，拆分它等于重写 broker。
//
// 本测试是该规则的测试形态；CI 中另有同等的自定义 go vet 分析器。
func TestStateIsPureData(t *testing.T) {
	assertPureData(t, reflect.TypeOf(State{}), "State")
}

func assertPureData(t *testing.T, typ reflect.Type, path string) {
	t.Helper()
	switch typ.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.String:
		return
	case reflect.Slice, reflect.Array:
		assertPureData(t, typ.Elem(), path+"[]")
	case reflect.Struct:
		// 明确拦住 sync 与 net 包的类型，这类字段是本契约最想防的东西。
		if pkg := typ.PkgPath(); pkg == "sync" || pkg == "sync/atomic" || pkg == "net" {
			t.Errorf("%s 的类型为 %s.%s，违反契约 04", path, pkg, typ.Name())
			return
		}
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			assertPureData(t, f.Type, path+"."+f.Name)
		}
	default:
		t.Errorf("%s 的类型为 %s（%s），违反契约 04：State 必须是纯数据",
			path, typ.Kind(), typ.String())
	}
}

// TestStateRoundTripsThroughSerialization 验证纯数据契约的实际收益：
// State 可以被序列化与还原，这是持久会话、会话迁移、跨节点接管的前提。
func TestStateRoundTripsThroughSerialization(t *testing.T) {
	st := sampleState()

	t.Run("gob", func(t *testing.T) {
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(st); err != nil {
			t.Fatalf("gob 编码失败，说明 State 不是纯数据: %v", err)
		}
		var got State
		if err := gob.NewDecoder(&buf).Decode(&got); err != nil {
			t.Fatal(err)
		}
		// gob 会把空切片还原为 nil，故用长度与内容逐项比较
		if got.ClientID != st.ClientID || got.Epoch != st.Epoch ||
			len(got.Subscriptions) != len(st.Subscriptions) ||
			len(got.Inflight) != len(st.Inflight) ||
			len(got.Queued) != len(st.Queued) {
			t.Errorf("gob 往返不一致:\n得到 %+v\n期望 %+v", got, *st)
		}
		if !bytes.Equal(got.Inflight[0].Payload, st.Inflight[0].Payload) {
			t.Error("在途消息负载未正确往返")
		}
		if got.Will.Topic != st.Will.Topic || !got.HasWill {
			t.Error("遗嘱未正确往返")
		}
	})

	t.Run("json", func(t *testing.T) {
		b, err := json.Marshal(st)
		if err != nil {
			t.Fatalf("json 编码失败: %v", err)
		}
		var got State
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(&got, st) {
			t.Errorf("json 往返不一致:\n得到 %+v\n期望 %+v", got, *st)
		}
	})
}

func sampleState() *State {
	return &State{
		ClientID:       "dev-001",
		Epoch:          cluster.MakeEpoch(3, 7),
		Owner:          "node-1",
		ProtoVer:       5,
		CleanStart:     false,
		ExpiryInterval: 3600,
		KeepAlive:      60,
		Thing:          "dev-001",
		Subscriptions: []Subscription{
			{Filter: "$aws/things/dev-001/shadow/update/delta", QoS: 1, ID: 7},
			{Filter: "sensors/#", Share: "grp", QoS: 0, NoLocal: true, RetainHandling: 2},
		},
		Inflight: []InflightMsg{{
			PacketID: 42, QoS: 1, State: AwaitPuback,
			Topic: "a/b", Payload: []byte("hello"), Props: []byte{0x00},
			Retries: 1, SentUnixMs: 1700000000000, Origin: "node-1", Seq: 99,
		}},
		Queued: []QueuedMsg{{
			QoS: 1, Topic: "c/d", Payload: []byte("queued"),
			SubIDs: []uint32{7}, Origin: "node-1", Seq: 100,
		}},
		InboundUnacked: []uint16{1, 2, 3},
		HasWill:        true,
		Will: WillMsg{
			Topic: "dev-001/offline", Payload: []byte("bye"),
			QoS: 1, Retain: true, DelaySeconds: 30,
		},
		Props: Props{
			ReceiveMaximum: 100, MaximumPacketSize: 146 * 1024, TopicAliasMaximum: 16,
			InboundAliases:  []TopicAlias{{Alias: 1, Topic: "a/b"}},
			OutboundAliases: []TopicAlias{{Alias: 2, Topic: "c/d"}},
		},
		CreatedUnixMs: 1700000000000,
	}
}

func TestStateCloneIsDeep(t *testing.T) {
	st := sampleState()
	c := st.Clone()
	if !reflect.DeepEqual(c, st) {
		t.Fatal("克隆体应与原状态相等")
	}
	// 改动克隆体不得影响原状态
	c.Subscriptions[0].QoS = 2
	c.Inflight[0].Payload[0] = 'X'
	c.Queued[0].SubIDs[0] = 99
	c.Will.Payload[0] = 'X'
	c.Props.InboundAliases[0].Alias = 9
	c.InboundUnacked[0] = 9

	if st.Subscriptions[0].QoS != 1 {
		t.Error("订阅被共享修改")
	}
	if string(st.Inflight[0].Payload) != "hello" {
		t.Error("在途负载被共享修改")
	}
	if st.Queued[0].SubIDs[0] != 7 {
		t.Error("队列订阅标识符被共享修改")
	}
	if string(st.Will.Payload) != "bye" {
		t.Error("遗嘱负载被共享修改")
	}
	if st.Props.InboundAliases[0].Alias != 1 {
		t.Error("别名表被共享修改")
	}
	if st.InboundUnacked[0] != 1 {
		t.Error("入站未确认表被共享修改")
	}
	// nil 切片的克隆仍为 nil
	empty := (&State{}).Clone()
	if empty.Subscriptions != nil || empty.Will.Payload != nil {
		t.Error("空状态的克隆不应产生非 nil 切片")
	}
}

func TestStateCheckEpoch(t *testing.T) {
	st := &State{Epoch: cluster.MakeEpoch(1, 5)}
	if err := st.CheckEpoch(cluster.MakeEpoch(1, 5)); err != nil {
		t.Errorf("相同 epoch 应通过，得到 %v", err)
	}
	for _, e := range []cluster.Epoch{
		cluster.MakeEpoch(1, 4), // 更旧
		cluster.MakeEpoch(1, 6), // 更新
		cluster.MakeEpoch(2, 1), // 新 term
		cluster.ZeroEpoch,
	} {
		if err := st.CheckEpoch(e); err == nil {
			t.Errorf("epoch %d 应被拒绝", e)
		}
	}
}

func TestStateExpired(t *testing.T) {
	const now = 1_700_000_000_000
	// 在线会话永不过期
	online := &State{ExpiryInterval: 10}
	if online.Expired(now) {
		t.Error("在线会话不应判为过期")
	}
	// 过期间隔为 0 表示断开即删除
	zero := &State{DisconnectUnixMs: now}
	if !zero.Expired(now) {
		t.Error("过期间隔为 0 的会话断开后应立即过期")
	}
	// 未到期
	fresh := &State{ExpiryInterval: 10, DisconnectUnixMs: now - 5_000}
	if fresh.Expired(now) {
		t.Error("断开 5 秒、过期间隔 10 秒的会话不应过期")
	}
	// 刚好到期
	exact := &State{ExpiryInterval: 10, DisconnectUnixMs: now - 10_000}
	if !exact.Expired(now) {
		t.Error("断开时长等于过期间隔时应过期")
	}
}

func TestStateIsV5(t *testing.T) {
	if !(&State{ProtoVer: 5}).IsV5() || (&State{ProtoVer: 4}).IsV5() {
		t.Error("IsV5 不正确")
	}
}
