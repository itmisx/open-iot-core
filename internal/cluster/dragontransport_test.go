package cluster

import (
	"context"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	dbconfig "github.com/lni/dragonboat/v4/config"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

// 自定义传输层的验收：两个**真的** NodeHost 跑在它上面并复制一条提案。
//
// 这条用例的意义不在覆盖率，在于它回答了换库之前唯一那个会推翻整件事的
// 问题：dragonboat 能不能不用它自带的传输。答不了的话，丢掉的是"一个端口
// 一套证书、按首字节分流、CN 认身份"这条写进四份部署文档的产品属性。
//
// 这里用明文 TCP 而不是 TLS：被测的是帧格式与收发接线，而 TLS 与
// CommonName 那一套是 tlsnode.go 里既有且已被 tls_test.go 与
// tests/cluster/run.sh 覆盖的代码，拨号函数原样复用即可（见 raftDialer）。

// kvSM 是用例用的最小状态机：记住最后一次写入的值。
type kvSM struct {
	mu   sync.Mutex
	last []byte
}

func (s *kvSM) Update(e sm.Entry) (sm.Result, error) {
	s.mu.Lock()
	s.last = append([]byte(nil), e.Cmd...)
	s.mu.Unlock()
	return sm.Result{Value: uint64(len(e.Cmd))}, nil
}

func (s *kvSM) Lookup(interface{}) (interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.last...), nil
}

func (s *kvSM) SaveSnapshot(w io.Writer, _ sm.ISnapshotFileCollection,
	_ <-chan struct{},
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := w.Write(s.last)
	return err
}

func (s *kvSM) RecoverFromSnapshot(r io.Reader, _ []sm.SnapshotFile,
	_ <-chan struct{},
) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.last = b
	s.mu.Unlock()
	return nil
}

func (s *kvSM) Close() error { return nil }

// dragonNode 是用例里的一个节点：监听、传输层与 NodeHost。
type dragonNode struct {
	addr string
	ln   net.Listener
	nh   *dragonboat.NodeHost
	tr   *dragonTransport
	smp  *kvSM
}

func startDragonNode(t *testing.T, replicaID uint64, members map[uint64]string) *dragonNode {
	t.Helper()
	addr := members[replicaID]
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("监听 %s 失败: %v", addr, err)
	}

	n := &dragonNode{addr: addr, ln: ln, smp: &kvSM{}}

	factory := &dragonTransportFactory{
		// 生产装配给的是 TLSGroup.dialRaftConn（含 CN 校验与分流标记）；
		// 这里只验帧与接线，所以是一次朴素的 TCP 拨号。
		dial: func(target string) (net.Conn, error) {
			return net.DialTimeout("tcp", target, 2*time.Second)
		},
		bound: func(tr *dragonTransport) { n.tr = tr },
	}

	nhc := dbconfig.NodeHostConfig{
		NodeHostDir:    filepath.Join(t.TempDir(), "nh"),
		RTTMillisecond: 50,
		RaftAddress:    addr,
	}
	nhc.Expert.TransportFactory = factory

	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		t.Fatalf("NewNodeHost 失败: %v", err)
	}
	n.nh = nh

	// 接受入站连接并交给传输层——生产里这一步在 TLSGroup.intake 的
	// markRaft 分支上，这里直接交，因为没有第二种协议要分流。
	go func() {
		for {
			c, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			if n.tr != nil {
				n.tr.Incoming(c)
			} else {
				_ = c.Close()
			}
		}
	}()

	cfg := dbconfig.Config{
		ReplicaID: replicaID, ShardID: 1,
		ElectionRTT: 10, HeartbeatRTT: 1, CheckQuorum: true,
		SnapshotEntries: 0,
	}
	if err := nh.StartReplica(members, false,
		func(uint64, uint64) sm.IStateMachine { return n.smp }, cfg); err != nil {
		t.Fatalf("StartReplica 失败: %v", err)
	}
	t.Cleanup(func() {
		nh.Close()
		_ = ln.Close()
	})
	return n
}

func TestDragonboatRunsOverOurTransport(t *testing.T) {
	members := map[uint64]string{
		1: freeLocalAddr(t),
		2: freeLocalAddr(t),
	}
	a := startDragonNode(t, 1, members)
	b := startDragonNode(t, 2, members)

	// 等选出 leader。选举要跑几个 RTT，等不到就是传输层没通。
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var leader *dragonboat.NodeHost
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); {
		if _, _, ok, err := a.nh.GetLeaderID(1); err == nil && ok {
			leader = a.nh
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if leader == nil {
		t.Fatal("20 秒内没有选出 leader：报文没有经自定义传输送达")
	}

	// 写一条，再从**另一个节点**读回来——这才证明它是复制过去的。
	want := []byte("over-our-transport")
	if _, err := leader.SyncPropose(ctx, leader.GetNoOPSession(1), want); err != nil {
		t.Fatalf("提案失败: %v", err)
	}

	var got []byte
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		v, err := b.nh.SyncRead(ctx, 1, nil)
		if err == nil {
			if bs, ok := v.([]byte); ok && len(bs) > 0 {
				got = bs
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if string(got) != string(want) {
		t.Fatalf("在另一个节点上读到 %q，期望 %q", got, want)
	}
}

// freeLocalAddr 取一个空闲的本地地址。
//
// 与 raftgroup_test.go 的 freeAddr 同一个理由：成员名单里必须是别人能
// 拨过来的那个地址，不能用 :0 让内核挑。
func freeLocalAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// 注：原先这里还有一条 TestDragonboatOverTheTLSMux，手工在 TLSGroup 上
// 架一个 NodeHost 来验"dragonboat 能跑在首字节分流之后"。换库完成之后
// 它被取代了——`startTLSGroup` 建的就是 dragonboat 组，于是 tls_test.go
// 里每一条用例都在验这件事，再留一条手工搭的反而会和真装配抢端口。
