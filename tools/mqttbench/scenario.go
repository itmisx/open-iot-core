// 四个压测场景：建连密度、一对一收发、扇出、影子写入。

package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/itmisx/open-iot-core/internal/mqtt/packet"
)

// ---------- 场景 ----------

// connectScenario 测的是建连速率与每连接内存。
//
// 两个数字必须同一轮测出来：建连速率高而每连接内存大，节点的实际容量仍然
// 由后者决定，分两轮跑就会各自挑一组好看的参数。
func (r *runner) connectScenario(res *result, n int, hold time.Duration, setup bool) error {
	names := r.names("c", n)
	if setup {
		if err := r.seed(names); err != nil {
			return err
		}
	}
	measureMemory := len(r.pubAddrs) == 1 && !r.reconnect
	var base memorySnapshot
	if measureMemory {
		var err error
		base, err = r.memorySnapshot()
		if err != nil {
			return err
		}
	}

	type connectWave struct {
		conns    []*client
		lat      []float64
		attempts []float64
		fails    int64
		resumed  int64
		elapsed  float64
		firstErr error
	}
	dialWave := func(wave string, addrShift int) connectWave {
		var (
			firstErr  sync.Once
			begun     atomic.Int64
			completed atomic.Int64
			succeeded atomic.Int64
			fails     atomic.Int64
			resumed   atomic.Int64
			w         = connectWave{
				conns: make([]*client, n), attempts: make([]float64, n),
			}
			startTime = time.Now()
		)
		progressDone := make(chan struct{})
		if r.connectProgress > 0 {
			go func() {
				ticker := time.NewTicker(r.connectProgress)
				defer ticker.Stop()
				for {
					select {
					case now := <-ticker.C:
						writeConnectProgress(os.Stderr, wave, n, begun.Load(), completed.Load(),
							succeeded.Load(), fails.Load(), now.Sub(startTime))
					case <-progressDone:
						return
					}
				}
			}()
		}
		r.forEach(n, func(i int) {
			begun.Add(1)
			t0 := time.Now()
			addrIndex := (i + addrShift) % len(r.pubAddrs)
			cl, derr := r.dial(r.pubAddrs[addrIndex], names[i])
			d := time.Since(t0).Seconds() * 1000
			w.attempts[i] = d
			if derr != nil {
				fails.Add(1)
				completed.Add(1)
				firstErr.Do(func() {
					w.firstErr = fmt.Errorf("clientId=%s target=%s: %w",
						names[i], r.pubAddrs[addrIndex], derr)
				})
				return
			}
			w.conns[i] = cl
			succeeded.Add(1)
			completed.Add(1)
			if cl.tlsResumed {
				resumed.Add(1)
			}
		})
		if r.connectProgress > 0 {
			close(progressDone)
			writeConnectProgress(os.Stderr, wave, n, begun.Load(), completed.Load(),
				succeeded.Load(), fails.Load(), time.Since(startTime))
		}
		w.elapsed = time.Since(startTime).Seconds()
		w.fails = fails.Load()
		w.resumed = resumed.Load()
		// 每条成功连接都去抢同一把锁追加延迟，会让负载发生器自己成为突发
		// 建连瓶颈。attempts/conns 已按唯一索引写入，结束后线性压紧一次即可。
		w.lat = make([]float64, 0, succeeded.Load())
		for i, cl := range w.conns {
			if cl != nil {
				w.lat = append(w.lat, w.attempts[i])
			}
		}
		return w
	}
	closeWave := func(w connectWave) {
		for _, cl := range w.conns {
			if cl != nil {
				cl.close()
			}
		}
	}

	if r.reconnect {
		cold := dialWave("cold", 0)
		if len(cold.lat) == 0 {
			return fmt.Errorf("冷启动没有一条连接建立成功；首个错误: %w", cold.firstErr)
		}
		closeWave(cold)
		// 给服务端处理断开，也让 TLS 1.3 NewSessionTicket 在客户端读
		// CONNACK 时进入每设备 cache。默认还会跨过每 clientId 1/s 的生产
		// 限流窗口；否则第二波量到的是限流拒绝，不是恢复握手性能。
		time.Sleep(r.reconnectDelay)
	}
	w := dialWave("measured", r.reconnectShift)
	connected := int64(len(w.lat))
	if connected == 0 {
		return fmt.Errorf("没有一条连接建立成功；首个错误: %w", w.firstErr)
	}

	if r.holdUntilUnix > 0 {
		var err error
		hold, err = r.connectHoldDuration(time.Now(), hold)
		if err != nil {
			closeWave(w)
			return err
		}
	}
	// 保持一段时间再量内存：建连刚结束时堆上还有一批尚未回收的握手临时对象，
	// 此刻读到的「每连接字节」会偏大，而偏大的方向正好让结论好看不起来——
	// 但它同样不是真的。
	time.Sleep(hold)
	held := connected
	if r.verifyHeld {
		var verifyErr error
		var verifyFailures int64
		held, verifyFailures, verifyErr = r.verifyHeldConnections(w.conns)
		w.fails += verifyFailures
		if w.firstErr == nil {
			w.firstErr = verifyErr
		}
		res.HeldVerified = true
		if held == 0 {
			closeWave(w)
			return fmt.Errorf("保持窗口结束后没有一条连接通过PING存活验证；首个错误: %w",
				verifyErr)
		}
	}
	var after memorySnapshot
	var sc float64
	if measureMemory {
		var err error
		after, err = r.memorySnapshot()
		if err != nil {
			return err
		}
		sc, _ = r.gauge("openiot_connections")
	}

	sort.Float64s(w.lat)
	res.ConnectPerSec = float64(connected) / w.elapsed
	res.ConnectP50Ms = pct(w.lat, 0.50)
	res.ConnectP99Ms = pct(w.lat, 0.99)
	res.ConnectMaxMs = w.lat[len(w.lat)-1]
	if len(w.attempts) > 0 {
		sort.Float64s(w.attempts)
		res.ConnectAttemptMaxMs = w.attempts[len(w.attempts)-1]
	}
	res.HeldConns = held
	recordConnectFailures(res, w.fails, w.firstErr)
	res.Seconds = w.elapsed
	res.TLSResumed = w.resumed
	var local runtime.MemStats
	runtime.ReadMemStats(&local)
	res.LoadGeneratorHeapAllocBytes = int64(local.HeapAlloc)
	res.LoadGeneratorRuntimeSysBytes = int64(local.Sys)
	res.LoadGeneratorRuntimePerConn = int64(local.Sys) / held
	if rss, ok := localProcessRSS(); ok {
		res.LoadGeneratorProcessRSSBytes = rss
		res.LoadGeneratorRSSBytesPerConn = rss / held
	}
	// 内存只在单节点那一轮报。
	//
	// 指标是从 -admin 指的那**一个**节点读的，而连接均摊在三个节点上：
	// 拿一个节点的堆增量去除以总连接数，得到的数既不是这个节点的也不是
	// 平均值——第一轮集群数据里它是 4466 字节，看起来像"集群下更省"，
	// 而真相只是分母大了三倍。
	// reconnect 的 base 是第一波关闭后的堆水位，其中含 TLS ticket key、
	// runtime span 与尚未归还的对象；拿第二波增量相减会得到虚假的低值。
	// 驻留内存只由单波密度轮报告。
	if measureMemory {
		res.ServerConns = int64(sc)
		res.HeapAllocBytes = after.heapAlloc
		res.RuntimeSysBytes = after.runtimeSys
		res.StackInuseBytes = after.stackInuse
		if after.rssOK && base.rssOK {
			res.ProcessRSSBytes = after.rss
		}
		// 连接数太少时，增量淹在 GC 噪声里，六次采样取最小值也挡不住——
		// 前后两个最小值可能落在堆周期的不同相位上，差值因此可以是负的
		// （实测 1,000 条时印出过 -145 字节/连接）。
		//
		// 这种情况下不报这个数，而不是报一个负数或把它夹到 0：夹到 0 会让
		// 「测不出来」长得和「几乎不占内存」一模一样，而负数会直接出现在
		// 结果表里。要这一栏就把连接数加上去。
		perConn := func(delta int64) int64 {
			if delta <= 0 {
				return 0
			}
			return delta / held
		}
		res.BytesPerConn = perConn(after.heapAlloc - base.heapAlloc)
		res.RuntimeSysPerConn = perConn(after.runtimeSys - base.runtimeSys)
		res.StackBytesPerConn = perConn(after.stackInuse - base.stackInuse)
		if after.rssOK && base.rssOK {
			res.RSSBytesPerConn = perConn(after.rss - base.rss)
		}
		if res.BytesPerConn == 0 {
			res.MemoryNote = fmt.Sprintf(
				"堆增量未超过 GC 噪声（%d 条连接），每连接内存本轮测不出；加大 -clients 再测",
				held)
		}
	}

	closeWave(w)
	return nil
}

// verifyHeldConnections 在保持窗口末尾做一次应用层往返。只检查TCP fd仍在并不
// 够：对端关闭可能尚未被本机观察到，半开连接也可能继续占着fd；PINGRESP证明
// broker读循环和共享writer在窗口末尾仍能为这条连接工作。
func (r *runner) verifyHeldConnections(conns []*client) (int64, int64, error) {
	total := 0
	for _, cl := range conns {
		if cl != nil {
			total++
		}
	}
	var (
		started   atomic.Int64
		completed atomic.Int64
		verified  atomic.Int64
		failed    atomic.Int64
		first     error
		once      sync.Once
		startTime = time.Now()
	)
	progressDone := make(chan struct{})
	if r.connectProgress > 0 {
		go func() {
			ticker := time.NewTicker(r.connectProgress)
			defer ticker.Stop()
			for {
				select {
				case now := <-ticker.C:
					writeConnectProgress(os.Stderr, "held-verify", total,
						started.Load(), completed.Load(), verified.Load(), failed.Load(),
						now.Sub(startTime))
				case <-progressDone:
					return
				}
			}
		}()
	}
	r.forEach(len(conns), func(i int) {
		cl := conns[i]
		if cl == nil {
			return // 建连失败已在dial wave里计过，不能重复算一次。
		}
		started.Add(1)
		_ = cl.c.SetDeadline(time.Now().Add(10 * time.Second))
		err := cl.send(&packet.Pingreq{})
		if err == nil {
			err = cl.flush()
		}
		if err == nil {
			var p packet.Packet
			p, err = cl.recv()
			if err == nil {
				if _, ok := p.(*packet.Pingresp); !ok {
					err = fmt.Errorf("期待PINGRESP，收到%s", p.Type())
				}
			}
		}
		_ = cl.c.SetDeadline(time.Time{})
		if err != nil {
			failed.Add(1)
			completed.Add(1)
			once.Do(func() { first = fmt.Errorf("连接索引%d保持验证失败: %w", i, err) })
			cl.close()
			conns[i] = nil
			return
		}
		verified.Add(1)
		completed.Add(1)
	})
	if r.connectProgress > 0 {
		close(progressDone)
		writeConnectProgress(os.Stderr, "held-verify", total, started.Load(),
			completed.Load(), verified.Load(), failed.Load(), time.Since(startTime))
	}
	return verified.Load(), failed.Load(), first
}

func writeConnectProgress(w io.Writer, wave string, total int, started, completed, succeeded,
	failures int64, elapsed time.Duration,
) {
	rate := 0.0
	if elapsed > 0 {
		rate = float64(completed) / elapsed.Seconds()
	}
	fmt.Fprintf(w,
		"connect-progress wave=%s total=%d started=%d completed=%d held=%d errors=%d in_flight=%d rate=%.1f/s elapsed=%s\n",
		wave, total, started, completed, succeeded, failures, started-completed, rate,
		elapsed.Round(time.Millisecond))
}

func recordConnectFailures(res *result, failures int64, firstErr error) {
	res.Errors = failures
	if firstErr != nil {
		res.FirstError = firstErr.Error()
	}
}

func (r *runner) connectHoldDuration(now time.Time, fallback time.Duration) (time.Duration, error) {
	if r.holdUntilUnix == 0 {
		return fallback, nil
	}
	until := time.Unix(r.holdUntilUnix, 0)
	if !now.Before(until) {
		return 0, fmt.Errorf("本片建连完成时已错过统一保持终点 %s",
			until.UTC().Format(time.RFC3339))
	}
	return until.Sub(now), nil
}

// localProcessRSS 返回 Linux /proc 的当前 RSS。非 Linux 或 procfs 不可用时
// 省略该字段；Linux 密度验收另外逐秒记录 VmHWM，覆盖握手期峰值。
func localProcessRSS() (int64, bool) {
	raw, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, false
	}
	fields := strings.Fields(string(raw))
	if len(fields) < 2 {
		return 0, false
	}
	pages, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil || pages < 0 {
		return 0, false
	}
	return pages * int64(os.Getpagesize()), true
}

// pairsScenario 是 N 组一对一的收发：每组一个发布者、一个订阅者，各走自己
// 的主题。它测的是「转发一条消息的成本」，与 fanout 的「复制一条消息的成本」
// 是两件事，不能用一个数字代表。
// 接线判定的常量。
//
// probeMarker 让探针消息与压测流量区分开：pump 的负载前 8 字节是时间戳，
// 任何取值都可能出现，所以不能靠内容猜。
var probeMarker = []byte("openiot-wiring-probe")

const (
	probeTimeout  = 5 * time.Second
	probeInterval = 100 * time.Millisecond
)

// probeWiring 在加压之前确认一条主题真的能从发布者走到订阅者。
//
// 它替掉的是「测量窗口结束时收到 0 条就报错」那条判定。那条判定同时承担了
// 两件互不相干的事：发现接线错误（扁平主题的精确订阅不跨节点传播，
// ARCH-017——单节点全绿、集群收 0 条），以及在**刻意过载**的那一档把一个
// 完全合法的结果报成失败。后者让整条 bench-e2e 走不完：offered 30 万时投递
// 本来就可能塌到 0，那正是这一档要记录的东西。
//
// 接线是加压之前就能确定的事实，过载不是。所以判定提前到这里用一条消息做，
// 测量窗口里的 0 就还原成它本来的含义——全丢，而不是没接通。
//
// 发布要重试：跨节点的兴趣与设备归属靠 mesh 周期通告，刚订阅完的那一瞬
// 对端可能还不知道这个订阅者。只发一条会把「还没通告到」误判成接线错误。
func (r *runner) probeWiring(pub, sub *client, topic string) error {
	deadline := time.Now().Add(probeTimeout)
	if err := sub.c.SetReadDeadline(deadline); err != nil {
		return err
	}
	defer func() { _ = sub.c.SetReadDeadline(time.Time{}) }()

	nextSend := time.Time{}
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("%s 上的探针消息在 %s 内没有到达订阅者", topic, probeTimeout)
		}
		if now := time.Now(); now.After(nextSend) {
			// 探针固定走 QoS 0：订阅可能是 QoS 1，但投递取两者的小值，
			// 于是这里不必代替 drain 回 PUBACK。
			p := &packet.Publish{Topic: []byte(topic), Payload: probeMarker, QoS: packet.QoS0}
			if err := pub.send(p); err != nil {
				return err
			}
			if err := pub.flush(); err != nil {
				return err
			}
			nextSend = now.Add(probeInterval)
		}
		if err := sub.c.SetReadDeadline(nextSend); err != nil {
			return err
		}
		got, err := sub.recv()
		if err != nil {
			if isTimeout(err) {
				continue // 这一轮没等到，再发一条。
			}
			return err
		}
		if pb, ok := got.(*packet.Publish); ok && bytes.Equal(pb.Payload, probeMarker) {
			return nil
		}
	}
}

// probeNodeRounds 是「按节点」判定时要探几个。
//
// 用于通配符订阅：它进兴趣表、按节点整体通告，所以通没通是**整个节点**的
// 事实，探满一轮地址轮转就覆盖了全部组合，再往后都是重复。
func (r *runner) probeNodeRounds(n int) int {
	rounds := len(r.pubAddrs)
	if len(r.subAddrs) > rounds {
		rounds = len(r.subAddrs)
	}
	if rounds > n {
		rounds = n
	}
	return rounds
}

func (r *runner) pairsScenario(res *result, n int, setup bool) error {
	pubNames := r.names("p", n)
	subNames := r.names("s", n)
	if setup {
		if err := r.seed(append(append([]string{}, pubNames...), subNames...)); err != nil {
			return err
		}
	}

	st := newStats()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	subsReady := make(chan struct{}, n)

	type pair struct {
		pub, sub *client
		topic    string
	}
	pairs := make([]pair, 0, n)
	for i := 0; i < n; i++ {
		// 主题落在订阅者自己的设备域里，而不是一个扁平主题。
		//
		// 这不是为了好看：精确订阅不跨节点传播（ARCH-017），跨节点的
		// 解析靠「设备域主题的归属由 thingName 算出」。用 bench/p/<i>
		// 这种扁平主题时，发布者所在的节点根本不知道别的节点上有谁订阅了
		// 它，一条也投不出去——而单节点下它是通的，于是这个错误只会在
		// 集群那一轮以「收到 0 条」的形式出现。probeWiring 现在把它提前
		// 到加压之前判定。
		topic := fmt.Sprintf("$aws/things/%s/%s", subNames[i], r.prefix)
		sub, err := r.dial(r.subAddrs[i%len(r.subAddrs)], subNames[i])
		if err != nil {
			return fmt.Errorf("订阅者 %d 建连失败: %w", i, err)
		}
		if err := sub.subscribe(topic, r.qos); err != nil {
			return fmt.Errorf("订阅者 %d 订阅失败: %w", i, err)
		}
		pub, err := r.dial(r.pubAddrs[i%len(r.pubAddrs)], pubNames[i])
		if err != nil {
			return fmt.Errorf("发布者 %d 建连失败: %w", i, err)
		}
		pairs = append(pairs, pair{pub: pub, sub: sub, topic: topic})
	}

	// 逐对探，不是只探前几对。
	//
	// 这一档用的是设备域主题，而跨节点解析靠「归属由 thingName 算出」
	// （ARCH-017）——归属是**每台设备**的事实，按设备随 mesh 通告，不是
	// 整个节点的。只探前几对时，剩下那些对的订阅者可能还没被发布者所在的
	// 节点认识，而加压一开始就投不出去：实测跨节点阶梯第一级丢了 44%，
	// 夹在两个零丢弃的级别中间，看上去像一个真实的拐点。
	for i := range pairs {
		if err := r.probeWiring(pairs[i].pub, pairs[i].sub, pairs[i].topic); err != nil {
			return fmt.Errorf("第 %d 对接线判定失败: %w", i, err)
		}
	}
	res.WiringVerified = true

	for i := range pairs {
		p := pairs[i]
		phase := float64(i) / float64(len(pairs))
		wg.Add(2)
		go func() { defer wg.Done(); p.sub.drain(st, stop); p.sub.goodbye(); p.sub.close() }()
		go func() {
			defer wg.Done()
			subsReady <- struct{}{}
			p.pub.pump(r, p.topic, phase, st, stop)
			p.pub.goodbye()
			p.pub.close()
		}()
	}
	for i := 0; i < n; i++ {
		<-subsReady
	}
	return r.measure(res, st, stop, &wg)
}

// fanoutScenario 是 1 个发布者对 N 个订阅者的同一主题。
func (r *runner) fanoutScenario(res *result, n int, setup bool) error {
	names := r.names("f", n+1)
	if setup {
		if err := r.seed(names); err != nil {
			return err
		}
	}
	// 扇出用通配符订阅加一个扁平主题：通配符过滤器会进兴趣表并跨节点
	// 通告（见 pairs 场景那段注释的另一半），于是同一个场景在集群下量到的
	// 是「发布者所在节点把一条消息转给其余两个节点，各自再本地扇出」。
	topic := r.prefix + "/fanout/t"
	filter := r.prefix + "/fanout/#"

	st := newStats()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	subs := make([]*client, 0, n)
	for i := 0; i < n; i++ {
		sub, err := r.dial(r.subAddrs[i%len(r.subAddrs)], names[i])
		if err != nil {
			return fmt.Errorf("订阅者 %d 建连失败: %w", i, err)
		}
		if err := sub.subscribe(filter, r.qos); err != nil {
			return fmt.Errorf("订阅者 %d 订阅失败: %w", i, err)
		}
		subs = append(subs, sub)
	}
	pub, err := r.dial(r.pubAddrs[0], names[n])
	if err != nil {
		return fmt.Errorf("发布者建连失败: %w", err)
	}

	// 每个节点上探一个订阅者就够：这一档要发现的是「通配符兴趣没有通告到
	// 某个节点」，而那是整节点的事实，不是单个订阅者的。探全部一千个只会
	// 让每轮多花几秒，发现不了别的东西。
	for i := 0; i < r.probeNodeRounds(len(subs)); i++ {
		if err := r.probeWiring(pub, subs[i], topic); err != nil {
			return fmt.Errorf("订阅者 %d 接线判定失败: %w", i, err)
		}
	}
	res.WiringVerified = true

	for i := range subs {
		sub := subs[i]
		wg.Add(1)
		go func() { defer wg.Done(); sub.drain(st, stop); sub.goodbye(); sub.close() }()
	}
	wg.Add(1)
	go func() { defer wg.Done(); pub.pump(r, topic, 0, st, stop); pub.goodbye(); pub.close() }()
	return r.measure(res, st, stop, &wg)
}

// shadowScenario 走影子更新主题，于是每条消息都要过一次复制日志。
//
// 判据取 accepted 的到达数而不是 PUBACK：PUBACK 只说明 broker 收下了报文，
// 而影子的那一步才是这个场景要量的东西。
func (r *runner) shadowScenario(res *result, n int, setup bool) error {
	names := r.names("h", n)
	if setup {
		if err := r.seed(names); err != nil {
			return err
		}
	}
	st := newStats()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < n; i++ {
		cl, err := r.dial(r.pubAddrs[i%len(r.pubAddrs)], names[i])
		if err != nil {
			return fmt.Errorf("客户端 %d 建连失败: %w", i, err)
		}
		acc := fmt.Sprintf("$aws/things/%s/shadow/update/accepted", names[i])
		if err := cl.subscribe(acc, packet.QoS0); err != nil {
			return fmt.Errorf("客户端 %d 订阅 accepted 失败: %w", i, err)
		}
		topic := fmt.Sprintf("$aws/things/%s/shadow/update", names[i])
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cl.shadowLoop(r, topic, st, stop)
			cl.goodbye()
			cl.close()
		}(i)
	}
	return r.measure(res, st, stop, &wg)
}

// measure 统一处理「预热 → 清零 → 测量 → 收尾」。
//
// 清零不可省：建连与首轮订阅把一批延迟很高的样本留在直方图里，不清零的
// p99 实际上是在描述建连阶段。
func (r *runner) measure(res *result, st *stats, stop chan struct{}, wg *sync.WaitGroup) error {
	time.Sleep(r.warmup)
	st.reset()
	t0 := time.Now()
	time.Sleep(r.duration)
	el := time.Since(t0).Seconds()
	pub, del, ack, errs := st.snapshot()
	p50, p99, pmax := st.percentiles()
	close(stop)
	wg.Wait()

	res.Seconds = el
	res.Published = pub
	res.Delivered = del
	res.PublishPerSec = float64(pub) / el
	res.DeliverPerSec = float64(del) / el
	if ack > 0 {
		res.AckedPerSec = float64(ack) / el
	}
	res.LatencyP50Ms = p50
	res.LatencyP99Ms = p99
	res.LatencyMaxMs = pmax
	res.Errors = errs
	// 已经做过接线判定时，投递 0 条是一个合法结果——刻意过载那一档要记录的
	// 正是它。没做过判定的场景（影子）保持原来的硬失败：那里的 0 只可能是
	// 没接通。
	if del == 0 && !res.WiringVerified {
		return errors.New("测量窗口内没有收到任何消息")
	}
	if pub == 0 {
		return errors.New("测量窗口内没有发出任何消息：发布侧没有工作，这不是过载")
	}
	return nil
}
