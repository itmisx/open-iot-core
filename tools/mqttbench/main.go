// Command mqttbench 是本项目自带的 MQTT 压测器（PRD TST-061）。
//
// 它存在的理由是：外部压测工具（emqtt-bench、mqtt-stresser）跑不了本项目
// 关心的那几个场景——跨节点的收发、经复制日志的影子写入、以及「连接密度
// 与每连接内存」这种要同时读服务端指标的测量。而这些数字一旦写进 README，
// 就必须有人能在自己机器上复现（TST-060、TST-064）。
//
// 它只依赖标准库与 internal/mqtt/packet：压测器自己不该引入第三方 MQTT
// 客户端，否则「数字为什么是这样」会多出一层无法排查的中间物。用本仓库的
// 编解码器是刻意的取舍——协议正确性由 tests/device-sdk 的真实 AWS SDK 实跑
// 保证（第 6 节第 9 条），这里要的只是把字节发对。
//
// 用法见 tests/bench/run.sh；单独跑：
//
//	mqttbench -scenario pairs -broker 127.0.0.1:1883 \
//	  -admin http://127.0.0.1:9090 -token "$(cat data/admin-token)" \
//	  -pairs 200 -qos 1 -duration 20s
package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/itmisx/open-iot-core/internal/mqtt/packet"
)

func main() {
	var (
		scenario      = flag.String("scenario", "pairs", "connect | pairs | fanout | shadow")
		brokers       = flag.String("broker", "127.0.0.1:1883", "发布侧 MQTT 明文地址，逗号分隔则轮转")
		subBroker     = flag.String("sub-broker", "", "订阅侧地址（留空表示与 -broker 相同）")
		admin         = flag.String("admin", "http://127.0.0.1:9090", "管理面地址，用于预置设备与读取指标")
		adminSourceIP = flag.String("admin-source-ip", "",
			"管理HTTP的专用本地源IP；大连接轮用于隔离设备临时端口池")
		token        = flag.String("token", "", "管理面令牌")
		clients      = flag.Int("clients", 1000, "connect/shadow 场景的客户端数")
		clientOffset = flag.Int("client-offset", 0,
			"设备编号起点；多个 load generator 并行时用不重叠区间")
		pairs     = flag.Int("pairs", 200, "pairs 场景的收发对数")
		subs      = flag.Int("subs", 1000, "fanout 场景的订阅者数")
		conc      = flag.Int("concurrency", 64, "建连并发度")
		setupConc = flag.Int("setup-concurrency", 32,
			"兼容参数；设备预置已改为有界批接口并按批串行提交")
		qos      = flag.Int("qos", 0, "发布 QoS，0 或 1")
		payload  = flag.Int("payload", 64, "负载字节数，不足 8 字节时按 8 计（前 8 字节是时间戳）")
		inflight = flag.Int("inflight", 16, "QoS 1 每个发布者的在途上限")
		rate     = flag.Int("rate", 0, "每个发布者的目标速率（条/秒），0 表示不限速")
		duration = flag.Duration("duration", 15*time.Second, "测量窗口")
		warmup   = flag.Duration("warmup", 3*time.Second, "预热窗口，不计入结果")
		hold     = flag.Duration("hold", 10*time.Second, "connect 场景建连后的保持时长")
		progress = flag.Duration("connect-progress", 5*time.Second,
			"connect 场景向 stderr 输出过程计数的间隔；0 关闭")
		holdUntil = flag.Int64("hold-until-unix", 0,
			"connect 建成后保持到该 Unix 秒；多压测机同步验收用，0 表示只用 -hold")
		verifyHeld = flag.Bool("verify-held", false,
			"connect 保持期结束后逐连接完成 PINGREQ/PINGRESP；最终密度验收必须启用")
		prefix    = flag.String("prefix", "bench", "设备名与主题前缀")
		setup     = flag.Bool("setup", true, "是否经管理面预置设备与策略")
		out       = flag.String("out", "", "把结果 JSON 追加写入该文件")
		label     = flag.String("label", "", "结果里的场景标签，默认取 -scenario")
		tlsCA     = flag.String("tls-ca", "", "启用 TLS 并信任该 PEM CA")
		tlsCert   = flag.String("tls-cert", "", "mTLS 客户端证书链 PEM")
		tlsKey    = flag.String("tls-key", "", "mTLS 客户端私钥 PEM")
		tlsName   = flag.String("tls-server-name", "", "TLS 服务端名称（留空从 broker 地址取）")
		proxyCert = flag.String("proxy-v2-cert", "",
			"以 TLS 终结器身份发送 PROXY v2，并用该 PEM 叶证书恢复设备身份；只衡量 broker 外移档")
		proxyFingerprintOnly = flag.Bool("proxy-v2-fingerprint-only", false,
			"只发送证书 SHA-256 紧凑 TLV；要求同时给出 -proxy-v2-cert")
		sourceIPs = flag.String("source-ip", "", "本地源 IP，逗号分隔并按 clientId 稳定轮转；用于突破单 IP 临时端口上限")
		reconnect = flag.Bool("reconnect", false,
			"先冷建连并关闭，再用相同 clientId 测第二波；TLS ticket cache 按设备隔离")
		reconnectDelay = flag.Duration("reconnect-delay", 1250*time.Millisecond,
			"两波之间等待；默认跨过服务端每 clientId 1/s 的安全限流窗口")
		reconnectShift = flag.Int("reconnect-shift", 0,
			"第二波 broker 地址轮转位移；用于验证跨节点 TLS session 恢复")
		// 持久会话那一档必须能单独压：两条路径在服务端的代价差一个数量级
		// ——非持久会话一次逻辑写都不发，而持久会话每次建连发一次
		// （CLU-129、CLU-130）。只压 clean 的话"集群建连很慢"会被整个
		// 归到共识上，而那部分流量根本没进共识。
		persistent = flag.Bool("persistent-session", false,
			"connect 场景用持久会话（MQTT 5，clean=false 且 session_expiry>0）")
		expiry = flag.Uint("session-expiry", 3600,
			"持久会话的过期间隔（秒），仅 -persistent-session 有效")
	)
	flag.Parse()
	_ = setupConc // 保留旧脚本兼容；单个元数据组不从并发批提交中获益。

	r := &runner{
		pubAddrs:        splitAddrs(*brokers),
		subAddrs:        splitAddrs(*subBroker),
		admin:           strings.TrimRight(*admin, "/"),
		token:           *token,
		qos:             packet.QoS(*qos),
		payload:         max(*payload, 8),
		inflight:        max(*inflight, 1),
		rate:            *rate,
		duration:        *duration,
		warmup:          *warmup,
		conc:            max(*conc, 1),
		prefix:          *prefix,
		clientOffset:    *clientOffset,
		holdUntilUnix:   *holdUntil,
		verifyHeld:      *verifyHeld,
		connectProgress: *progress,
		reconnect:       *reconnect,
		reconnectDelay:  *reconnectDelay,
		reconnectShift:  *reconnectShift,
	}
	var adminIP net.IP
	if *adminSourceIP != "" {
		adminIP = net.ParseIP(*adminSourceIP)
		if adminIP == nil {
			die("无效的 -admin-source-ip %q（只接受 IP 字面量）", *adminSourceIP)
		}
	}
	r.adminHTTP = newAdminHTTPClient(adminIP)
	if *sourceIPs != "" {
		for _, raw := range splitAddrs(*sourceIPs) {
			ip := net.ParseIP(raw)
			if ip == nil {
				die("无效的 -source-ip %q（只接受 IP 字面量）", raw)
			}
			r.sourceIPs = append(r.sourceIPs, ip)
		}
	}
	r.initDialers()
	if *reconnectShift != 0 && !*reconnect {
		die("-reconnect-shift 只能与 -reconnect 一起使用")
	}
	if *reconnectShift < 0 {
		die("-reconnect-shift 不能为负数")
	}
	if *reconnectDelay < 0 {
		die("-reconnect-delay 不能为负数")
	}
	if *clientOffset < 0 {
		die("-client-offset 不能为负数")
	}
	if *holdUntil < 0 {
		die("-hold-until-unix 不能为负数")
	}
	if *progress < 0 {
		die("-connect-progress 不能为负数")
	}
	if *clients > 0 && *clientOffset > int(^uint(0)>>1)-*clients {
		die("-client-offset 与 -clients 之和溢出")
	}
	if *tlsCA != "" {
		roots := x509.NewCertPool()
		pem, err := os.ReadFile(*tlsCA)
		if err != nil || !roots.AppendCertsFromPEM(pem) {
			die("读取 TLS CA %s 失败: %v", *tlsCA, err)
		}
		cfg := &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12, ServerName: *tlsName}
		if (*tlsCert == "") != (*tlsKey == "") {
			die("-tls-cert 与 -tls-key 必须一起提供")
		}
		if *tlsCert != "" {
			cert, err := tls.LoadX509KeyPair(*tlsCert, *tlsKey)
			if err != nil {
				die("读取 mTLS 客户端证书失败: %v", err)
			}
			cfg.Certificates = []tls.Certificate{cert}
		}
		r.tlsConfig = cfg
	}
	if *proxyCert != "" {
		if *tlsCA != "" || *tlsCert != "" || *tlsKey != "" {
			die("-proxy-v2-cert 与客户端直连 TLS 参数不能同时使用")
		}
		raw, err := os.ReadFile(*proxyCert)
		if err != nil {
			die("读取 PROXY v2 客户端证书 %s 失败: %v", *proxyCert, err)
		}
		block, _ := pem.Decode(raw)
		if block == nil || block.Type != "CERTIFICATE" {
			die("%s 不含 PEM CERTIFICATE", *proxyCert)
		}
		if _, err := x509.ParseCertificate(block.Bytes); err != nil {
			die("解析 PROXY v2 客户端证书 %s 失败: %v", *proxyCert, err)
		}
		r.proxyCertDER = append([]byte(nil), block.Bytes...)
		r.proxyFingerprintOnly = *proxyFingerprintOnly
	} else if *proxyFingerprintOnly {
		die("-proxy-v2-fingerprint-only 要求同时给出 -proxy-v2-cert")
	}
	if *persistent {
		if *expiry == 0 {
			die("-persistent-session 要求 -session-expiry 大于 0：" +
				"过期间隔为 0 的会话是连接本地的，与 clean 那一档没有区别")
		}
		r.persistent = true
		r.expiry = uint32(*expiry)
	}
	if len(r.subAddrs) == 0 {
		r.subAddrs = r.pubAddrs
	}
	if r.qos > packet.QoS1 {
		die("只支持 QoS 0 与 QoS 1：QoS 2 的四步握手测的是重传簿记，不是吞吐")
	}

	res := &result{
		Scenario:     *label,
		QoS:          int(r.qos),
		Payload:      r.payload,
		Nodes:        len(r.pubAddrs),
		ClientOffset: *clientOffset,
	}
	if res.Scenario == "" {
		res.Scenario = *scenario
	}

	var err error
	switch *scenario {
	case "connect":
		res.Clients = *clients
		err = r.connectScenario(res, *clients, *hold, *setup)
	case "pairs":
		res.Clients = *pairs * 2
		err = r.pairsScenario(res, *pairs, *setup)
	case "fanout":
		res.Clients = *subs + 1
		err = r.fanoutScenario(res, *subs, *setup)
	case "shadow":
		res.Clients = *clients
		err = r.shadowScenario(res, *clients, *setup)
	default:
		die("未知场景 %q", *scenario)
	}
	if err != nil {
		die("%v", err)
	}

	b, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(b))
	if *out != "" {
		f, ferr := os.OpenFile(*out, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if ferr != nil {
			die("写结果文件失败: %v", ferr)
		}
		line, _ := json.Marshal(res)
		_, _ = f.Write(append(line, '\n'))
		_ = f.Close()
	}
}

// result 是一次场景的结果。字段名直接进 JSON，docs/baseline 下的原始数据
// 就是这些行的拼接——报告里的每个数字都能在原始数据里找到出处。
type result struct {
	Scenario     string `json:"scenario"`
	Nodes        int    `json:"nodes"`
	Clients      int    `json:"clients"`
	ClientOffset int    `json:"clientOffset"`
	QoS          int    `json:"qos"`
	Payload      int    `json:"payloadBytes"`

	ConnectPerSec                float64 `json:"connectPerSec,omitempty"`
	ConnectP50Ms                 float64 `json:"connectP50Ms,omitempty"`
	ConnectP99Ms                 float64 `json:"connectP99Ms,omitempty"`
	ConnectMaxMs                 float64 `json:"connectMaxMs,omitempty"`
	ConnectAttemptMaxMs          float64 `json:"connectAttemptMaxMs,omitempty"`
	BytesPerConn                 int64   `json:"bytesPerConn,omitempty"`
	HeldConns                    int64   `json:"heldConns,omitempty"`
	HeldVerified                 bool    `json:"heldVerified,omitempty"`
	WiringVerified               bool    `json:"wiringVerified,omitempty"`
	MemoryNote                   string  `json:"memoryNote,omitempty"`
	ServerConns                  int64   `json:"serverConns,omitempty"`
	HeapAllocBytes               int64   `json:"heapAllocBytes,omitempty"`
	RuntimeSysBytes              int64   `json:"runtimeSysBytes,omitempty"`
	RuntimeSysPerConn            int64   `json:"runtimeSysPerConn,omitempty"`
	StackInuseBytes              int64   `json:"stackInuseBytes,omitempty"`
	StackBytesPerConn            int64   `json:"stackBytesPerConn,omitempty"`
	ProcessRSSBytes              int64   `json:"processRssBytes,omitempty"`
	RSSBytesPerConn              int64   `json:"rssBytesPerConn,omitempty"`
	TLSResumed                   int64   `json:"tlsResumed,omitempty"`
	LoadGeneratorHeapAllocBytes  int64   `json:"loadGeneratorHeapAllocBytes,omitempty"`
	LoadGeneratorRuntimeSysBytes int64   `json:"loadGeneratorRuntimeSysBytes,omitempty"`
	LoadGeneratorProcessRSSBytes int64   `json:"loadGeneratorProcessRssBytes,omitempty"`
	LoadGeneratorRuntimePerConn  int64   `json:"loadGeneratorRuntimeSysPerConn,omitempty"`
	LoadGeneratorRSSBytesPerConn int64   `json:"loadGeneratorRssBytesPerConn,omitempty"`

	PublishPerSec float64 `json:"publishPerSec,omitempty"`
	DeliverPerSec float64 `json:"deliverPerSec,omitempty"`
	AckedPerSec   float64 `json:"ackedPerSec,omitempty"`
	LatencyP50Ms  float64 `json:"latencyP50Ms,omitempty"`
	LatencyP99Ms  float64 `json:"latencyP99Ms,omitempty"`
	LatencyMaxMs  float64 `json:"latencyMaxMs,omitempty"`
	Published     int64   `json:"published,omitempty"`
	Delivered     int64   `json:"delivered,omitempty"`
	Errors        int64   `json:"errors,omitempty"`
	FirstError    string  `json:"firstError,omitempty"`
	Seconds       float64 `json:"seconds,omitempty"`
}

type runner struct {
	pubAddrs  []string
	subAddrs  []string
	admin     string
	adminHTTP *http.Client
	token     string
	qos       packet.QoS
	payload   int
	inflight  int
	rate      int
	duration  time.Duration
	warmup    time.Duration
	conc      int
	prefix    string
	// clientOffset 让多个压测进程共享 prefix，同时使用互不重叠的设备编号。
	clientOffset int
	// holdUntilUnix 让多个压测进程在同一个绝对时刻之前都保持连接。
	holdUntilUnix int64
	// verifyHeld 在保持窗口末尾做一次应用层PING往返。只数曾经成功的CONNECT
	// 会把窗口内已被服务端断开的socket也写成held，不能用于百万并发验收。
	verifyHeld bool
	// connectProgress 让百万连接轮在最终 JSON 之前也能区分仍在前进、尾部
	// 超时与完全停滞。只读原子计数，不遍历连接，也不进入建连热路径。
	connectProgress time.Duration
	// persistent 为真时建连用持久会话，于是每条连接在集群下都要提一条
	// 复制日志（见 -persistent-session 的说明）。
	persistent bool
	expiry     uint32
	tlsConfig  *tls.Config
	// proxyCertDER 非空时，压测器模拟已完成 mTLS 校验的受信终结器，在
	// TCP 建连后、MQTT CONNECT 前发送严格的 PROXY v2 身份 header。
	// 这一档只衡量 broker；终结器自身资源必须另行计量。
	proxyCertDER         []byte
	proxyFingerprintOnly bool
	sourceIPs            []net.IP
	// net.Dialer 的方法允许并发调用。默认路径和每个显式源 IP 各复用一份
	// 只读配置，避免百万冷连为相同 Timeout/LocalAddr 重建百万个对象。
	dialer         net.Dialer
	sourceDialers  []net.Dialer
	reconnect      bool
	reconnectDelay time.Duration
	reconnectShift int
	// tlsSessions 按 clientId 隔离 ticket，但不为百万设备各造一个
	// LRU/mutex/map。固定分片表的值才是真正的 ClientSessionState。
	tlsSessions deviceTLSSessionCache
}
