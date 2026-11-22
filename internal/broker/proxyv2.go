package broker

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"time"

	"github.com/itmisx/open-iot-core/internal/meta"
)

// ProxyV2TLVClientCert 是 OpenIoT 在 PROXY protocol v2 中使用的私有 TLV。
// value 的稳定线格式为：
//
//	byte    format version（当前为 1）
//	uint16  TLS version（网络字节序，0x0303 或 0x0304）
//	byte    ALPN 长度 N
//	[N]byte ALPN
//	[]byte  叶证书 DER
//
// 传叶证书而不是传一个由代理声称的指纹，使 broker 能自己计算主键并完整
// 构造证书策略上下文。信任边界仍是 TrustedProxies：该链路必须位于私网、
// loopback 或 Unix socket 等不可由设备直接访问的网络中。
const ProxyV2TLVClientCert byte = 0xe0

// ProxyV2TLVCertFingerprint 是高密度终结器使用的紧凑身份 TLV。线格式前缀
// 与 ProxyV2TLVClientCert 相同，尾部改为 32 B 原始 SHA-256 或 64 B 十六进制。
// 它适配 HAProxy set-proxy-v2-tlv-fmt 一类只能方便导出证书摘要的终结器。
// 未携带 DER 时证书 DN/序列号变量为空；引用它们的策略会按既有 fail-closed
// 语义拒绝，而基于 Thing/ClientId 的常见策略不受影响。
const ProxyV2TLVCertFingerprint byte = 0xe1

const (
	proxyV2MaxHeader     = 16 << 10
	proxyV2FormatVersion = 1
	defaultProxyTimeout  = 3 * time.Second
)

var proxyV2Signature = []byte("\r\n\r\n\x00\r\nQUIT\n")

// ProxyOptions 配置受信 PROXY protocol v2 身份入口。
type ProxyOptions struct {
	// TrustedProxies 按原始 TCP peer 地址判定，而不是按 header 里的 source。
	// 至少要有一项；空 allowlist 永远不允许启动。
	TrustedProxies []netip.Prefix
	// HeaderTimeout 限制代理发完 header 的时间，避免慢连接占住 goroutine。
	HeaderTimeout time.Duration
}

func (o *ProxyOptions) validate() error {
	if len(o.TrustedProxies) == 0 {
		return errors.New("broker: PROXY v2 监听器缺少 trusted proxies")
	}
	for _, p := range o.TrustedProxies {
		if !p.IsValid() {
			return errors.New("broker: PROXY v2 trusted proxy 含无效 CIDR")
		}
	}
	if o.HeaderTimeout <= 0 {
		o.HeaderTimeout = defaultProxyTimeout
	}
	return nil
}

func readProxyV2Identity(nc net.Conn, peer netip.AddrPort, opt ProxyOptions) (ConnInfo, error) {
	if !trustedProxy(peer.Addr(), opt.TrustedProxies) {
		return ConnInfo{}, fmt.Errorf("broker: PROXY v2 来源 %s 不受信", peer.Addr())
	}
	// contract:allow-time-now 网络 I/O deadline 使用单调时钟，只限制慢 header，
	// 不参与业务版本、排序或可重放的状态。
	if err := nc.SetReadDeadline(time.Now().Add(opt.HeaderTimeout)); err != nil {
		return ConnInfo{}, err
	}
	defer func() { _ = nc.SetReadDeadline(time.Time{}) }()

	var fixed [16]byte
	if _, err := io.ReadFull(nc, fixed[:]); err != nil {
		return ConnInfo{}, fmt.Errorf("broker: 读取 PROXY v2 header: %w", err)
	}
	if !bytes.Equal(fixed[:12], proxyV2Signature) {
		return ConnInfo{}, errors.New("broker: PROXY v2 签名无效")
	}
	if fixed[12] != 0x21 { // version 2, PROXY command; LOCAL 不携带受信身份
		return ConnInfo{}, fmt.Errorf("broker: PROXY v2 version/command 0x%02x 不受支持", fixed[12])
	}
	length := int(binary.BigEndian.Uint16(fixed[14:16]))
	if length > proxyV2MaxHeader {
		return ConnInfo{}, fmt.Errorf("broker: PROXY v2 header 长度 %d 超过上限", length)
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(nc, body); err != nil {
		return ConnInfo{}, fmt.Errorf("broker: 读取 PROXY v2 body: %w", err)
	}

	var source netip.AddrPort
	var addrLen int
	switch fixed[13] { // family + STREAM protocol
	case 0x11:
		addrLen = 12
		if len(body) < addrLen {
			return ConnInfo{}, errors.New("broker: PROXY v2 IPv4 地址被截断")
		}
		source = netip.AddrPortFrom(netip.AddrFrom4([4]byte(body[:4])), binary.BigEndian.Uint16(body[8:10]))
	case 0x21:
		addrLen = 36
		if len(body) < addrLen {
			return ConnInfo{}, errors.New("broker: PROXY v2 IPv6 地址被截断")
		}
		var ip [16]byte
		copy(ip[:], body[:16])
		source = netip.AddrPortFrom(netip.AddrFrom16(ip), binary.BigEndian.Uint16(body[32:34]))
	default:
		return ConnInfo{}, fmt.Errorf("broker: PROXY v2 family/protocol 0x%02x 不受支持", fixed[13])
	}

	identity, err := parseProxyV2TLVs(body[addrLen:])
	if err != nil {
		return ConnInfo{}, err
	}
	return connInfoFromProxyIdentity(source, identity)
}

func connInfoFromProxyIdentity(source netip.AddrPort, identity proxyV2Identity) (ConnInfo, error) {
	info := ConnInfo{Remote: source, HasCert: true, TLSVersion: identity.tlsVersion, ALPN: identity.alpn}
	if len(identity.certDER) == 0 {
		info.CertFP = identity.fingerprint
		return info, nil
	}
	leaf, err := x509.ParseCertificate(identity.certDER)
	if err != nil {
		return ConnInfo{}, fmt.Errorf("broker: PROXY v2 客户端证书无效: %w", err)
	}
	info.CertFP = meta.CertFingerprint(sha256.Sum256(leaf.Raw))
	if identity.hasFingerprint && identity.fingerprint != info.CertFP {
		return ConnInfo{}, errors.New("broker: PROXY v2 证书与指纹 TLV 不一致")
	}
	info.CertSerial = serialHex(leaf)
	info.CertSubject = dnFrom(leaf.Subject)
	info.CertIssuer = dnFrom(leaf.Issuer)
	return info, nil
}

func trustedProxy(peer netip.Addr, trusted []netip.Prefix) bool {
	if !peer.IsValid() {
		return false
	}
	peer = peer.Unmap()
	for _, p := range trusted {
		if p.Contains(peer) || p.Contains(peer.Unmap()) {
			return true
		}
	}
	return false
}

type proxyV2Identity struct {
	certDER        []byte
	fingerprint    meta.CertFingerprint
	hasFingerprint bool
	tlsVersion     uint16
	alpn           string
}

func parseProxyV2TLVs(buf []byte) (proxyV2Identity, error) {
	var out proxyV2Identity
	for len(buf) > 0 {
		if len(buf) < 3 {
			return out, errors.New("broker: PROXY v2 TLV header 被截断")
		}
		t, n := buf[0], int(binary.BigEndian.Uint16(buf[1:3]))
		buf = buf[3:]
		if n > len(buf) {
			return out, errors.New("broker: PROXY v2 TLV value 被截断")
		}
		value := buf[:n]
		buf = buf[n:]
		if t != ProxyV2TLVClientCert && t != ProxyV2TLVCertFingerprint {
			continue
		}
		if (t == ProxyV2TLVClientCert && out.certDER != nil) ||
			(t == ProxyV2TLVCertFingerprint && out.hasFingerprint) {
			return out, errors.New("broker: PROXY v2 客户端身份 TLV 重复")
		}
		if len(value) < 5 || value[0] != proxyV2FormatVersion {
			return out, errors.New("broker: PROXY v2 客户端身份 TLV 格式无效")
		}
		version := binary.BigEndian.Uint16(value[1:3])
		if version != tls.VersionTLS12 && version != tls.VersionTLS13 {
			return out, fmt.Errorf("broker: PROXY v2 TLS version 0x%04x 不受支持", version)
		}
		alpnLen := int(value[3])
		if 4+alpnLen >= len(value) {
			return out, errors.New("broker: PROXY v2 客户端身份缺失")
		}
		alpn := string(value[4 : 4+alpnLen])
		if out.tlsVersion != 0 && (out.tlsVersion != version || out.alpn != alpn) {
			return out, errors.New("broker: PROXY v2 身份 TLV 的 TLS 事实不一致")
		}
		out.tlsVersion, out.alpn = version, alpn
		payload := value[4+alpnLen:]
		if t == ProxyV2TLVClientCert {
			out.certDER = payload
			continue
		}
		switch len(payload) {
		case len(out.fingerprint):
			copy(out.fingerprint[:], payload)
		case hex.EncodedLen(len(out.fingerprint)):
			if _, err := hex.Decode(out.fingerprint[:], payload); err != nil {
				return out, errors.New("broker: PROXY v2 证书指纹不是合法十六进制")
			}
		default:
			return out, fmt.Errorf("broker: PROXY v2 证书指纹长度 %d 无效", len(payload))
		}
		out.hasFingerprint = true
	}
	if out.certDER == nil && !out.hasFingerprint {
		return out, errors.New("broker: PROXY v2 缺少客户端身份 TLV")
	}
	return out, nil
}
