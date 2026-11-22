package broker

import (
	"errors"

	"github.com/itmisx/open-iot-core/internal/authz"
	"github.com/itmisx/open-iot-core/internal/events"
	"github.com/itmisx/open-iot-core/internal/meta"
	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
)

// CertAuthenticator 以 X.509 客户端证书认证（PRD AUTH-001）。
//
// 它只查本地热视图：证书指纹到状态、到绑定设备、到策略集。
// 整条路径不触网——MetaView 的签名本身就承诺了这一点（契约 05），
// 而认证位于 CONNECT 路径上，是吞吐最敏感的位置。
type CertAuthenticator struct {
	// RequireThingMatchesClientID 要求绑定设备名与 clientId 一致。
	//
	// AWS 在使用 Thing 类策略变量时隐含此要求（PRD AUTH-034），
	// 但那是策略求值层的约束。在认证层开启本选项可以更早拒绝，
	// 使错误信息更明确；默认关闭以保持与 AWS 一致的行为。
	RequireThingMatchesClientID bool
}

// Authenticate 实现 Authenticator。
func (a CertAuthenticator) Authenticate(v *meta.View, info ConnInfo, clientID string,
	_, _ []byte,
) (Principal, reason.Code) {
	if !info.HasCert {
		return Principal{}, reason.BadUserNameOrPassword
	}
	cert, ok := v.Cert(info.CertFP)
	if !ok {
		// 证书未登记，直接拒绝。
		//
		// 原因码用 NotAuthorized 而不是 ClientIdentifierNotValid：
		// 后者会告诉设备方"你的 clientId 不对"，把排查引向完全错误的
		// 方向——真正的问题是这张证书没有被登记。它同时也泄露更少：
		// 与"已吊销"用同一个码，外部无法据此探测某个指纹是否已登记。
		return Principal{}, reason.NotAuthorized
	}
	if !cert.Status.CanConnect() {
		// 状态与原因码的对应要精确，否则运维无法从事件区分
		// 「没激活」「被吊销」「待激活」三种情形（PRD AUTH-013）。
		switch cert.Status {
		case meta.CertRevoked:
			return Principal{}, reason.NotAuthorized
		case meta.CertInactive, meta.CertPendingActivation:
			return Principal{}, reason.NotAuthorized
		default:
			// 未知状态同样归为未授权：它要么是新增的状态值，
			// 要么是数据损坏，两种情形都不该放行，也都与 clientId 无关。
			return Principal{}, reason.NotAuthorized
		}
	}
	if a.RequireThingMatchesClientID && cert.Thing != "" && cert.Thing != clientID {
		return Principal{}, reason.ClientIdentifierNotValid
	}

	p := Principal{
		Kind:       PrincipalCert,
		CertFP:     info.CertFP,
		HasFP:      true,
		Thing:      cert.Thing,
		AuthMethod: events.AuthMTLS,
		AuthzCtx: authz.Context{
			ClientID:      clientID,
			Thing:         cert.Thing,
			ThingAttached: cert.Thing != "",
			Cert: &authz.CertContext{
				SerialNumber: info.CertSerial,
				Subject:      info.CertSubject,
				Issuer:       info.CertIssuer,
			},
		},
	}
	p.Policies = policiesFor(v, info.CertFP, true, cert.Thing)
	if len(p.Policies) == 0 {
		// 没有任何策略等价于隐式拒绝一切，与其让它在首次发布时才暴露，
		// 不如在连接时就明确拒绝——否则排障者会看到「连上了但什么都不能做」。
		return Principal{}, reason.NotAuthorized
	}
	return p, reason.Success
}

// InsecureNoAuth 跳过认证，把全部连接当作受信主体。
//
// 它只应用于测试与明文回环场景。Acknowledged 必须显式置为 true，
// 使它不可能被误用：一个默认可用的「放行一切」认证器是生产事故的温床。
type InsecureNoAuth struct {
	// Acknowledged 必须为 true，否则 Authenticate 一律拒绝。
	Acknowledged bool
	// Policies 是赋予所有连接的策略集。为空时所有动作都会被隐式拒绝，
	// 因为策略求值的默认结果是拒绝。
	Policies []*authz.CompiledSet
	// Thing 把所有连接绑定到同一设备名；为空表示用 clientId 作为设备名。
	Thing string
}

// ErrInsecureNotAcknowledged 表示 InsecureNoAuth 未被显式确认。
var ErrInsecureNotAcknowledged = errors.New(
	"broker: InsecureNoAuth 须显式设置 Acknowledged，它会放行一切连接")

// Authenticate 实现 Authenticator。
func (a InsecureNoAuth) Authenticate(_ *meta.View, _ ConnInfo, clientID string,
	_, _ []byte,
) (Principal, reason.Code) {
	if !a.Acknowledged {
		return Principal{}, reason.NotAuthorized
	}
	thing := a.Thing
	if thing == "" {
		thing = clientID
	}
	return Principal{
		Kind:       PrincipalNone,
		Thing:      thing,
		Policies:   a.Policies,
		AuthMethod: events.AuthNone,
		AuthzCtx: authz.Context{
			ClientID:      clientID,
			Thing:         thing,
			ThingAttached: true,
		},
	}, reason.Success
}

// TransportIdentity 把监听器已经验过的 access key 绑到设备。
//
// 密钥核对发生在 WebSocket 握手，这里不再碰密钥。access key 指向一个
// 设备名，策略取该设备已附加的策略集。没有 access key 的连接交给 Next，
// 那是证书认证的路径。有 access key 但绑不上设备时直接拒绝，不能落到
// Next：否则一把验签通过却没有策略的钥匙会被当成「没有证书的普通连接」放行。
type TransportIdentity struct {
	Next Authenticator
	// Key 返回这把 access key 所代表的设备。不认识返回 false。
	Key func(accessKeyID string) (thing string, ok bool)
}

// Authenticate 实现 Authenticator。
func (t TransportIdentity) Authenticate(v *meta.View, info ConnInfo, clientID string,
	username, password []byte,
) (Principal, reason.Code) {
	if info.AccessKeyID == "" {
		if t.Next == nil {
			return Principal{}, reason.NotAuthorized
		}
		return t.Next.Authenticate(v, info, clientID, username, password)
	}
	if t.Key == nil {
		return Principal{}, reason.NotAuthorized
	}
	thing, ok := t.Key(info.AccessKeyID)
	if !ok || thing == "" {
		return Principal{}, reason.NotAuthorized
	}
	p := Principal{
		Kind:       PrincipalSigV4,
		Thing:      thing,
		AuthMethod: events.AuthSigV4,
		AuthzCtx: authz.Context{
			ClientID:      clientID,
			Thing:         thing,
			ThingAttached: true,
		},
	}
	p.Policies = policiesFor(v, meta.CertFingerprint{}, false, thing)
	if len(p.Policies) == 0 {
		return Principal{}, reason.NotAuthorized
	}
	return p, reason.Success
}

var (
	_ Authenticator = CertAuthenticator{}
	_ Authenticator = InsecureNoAuth{}
	_ Authenticator = TransportIdentity{}
)
