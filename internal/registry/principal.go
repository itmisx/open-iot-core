package registry

import (
	"context"
	"errors"
	"fmt"
)

// 证书与设备的关联（PRD REG-060 至 REG-065）。
//
// AWS 把证书称作 principal，关联是多对多：一张证书可以挂到多台设备上。
// 本项目是一对一，这是有意的收窄——鉴权路径要用它解析
// ${iot:Connection.Thing.ThingName}，而策略变量只能有一个值
// （见 types.go 对 Certificate.Thing 的说明）。因此：
//
//   - Attach 覆盖原有绑定，而不是追加；
//   - ListPrincipalThings 返回零或一个设备。
//
// 收窄之后仍保留这组操作，是因为"把已有证书改绑到另一台设备"是真实
// 需求（换机、RMA、测试环境搬迁），而在它之前唯一的做法是删掉证书
// 重签——那会让设备侧也得换一次密钥，代价完全不必要。

// AttachThingPrincipal 把证书绑定到设备。
//
// 覆盖原有绑定。若原绑定非空且与新值不同，使用该证书的活连接会被
// 断开（PKI-109）：连接的设备身份在握手时固定，不断开的话凭据已经
// 移走而权限还留在原处。
func (r *Registry) AttachThingPrincipal(ctx context.Context, thing, fingerprint string) error {
	if thing == "" {
		return errors.New("registry: 须给出设备名")
	}
	return r.setCertThing(ctx, fingerprint, thing)
}

// DetachThingPrincipal 解除证书与设备的绑定。
//
// thing 须与当前绑定一致，不一致则报错而不是静默成功：调用方多半
// 拿的是过期的信息，让它以为解绑了某个其实没动的关联更危险。
//
// 解绑后证书仍然存在且状态不变，但它不再能解析设备变量，因此多数
// 策略下已无法连接。要彻底停用请用吊销（SetCertificateStatus）。
func (r *Registry) DetachThingPrincipal(ctx context.Context, thing, fingerprint string) error {
	cur, err := r.GetCertificate(fingerprint)
	if err != nil {
		return err
	}
	if cur.Thing == "" {
		return fmt.Errorf("%w: 证书未绑定任何设备", ErrNotFound)
	}
	if thing != "" && cur.Thing != thing {
		return fmt.Errorf("registry: 证书当前绑定的是 %q，不是 %q", cur.Thing, thing)
	}
	return r.setCertThing(ctx, fingerprint, "")
}

// ListThingPrincipals 列出绑定某设备的证书指纹。
//
// 与 CertificatesOfThing 同义，另起一个名字是为了与 AWS 的动作名
// 对齐，使兼容面（v0.1.5）可以直接落在它上面。
func (r *Registry) ListThingPrincipals(thing string) ([]string, error) {
	return r.CertificatesOfThing(thing)
}

// ListPrincipalThings 列出某证书绑定的设备。
//
// 返回切片而不是单个字符串：本项目是一对一，但 AWS 的同名动作返回
// 列表，而兼容面要落在这上面。返回类型若现在定成标量，将来放宽到
// 多对多时整条链路的签名都要改。
func (r *Registry) ListPrincipalThings(fingerprint string) ([]string, error) {
	c, err := r.GetCertificate(fingerprint)
	if err != nil {
		return nil, err
	}
	if c.Thing == "" {
		return nil, nil
	}
	return []string{c.Thing}, nil
}

// setCertThing 改写证书的设备绑定。
//
// 走与 PutCertificate 相同的写入路径（热视图先于冷层），因此绑定
// 变更与吊销一样即时生效，且会产出带 PrevThing 的 Change。
func (r *Registry) setCertThing(ctx context.Context, fingerprint, thing string) error {
	cur, err := r.GetCertificate(fingerprint)
	if err != nil {
		return err
	}
	if cur.Thing == thing {
		// 幂等：已经是这个绑定就什么也不做。
		//
		// 不空跑一次写入，是为了不产生一条 PrevThing 等于 Thing 的
		// Change——它不触发断连，但会在事件流与审计里留下一条看起来
		// 发生过改绑的记录。
		return nil
	}
	next := *cur
	next.Thing = thing
	return r.PutCertificate(ctx, next)
}
