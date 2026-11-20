package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/itmisx/open-iot-core/internal/registry"
)

// importFileRegistry 把文件化注册表的内容导入控制面。
//
// 文件化注册表是 v0.1 内核阶段的临时实现（见 registry.go 的说明），
// 它只在启动时载入，因此不满足 PKI-030「吊销即时断连」。控制面落地后，
// 它退化为一个一次性的导入来源：`openiotd init` 写出的示例策略与
// `cert issue` 登记的证书仍经它产生，导入后由控制面接管。
//
// 导入是幂等的：已存在的记录会被覆盖为相同内容，重复启动不会重复创建。
// 返回本次新导入的条数，供启动日志区分"首次迁移"与"常规启动"。
func (s *Server) importFileRegistry(ctx context.Context) (int, error) {
	n := 0

	policies, err := s.reg.LoadPolicies(authzEnv(&s.cfg))
	if err != nil {
		return 0, err
	}
	for _, p := range policies {
		if _, gerr := s.reg2.GetPolicy(p.Name); gerr == nil {
			continue // 已在控制面中
		} else if !errors.Is(gerr, registry.ErrNotFound) {
			return n, gerr
		}
		doc, rerr := s.reg.PolicyDocument(p.Name)
		if rerr != nil {
			return n, rerr
		}
		if err := s.reg2.PutPolicy(ctx, p.Name, doc); err != nil {
			return n, fmt.Errorf("导入策略 %s 失败: %w", p.Name, err)
		}
		n++
	}

	certs, err := s.reg.LoadCerts()
	if err != nil {
		return n, err
	}
	for _, c := range certs {
		if _, gerr := s.reg2.GetCertificate(c.Fingerprint); gerr == nil {
			continue
		} else if !errors.Is(gerr, registry.ErrNotFound) {
			return n, gerr
		}
		// 证书绑定的设备须先存在于控制面。文件化注册表没有独立的
		// 设备记录，设备是随证书隐含创建的，这里补上。
		if c.Thing != "" {
			if _, gerr := s.reg2.GetThing(c.Thing); errors.Is(gerr, registry.ErrNotFound) {
				if err := s.reg2.PutThing(ctx, registry.Thing{
					Name: c.Thing, PolicyName: c.Policy,
				}); err != nil {
					return n, fmt.Errorf("导入设备 %s 失败: %w", c.Thing, err)
				}
				n++
			} else if gerr != nil {
				return n, gerr
			}
		}
		if err := s.reg2.PutCertificate(ctx, registry.Certificate{
			Fingerprint: c.Fingerprint, Status: c.Status, Thing: c.Thing,
			PolicyName: c.Policy, Subject: c.Subject, RevokeReason: c.Reason,
		}); err != nil {
			return n, fmt.Errorf("导入证书 %s 失败: %w", shortFP(c.Fingerprint), err)
		}
		n++
	}
	return n, nil
}
