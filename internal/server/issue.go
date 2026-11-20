package server

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/itmisx/open-iot-core/internal/config"
	"github.com/itmisx/open-iot-core/internal/pki"
)

// IssueDeviceRequest 是签发设备证书的参数。
type IssueDeviceRequest struct {
	// Thing 是设备名。它会被策略变量 ${iot:Connection.Thing.ThingName} 引用。
	Thing string
	// CommonName 为空时取 Thing。
	CommonName string
	// Policy 是策略文档名。
	Policy string
	// Validity 为 0 时取策略默认值。
	Validity time.Duration
	// OutDir 是证书与私钥的输出目录。
	OutDir string
}

// IssueDeviceResult 是签发结果。
type IssueDeviceResult struct {
	Fingerprint string
	CertPath    string
	KeyPath     string
	ChainPath   string
	NotAfter    time.Time
}

// IssueDevice 签发一张设备证书并登记到注册表。
//
// 私钥只在此处落盘一次，CA 不留存（PRD PKI-010）。它被写到调用方指定的
// 目录而不是 data dir：data dir 是服务端的，设备私钥应当交给设备，
// 把它留在服务端的数据目录里只会增加一个泄露点。
func IssueDevice(cfg *config.Config, ca *pki.CA, reg *Registry,
	req IssueDeviceRequest,
) (IssueDeviceResult, error) {
	var res IssueDeviceResult
	if ca == nil {
		return res, fmt.Errorf("未启用内嵌 PKI，无法签发")
	}
	if req.Thing == "" {
		return res, fmt.Errorf("须给出设备名（--thing）")
	}
	if req.Policy == "" {
		return res, fmt.Errorf("须给出策略名（--policy）")
	}
	// 策略必须已存在：允许绑定一个不存在的策略，等于签发一张注定
	// 无法连接的证书，而失败要等到设备第一次连接时才暴露。
	policies, err := reg.LoadPolicies(authzEnv(cfg))
	if err != nil {
		return res, err
	}
	found := false
	for _, p := range policies {
		if p.Name == req.Policy {
			found = true
			break
		}
	}
	if !found {
		return res, fmt.Errorf(
			"策略 %q 不存在；请先在 %s 下创建 %s.json",
			req.Policy, reg.PoliciesDir(), req.Policy)
	}

	cn := req.CommonName
	if cn == "" {
		cn = req.Thing
	}
	issued, err := ca.IssueKeyPair(pki.IssueRequest{
		CommonName: cn,
		Validity:   issueDeviceValidity(cfg, req.Validity),
	})
	if err != nil {
		return res, fmt.Errorf("签发失败: %w", err)
	}

	outDir := req.OutDir
	if outDir == "" {
		outDir = "."
	}
	if err := os.MkdirAll(outDir, 0o700); err != nil {
		return res, fmt.Errorf("创建输出目录 %s 失败: %w", outDir, err)
	}
	res = IssueDeviceResult{
		Fingerprint: issued.Fingerprint.String(),
		CertPath:    filepath.Join(outDir, req.Thing+".pem"),
		KeyPath:     filepath.Join(outDir, req.Thing+".key"),
		ChainPath:   filepath.Join(outDir, "ca-chain.pem"),
		NotAfter:    issued.NotAfter,
	}
	writes := []struct {
		path string
		data []byte
		perm os.FileMode
	}{
		{res.CertPath, issued.CertPEM, 0o644},
		{res.KeyPath, issued.KeyPEM, 0o600},
		{res.ChainPath, issued.ChainPEM, 0o644},
	}
	for _, w := range writes {
		if err := os.WriteFile(w.path, w.data, w.perm); err != nil {
			return res, fmt.Errorf("写入 %s 失败: %w", w.path, err)
		}
	}

	if err := reg.AddCert(certRecord{
		Fingerprint: res.Fingerprint,
		Thing:       req.Thing,
		Policy:      req.Policy,
		Status:      statusName(certActiveStatus()),
		Subject:     cn,
	}); err != nil {
		return res, fmt.Errorf("登记证书失败: %w", err)
	}
	return res, nil
}
