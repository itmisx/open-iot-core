package server

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/itmisx/open-iot-core/internal/authz"
	"github.com/itmisx/open-iot-core/internal/meta"
)

// 本文件实现 v0.1 内核阶段的文件化注册表。
//
// 它**不是**最终形态：PRD 规定元数据的权威来源是 Raft 日志加 bbolt
// 冷层（契约 05、06）。这里用 data dir 下的 JSON 文件代替，目的只有一个——
// 让二进制现在就能跑通「签证书、绑策略、设备接入、吊销断连」的完整链路。
// 没有它，serve 启动起来是一个拒绝所有设备的空 broker，无法验证任何东西。
//
// 两点刻意的设计，使它将来被替换时不会留下包袱：
//   - 读写只经 meta.Store 的命令接口，与 Raft 实现走同一条路径；
//   - 文件格式带 format_version，且未知字段一律报错。

// RegistryFormatVersion 是注册表文件的格式版本。
const RegistryFormatVersion = 1

const (
	dirPolicies   = "policies"
	dirRegistry   = "registry"
	fileCerts     = "certs.json"
	policySuffix  = ".json"
	certStatusLen = 64 // 指纹的十六进制长度
)

// certRecord 是一条证书登记。
type certRecord struct {
	// Fingerprint 是证书 DER 的 SHA-256，小写十六进制（PRD PKI-102）。
	Fingerprint string `json:"fingerprint"`
	// Thing 是绑定的设备名。
	Thing string `json:"thing"`
	// Policy 是策略文档名，对应 policies/<name>.json。
	Policy string `json:"policy"`
	// Status 取 ACTIVE | INACTIVE | REVOKED | PENDING_ACTIVATION。
	Status string `json:"status"`
	// Reason 在吊销时记录原因，进入审计。
	Reason string `json:"reason,omitempty"`
	// Subject 仅供人阅读，不参与任何判定。
	Subject string `json:"subject,omitempty"`
}

// certFile 是 registry/certs.json 的内容。
type certFile struct {
	FormatVersion int          `json:"format_version"`
	Certs         []certRecord `json:"certs"`
}

// statusNames 把文件里的字符串映射到内部状态。
//
// 用显式表而不是 String() 反查：状态名是写进用户文件的契约，
// 它不应随内部枚举的 String 实现变化而改变。
var statusNames = map[string]meta.CertStatus{
	"ACTIVE":             meta.CertActive,
	"INACTIVE":           meta.CertInactive,
	"REVOKED":            meta.CertRevoked,
	"PENDING_ACTIVATION": meta.CertPendingActivation,
}

func statusName(s meta.CertStatus) string {
	for name, v := range statusNames {
		if v == s {
			return name
		}
	}
	return "INACTIVE"
}

// Registry 读写 data dir 下的注册表文件。
type Registry struct {
	policiesDir string
	certsPath   string
}

// NewRegistry 绑定到数据目录。
func NewRegistry(root string) *Registry {
	return &Registry{
		policiesDir: filepath.Join(root, dirPolicies),
		certsPath:   filepath.Join(root, dirRegistry, fileCerts),
	}
}

// EnsureDirs 创建注册表所需目录。
func (r *Registry) EnsureDirs() error {
	for _, d := range []string{r.policiesDir, filepath.Dir(r.certsPath)} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return fmt.Errorf("创建目录 %s 失败: %w", d, err)
		}
	}
	return nil
}

// PoliciesDir 返回策略文档目录，供提示信息使用。
func (r *Registry) PoliciesDir() string { return r.policiesDir }

// CertsPath 返回证书登记文件路径。
func (r *Registry) CertsPath() string { return r.certsPath }

// LoadPolicies 读取并编译全部策略文档。
//
// 返回按名字排序的结果：策略集编号由顺序决定，而编号必须跨重启稳定，
// 否则重启后证书绑定的编号会指向另一份策略——这是最危险的一类静默错误，
// 它让设备拿到别的设备的权限。
func (r *Registry) LoadPolicies(env authz.Env) ([]CompiledPolicy, error) {
	entries, err := os.ReadDir(r.policiesDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("读取策略目录 %s 失败: %w", r.policiesDir, err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), policySuffix) {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)

	out := make([]CompiledPolicy, 0, len(names))
	for _, fn := range names {
		path := filepath.Join(r.policiesDir, fn)
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("读取策略 %s 失败: %w", path, err)
		}
		name := strings.TrimSuffix(fn, policySuffix)
		cs, err := authz.CompileJSON(name, data, env)
		if err != nil {
			// 策略编译失败必须让启动失败：带着一份语法错误的策略继续运行，
			// 等于把该策略下的所有设备静默降权或提权。
			return nil, fmt.Errorf("策略 %s 编译失败: %w", path, err)
		}
		out = append(out, CompiledPolicy{Name: name, Set: cs})
	}
	return out, nil
}

// PolicyDocument 返回一份策略的原始文档。
//
// 导入控制面时需要原文而不是编译结果：控制面会自己重新编译，
// 并保留原文供控制台展示。
func (r *Registry) PolicyDocument(name string) ([]byte, error) {
	path := filepath.Join(r.policiesDir, name+policySuffix)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取策略 %s 失败: %w", path, err)
	}
	return data, nil
}

// CompiledPolicy 是一份已编译的策略。
type CompiledPolicy struct {
	Name string
	Set  *authz.CompiledSet
}

// LoadCerts 读取证书登记。
func (r *Registry) LoadCerts() ([]certRecord, error) {
	data, err := os.ReadFile(r.certsPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("读取 %s 失败: %w", r.certsPath, err)
	}
	var f certFile
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("解析 %s 失败: %w", r.certsPath, err)
	}
	if f.FormatVersion > RegistryFormatVersion {
		return nil, fmt.Errorf("%s 的 format_version 为 %d，本程序最高支持 %d",
			r.certsPath, f.FormatVersion, RegistryFormatVersion)
	}
	return f.Certs, nil
}

// AddCert 追加或覆盖一条证书登记。
//
// 以指纹为键覆盖而非追加重复项：重复指纹会让"哪一条生效"取决于
// 遍历顺序，而其中一条可能是 REVOKED。
func (r *Registry) AddCert(rec certRecord) error {
	if err := rec.validate(); err != nil {
		return err
	}
	certs, err := r.LoadCerts()
	if err != nil {
		return err
	}
	replaced := false
	for i := range certs {
		if certs[i].Fingerprint == rec.Fingerprint {
			certs[i] = rec
			replaced = true
			break
		}
	}
	if !replaced {
		certs = append(certs, rec)
	}
	return r.writeCerts(certs)
}

// SetCertStatus 改变一条证书的状态，用于吊销。
func (r *Registry) SetCertStatus(fingerprint, status, reason string) error {
	if _, ok := statusNames[status]; !ok {
		return fmt.Errorf("未知的证书状态 %q；可取 %s", status, knownStatuses())
	}
	certs, err := r.LoadCerts()
	if err != nil {
		return err
	}
	for i := range certs {
		if certs[i].Fingerprint == fingerprint {
			certs[i].Status = status
			certs[i].Reason = reason
			return r.writeCerts(certs)
		}
	}
	return fmt.Errorf("指纹 %s 未登记", fingerprint)
}

func (r *Registry) writeCerts(certs []certRecord) error {
	if err := r.EnsureDirs(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(&certFile{
		FormatVersion: RegistryFormatVersion, Certs: certs,
	}, "", "  ")
	if err != nil {
		return fmt.Errorf("编码证书登记失败: %w", err)
	}
	data = append(data, '\n')
	tmp := r.certsPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("写入 %s 失败: %w", tmp, err)
	}
	if err := os.Rename(tmp, r.certsPath); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("替换 %s 失败: %w", r.certsPath, err)
	}
	return nil
}

func (rec *certRecord) validate() error {
	if len(rec.Fingerprint) != certStatusLen {
		return fmt.Errorf("指纹 %q 长度应为 %d 个十六进制字符",
			rec.Fingerprint, certStatusLen)
	}
	if _, err := hex.DecodeString(rec.Fingerprint); err != nil {
		return fmt.Errorf("指纹 %q 不是合法的十六进制", rec.Fingerprint)
	}
	if rec.Fingerprint != strings.ToLower(rec.Fingerprint) {
		return fmt.Errorf("指纹须为小写十六进制（PRD PKI-102）")
	}
	if _, ok := statusNames[rec.Status]; !ok {
		return fmt.Errorf("未知的证书状态 %q；可取 %s", rec.Status, knownStatuses())
	}
	if rec.Policy == "" {
		return errors.New("证书须绑定策略：未绑定策略的证书无法连接")
	}
	return nil
}

func knownStatuses() string {
	out := make([]string, 0, len(statusNames))
	for k := range statusNames {
		out = append(out, k)
	}
	sort.Strings(out)
	return strings.Join(out, "、")
}

func parseFingerprint(s string) (meta.CertFingerprint, error) {
	var fp meta.CertFingerprint
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != len(fp) {
		return fp, fmt.Errorf("指纹 %q 无效", s)
	}
	copy(fp[:], b)
	return fp, nil
}

// shortFP 截短指纹用于日志，避免日志行被 64 个字符占满。
func shortFP(s string) string {
	if len(s) <= 16 {
		return s
	}
	return s[:16] + "…"
}

// certActiveStatus 返回可连接状态，供签发流程使用。
func certActiveStatus() meta.CertStatus { return meta.CertActive }
