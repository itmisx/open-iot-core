package registry

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"

	"github.com/itmisx/open-iot-core/internal/store"
)

// 自有 CA 的登记（PKI-040…043）。
//
// 验证证书必须由这张 CA 签发，并且使用者是本节点的注册码。
// 做不到这两件事，就说明调用方没有那把私钥。自动注册（JITP）不在这里：
// 那条路径已经从产品里拿掉了，打开它会把「登记一张 CA」变成「替未知设备签发」。

type caRecord struct {
	V        int    `json:"v"`
	ID       string `json:"id"`
	PEM      string `json:"pem"`
	Subject  string `json:"subject"`
	Status   string `json:"status"`
	NotAfter int64  `json:"notAfter,omitempty"`
	Created  int64  `json:"created"`
	Updated  int64  `json:"updated"`
}

// CACertificate 是一张已注册 CA 的对外形状。
type CACertificate struct {
	ID       string
	PEM      string
	Subject  string
	Status   string
	NotAfter int64
	Created  int64
	Updated  int64
}

type codeRecord struct {
	V    int    `json:"v"`
	Code string `json:"code"`
}

// RegistrationCode 返回本节点的注册码，没有就生成一个并留下。
func (r *Registry) RegistrationCode(ctx context.Context) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := store.Key(store.PrefixSystem, "regcode")
	raw, err := r.kv.Get(key)
	if err == nil {
		var rec codeRecord
		if decodeJSON(raw, &rec) == nil && rec.Code != "" {
			return rec.Code, nil
		}
	} else if !errors.Is(err, store.ErrNotFound) {
		return "", err
	}
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	rec := codeRecord{V: 1, Code: hex.EncodeToString(buf[:])}
	op, err := setOp(key, rec)
	if err != nil {
		return "", err
	}
	if err := r.commitRecordsLocked(ctx, op); err != nil {
		return "", err
	}
	return rec.Code, nil
}

// RegisterCA 登记一张自有 CA。
//
// active 为假时状态是 INACTIVE：登记成功不等于马上用它来接收设备证书。
func (r *Registry) RegisterCA(ctx context.Context, pemText, verificationPEM string, active bool) (*CACertificate, error) {
	ca, err := parseCertPEM(pemText)
	if err != nil {
		return nil, err
	}
	if !ca.IsCA {
		return nil, errors.New("registry: 这不是一张 CA 证书")
	}
	proof, err := parseCertPEM(verificationPEM)
	if err != nil {
		return nil, fmt.Errorf("registry: 验证证书无法解析: %w", err)
	}
	if err := proof.CheckSignatureFrom(ca); err != nil {
		return nil, errors.New("registry: 验证证书不是由这张 CA 签发")
	}
	code, err := r.RegistrationCode(ctx)
	if err != nil {
		return nil, err
	}
	if proof.Subject.CommonName != code {
		return nil, errors.New("registry: 验证证书的使用者与注册码不一致")
	}
	sum := sha256.Sum256(ca.Raw)
	id := hex.EncodeToString(sum[:])
	status := StatusInactive
	if active {
		status = StatusActive
	}
	rec := caRecord{
		V: 1, ID: id, PEM: pemText, Subject: ca.Subject.String(),
		Status: status, NotAfter: ca.NotAfter.UnixMilli(),
		Created: r.now(), Updated: r.now(),
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, err := r.getCALocked(id); err == nil {
		return nil, ErrAlreadyExists
	} else if !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	n, err := r.countCASubjectLocked(rec.Subject)
	if err != nil {
		return nil, err
	}
	if n >= r.caSubjectLimit {
		return nil, fmt.Errorf("%w: 同 subject 的 CA 已达上限 %d", ErrLimitExceeded, r.caSubjectLimit)
	}
	op, err := setOp(store.Key(store.PrefixCA, id), rec)
	if err != nil {
		return nil, err
	}
	if err := r.commitRecordsLocked(ctx, op); err != nil {
		return nil, err
	}
	return rec.view(), nil
}

// GetCA 读取一张已注册 CA。
func (r *Registry) GetCA(id string) (*CACertificate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, err := r.getCALocked(id)
	if err != nil {
		return nil, err
	}
	return rec.view(), nil
}

// UpdateCAStatus 只改状态。ACTIVE 与 INACTIVE 之外的值拒绝。
func (r *Registry) UpdateCAStatus(ctx context.Context, id, status string) error {
	if status != StatusActive && status != StatusInactive {
		return errors.New("registry: CA 状态只接受 ACTIVE 或 INACTIVE")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, err := r.getCALocked(id)
	if err != nil {
		return err
	}
	rec.Status = status
	rec.Updated = r.now()
	op, oerr := setOp(store.Key(store.PrefixCA, id), rec)
	if oerr != nil {
		return oerr
	}
	return r.commitRecordsLocked(ctx, op)
}

// DeleteCA 删掉登记。已经用它注册的设备证书留着，它们的签名关系不靠这条记录维持。
func (r *Registry) DeleteCA(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, err := r.getCALocked(id); err != nil {
		return err
	}
	return r.commitRecordsLocked(ctx, deleteOp(store.Key(store.PrefixCA, id)))
}

// ListCAs 按证书编号分页。
func (r *Registry) ListCAs(after string, limit int) ([]CACertificate, string, error) {
	if limit <= 0 {
		limit = 25
	}
	if limit > 100 {
		limit = 100
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []CACertificate
	var next string
	err := store.ScanPrefix(r.kv, store.PrefixCA, "", func(parts []string, value []byte) bool {
		id := parts[0]
		if id <= after {
			return true
		}
		var rec caRecord
		if decodeJSON(value, &rec) != nil {
			return true
		}
		if len(out) == limit {
			next = out[len(out)-1].ID
			return false
		}
		out = append(out, *rec.view())
		return true
	})
	return out, next, err
}

// RegisterExternalCert 登记一张已有的设备证书。
//
// caPEM 非空时，证书必须由一张状态为 ACTIVE 的已注册 CA 签发。
// 空表示无 CA 注册（PKI-042）。策略可以后补，与签发路径相同。
func (r *Registry) RegisterExternalCert(certPEM, caPEM, status string) (*Certificate, error) {
	leaf, err := parseCertPEM(certPEM)
	if err != nil {
		return nil, err
	}
	if status == "" {
		status = StatusActive
	}
	switch status {
	case StatusActive, StatusInactive, StatusRevoked, StatusPendingActivation:
	default:
		return nil, errors.New("registry: 证书状态无法识别")
	}
	if caPEM != "" {
		parent, err := parseCertPEM(caPEM)
		if err != nil {
			return nil, fmt.Errorf("registry: CA 证书无法解析: %w", err)
		}
		if err := leaf.CheckSignatureFrom(parent); err != nil {
			return nil, errors.New("registry: 设备证书不是由给出的 CA 签发")
		}
		sum := sha256.Sum256(parent.Raw)
		id := hex.EncodeToString(sum[:])
		ca, err := r.GetCA(id)
		if err != nil {
			return nil, fmt.Errorf("registry: CA 尚未注册: %w", err)
		}
		if ca.Status != StatusActive {
			return nil, errors.New("registry: CA 不是 ACTIVE，不能用它注册设备证书")
		}
	}
	sum := sha256.Sum256(leaf.Raw)
	fp := hex.EncodeToString(sum[:])
	c := Certificate{
		Fingerprint: fp, Status: status, PEM: certPEM,
		Subject: leaf.Subject.String(), Issuer: leaf.Issuer.String(),
		NotAfterUnixMs: leaf.NotAfter.UnixMilli(),
	}
	if err := r.RegisterCertificate(context.Background(), c); err != nil {
		return nil, err
	}
	return &c, nil
}

func (r *Registry) getCALocked(id string) (*caRecord, error) {
	raw, err := r.kv.Get(store.Key(store.PrefixCA, id))
	if errors.Is(err, store.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var rec caRecord
	if err := decodeJSON(raw, &rec); err != nil {
		return nil, fmt.Errorf("registry: CA 记录损坏 %s: %w", id, err)
	}
	return &rec, nil
}

func (r *Registry) countCASubjectLocked(subject string) (int, error) {
	n := 0
	err := store.ScanPrefix(r.kv, store.PrefixCA, "", func(_ []string, value []byte) bool {
		var rec caRecord
		if decodeJSON(value, &rec) == nil && rec.Subject == subject {
			n++
		}
		return true
	})
	return n, err
}

func (rec *caRecord) view() *CACertificate {
	return &CACertificate{
		ID: rec.ID, PEM: rec.PEM, Subject: rec.Subject, Status: rec.Status,
		NotAfter: rec.NotAfter, Created: rec.Created, Updated: rec.Updated,
	}
}

func parseCertPEM(s string) (*x509.Certificate, error) {
	rest := []byte(s)
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			return nil, errors.New("registry: 不是 PEM 证书")
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("registry: 证书无法解析: %w", err)
		}
		return cert, nil
	}
}
