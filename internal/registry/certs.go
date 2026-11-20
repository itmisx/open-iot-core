package registry

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"

	"github.com/itmisx/open-iot-core/internal/meta"
	"github.com/itmisx/open-iot-core/internal/store"
)

type certRecord struct {
	V            int    `json:"v"`
	Fingerprint  string `json:"fp"`
	Status       string `json:"status"`
	Thing        string `json:"thing,omitempty"`
	PolicyName   string `json:"policy,omitempty"`
	Subject      string `json:"subject,omitempty"`
	Issuer       string `json:"issuer,omitempty"`
	NotAfter     int64  `json:"notAfter,omitempty"`
	RevokeReason string `json:"revokeReason,omitempty"`
	Created      int64  `json:"created"`
	Updated      int64  `json:"updated"`
	PEM          string `json:"pem,omitempty"`
}

// PutCertificate 登记或更新一张证书。证书必须已经绑上策略。
func (r *Registry) PutCertificate(ctx context.Context, c Certificate) error {
	return r.writeCertificate(ctx, c, false)
}

// RegisterCertificate 登记一张尚未附加策略的证书。
//
// 兼容面先签发再 AttachPolicy。若在这里就要求策略，那条路径走不通。
// 没有策略的证书在连接时会被拒绝，策略附加上之后才放行。
// 指纹已存在时返回 ErrAlreadyExists，不把已有登记盖掉。
func (r *Registry) RegisterCertificate(ctx context.Context, c Certificate) error {
	return r.writeCertificate(ctx, c, true)
}

func (r *Registry) writeCertificate(ctx context.Context, c Certificate, createUnbound bool) error {
	if err := ValidateFingerprint(c.Fingerprint); err != nil {
		return err
	}
	if c.Status == "" {
		c.Status = StatusActive
	}
	if err := ValidateStatus(c.Status); err != nil {
		return err
	}
	if c.PolicyName == "" && !createUnbound {
		// 未绑定策略的证书无法连接（CertAuthenticator 会拒绝）。
		// 管理面登记时拒绝，使错误落在真正的原因上，而不是等到
		// 设备第一次连接时才以一条含义模糊的拒绝出现。
		return errors.New("registry: 证书须绑定策略")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if c.PolicyName != "" {
		if _, _, ok := r.policyEntryLocked(c.PolicyName); !ok {
			return fmt.Errorf("registry: 策略 %q 不存在；请先创建它", c.PolicyName)
		}
	}
	if c.Thing != "" {
		if _, err := r.getThingRecordLocked(c.Thing); err != nil {
			if errors.Is(err, ErrNotFound) {
				return fmt.Errorf("registry: 设备 %q 不存在；请先创建它", c.Thing)
			}
			return err
		}
	}

	now := r.now()
	rec := &certRecord{
		V: certRecordVersion, Fingerprint: c.Fingerprint, Status: c.Status,
		Thing: c.Thing, PolicyName: c.PolicyName,
		Subject: c.Subject, Issuer: c.Issuer, NotAfter: c.NotAfterUnixMs,
		RevokeReason: c.RevokeReason, Created: now, Updated: now,
		PEM: c.PEM,
	}
	if old, err := r.getCertRecordLocked(c.Fingerprint); err == nil {
		if createUnbound {
			return ErrAlreadyExists
		}
		rec.Created = old.Created
		if rec.PEM == "" {
			rec.PEM = old.PEM
		}
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}

	return r.applyCertLocked(ctx, rec, true)
}

// SetCertificateStatus 改变证书状态。
//
// 这是 PRD PKI-030「吊销即时断连」的落点。写入热视图会使
// meta.Store 发出一条 Change，broker 的 watchMeta 据此踢掉
// 所有正在使用该证书的连接——**不需要重启节点**。
//
// 顺序是先热视图再冷层：反过来的话，两步之间的窗口里冷层说"已吊销"
// 而热视图还放行，而那个窗口正是吊销失效的样子。先改热视图则最坏
// 情况是冷层落后（展示上暂时不一致），而安全判定已经生效。
func (r *Registry) SetCertificateStatus(ctx context.Context, fp, status, reason string) error {
	if err := ValidateFingerprint(fp); err != nil {
		return err
	}
	if err := ValidateStatus(status); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	rec, err := r.getCertRecordLocked(fp)
	if err != nil {
		return err
	}
	rec.Status = status
	rec.RevokeReason = reason
	rec.Updated = r.now()

	return r.applyCertLocked(ctx, rec, true)
}

// certCmdLocked 把证书记录翻译成一条热视图命令（不带冷层记录）。
//
// 命令的构造与"这条命令往哪去"分开，是因为有两个去处：控制面的写入要
// 经复制日志，而重建热视图一条日志都不该写（HydrateHot）。两处各构造
// 一遍的结局是字段漏一个而只在其中一条路上漏——那种不一致在重建之后
// 才显形。
func (r *Registry) certCmdLocked(rec *certRecord) (meta.Command, error) {
	ps := meta.NoPolicySet
	if rec.PolicyName != "" {
		id, _, ok := r.policyEntryLocked(rec.PolicyName)
		if !ok {
			return meta.Command{}, fmt.Errorf("registry: 证书 %s 绑定的策略 %q 不存在",
				shortFP(rec.Fingerprint), rec.PolicyName)
		}
		ps = id
	}
	fp, err := parseFingerprint(rec.Fingerprint)
	if err != nil {
		return meta.Command{}, err
	}
	return meta.Command{
		Kind: meta.CmdSetCert,
		Cert: &meta.CertCmd{
			Fingerprint: fp, Status: statusToMeta(rec.Status),
			PolicySet: ps, Thing: rec.Thing, Reason: rec.RevokeReason,
		},
	}, nil
}

// emitCertLocked 把证书记录交给重建回调，不写冷层也不提日志。
func (r *Registry) emitCertLocked(apply func(meta.Command) error, rec *certRecord) error {
	cmd, err := r.certCmdLocked(rec)
	if err != nil {
		return err
	}
	return apply(cmd)
}

// applyCertLocked 把证书记录写进热视图，withRecord 为真时同一条命令
// 带上冷层记录。
func (r *Registry) applyCertLocked(ctx context.Context, rec *certRecord, withRecord bool) error {
	cmd, err := r.certCmdLocked(rec)
	if err != nil {
		return err
	}
	if withRecord {
		op, err := setOp(store.Key(store.PrefixCert, rec.Fingerprint), rec)
		if err != nil {
			return err
		}
		cmd.Records = []meta.RecordOp{op}
	}
	if _, err := r.meta.Apply(ctx, cmd); err != nil {
		return fmt.Errorf("registry: 写入证书热视图失败: %w", err)
	}
	return nil
}

// GetCertificate 读取一张证书。
func (r *Registry) GetCertificate(fp string) (*Certificate, error) {
	rec, err := r.getCertRecordLocked(fp)
	if err != nil {
		return nil, err
	}
	return &Certificate{
		Fingerprint: rec.Fingerprint, Status: rec.Status, Thing: rec.Thing,
		PolicyName: rec.PolicyName, Subject: rec.Subject, Issuer: rec.Issuer,
		NotAfterUnixMs: rec.NotAfter, RevokeReason: rec.RevokeReason,
		CreatedAtUnixMs: rec.Created, UpdatedAtUnixMs: rec.Updated,
		PEM: rec.PEM,
	}, nil
}

// SetCertificatePolicy 改写证书上的那一份策略。空字符串表示解除。
//
// 已经是这个策略时不做写入：空跑会产生一条没有变化的变更记录。
func (r *Registry) SetCertificatePolicy(ctx context.Context, fp, policy string) error {
	if err := ValidateFingerprint(fp); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, err := r.getCertRecordLocked(fp)
	if err != nil {
		return err
	}
	if policy != "" {
		if _, _, ok := r.policyEntryLocked(policy); !ok {
			return fmt.Errorf("registry: 策略 %q 不存在；请先创建它", policy)
		}
	}
	if rec.PolicyName == policy {
		return nil
	}
	rec.PolicyName = policy
	rec.Updated = r.now()
	return r.applyCertLocked(ctx, rec, true)
}

func (r *Registry) getCertRecordLocked(fp string) (*certRecord, error) {
	raw, err := r.kv.Get(store.Key(store.PrefixCert, fp))
	if errors.Is(err, store.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var rec certRecord
	if derr := decodeJSON(raw, &rec); derr != nil {
		return nil, fmt.Errorf("registry: 证书记录损坏 %s: %w", shortFP(fp), derr)
	}
	return &rec, nil
}

// DeleteCertificate 删除一张证书的登记。
//
// 删除之前先把它置为 REVOKED 并踢掉在用连接：仅删除登记不会触发
// 「变为不可连接」的转换，于是在用的连接会一直留着，
// 直到它自己断开——那等于一次未完成的吊销（PRD PKI-030）。
func (r *Registry) DeleteCertificate(ctx context.Context, fp string) error {
	if err := ValidateFingerprint(fp); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	rec, err := r.getCertRecordLocked(fp)
	if err != nil {
		return err
	}
	if rec.Status != StatusRevoked {
		rec.Status = StatusRevoked
		rec.RevokeReason = "证书登记被删除"
		if err := r.applyCertLocked(ctx, rec, true); err != nil {
			return err
		}
	}
	mfp, err := parseFingerprint(fp)
	if err != nil {
		return err
	}
	if _, err := r.meta.Apply(ctx, meta.Command{
		Kind: meta.CmdDeleteCert, Cert: &meta.CertCmd{Fingerprint: mfp},
		Records: []meta.RecordOp{deleteOp(store.Key(store.PrefixCert, fp))},
	}); err != nil {
		return fmt.Errorf("registry: 删除证书热视图失败: %w", err)
	}
	return nil
}

// ListCertificates 分页列举证书。
func (r *Registry) ListCertificates(q CertQuery) (*CertPage, error) {
	page := &CertPage{}
	limit := q.limit()
	err := store.ScanPrefix(r.kv, store.PrefixCert, "", func(parts []string, value []byte) bool {
		fp := parts[0]
		if fp <= q.After {
			return true
		}
		var rec certRecord
		if decodeJSON(value, &rec) != nil {
			return true
		}
		if q.Status != "" && rec.Status != q.Status {
			return true
		}
		if q.Thing != "" && rec.Thing != q.Thing {
			return true
		}
		if len(page.Certificates) == limit {
			page.NextAfter = page.Certificates[len(page.Certificates)-1].Fingerprint
			return false
		}
		page.Certificates = append(page.Certificates, Certificate{
			Fingerprint: rec.Fingerprint, Status: rec.Status, Thing: rec.Thing,
			PolicyName: rec.PolicyName, Subject: rec.Subject, Issuer: rec.Issuer,
			NotAfterUnixMs: rec.NotAfter, RevokeReason: rec.RevokeReason,
			CreatedAtUnixMs: rec.Created, UpdatedAtUnixMs: rec.Updated,
		})
		return true
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(page.Certificates, func(i, j int) bool {
		return page.Certificates[i].Fingerprint < page.Certificates[j].Fingerprint
	})
	return page, nil
}

// CertPage 是一页证书。
type CertPage struct {
	Certificates []Certificate `json:"certificates"`
	NextAfter    string        `json:"nextAfter,omitempty"`
}

// CertQuery 是证书列举的筛选条件。
type CertQuery struct {
	Status string
	Thing  string
	After  string
	Limit  int
}

func (q CertQuery) limit() int {
	if q.Limit <= 0 {
		return DefaultPageLimit
	}
	if q.Limit > MaxPageLimit {
		return MaxPageLimit
	}
	return q.Limit
}

// certsOfThingLocked 找出绑定某设备的证书指纹，最多返回 limit 个。
func (r *Registry) certsOfThingLocked(thing string, limit int) ([]string, error) {
	var out []string
	err := store.ScanPrefix(r.kv, store.PrefixCert, "", func(parts []string, value []byte) bool {
		var rec certRecord
		if decodeJSON(value, &rec) == nil && rec.Thing == thing {
			out = append(out, parts[0])
		}
		return len(out) < limit
	})
	return out, err
}

// CertificatesOfThing 列出绑定某设备的证书指纹。
func (r *Registry) CertificatesOfThing(thing string) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.certsOfThingLocked(thing, MaxPageLimit)
}

// statusToMeta 把字符串状态映射到热视图的枚举。
//
// 用显式映射而不是枚举的 String 反查：状态名是写进 API 与存储的契约，
// 它不应随内部枚举的 String 实现变化而改变。
func statusToMeta(s string) meta.CertStatus {
	switch s {
	case StatusActive:
		return meta.CertActive
	case StatusInactive:
		return meta.CertInactive
	case StatusRevoked:
		return meta.CertRevoked
	case StatusPendingActivation:
		return meta.CertPendingActivation
	}
	// 未知状态按最保守的方式处理：不可连接。
	// 返回 CertActive 会让一个拼错的状态值变成放行。
	return meta.CertInactive
}

func parseFingerprint(s string) (meta.CertFingerprint, error) {
	var fp meta.CertFingerprint
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != len(fp) {
		return fp, fmt.Errorf("registry: 指纹 %q 无效", s)
	}
	copy(fp[:], b)
	return fp, nil
}
