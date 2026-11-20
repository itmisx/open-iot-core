package registry

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/authz"
	"github.com/itmisx/open-iot-core/internal/meta"
	"github.com/itmisx/open-iot-core/internal/store"
)

// TestRegisterCARequiresProof 验证登记 CA 必须交出由它签发、使用者等于注册码的证书。
//
// 去掉使用者比对或签名校验之后，任何人拿一张 CA 证书就能登记，
// 随后用它注册设备证书。
func TestRegisterCARequiresProof(t *testing.T) {
	g := newCARig(t, 10)
	code, err := g.r.RegistrationCode(ctx)
	if err != nil || code == "" {
		t.Fatal(err)
	}
	caPEM, caKey := issueCA(t, "factory")
	if _, err := g.r.RegisterCA(ctx, caPEM, issueLeaf(t, mustCert(t, caPEM), caKey, "not-the-code"), true); err == nil {
		t.Fatal("使用者不是注册码时应当拒绝")
	}
	otherPEM, otherKey := issueCA(t, "other")
	if _, err := g.r.RegisterCA(ctx, caPEM, issueLeaf(t, mustCert(t, otherPEM), otherKey, code), true); err == nil {
		t.Fatal("不是这张 CA 签发的验证证书应当拒绝")
	}
	got, err := g.r.RegisterCA(ctx, caPEM, issueLeaf(t, mustCert(t, caPEM), caKey, code), true)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusActive || got.ID == "" {
		t.Fatalf("登记结果 = %+v", got)
	}

	leaf := issueLeaf(t, mustCert(t, caPEM), caKey, "device")
	cert, err := g.r.RegisterExternalCert(leaf, caPEM, "")
	if err != nil {
		t.Fatal(err)
	}
	if cert.Fingerprint == "" {
		t.Fatal("注册后没有证书编号")
	}

	inactivePEM, inactiveKey := issueCA(t, "idle")
	idle, err := g.r.RegisterCA(ctx, inactivePEM, issueLeaf(t, mustCert(t, inactivePEM), inactiveKey, code), false)
	if err != nil {
		t.Fatal(err)
	}
	if idle.Status != StatusInactive {
		t.Fatalf("未激活的 CA 状态 = %s", idle.Status)
	}
	if _, err := g.r.RegisterExternalCert(
		issueLeaf(t, mustCert(t, inactivePEM), inactiveKey, "dev2"), inactivePEM, "",
	); err == nil {
		t.Fatal("INACTIVE 的 CA 不该能注册设备证书")
	}
	alone := issueLeaf(t, mustCert(t, otherPEM), otherKey, "solo")
	if _, err := g.r.RegisterExternalCert(alone, "", ""); err != nil {
		t.Fatal(err)
	}
}

// TestCASubjectLimit 验证同一 subject 的 CA 有上限。
//
// 上限判断若被拿掉，同一张 subject 可以无限登记。
func TestCASubjectLimit(t *testing.T) {
	g := newCARig(t, 2)
	code, err := g.r.RegistrationCode(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		pemText, key := issueCA(t, "factory")
		if _, err := g.r.RegisterCA(ctx, pemText, issueLeaf(t, mustCert(t, pemText), key, code), true); err != nil {
			t.Fatal(err)
		}
	}
	pemText, key := issueCA(t, "factory")
	_, err = g.r.RegisterCA(ctx, pemText, issueLeaf(t, mustCert(t, pemText), key, code), true)
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("第三张同 subject 的 CA = %v", err)
	}
}

func newCARig(t *testing.T, limit int) *rig {
	t.Helper()
	g := &rig{t: t, ms: testNowMs}
	g.kv = store.NewMemKV()
	g.meta = meta.NewLocalStore(meta.LocalOptions{
		Now: g.now, ChangeBuffer: 64, Cold: g.kv,
		Compile: testCompile(authz.DefaultEnv()),
	})
	r, err := New(Options{
		KV: g.kv, Meta: g.meta, Env: authz.DefaultEnv(), Now: g.now,
		CASubjectLimit: limit,
	})
	if err != nil {
		t.Fatal(err)
	}
	g.r = r
	t.Cleanup(func() {
		_ = g.meta.Close()
		_ = g.kv.Close()
	})
	return g
}

func issueCA(t *testing.T, cn string) (string, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), key
}

func issueLeaf(t *testing.T, parent *x509.Certificate, key *ecdsa.PrivateKey, cn string) string {
	t.Helper()
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

func mustCert(t *testing.T, pemText string) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode([]byte(pemText))
	if block == nil {
		t.Fatal("不是 PEM")
	}
	c, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
