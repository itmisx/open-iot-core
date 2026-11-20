package server

import (
	"crypto/x509"
	"encoding/pem"

	"github.com/itmisx/open-iot-core/internal/awsapi"
	"github.com/itmisx/open-iot-core/internal/pki"
)

// caIssuer 把内嵌 CA 适配成兼容面的签发接口。
//
// 私钥只放进返回值。签发策略要求主题来自 CSR 时，这里不再塞一个
// 本节点生成的通用名，否则那条策略等于没生效。
type caIssuer struct {
	ca *pki.CA
}

func (c caIssuer) IssueKeyPair(cn string) (awsapi.IssuedCert, error) {
	issued, err := c.ca.IssueKeyPair(pki.IssueRequest{CommonName: cn})
	if err != nil {
		return awsapi.IssuedCert{}, err
	}
	return issuedCert(issued)
}

func (c caIssuer) SignCSR(csrPEM []byte, cn string) (awsapi.IssuedCert, error) {
	if c.ca.Policy().SubjectSource == pki.SubjectFromCSR {
		cn = ""
	}
	issued, err := c.ca.SignCSR(csrPEM, pki.IssueRequest{CommonName: cn})
	if err != nil {
		return awsapi.IssuedCert{}, err
	}
	return issuedCert(issued)
}

func issuedCert(in *pki.Issued) (awsapi.IssuedCert, error) {
	pub, err := marshalPublicPEM(in.Cert)
	if err != nil {
		return awsapi.IssuedCert{}, err
	}
	return awsapi.IssuedCert{
		Fingerprint:    in.Fingerprint.String(),
		CertPEM:        in.CertPEM,
		KeyPEM:         in.KeyPEM,
		PublicPEM:      pub,
		Subject:        in.Cert.Subject.CommonName,
		Issuer:         in.Cert.Issuer.String(),
		NotAfterUnixMs: in.NotAfter.UnixMilli(),
	}, nil
}

func marshalPublicPEM(cert *x509.Certificate) ([]byte, error) {
	der, err := x509.MarshalPKIXPublicKey(cert.PublicKey)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}), nil
}
