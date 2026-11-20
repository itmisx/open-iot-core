package awsapi

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/itmisx/open-iot-core/internal/registry"
	"github.com/itmisx/open-iot-core/internal/trace"
)

// IssuedCert 是一次签发的结果。私钥只在这一次出现。
type IssuedCert struct {
	Fingerprint    string
	CertPEM        []byte
	KeyPEM         []byte
	PublicPEM      []byte
	Subject        string
	Issuer         string
	NotAfterUnixMs int64
}

// CertIssuer 用本节点的 CA 签发。为空表示没有内嵌 CA。
type CertIssuer interface {
	IssueKeyPair(commonName string) (IssuedCert, error)
	SignCSR(csrPEM []byte, commonName string) (IssuedCert, error)
}

func (h Handler) arn(kind, id string) string {
	return "arn:" + h.Partition + ":iot:" + h.Region + ":" + h.Account + ":" + kind + "/" + id
}

func (h Handler) parseTarget(s string) (kind, id string, ok bool) {
	prefix := "arn:" + h.Partition + ":iot:" + h.Region + ":" + h.Account + ":"
	rest, found := strings.CutPrefix(s, prefix)
	if !found {
		return "", "", false
	}
	kind, id, found = strings.Cut(rest, "/")
	if !found || id == "" || strings.Contains(id, "/") {
		return "", "", false
	}
	if kind != "cert" && kind != "thing" {
		return "", "", false
	}
	return kind, id, true
}

func (h Handler) createPolicy(w http.ResponseWriter, r *http.Request) {
	var in struct {
		PolicyDocument string `json:"policyDocument"`
		Tags           []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"tags"`
	}
	if !decodeBody(w, r, &in) {
		return
	}
	if len(in.Tags) > 0 {
		writeAWSError(w, "InvalidRequestException", "不支持标签")
		return
	}
	name := r.PathValue("policyName")
	if err := h.Things.CreatePolicy(r.Context(), name, []byte(in.PolicyDocument)); err != nil {
		writeRegistry(w, err)
		return
	}
	h.note(r, "CreatePolicy", name)
	writeJSON(w, http.StatusOK, map[string]string{
		"policyName": name, "policyArn": h.arn("policy", name),
		"policyDocument": in.PolicyDocument, "policyVersionId": "1",
	})
}

func (h Handler) getPolicy(w http.ResponseWriter, r *http.Request) {
	p, err := h.Things.GetPolicy(r.PathValue("policyName"))
	if err != nil {
		writeRegistry(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"policyName": p.Name, "policyArn": h.arn("policy", p.Name),
		"policyDocument": string(p.Document), "defaultVersionId": p.DefaultVersion,
	})
}

func (h Handler) deletePolicy(w http.ResponseWriter, r *http.Request) {
	if err := h.Things.DeletePolicy(r.Context(), r.PathValue("policyName")); err != nil {
		writeRegistry(w, err)
		return
	}
	h.note(r, "DeletePolicy", r.PathValue("policyName"))
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (h Handler) listPolicies(w http.ResponseWriter, r *http.Request) {
	if msg := rejectDescending(r.URL.Query()); msg != "" {
		writeAWSError(w, "InvalidRequestException", msg)
		return
	}
	limit, msg := limitParam(r.URL.Query(), "pageSize")
	if msg != "" {
		writeAWSError(w, "InvalidRequestException", msg)
		return
	}
	names, err := h.Things.ListPolicies()
	if err != nil {
		writeRegistry(w, err)
		return
	}
	page, next := pageAfter(names, r.URL.Query().Get("marker"), limit)
	out := make([]map[string]string, 0, len(page))
	for _, name := range page {
		out = append(out, map[string]string{
			"policyName": name, "policyArn": h.arn("policy", name),
		})
	}
	resp := map[string]any{"policies": out}
	if next != "" {
		resp["nextMarker"] = next
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h Handler) createPolicyVersion(w http.ResponseWriter, r *http.Request) {
	var in struct {
		PolicyDocument string `json:"policyDocument"`
	}
	if !decodeBody(w, r, &in) {
		return
	}
	setDefault, msg := queryBool(r.URL.Query(), "setAsDefault", false)
	if msg != "" {
		writeAWSError(w, "InvalidRequestException", msg)
		return
	}
	name := r.PathValue("policyName")
	id, err := h.Things.CreatePolicyVersion(r.Context(), name, []byte(in.PolicyDocument), setDefault)
	if err != nil {
		writeRegistry(w, err)
		return
	}
	h.note(r, "CreatePolicyVersion", name+"/"+id)
	writeJSON(w, http.StatusOK, map[string]any{
		"policyArn": h.arn("policy", name), "policyDocument": in.PolicyDocument,
		"policyVersionId": id, "isDefaultVersion": setDefault,
	})
}

func (h Handler) getPolicyVersion(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("policyName")
	v, err := h.Things.GetPolicyVersion(name, r.PathValue("policyVersionId"))
	if err != nil {
		writeRegistry(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"policyName": name, "policyArn": h.arn("policy", name),
		"policyDocument": string(v.Document), "policyVersionId": v.ID,
		"isDefaultVersion": v.IsDefault, "creationDate": unixSec(v.CreatedAtUnixMs),
	})
}

func (h Handler) listPolicyVersions(w http.ResponseWriter, r *http.Request) {
	vs, err := h.Things.ListPolicyVersions(r.PathValue("policyName"))
	if err != nil {
		writeRegistry(w, err)
		return
	}
	out := make([]map[string]any, 0, len(vs))
	for _, v := range vs {
		out = append(out, map[string]any{
			"versionId": v.ID, "isDefaultVersion": v.IsDefault,
			"createDate": unixSec(v.CreatedAtUnixMs),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"policyVersions": out})
}

func (h Handler) deletePolicyVersion(w http.ResponseWriter, r *http.Request) {
	err := h.Things.DeletePolicyVersion(r.PathValue("policyName"), r.PathValue("policyVersionId"))
	if err != nil {
		writeRegistry(w, err)
		return
	}
	h.note(r, "DeletePolicyVersion", r.PathValue("policyName")+"/"+r.PathValue("policyVersionId"))
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (h Handler) setDefaultPolicyVersion(w http.ResponseWriter, r *http.Request) {
	err := h.Things.SetDefaultPolicyVersion(r.Context(),
		r.PathValue("policyName"), r.PathValue("policyVersionId"))
	if err != nil {
		writeRegistry(w, err)
		return
	}
	h.note(r, "SetDefaultPolicyVersion", r.PathValue("policyName")+"/"+r.PathValue("policyVersionId"))
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (h Handler) attachPolicy(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Target string `json:"target"`
	}
	if !decodeBody(w, r, &in) {
		return
	}
	h.bindPolicy(w, r, in.Target, r.PathValue("policyName"), true)
}

func (h Handler) detachPolicy(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Target string `json:"target"`
	}
	if !decodeBody(w, r, &in) {
		return
	}
	h.bindPolicy(w, r, in.Target, r.PathValue("policyName"), false)
}

func (h Handler) bindPolicy(w http.ResponseWriter, r *http.Request, target, policy string, attach bool) {
	kind, id, ok := h.parseTarget(target)
	if !ok {
		writeAWSError(w, "InvalidRequestException", "target 须是本节点的证书或设备 ARN")
		return
	}
	switch kind {
	case "cert":
		cur, err := h.Things.GetCertificate(id)
		if err != nil {
			writeRegistry(w, err)
			return
		}
		next := ""
		if attach {
			if cur.PolicyName != "" && cur.PolicyName != policy {
				writeAWSError(w, "LimitExceededException", "每个证书只能附加一份策略")
				return
			}
			next = policy
		} else if cur.PolicyName != policy {
			writeAWSError(w, "ResourceNotFoundException", "策略未附加到该证书")
			return
		}
		if err := h.Things.SetCertificatePolicy(r.Context(), id, next); err != nil {
			writeRegistry(w, err)
			return
		}
	case "thing":
		cur, err := h.Things.GetThing(id)
		if err != nil {
			writeRegistry(w, err)
			return
		}
		next := ""
		if attach {
			if cur.PolicyName != "" && cur.PolicyName != policy {
				writeAWSError(w, "LimitExceededException", "每台设备只能附加一份策略")
				return
			}
			next = policy
		} else if cur.PolicyName != policy {
			writeAWSError(w, "ResourceNotFoundException", "策略未附加到该设备")
			return
		}
		if err := h.Things.SetThingPolicy(r.Context(), id, next); err != nil {
			writeRegistry(w, err)
			return
		}
	}
	action := "DetachPolicy"
	if attach {
		action = "AttachPolicy"
	}
	h.note(r, action, policy+" -> "+kind+"/"+id)
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (h Handler) listAttachedPolicies(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Get("recursive") == "true" {
		writeAWSError(w, "InvalidRequestException", "不支持沿设备组继承策略")
		return
	}
	if q.Has("recursive") && q.Get("recursive") != "false" && q.Get("recursive") != "true" {
		writeAWSError(w, "InvalidRequestException", "recursive 只能是 true 或 false")
		return
	}
	kind, id, ok := h.parseTarget(r.PathValue("target"))
	if !ok {
		writeAWSError(w, "InvalidRequestException", "target 须是本节点的证书或设备 ARN")
		return
	}
	var name string
	switch kind {
	case "cert":
		cur, err := h.Things.GetCertificate(id)
		if err != nil {
			writeRegistry(w, err)
			return
		}
		name = cur.PolicyName
	case "thing":
		cur, err := h.Things.GetThing(id)
		if err != nil {
			writeRegistry(w, err)
			return
		}
		name = cur.PolicyName
	}
	policies := []map[string]string{}
	if name != "" {
		policies = append(policies, map[string]string{
			"policyName": name, "policyArn": h.arn("policy", name),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"policies": policies})
}

func (h Handler) listTargetsForPolicy(w http.ResponseWriter, r *http.Request) {
	limit, msg := limitParam(r.URL.Query(), "pageSize")
	if msg != "" {
		writeAWSError(w, "InvalidRequestException", msg)
		return
	}
	targets, next, err := h.Things.ListPolicyTargets(
		r.PathValue("policyName"), r.URL.Query().Get("marker"), limit)
	if err != nil {
		writeRegistry(w, err)
		return
	}
	out := make([]string, 0, len(targets))
	for _, t := range targets {
		out = append(out, h.arn(t.Kind, t.ID))
	}
	resp := map[string]any{"targets": out}
	if next != "" {
		resp["nextMarker"] = next
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h Handler) attachThingPrincipal(w http.ResponseWriter, r *http.Request) {
	h.setThingPrincipal(w, r, true)
}

func (h Handler) detachThingPrincipal(w http.ResponseWriter, r *http.Request) {
	h.setThingPrincipal(w, r, false)
}

func (h Handler) setThingPrincipal(w http.ResponseWriter, r *http.Request, attach bool) {
	if t := r.URL.Query().Get("thingPrincipalType"); t != "" && t != "EXCLUSIVE_THING" {
		writeAWSError(w, "InvalidRequestException", "不支持 NON_EXCLUSIVE_THING：一张证书只绑定一台设备")
		return
	}
	fp, ok := h.certPrincipal(w, r)
	if !ok {
		return
	}
	thing := r.PathValue("thingName")
	var err error
	if attach {
		err = h.Things.AttachThingPrincipal(r.Context(), thing, fp)
	} else {
		err = h.Things.DetachThingPrincipal(r.Context(), thing, fp)
	}
	if err != nil {
		writeRegistry(w, err)
		return
	}
	action := "DetachThingPrincipal"
	if attach {
		action = "AttachThingPrincipal"
	}
	h.note(r, action, fp+" -> "+thing)
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (h Handler) listThingPrincipals(w http.ResponseWriter, r *http.Request) {
	thing := r.PathValue("thingName")
	if _, err := h.Things.GetThing(thing); err != nil {
		writeRegistry(w, err)
		return
	}
	limit, msg := limitParam(r.URL.Query(), "maxResults")
	if msg != "" {
		writeAWSError(w, "InvalidRequestException", msg)
		return
	}
	page, err := h.Things.ListCertificates(registry.CertQuery{
		Thing: thing, After: r.URL.Query().Get("nextToken"), Limit: limit,
	})
	if err != nil {
		writeRegistry(w, err)
		return
	}
	out := make([]string, 0, len(page.Certificates))
	for _, c := range page.Certificates {
		out = append(out, h.arn("cert", c.Fingerprint))
	}
	resp := map[string]any{"principals": out}
	if page.NextAfter != "" {
		resp["nextToken"] = page.NextAfter
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h Handler) listPrincipalThings(w http.ResponseWriter, r *http.Request) {
	fp, ok := h.certPrincipal(w, r)
	if !ok {
		return
	}
	limit, msg := limitParam(r.URL.Query(), "maxResults")
	if msg != "" {
		writeAWSError(w, "InvalidRequestException", msg)
		return
	}
	names, err := h.Things.ListPrincipalThings(fp)
	if err != nil {
		writeRegistry(w, err)
		return
	}
	token := r.URL.Query().Get("nextToken")
	var page []string
	next := ""
	for _, name := range names {
		if name <= token {
			continue
		}
		if len(page) == limit {
			next = page[len(page)-1]
			break
		}
		page = append(page, name)
	}
	if page == nil {
		page = []string{}
	}
	resp := map[string]any{"things": page}
	if next != "" {
		resp["nextToken"] = next
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h Handler) certPrincipal(w http.ResponseWriter, r *http.Request) (string, bool) {
	raw := r.Header.Get("x-amzn-principal")
	if raw == "" {
		writeAWSError(w, "InvalidRequestException", "缺少 x-amzn-principal")
		return "", false
	}
	kind, id, ok := h.parseTarget(raw)
	if !ok || kind != "cert" {
		writeAWSError(w, "InvalidRequestException", "principal 须是本节点的证书 ARN")
		return "", false
	}
	return id, true
}

func (h Handler) createKeys(w http.ResponseWriter, r *http.Request) {
	h.issue(w, r, nil)
}

func (h Handler) createFromCSR(w http.ResponseWriter, r *http.Request) {
	var in struct {
		CertificateSigningRequest string `json:"certificateSigningRequest"`
	}
	if !decodeBody(w, r, &in) {
		return
	}
	if in.CertificateSigningRequest == "" {
		writeAWSError(w, "InvalidRequestException", "缺少 certificateSigningRequest")
		return
	}
	h.issue(w, r, []byte(in.CertificateSigningRequest))
}

func (h Handler) issue(w http.ResponseWriter, r *http.Request, csr []byte) {
	if h.Issuer == nil {
		writeAWSError(w, "ServiceUnavailableException", "未启用内嵌 PKI，无法签发")
		return
	}
	active, msg := queryBool(r.URL.Query(), "setAsActive", true)
	if msg != "" {
		writeAWSError(w, "InvalidRequestException", msg)
		return
	}
	cn, err := newCertCN()
	if err != nil {
		writeAWSError(w, "InternalFailureException", "内部错误")
		return
	}
	ctx, sp := trace.Start(r.Context(), "pki.issue")
	defer sp.End()
	var issued IssuedCert
	if csr == nil {
		issued, err = h.Issuer.IssueKeyPair(cn)
	} else {
		issued, err = h.Issuer.SignCSR(csr, cn)
	}
	if err != nil {
		sp.SetError()
		writeAWSError(w, "InvalidRequestException", err.Error())
		return
	}
	sp.Set("cert.fingerprint", issued.Fingerprint)
	status := registry.StatusActive
	if !active {
		status = registry.StatusInactive
	}
	err = h.Things.RegisterCertificate(ctx, registry.Certificate{
		Fingerprint: issued.Fingerprint, Status: status,
		Subject: issued.Subject, Issuer: issued.Issuer,
		NotAfterUnixMs: issued.NotAfterUnixMs, PEM: string(issued.CertPEM),
	})
	if err != nil {
		sp.SetError()
		writeRegistry(w, err)
		return
	}
	resp := map[string]any{
		"certificateArn": h.arn("cert", issued.Fingerprint),
		"certificateId":  issued.Fingerprint,
		"certificatePem": string(issued.CertPEM),
	}
	if csr == nil {
		resp["keyPair"] = map[string]string{
			"PublicKey":  string(issued.PublicPEM),
			"PrivateKey": string(issued.KeyPEM),
		}
	}
	action := "CreateCertificateFromCsr"
	if csr == nil {
		action = "CreateKeysAndCertificate"
	}
	h.note(r, action, issued.Fingerprint)
	writeJSON(w, http.StatusOK, resp)
}

func (h Handler) describeCertificate(w http.ResponseWriter, r *http.Request) {
	c, err := h.Things.GetCertificate(r.PathValue("certificateId"))
	if err != nil {
		writeRegistry(w, err)
		return
	}
	desc := map[string]any{
		"certificateArn":   h.arn("cert", c.Fingerprint),
		"certificateId":    c.Fingerprint,
		"status":           c.Status,
		"certificatePem":   c.PEM,
		"ownedBy":          h.Account,
		"creationDate":     unixSec(c.CreatedAtUnixMs),
		"lastModifiedDate": unixSec(c.UpdatedAtUnixMs),
	}
	if c.NotAfterUnixMs > 0 {
		desc["validity"] = map[string]float64{"notAfter": unixSec(c.NotAfterUnixMs)}
	}
	writeJSON(w, http.StatusOK, map[string]any{"certificateDescription": desc})
}

func (h Handler) updateCertificate(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("newStatus")
	if status == "" {
		writeAWSError(w, "InvalidRequestException", "缺少 newStatus")
		return
	}
	reason := ""
	if status == registry.StatusRevoked {
		reason = "控制面更新状态"
	}
	if err := h.Things.SetCertificateStatus(r.Context(), r.PathValue("certificateId"), status, reason); err != nil {
		writeRegistry(w, err)
		return
	}
	h.note(r, "UpdateCertificate", r.PathValue("certificateId")+" -> "+status)
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (h Handler) deleteCertificate(w http.ResponseWriter, r *http.Request) {
	force, msg := queryBool(r.URL.Query(), "forceDelete", false)
	if msg != "" {
		writeAWSError(w, "InvalidRequestException", msg)
		return
	}
	fp := r.PathValue("certificateId")
	cur, err := h.Things.GetCertificate(fp)
	if err != nil {
		writeRegistry(w, err)
		return
	}
	if !force {
		if cur.Status == registry.StatusActive {
			writeAWSError(w, "InvalidRequestException", "活动证书须先停用，或带上 forceDelete")
			return
		}
		if cur.Thing != "" || cur.PolicyName != "" {
			writeAWSError(w, "InvalidRequestException", "证书仍关联设备或策略，或带上 forceDelete")
			return
		}
	}
	if err := h.Things.DeleteCertificate(r.Context(), fp); err != nil {
		writeRegistry(w, err)
		return
	}
	h.note(r, "DeleteCertificate", fp)
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (h Handler) listCertificates(w http.ResponseWriter, r *http.Request) {
	if msg := rejectDescending(r.URL.Query()); msg != "" {
		writeAWSError(w, "InvalidRequestException", msg)
		return
	}
	limit, msg := limitParam(r.URL.Query(), "pageSize")
	if msg != "" {
		writeAWSError(w, "InvalidRequestException", msg)
		return
	}
	page, err := h.Things.ListCertificates(registry.CertQuery{
		After: r.URL.Query().Get("marker"), Limit: limit,
	})
	if err != nil {
		writeRegistry(w, err)
		return
	}
	out := make([]map[string]any, 0, len(page.Certificates))
	for _, c := range page.Certificates {
		out = append(out, map[string]any{
			"certificateArn": h.arn("cert", c.Fingerprint),
			"certificateId":  c.Fingerprint,
			"status":         c.Status,
			"creationDate":   unixSec(c.CreatedAtUnixMs),
		})
	}
	resp := map[string]any{"certificates": out}
	if page.NextAfter != "" {
		resp["nextMarker"] = page.NextAfter
	}
	writeJSON(w, http.StatusOK, resp)
}

func newCertCN() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "cert-" + hex.EncodeToString(b[:]), nil
}

func limitParam(q url.Values, key string) (int, string) {
	if !q.Has(key) {
		return 25, ""
	}
	n, err := strconv.Atoi(q.Get(key))
	if err != nil || n < 1 || n > maxPage {
		return 0, key + " 须在 1 到 250 之间"
	}
	return n, ""
}

func queryBool(q url.Values, key string, def bool) (bool, string) {
	if !q.Has(key) {
		return def, ""
	}
	switch q.Get(key) {
	case "true":
		return true, ""
	case "false":
		return false, ""
	default:
		return false, key + " 只能是 true 或 false"
	}
}

func rejectDescending(q url.Values) string {
	if !q.Has("isAscendingOrder") {
		return ""
	}
	switch q.Get("isAscendingOrder") {
	case "true":
		return ""
	case "false":
		return "本节点只按主键升序排列"
	default:
		return "isAscendingOrder 只能是 true 或 false"
	}
}

func pageAfter(sorted []string, marker string, limit int) (page []string, next string) {
	for _, s := range sorted {
		if s <= marker {
			continue
		}
		if len(page) == limit {
			next = page[len(page)-1]
			return
		}
		page = append(page, s)
	}
	return
}

func unixSec(ms int64) float64 { return float64(ms) / 1000 }
