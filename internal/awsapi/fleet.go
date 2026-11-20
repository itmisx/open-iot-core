package awsapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/itmisx/open-iot-core/internal/quota"
	"github.com/itmisx/open-iot-core/internal/registry"
)

// 控制面后半：自有 CA、检索、角色别名。
//
// 这一片里 admin 和 operator 的分界是「动不动信任根」：注册、改、删自有
// CA 决定了往后哪些设备证书算数，发凭证同理，都给 admin；用已有的 CA
// 签一张设备证书是日常开通，给 operator。取注册码本身只是个串，但它只在
// 注册自有 CA 这套动作里有用，和那几条同级才说得清。
//
// 这些 action 的响应仍按兼容面的冻结形状，不随控制台改。
// 自动注册相关字段一律拒绝：那是已经删掉的开通路径，悄悄忽略会让调用方以为配上了。

func (h Handler) mountFleet(mux *http.ServeMux) {
	p := Prefix
	mux.HandleFunc("GET "+p+"/registrationcode", h.authed(RoleAdmin, h.getRegistrationCode))
	mux.HandleFunc("POST "+p+"/cacertificate", h.authed(RoleAdmin, h.registerCA))
	mux.HandleFunc("GET "+p+"/cacertificate/{certificateId}", h.authed(RoleReader, h.describeCA))
	mux.HandleFunc("PUT "+p+"/cacertificate/{certificateId}", h.authed(RoleAdmin, h.updateCA))
	mux.HandleFunc("DELETE "+p+"/cacertificate/{certificateId}", h.authed(RoleAdmin, h.deleteCA))
	mux.HandleFunc("GET "+p+"/cacertificates", h.authed(RoleReader, h.listCAs))
	mux.HandleFunc("POST "+p+"/certificate/register", h.authed(RoleOperator, h.registerCert))
	mux.HandleFunc("POST "+p+"/certificate/register-no-ca", h.authed(RoleOperator, h.registerCertNoCA))

	mux.HandleFunc("POST "+p+"/indices/search", h.authed(RoleReader, h.searchIndex))
	mux.HandleFunc("GET "+p+"/indexing/config", h.authed(RoleReader, h.getIndexing))
	mux.HandleFunc("PUT "+p+"/indexing/config", h.authed(RoleOperator, h.putIndexing))

	mux.HandleFunc("POST "+p+"/role-aliases/{roleAlias}", h.authed(RoleOperator, h.createRoleAlias))
	mux.HandleFunc("GET "+p+"/role-aliases/{roleAlias}", h.authed(RoleReader, h.describeRoleAlias))
	mux.HandleFunc("PUT "+p+"/role-aliases/{roleAlias}", h.authed(RoleOperator, h.updateRoleAlias))
	mux.HandleFunc("DELETE "+p+"/role-aliases/{roleAlias}", h.authed(RoleOperator, h.deleteRoleAlias))
	mux.HandleFunc("GET "+p+"/role-aliases", h.authed(RoleReader, h.listRoleAliases))
	mux.HandleFunc("GET "+p+"/role-aliases/{roleAlias}/credentials", h.authed(RoleAdmin, h.roleAliasCredentials))
}

// Limiter 判定证书注册速率。为空时不限速，单测可以不装配。
// Online 与 Shadow 供检索使用。为空时对应字段当不存在。
func (h Handler) allowRegister(w http.ResponseWriter, r *http.Request) bool {
	if h.Limiter == nil {
		return true
	}
	if h.Limiter.AllowGlobal(r.Context(), quota.CertRegisterRate, "", 1) {
		return true
	}
	writeAWSError(w, "ThrottlingException", "证书注册速率已用完")
	return false
}

func (h Handler) getRegistrationCode(w http.ResponseWriter, r *http.Request) {
	code, err := h.Things.RegistrationCode(r.Context())
	if err != nil {
		writeRegistry(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"registrationCode": code})
}

type registerCABody struct {
	CaCertificate           string          `json:"caCertificate"`
	VerificationCertificate string          `json:"verificationCertificate"`
	SetAsActive             bool            `json:"setAsActive"`
	AllowAutoRegistration   bool            `json:"allowAutoRegistration"`
	RegistrationConfig      json.RawMessage `json:"registrationConfig"`
}

func (h Handler) registerCA(w http.ResponseWriter, r *http.Request) {
	var in registerCABody
	if !decodeBody(w, r, &in) {
		return
	}
	if in.AllowAutoRegistration || len(in.RegistrationConfig) > 0 {
		writeAWSError(w, "InvalidRequestException", "不支持自动注册")
		return
	}
	if !h.allowRegister(w, r) {
		return
	}
	ca, err := h.Things.RegisterCA(r.Context(), in.CaCertificate, in.VerificationCertificate, in.SetAsActive)
	if err != nil {
		writeRegistry(w, err)
		return
	}
	h.note(r, "RegisterCACertificate", ca.ID)
	writeJSON(w, http.StatusOK, map[string]string{"certificateId": ca.ID})
}

func (h Handler) describeCA(w http.ResponseWriter, r *http.Request) {
	ca, err := h.Things.GetCA(r.PathValue("certificateId"))
	if err != nil {
		writeRegistry(w, err)
		return
	}
	writeJSON(w, http.StatusOK, caJSON(ca))
}

type updateCABody struct {
	NewStatus                 string `json:"newStatus"`
	NewAutoRegistrationStatus string `json:"newAutoRegistrationStatus"`
	RemoveAutoRegistration    bool   `json:"removeAutoRegistration"`
}

func (h Handler) updateCA(w http.ResponseWriter, r *http.Request) {
	var in updateCABody
	if !decodeBody(w, r, &in) {
		return
	}
	if in.NewAutoRegistrationStatus != "" || in.RemoveAutoRegistration {
		writeAWSError(w, "InvalidRequestException", "不支持自动注册")
		return
	}
	id := r.PathValue("certificateId")
	if err := h.Things.UpdateCAStatus(r.Context(), id, in.NewStatus); err != nil {
		writeRegistry(w, err)
		return
	}
	h.note(r, "UpdateCACertificate", id)
	writeJSON(w, http.StatusOK, map[string]string{"certificateId": id})
}

func (h Handler) deleteCA(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("certificateId")
	if err := h.Things.DeleteCA(r.Context(), id); err != nil {
		writeRegistry(w, err)
		return
	}
	h.note(r, "DeleteCACertificate", id)
	w.WriteHeader(http.StatusOK)
}

func (h Handler) listCAs(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("pageSize"))
	list, next, err := h.Things.ListCAs(r.URL.Query().Get("marker"), limit)
	if err != nil {
		writeRegistry(w, err)
		return
	}
	out := make([]map[string]any, 0, len(list))
	for i := range list {
		out = append(out, map[string]any{
			"certificateId": list[i].ID,
			"status":        list[i].Status,
			"creationDate":  rfc3339(list[i].Created),
		})
	}
	body := map[string]any{"certificates": out}
	if next != "" {
		body["nextMarker"] = next
	}
	writeJSON(w, http.StatusOK, body)
}

type registerCertBody struct {
	CertificatePem   string `json:"certificatePem"`
	CaCertificatePem string `json:"caCertificatePem"`
	Status           string `json:"status"`
}

func (h Handler) registerCert(w http.ResponseWriter, r *http.Request) {
	h.registerCertCommon(w, r, true)
}

func (h Handler) registerCertNoCA(w http.ResponseWriter, r *http.Request) {
	h.registerCertCommon(w, r, false)
}

func (h Handler) registerCertCommon(w http.ResponseWriter, r *http.Request, withCA bool) {
	var in registerCertBody
	if !decodeBody(w, r, &in) {
		return
	}
	if !withCA && in.CaCertificatePem != "" {
		writeAWSError(w, "InvalidRequestException", "无 CA 注册不接受 caCertificatePem")
		return
	}
	if withCA && in.CaCertificatePem == "" {
		writeAWSError(w, "InvalidRequestException", "缺少 caCertificatePem")
		return
	}
	if !h.allowRegister(w, r) {
		return
	}
	cert, err := h.Things.RegisterExternalCert(in.CertificatePem, in.CaCertificatePem, in.Status)
	if err != nil {
		writeRegistry(w, err)
		return
	}
	action := "RegisterCertificate"
	if !withCA {
		action = "RegisterCertificateWithoutCA"
	}
	h.note(r, action, cert.Fingerprint)
	writeJSON(w, http.StatusOK, map[string]string{"certificateId": cert.Fingerprint})
}

func caJSON(ca *registry.CACertificate) map[string]any {
	return map[string]any{
		"certificateId":    ca.ID,
		"certificatePem":   ca.PEM,
		"subject":          ca.Subject,
		"status":           ca.Status,
		"notAfter":         rfc3339(ca.NotAfter),
		"creationDate":     rfc3339(ca.Created),
		"lastModifiedDate": rfc3339(ca.Updated),
	}
}

func rfc3339(ms int64) string {
	if ms == 0 {
		return ""
	}
	return time.UnixMilli(ms).UTC().Format(time.RFC3339)
}

type searchBody struct {
	QueryString string `json:"queryString"`
	MaxResults  int    `json:"maxResults"`
	NextToken   string `json:"nextToken"`
}

func (h Handler) searchIndex(w http.ResponseWriter, r *http.Request) {
	var in searchBody
	if !decodeBody(w, r, &in) {
		return
	}
	cfg, err := h.Things.Indexing()
	if err != nil {
		writeRegistry(w, err)
		return
	}
	env := registry.SearchEnv{}
	if cfg.ConnMode == registry.ConnStatus && h.Online != nil {
		env.Online = h.Online
	}
	if cfg.ThingMode == registry.IndexShadow && h.Shadow != nil {
		env.Reported = func(thing string) (map[string]any, bool) {
			rep, _, ok := h.Shadow(thing)
			return rep, ok
		}
		env.Desired = func(thing string) (map[string]any, bool) {
			_, des, ok := h.Shadow(thing)
			return des, ok
		}
	}
	hits, next, err := h.Things.Search(in.QueryString, in.NextToken, in.MaxResults, env)
	if err != nil {
		if errors.Is(err, registry.ErrIndexOff) || errors.Is(err, registry.ErrBadQuery) {
			writeAWSError(w, "InvalidRequestException", err.Error())
			return
		}
		writeRegistry(w, err)
		return
	}
	things := make([]map[string]any, 0, len(hits))
	for _, hit := range hits {
		item := map[string]any{
			"thingName":  hit.Thing.Name,
			"thingId":    hit.Thing.Name,
			"attributes": hit.Thing.Attributes,
		}
		if item["attributes"] == nil {
			item["attributes"] = map[string]string{}
		}
		if hit.Connected != nil {
			item["connectivity"] = map[string]any{"connected": *hit.Connected}
		}
		if hit.Shadow != "" {
			item["shadow"] = hit.Shadow
		}
		things = append(things, item)
	}
	body := map[string]any{"things": things}
	if next != "" {
		body["nextToken"] = next
	}
	writeJSON(w, http.StatusOK, body)
}

type indexBody struct {
	ThingIndexingMode             string `json:"thingIndexingMode"`
	ThingConnectivityIndexingMode string `json:"thingConnectivityIndexingMode"`
}

func (h Handler) getIndexing(w http.ResponseWriter, r *http.Request) {
	cfg, err := h.Things.Indexing()
	if err != nil {
		writeRegistry(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"thingIndexingConfiguration": indexBody{
			ThingIndexingMode:             cfg.ThingMode,
			ThingConnectivityIndexingMode: cfg.ConnMode,
		},
	})
}

func (h Handler) putIndexing(w http.ResponseWriter, r *http.Request) {
	var in struct {
		ThingIndexingConfiguration indexBody `json:"thingIndexingConfiguration"`
	}
	if !decodeBody(w, r, &in) {
		return
	}
	err := h.Things.SetIndexing(r.Context(), registry.IndexConfig{
		ThingMode: in.ThingIndexingConfiguration.ThingIndexingMode,
		ConnMode:  in.ThingIndexingConfiguration.ThingConnectivityIndexingMode,
	})
	if err != nil {
		writeRegistry(w, err)
		return
	}
	h.note(r, "UpdateIndexingConfiguration", in.ThingIndexingConfiguration.ThingIndexingMode)
	w.WriteHeader(http.StatusOK)
}

type aliasBody struct {
	RoleArn                   string `json:"roleArn"`
	CredentialDurationSeconds int    `json:"credentialDurationSeconds"`
}

func (h Handler) createRoleAlias(w http.ResponseWriter, r *http.Request) {
	h.writeAlias(w, r, true)
}

func (h Handler) updateRoleAlias(w http.ResponseWriter, r *http.Request) {
	h.writeAlias(w, r, false)
}

func (h Handler) writeAlias(w http.ResponseWriter, r *http.Request, create bool) {
	var in aliasBody
	if !decodeBody(w, r, &in) {
		return
	}
	name := r.PathValue("roleAlias")
	alias, err := h.Things.PutRoleAlias(r.Context(), name, in.RoleArn, in.CredentialDurationSeconds, create)
	if err != nil {
		writeRegistry(w, err)
		return
	}
	action := "UpdateRoleAlias"
	if create {
		action = "CreateRoleAlias"
	}
	h.note(r, action, name)
	writeJSON(w, http.StatusOK, aliasJSON(alias))
}

func (h Handler) describeRoleAlias(w http.ResponseWriter, r *http.Request) {
	alias, err := h.Things.GetRoleAlias(r.PathValue("roleAlias"))
	if err != nil {
		writeRegistry(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"roleAliasDescription": aliasJSON(alias)})
}

func (h Handler) deleteRoleAlias(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("roleAlias")
	if err := h.Things.DeleteRoleAlias(r.Context(), name); err != nil {
		writeRegistry(w, err)
		return
	}
	h.note(r, "DeleteRoleAlias", name)
	w.WriteHeader(http.StatusOK)
}

func (h Handler) listRoleAliases(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("pageSize"))
	names, next, err := h.Things.ListRoleAliases(r.URL.Query().Get("marker"), limit)
	if err != nil {
		writeRegistry(w, err)
		return
	}
	body := map[string]any{"roleAliases": names}
	if next != "" {
		body["nextMarker"] = next
	}
	writeJSON(w, http.StatusOK, body)
}

func (h Handler) roleAliasCredentials(w http.ResponseWriter, r *http.Request) {
	if _, err := h.Things.GetRoleAlias(r.PathValue("roleAlias")); err != nil {
		writeRegistry(w, err)
		return
	}
	writeAWSError(w, "ServiceUnavailableException", "本节点不签发云临时凭证")
}

func aliasJSON(a *registry.RoleAlias) map[string]any {
	return map[string]any{
		"roleAlias":                 a.Name,
		"roleArn":                   a.RoleARN,
		"credentialDurationSeconds": a.Duration,
		"creationDate":              rfc3339(a.Created),
		"lastModifiedDate":          rfc3339(a.Updated),
	}
}
