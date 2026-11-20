package server

import (
	"net/http"

	"github.com/itmisx/open-iot-core/internal/registry"
)

// 兼容面 access key 的管理端点（API-125 至 API-127）。
//
// 只给 admin（permit 里按路径判）：发一把 admin 的 access key 等于
// 把整个控制面交出去，这件事不该由 operator 做。
//
// 形状刻意与用户管理（UI-067）一致：列表加「新建」对话框，密钥材料
// 只在创建的响应里出现一次。一致不是为了好看——两处都是「发凭证」，
// 用法不同会让人以为其中一处还能再取回密钥。

type awsKeyJSON struct {
	ID       string `json:"id"`
	Role     string `json:"role"`
	Note     string `json:"note,omitempty"`
	Created  int64  `json:"created"`
	Expires  int64  `json:"expires,omitempty"`
	Disabled bool   `json:"disabled"`
}

func awsKeyView(k registry.AWSKey) awsKeyJSON {
	return awsKeyJSON{
		ID: k.ID, Role: k.Role, Note: k.Note,
		Created: k.Created, Expires: k.Expires, Disabled: k.Disabled,
	}
}

func (a *adminServer) handleListAWSKeys(w http.ResponseWriter, r *http.Request) {
	keys, err := a.s.ListAWSKeys()
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "读不出 access key 列表", err.Error())
		return
	}
	out := make([]awsKeyJSON, 0, len(keys))
	for _, k := range keys {
		out = append(out, awsKeyView(k))
	}
	writeJSON(w, http.StatusOK, map[string]any{"keys": out})
}

func (a *adminServer) handleCreateAWSKey(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Role string `json:"role"`
		Note string `json:"note"`
		// Expires 是 Unix 毫秒，零表示永不过期。
		Expires int64 `json:"expires"`
	}
	if !decodeBody(w, r, &in) {
		return
	}
	id, secret, err := a.s.CreateAWSKey(r.Context(), in.Role, in.Note, in.Expires)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error(), "")
		return
	}
	a.audit(r, "CreateAWSKey", id)
	// secret 只在这里出现一次。审计里记 id，不记 secret。
	writeJSON(w, http.StatusOK, map[string]any{
		"id": id, "secret": secret, "role": in.Role, "note": in.Note, "expires": in.Expires,
	})
}

func (a *adminServer) handlePatchAWSKey(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Disabled *bool `json:"disabled"`
	}
	if !decodeBody(w, r, &in) {
		return
	}
	if in.Disabled == nil {
		writeAPIError(w, http.StatusBadRequest, "请求体里没有 disabled", "本端点目前只改停用状态")
		return
	}
	id := r.PathValue("id")
	k, err := a.s.SetAWSKeyDisabled(r.Context(), id, *in.Disabled)
	if err != nil {
		writeRegistryError(w, err)
		return
	}
	action := "EnableAWSKey"
	if *in.Disabled {
		action = "DisableAWSKey"
	}
	a.audit(r, action, id)
	writeJSON(w, http.StatusOK, awsKeyView(*k))
}

func (a *adminServer) handleDeleteAWSKey(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := a.s.DeleteAWSKey(r.Context(), id); err != nil {
		writeRegistryError(w, err)
		return
	}
	a.audit(r, "DeleteAWSKey", id)
	w.WriteHeader(http.StatusNoContent)
}
