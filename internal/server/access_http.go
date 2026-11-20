package server

import (
	"net/http"
)

func (a *adminServer) identify(r *http.Request) (principal, bool) {
	if tok := requestToken(r); tok != "" {
		if checkAdminToken(a.token, tok) {
			return principal{Name: "admin", Role: roleAdmin}, true
		}
		if p, ok := a.s.access.UserByToken(tok); ok {
			return p, true
		}
	}
	return principal{}, false
}

func (a *adminServer) handleSession(w http.ResponseWriter, r *http.Request) {
	p, _ := principalFrom(r.Context())
	writeJSON(w, http.StatusOK, map[string]string{"name": p.Name, "role": p.Role})
}

func (a *adminServer) handleListUsers(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"users": a.s.access.Users()})
}

func (a *adminServer) handlePutUser(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Role string `json:"role"`
	}
	if !decodeBody(w, r, &in) {
		return
	}
	name := r.PathValue("name")
	tok, err := a.s.access.PutUser(name, in.Role)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error(), "")
		return
	}
	a.audit(r, "PutUser", name)
	writeJSON(w, http.StatusOK, map[string]string{
		"name": name, "role": in.Role, "token": tok,
	})
}

func (a *adminServer) handleDeleteUser(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := a.s.access.DeleteUser(name); err != nil {
		writeAPIError(w, http.StatusNotFound, "没有这个用户", "")
		return
	}
	a.audit(r, "DeleteUser", name)
	w.WriteHeader(http.StatusNoContent)
}
