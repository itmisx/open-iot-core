package registry

import (
	"context"
	"errors"
	"fmt"

	"github.com/itmisx/open-iot-core/internal/store"
)

// 角色别名只记下「这个名字对应哪个角色 ARN」。
//
// 本节点不签发云上的临时凭证。别名存在，是为了让控制面能管理这张对照表；
// 拿它去换访问密钥会得到明确的拒绝，而不是一张看起来能用的假凭证。

const (
	aliasDurationMin     = 900
	aliasDurationMax     = 43200
	aliasDurationDefault = 3600
)

type aliasRecord struct {
	V        int    `json:"v"`
	Name     string `json:"name"`
	RoleARN  string `json:"roleArn"`
	Duration int    `json:"duration"`
	Created  int64  `json:"created"`
	Updated  int64  `json:"updated"`
}

// RoleAlias 是一条角色别名。
type RoleAlias struct {
	Name     string
	RoleARN  string
	Duration int
	Created  int64
	Updated  int64
}

// PutRoleAlias 创建或更新一条别名。create 为真时已存在则拒绝。
func (r *Registry) PutRoleAlias(ctx context.Context, name, roleARN string, duration int, create bool) (*RoleAlias, error) {
	if err := validateAliasName(name); err != nil {
		return nil, err
	}
	if len(roleARN) < 8 || len(roleARN) > 2048 || roleARN[:4] != "arn:" {
		return nil, errors.New("registry: roleArn 必须是 ARN")
	}
	if duration == 0 {
		duration = aliasDurationDefault
	}
	if duration < aliasDurationMin || duration > aliasDurationMax {
		return nil, fmt.Errorf("registry: 凭证有效期须在 %d 到 %d 秒之间", aliasDurationMin, aliasDurationMax)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	old, err := r.getAliasLocked(name)
	if create {
		if err == nil {
			return nil, ErrAlreadyExists
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
		old = &aliasRecord{V: 1, Name: name, Created: r.now()}
	} else if err != nil {
		return nil, err
	}
	old.RoleARN = roleARN
	old.Duration = duration
	old.Updated = r.now()
	op, err := setOp(store.Key(store.PrefixAlias, name), old)
	if err != nil {
		return nil, err
	}
	if err := r.commitRecordsLocked(ctx, op); err != nil {
		return nil, err
	}
	return old.view(), nil
}

// GetRoleAlias 读取一条别名。
func (r *Registry) GetRoleAlias(name string) (*RoleAlias, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, err := r.getAliasLocked(name)
	if err != nil {
		return nil, err
	}
	return rec.view(), nil
}

// DeleteRoleAlias 删掉一条别名。
func (r *Registry) DeleteRoleAlias(ctx context.Context, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, err := r.getAliasLocked(name); err != nil {
		return err
	}
	return r.commitRecordsLocked(ctx, deleteOp(store.Key(store.PrefixAlias, name)))
}

// ListRoleAliases 按名字分页。
func (r *Registry) ListRoleAliases(after string, limit int) ([]string, string, error) {
	if limit <= 0 {
		limit = 25
	}
	if limit > 100 {
		limit = 100
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	var next string
	err := store.ScanPrefix(r.kv, store.PrefixAlias, "", func(parts []string, _ []byte) bool {
		name := parts[0]
		if name <= after {
			return true
		}
		if len(out) == limit {
			next = out[len(out)-1]
			return false
		}
		out = append(out, name)
		return true
	})
	return out, next, err
}

func (r *Registry) getAliasLocked(name string) (*aliasRecord, error) {
	raw, err := r.kv.Get(store.Key(store.PrefixAlias, name))
	if errors.Is(err, store.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var rec aliasRecord
	if err := decodeJSON(raw, &rec); err != nil {
		return nil, fmt.Errorf("registry: 角色别名记录损坏 %s: %w", name, err)
	}
	return &rec, nil
}

func (rec *aliasRecord) view() *RoleAlias {
	return &RoleAlias{
		Name: rec.Name, RoleARN: rec.RoleARN, Duration: rec.Duration,
		Created: rec.Created, Updated: rec.Updated,
	}
}

func validateAliasName(name string) error {
	if name == "" || len(name) > 128 {
		return errors.New("registry: 角色别名长度须在 1 到 128")
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '_' || c == '-' || c == ':' || c == '@':
		default:
			return errors.New("registry: 角色别名含有不允许的字符")
		}
	}
	return nil
}
