package registry

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/itmisx/open-iot-core/internal/store"
)

// 兼容面的 access key（AUTH-086）。
//
// 它和角色别名、已注册 CA 一样只进冷层、不进热视图：验签发生在控制面的
// HTTP 请求上，不在连接路径上，所以读一次 KV 是可以接受的——而热视图的
// 位置是留给"每条消息都要查"的那几样的（AUTH-010 的无 IO 要求针对的是
// 认证连接那条路径）。
//
// secret 以密文存放（AUTH-087）。本包不做加解密：它存的是调用方给的
// 一段不透明字节。密钥材料的生成与校验留在 server 层，registry 只负责
// "这条记录跟着同一条复制日志走"。
//
// 角色名的合法值由 awsapi 定义，本包不校验它们——awsapi 依赖 registry，
// 反过来引用会成环。这里只校验形状。真正的安全保证不在这一层：判权限
// 时认不出的角色一律拒绝（AUTH-081），所以一个写坏的角色字符串只会让
// 这把钥匙什么都做不了，不会多给任何权限。
type awsKeyRecord struct {
	V       int    `json:"v"`
	ID      string `json:"id"`
	Role    string `json:"role"`
	Secret  []byte `json:"secret"`
	Note    string `json:"note"`
	Created int64  `json:"created"`
	// Expires 是 Unix 毫秒，零表示永不过期。
	Expires int64 `json:"expires"`
	// Disabled 为真时这把钥匙立刻失效，但记录还在。
	//
	// 与删除并存，因为两者的用途不同：怀疑泄露时要的是"马上停掉并且
	// 留着痕迹"，而删除之后审计日志里的 `ak:AKIA...` 再也对不上人。
	Disabled bool `json:"disabled"`
}

// AWSKey 是一把 access key 对外的样子。**不含密钥材料**。
//
// secret 单独由 KeySecret 取。分开是为了让"把列表直接序列化成响应"
// 这种最自然的写法不可能泄露密钥材料——存储里它是明文（AUTH-092），
// 这条分界因此比加密时更要紧。
type AWSKey struct {
	ID       string
	Role     string
	Note     string
	Created  int64
	Expires  int64
	Disabled bool
}

const (
	awsKeyIDMax   = 128
	awsKeyNoteMax = 256
	awsKeyRoleMax = 32
	awsKeyMaxLen  = 4096
)

// ErrKeyUnusable 表示钥匙存在但当前不能用（停用或已过期）。
//
// 它不对外区分于"没有这把钥匙"：验签那一层把两者合成同一个拒绝
// （AUTH-075），否则停用一把钥匙反而成了一个探测它是否存在的接口。
var ErrKeyUnusable = errors.New("registry: access key 已停用或已过期")

// PutAWSKey 新建一把 access key。已存在则拒绝。
//
// secret 是密钥材料本身，本包不解释它，只管它跟着同一条复制日志走。
func (r *Registry) PutAWSKey(ctx context.Context, id, role, note string, secret []byte, expires int64) (*AWSKey, error) {
	if err := validateAWSKeyID(id); err != nil {
		return nil, err
	}
	if role == "" || len(role) > awsKeyRoleMax {
		return nil, fmt.Errorf("registry: 角色须为 1 到 %d 个字符", awsKeyRoleMax)
	}
	if len(note) > awsKeyNoteMax {
		return nil, fmt.Errorf("registry: 备注不得超过 %d 个字符", awsKeyNoteMax)
	}
	if len(secret) == 0 || len(secret) > awsKeyMaxLen {
		return nil, errors.New("registry: 密钥材料为空或过长")
	}
	if expires < 0 {
		return nil, errors.New("registry: 过期时刻不得为负")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, err := r.getAWSKeyLocked(id); err == nil {
		return nil, ErrAlreadyExists
	} else if !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	rec := &awsKeyRecord{
		V: 1, ID: id, Role: role, Secret: secret, Note: note,
		Created: r.now(), Expires: expires,
	}
	op, err := setOp(store.Key(store.PrefixAWSKey, id), rec)
	if err != nil {
		return nil, err
	}
	if err := r.commitRecordsLocked(ctx, op); err != nil {
		return nil, err
	}
	return rec.view(), nil
}

// SetAWSKeyDisabled 停用或恢复一把钥匙。
func (r *Registry) SetAWSKeyDisabled(ctx context.Context, id string, disabled bool) (*AWSKey, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, err := r.getAWSKeyLocked(id)
	if err != nil {
		return nil, err
	}
	if rec.Disabled == disabled {
		return rec.view(), nil
	}
	rec.Disabled = disabled
	op, err := setOp(store.Key(store.PrefixAWSKey, id), rec)
	if err != nil {
		return nil, err
	}
	if err := r.commitRecordsLocked(ctx, op); err != nil {
		return nil, err
	}
	return rec.view(), nil
}

// DeleteAWSKey 删掉一把钥匙。
func (r *Registry) DeleteAWSKey(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, err := r.getAWSKeyLocked(id); err != nil {
		return err
	}
	return r.commitRecordsLocked(ctx, deleteOp(store.Key(store.PrefixAWSKey, id)))
}

// GetAWSKey 读一把钥匙的元信息，不含密文。
func (r *Registry) GetAWSKey(id string) (*AWSKey, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, err := r.getAWSKeyLocked(id)
	if err != nil {
		return nil, err
	}
	return rec.view(), nil
}

// KeySecret 取一把**当前可用**的钥匙的 secret 与角色。
//
// 停用、过期、不存在三者返回的错误各不相同，但调用方（验签那一层）
// 必须把它们合成同一个拒绝。这里仍然分开返回，是为了让审计和排查
// 能说出"这把钥匙过期了"——那句话只写进本节点的日志，不进响应。
func (r *Registry) KeySecret(id string) (secret []byte, role string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, err := r.getAWSKeyLocked(id)
	if err != nil {
		return nil, "", err
	}
	if rec.Disabled {
		return nil, "", ErrKeyUnusable
	}
	if rec.Expires != 0 && r.now() >= rec.Expires {
		return nil, "", ErrKeyUnusable
	}
	out := make([]byte, len(rec.Secret))
	copy(out, rec.Secret)
	return out, rec.Role, nil
}

// ListAWSKeys 按 id 升序列出全部钥匙。
//
// 不分页：这张表的规模是人手工发的钥匙数，量级是个位到两位数，
// 分页只会让调用方多写一段永远走不到的循环。
func (r *Registry) ListAWSKeys() ([]AWSKey, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []AWSKey{}
	err := store.ScanPrefix(r.kv, store.PrefixAWSKey, "", func(parts []string, value []byte) bool {
		var rec awsKeyRecord
		if err := decodeJSON(value, &rec); err != nil {
			// 坏掉的单条记录不该让整张表读不出来：那会让"删掉它"
			// 这个唯一的修法也做不了。
			out = append(out, AWSKey{ID: parts[0], Role: "", Note: "记录损坏"})
			return true
		}
		out = append(out, *rec.view())
		return true
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (r *Registry) getAWSKeyLocked(id string) (*awsKeyRecord, error) {
	raw, err := r.kv.Get(store.Key(store.PrefixAWSKey, id))
	if errors.Is(err, store.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var rec awsKeyRecord
	if err := decodeJSON(raw, &rec); err != nil {
		return nil, fmt.Errorf("registry: access key 记录损坏 %s: %w", id, err)
	}
	return &rec, nil
}

func (rec *awsKeyRecord) view() *AWSKey {
	return &AWSKey{
		ID: rec.ID, Role: rec.Role, Note: rec.Note,
		Created: rec.Created, Expires: rec.Expires, Disabled: rec.Disabled,
	}
}

// validateAWSKeyID 校验 id 的形状。
//
// 只收 ASCII 字母数字：id 会进键、进审计日志、进错误信息。允许任意
// 字节的话，一个带换行的 id 就能在 audit.log 里伪造出一整行。
func validateAWSKeyID(id string) error {
	if id == "" || len(id) > awsKeyIDMax {
		return fmt.Errorf("registry: access key id 须为 1 到 %d 个字符", awsKeyIDMax)
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		ok := (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
		if !ok {
			return errors.New("registry: access key id 只能是 ASCII 字母和数字")
		}
	}
	if strings.TrimSpace(id) != id {
		return errors.New("registry: access key id 不得有空白")
	}
	return nil
}
