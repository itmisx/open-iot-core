package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/itmisx/open-iot-core/internal/store"
)

// 检索在本机完成，不接外部搜索引擎（REG-041）。
//
// 关掉时 Search 直接拒绝，列举仍按设备名前缀扫描（REG-043）。
// 语句是空格分隔的 AND。字段只接受 thingName、attributes.<名>、
// connectivity.connected、shadow.reported.<路径>、shadow.desired.<路径>。
// thingName 的值可以用 * 结尾表示前缀。

const (
	IndexOff      = "OFF"
	IndexRegistry = "REGISTRY"
	IndexShadow   = "REGISTRY_AND_SHADOW"
	ConnOff       = "OFF"
	ConnStatus    = "STATUS"
)

type indexRecord struct {
	V         int    `json:"v"`
	ThingMode string `json:"thingMode"`
	ConnMode  string `json:"connMode"`
}

// IndexConfig 是检索开关。
type IndexConfig struct {
	ThingMode string
	ConnMode  string
}

// SearchEnv 是检索时向别的包要的只读事实。
//
// 注册表不依赖影子和连接表。缺了回调，对应的字段就当不存在。
type SearchEnv struct {
	Reported func(thing string) (map[string]any, bool)
	Desired  func(thing string) (map[string]any, bool)
	Online   func(thing string) bool
}

// SearchHit 是一条检索结果。
type SearchHit struct {
	Thing     Thing
	Connected *bool
	Shadow    string
}

// Indexing 返回当前开关。没有记录时两个都是 OFF。
func (r *Registry) Indexing() (IndexConfig, error) {
	raw, err := r.kv.Get(store.Key(store.PrefixSystem, "index"))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return IndexConfig{ThingMode: IndexOff, ConnMode: ConnOff}, nil
		}
		return IndexConfig{}, err
	}
	var rec indexRecord
	if err := decodeJSON(raw, &rec); err != nil {
		return IndexConfig{}, fmt.Errorf("registry: 检索配置损坏: %w", err)
	}
	if rec.ThingMode == "" {
		rec.ThingMode = IndexOff
	}
	if rec.ConnMode == "" {
		rec.ConnMode = ConnOff
	}
	return IndexConfig{ThingMode: rec.ThingMode, ConnMode: rec.ConnMode}, nil
}

// SetIndexing 打开或关闭检索。非法取值拒绝，避免一个拼写错误把索引悄悄关掉。
func (r *Registry) SetIndexing(ctx context.Context, cfg IndexConfig) error {
	switch cfg.ThingMode {
	case IndexOff, IndexRegistry, IndexShadow:
	default:
		return fmt.Errorf("%w: 设备索引取值无法识别", ErrBadQuery)
	}
	switch cfg.ConnMode {
	case ConnOff, ConnStatus:
	default:
		return fmt.Errorf("%w: 连接索引取值无法识别", ErrBadQuery)
	}
	op, err := setOp(store.Key(store.PrefixSystem, "index"), indexRecord{
		V: 1, ThingMode: cfg.ThingMode, ConnMode: cfg.ConnMode,
	})
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.commitRecordsLocked(ctx, op)
}

// Search 按语句返回一页设备。
func (r *Registry) Search(query, after string, limit int, env SearchEnv) ([]SearchHit, string, error) {
	cfg, err := r.Indexing()
	if err != nil {
		return nil, "", err
	}
	if cfg.ThingMode == IndexOff {
		return nil, "", ErrIndexOff
	}
	terms, err := parseQuery(query)
	if err != nil {
		return nil, "", err
	}
	for _, t := range terms {
		if t.kind == termShadow && cfg.ThingMode != IndexShadow {
			return nil, "", fmt.Errorf("%w: 影子索引未开启", ErrBadQuery)
		}
		if t.kind == termConn && cfg.ConnMode != ConnStatus {
			return nil, "", fmt.Errorf("%w: 连接索引未开启", ErrBadQuery)
		}
	}
	if limit <= 0 {
		limit = 25
	}
	if limit > 100 {
		limit = 100
	}
	var out []SearchHit
	var next string
	err = store.ScanPrefix(r.kv, store.PrefixThing, "", func(parts []string, value []byte) bool {
		name := parts[0]
		if name <= after {
			return true
		}
		var rec thingRecord
		if decodeJSON(value, &rec) != nil {
			return true
		}
		thing := Thing{
			Name: rec.Name, Attributes: rec.Attributes, PolicyName: rec.PolicyName,
			CreatedAtUnixMs: rec.Created, UpdatedAtUnixMs: rec.Updated, Version: rec.Version,
		}
		if thing.Version < 1 {
			thing.Version = 1
		}
		hit, ok := matchThing(thing, terms, env)
		if !ok {
			return true
		}
		if len(out) == limit {
			next = out[len(out)-1].Thing.Name
			return false
		}
		out = append(out, hit)
		return true
	})
	return out, next, err
}

const (
	termName = iota
	termAttr
	termConn
	termShadow
)

type queryTerm struct {
	kind     int
	key      string
	value    string
	prefix   bool
	reported bool
}

func parseQuery(q string) ([]queryTerm, error) {
	fields := strings.Fields(q)
	if len(fields) == 0 {
		return nil, fmt.Errorf("%w: 语句为空", ErrBadQuery)
	}
	out := make([]queryTerm, 0, len(fields))
	for _, f := range fields {
		key, val, ok := strings.Cut(f, ":")
		if !ok || key == "" || val == "" {
			return nil, fmt.Errorf("%w: %s", ErrBadQuery, f)
		}
		switch {
		case key == "thingName":
			prefix := strings.HasSuffix(val, "*")
			if prefix {
				val = strings.TrimSuffix(val, "*")
			}
			out = append(out, queryTerm{kind: termName, value: val, prefix: prefix})
		case strings.HasPrefix(key, "attributes."):
			name := strings.TrimPrefix(key, "attributes.")
			if name == "" {
				return nil, fmt.Errorf("%w: %s", ErrBadQuery, f)
			}
			out = append(out, queryTerm{kind: termAttr, key: name, value: val})
		case key == "connectivity.connected":
			if val != "true" && val != "false" {
				return nil, fmt.Errorf("%w: %s", ErrBadQuery, f)
			}
			out = append(out, queryTerm{kind: termConn, value: val})
		case strings.HasPrefix(key, "shadow.reported."), strings.HasPrefix(key, "shadow.desired."):
			reported := strings.HasPrefix(key, "shadow.reported.")
			path := strings.TrimPrefix(strings.TrimPrefix(key, "shadow.reported."), "shadow.desired.")
			if path == "" {
				return nil, fmt.Errorf("%w: %s", ErrBadQuery, f)
			}
			out = append(out, queryTerm{kind: termShadow, key: path, value: val, reported: reported})
		default:
			return nil, fmt.Errorf("%w: %s", ErrBadQuery, f)
		}
	}
	return out, nil
}

func matchThing(thing Thing, terms []queryTerm, env SearchEnv) (SearchHit, bool) {
	hit := SearchHit{Thing: thing}
	for _, t := range terms {
		switch t.kind {
		case termName:
			if t.prefix {
				if !strings.HasPrefix(thing.Name, t.value) {
					return SearchHit{}, false
				}
			} else if thing.Name != t.value {
				return SearchHit{}, false
			}
		case termAttr:
			if thing.Attributes[t.key] != t.value {
				return SearchHit{}, false
			}
		case termConn:
			online := env.Online != nil && env.Online(thing.Name)
			want := t.value == "true"
			if online != want {
				return SearchHit{}, false
			}
			hit.Connected = &online
		case termShadow:
			var doc map[string]any
			var ok bool
			if t.reported {
				if env.Reported != nil {
					doc, ok = env.Reported(thing.Name)
				}
			} else if env.Desired != nil {
				doc, ok = env.Desired(thing.Name)
			}
			if !ok || fmt.Sprint(walkPath(doc, t.key)) != t.value {
				return SearchHit{}, false
			}
		}
	}
	if env.Reported != nil || env.Desired != nil {
		body := map[string]any{}
		if env.Reported != nil {
			if doc, ok := env.Reported(thing.Name); ok {
				body["reported"] = doc
			}
		}
		if env.Desired != nil {
			if doc, ok := env.Desired(thing.Name); ok {
				body["desired"] = doc
			}
		}
		if len(body) > 0 {
			raw, err := json.Marshal(body)
			if err == nil {
				hit.Shadow = string(raw)
			}
		}
	}
	if hit.Connected == nil && env.Online != nil {
		on := env.Online(thing.Name)
		hit.Connected = &on
	}
	return hit, true
}

func walkPath(doc map[string]any, path string) any {
	var cur any = doc
	for _, part := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[part]
	}
	switch v := cur.(type) {
	case string:
		return v
	case float64:
		if v == float64(int64(v)) {
			return strconv.FormatInt(int64(v), 10)
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	default:
		return v
	}
}
