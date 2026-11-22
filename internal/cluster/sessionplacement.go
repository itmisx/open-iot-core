package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// sessionPlacement 是会话 Raft 分片的确定性副本放置表。
//
// 当前运行路径仍使用 RF=all；先把「哪些节点承载哪个分片」从
// StartSessionShards 的隐含假设抽出来，后续才能安全地切到 RF=3。
// 放置输入必须来自持久化的初始成员表，不能每次按当前成员临时重算：
// 加一个节点若直接改变既有分片的副本集，而没有先做 Raft 成员迁移，
// 等于让新节点把空目录当成已有日志启动。
type sessionPlacement struct {
	base     uint64
	count    uint64
	replicas []uint64
	byShard  [][]uint64
}

// newSessionPlacement 按 replica ID 的稳定顺序轮转放置副本。
// rf=0 表示所有有效副本，用于保持当前 RF=all 行为。
func newSessionPlacement(replicas map[NodeID]uint64, count uint64, rf int) (sessionPlacement, error) {
	return newShardPlacement(shardSessionBase, replicas, count, rf)
}

func newShardPlacement(base uint64, replicas map[NodeID]uint64, count uint64, rf int) (sessionPlacement, error) {
	if base == 0 {
		return sessionPlacement{}, errors.New("cluster: 分片起始号必须大于 0")
	}
	if count == 0 {
		return sessionPlacement{}, errors.New("cluster: 会话分片数必须大于 0")
	}
	ids := make([]uint64, 0, len(replicas))
	seen := make(map[uint64]struct{}, len(replicas))
	for _, id := range replicas {
		if id == 0 {
			continue
		}
		if _, exists := seen[id]; exists {
			return sessionPlacement{}, fmt.Errorf("cluster: 重复的副本号 %d", id)
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return sessionPlacement{}, errors.New("cluster: 会话分片没有可用副本")
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if rf == 0 {
		rf = len(ids)
	}
	if rf < 1 || rf > len(ids) {
		return sessionPlacement{}, fmt.Errorf("cluster: 会话副本因子 %d 超出节点数 %d", rf, len(ids))
	}

	p := sessionPlacement{
		base: base, count: count, replicas: ids,
		byShard: make([][]uint64, count),
	}
	for i := uint64(0); i < count; i++ {
		members := make([]uint64, rf)
		start := int(i % uint64(len(ids)))
		for j := 0; j < rf; j++ {
			members[j] = ids[(start+j)%len(ids)]
		}
		p.byShard[i] = members
	}
	return p, nil
}

func (p sessionPlacement) offset(shard uint64) (uint64, bool) {
	if shard < p.base || shard >= p.base+p.count {
		return 0, false
	}
	return shard - p.base, true
}

func (p sessionPlacement) leader(shard uint64) (uint64, bool) {
	off, ok := p.offset(shard)
	if !ok || len(p.byShard[off]) == 0 {
		return 0, false
	}
	return p.byShard[off][0], true
}

func (p sessionPlacement) members(shard uint64) []uint64 {
	off, ok := p.offset(shard)
	if !ok {
		return nil
	}
	return append([]uint64(nil), p.byShard[off]...)
}

func (p sessionPlacement) contains(shard, replica uint64) bool {
	off, ok := p.offset(shard)
	if !ok {
		return false
	}
	for _, member := range p.byShard[off] {
		if member == replica {
			return true
		}
	}
	return false
}

func (p sessionPlacement) leaders() []uint64 {
	out := make([]uint64, p.count)
	for i := uint64(0); i < p.count; i++ {
		out[i] = p.byShard[i][0]
	}
	return out
}

const sessionLayoutName = "session-layout.json"

type sessionLayoutReplica struct {
	Node    NodeID `json:"node"`
	Replica uint64 `json:"replica"`
}

type sessionLayoutRecord struct {
	Version int                    `json:"version"`
	Count   uint64                 `json:"count"`
	RF      int                    `json:"rf"`
	All     bool                   `json:"all"`
	Members []sessionLayoutReplica `json:"members"`
}

func makeSessionLayout(count uint64, requestedRF int, replicas map[NodeID]uint64) sessionLayoutRecord {
	members := make([]sessionLayoutReplica, 0, len(replicas))
	for node, replica := range replicas {
		if replica != 0 {
			members = append(members, sessionLayoutReplica{Node: node, Replica: replica})
		}
	}
	sort.Slice(members, func(i, j int) bool {
		if members[i].Replica != members[j].Replica {
			return members[i].Replica < members[j].Replica
		}
		return members[i].Node < members[j].Node
	})
	rf := requestedRF
	if rf == 0 {
		rf = len(members)
	}
	return sessionLayoutRecord{Version: 1, Count: count, RF: rf, All: requestedRF == 0, Members: members}
}

func ensureSessionLayout(dir string, want sessionLayoutRecord, hasExistingSession bool) error {
	return ensureShardLayout(dir, sessionLayoutName, "会话", want, hasExistingSession)
}

func ensureShardLayout(dir, name, label string, want sessionLayoutRecord, hasExisting bool) error {
	if dir == "" {
		return fmt.Errorf("cluster: %s布局缺少数据目录", label)
	}
	if name == "" || filepath.Base(name) != name {
		return fmt.Errorf("cluster: %s布局文件名无效 %q", label, name)
	}
	path := filepath.Join(dir, name)
	raw, err := os.ReadFile(path)
	if err == nil {
		var got sessionLayoutRecord
		if err := json.Unmarshal(raw, &got); err != nil {
			return fmt.Errorf("cluster: %s布局 %s 损坏: %w", label, path, err)
		}
		wantRaw, _ := json.Marshal(want)
		gotRaw, _ := json.Marshal(got)
		if string(wantRaw) != string(gotRaw) {
			return fmt.Errorf("cluster: %s布局不可变：磁盘为 %s，配置为 %s；须先执行 Raft 副本迁移", label, gotRaw, wantRaw)
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return fmt.Errorf("cluster: 读取%s布局 %s 失败: %w", label, path, err)
	}
	if hasExisting && !want.All {
		return fmt.Errorf("cluster: 现有 RF=all %s日志不能直接切为选择性副本；须先执行 Raft 副本迁移", label)
	}
	raw, err = json.MarshalIndent(want, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	tmp, err := os.CreateTemp(dir, "."+name+"-*.tmp")
	if err != nil {
		return fmt.Errorf("cluster: 创建%s布局临时文件失败: %w", label, err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("cluster: 安装%s布局失败: %w", label, err)
	}
	// rename 之后必须同步目录项。
	//
	// 不做这一步，布局文件在断电后可能整个丢失：内容已落盘，而目录项还在
	// 页缓存里。而这个文件是分片放置的**不可变锚点**——丢了它，下一次启动
	// 会把当时的配置当成首次布局静默接受，于是副本可能在没有该分片日志的
	// 节点上起来。同仓库 internal/node 与 internal/store 的原子写都做了这
	// 一步（标准一旦立下即对全部同类生效）。
	return syncShardLayoutDir(dir)
}

// syncShardLayoutDir 同步目录项，使上一步的 rename 落盘。
//
// 与 internal/node 的 syncDir 同一个理由与同一个容忍：某些文件系统不支持对
// 目录 fsync，那不是致命错误——布局文件本身已经 fsync 过，这一步只为了让
// 目录项也落盘。
func syncShardLayoutDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("cluster: 打开目录 %s 失败: %w", dir, err)
	}
	defer func() { _ = f.Close() }()
	_ = f.Sync()
	return nil
}
