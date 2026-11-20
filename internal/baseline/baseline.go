// Package baseline 读取公开的内存基线，并判断一次测量有没有超出 10%。
//
// 门禁用的是字节数。耗时随机器、负载和竞态检测器变化，写进基线就是
// 发布无法复现的数字。竞态检测器会放大分配，那种运行下不做 10% 比较，
// 绝对预算仍由各测试自己守。
package baseline

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Memory 是 docs/baseline/memory.json 的形状。
//
// 三项对应百万设备热视图、百万订阅的订阅树、一条连接在堆上的字节。
type Memory struct {
	HotViewBytesPerDevice    int `json:"hotViewBytesPerDevice"`
	TrieBytesPerSubscription int `json:"trieBytesPerSubscription"`
	ConnBytes                int `json:"connBytes"`
}

// Load 从模块根的 docs/baseline/memory.json 读取基线。
func Load() (Memory, error) {
	p, err := file()
	if err != nil {
		return Memory{}, err
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return Memory{}, err
	}
	var m Memory
	if err := json.Unmarshal(b, &m); err != nil {
		return Memory{}, err
	}
	return m, nil
}

// Check 在测量值高于记录的 110% 或低于一半时返回错误。
//
// 低于一半通常是对象被回收了，测量没有量到该量的结构。
// 结构真的变小也要改基线，否则公开的数字仍是旧的。
// 竞态检测下直接通过：那种堆不能和这份数字比。
func Check(name string, got int) error {
	if Race {
		return nil
	}
	m, err := Load()
	if err != nil {
		return err
	}
	rec, err := m.recorded(name)
	if err != nil {
		return err
	}
	if rec <= 0 {
		return fmt.Errorf("%s 的基线是 %d", name, rec)
	}
	limit := rec * 110 / 100
	floor := rec / 2
	if got > limit {
		return fmt.Errorf("%s = %d 字节，基线 %d，上限 %d（10%%）。确认是结构变了再改 docs/baseline/memory.json",
			name, got, rec, limit)
	}
	if got < floor {
		return fmt.Errorf("%s = %d 字节，低于基线 %d 的一半。对象可能没留住；若结构确实变小，改 docs/baseline/memory.json",
			name, got, rec)
	}
	return nil
}

func (m Memory) recorded(name string) (int, error) {
	switch name {
	case "hotViewBytesPerDevice":
		return m.HotViewBytesPerDevice, nil
	case "trieBytesPerSubscription":
		return m.TrieBytesPerSubscription, nil
	case "connBytes":
		return m.ConnBytes, nil
	default:
		return 0, fmt.Errorf("没有这项基线 %q", name)
	}
}

func file() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		cand := filepath.Join(dir, "go.mod")
		if st, err := os.Stat(cand); err == nil && !st.IsDir() {
			return filepath.Join(dir, "docs", "baseline", "memory.json"), nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("找不到 go.mod")
		}
		dir = parent
	}
}
