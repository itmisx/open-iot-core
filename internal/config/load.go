package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	yaml "go.yaml.in/yaml/v3"
)

// ErrNoConfigFile 表示指定的配置文件不存在。
var ErrNoConfigFile = errors.New("config: 配置文件不存在")

// LoadFile 从 YAML 文件加载配置，未指定的字段取默认值。
func LoadFile(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Config{}, fmt.Errorf("%w: %s", ErrNoConfigFile, path)
		}
		return Config{}, fmt.Errorf("config: 读取配置文件失败: %w", err)
	}
	cfg, err := Decode(bytes.NewReader(data))
	if err != nil {
		return Config{}, fmt.Errorf("config: %s: %w", path, err)
	}
	return cfg, nil
}

// Decode 从 YAML 解码配置，未出现的字段保留默认值。
//
// 未知字段一律报错，不忽略。理由与 authz 拒绝 Condition 相同：
// 配置键打错字若被静默忽略，失败方式是"悄悄按默认值跑"——
// 一个本想把 require_client_cert 设为 true 的部署，会在无人察觉的情况下
// 以 false 运行。报错是唯一能让这类错误立刻可见的做法。
func Decode(r io.Reader) (Config, error) {
	cfg := Default()
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		if errors.Is(err, io.EOF) {
			// 空文件：全部取默认值，这是合法的
			return cfg, nil
		}
		return Config{}, fmt.Errorf("解析失败: %w", err)
	}
	// 多文档 YAML 不受支持：第二个文档会静默覆盖第一个的部分字段，
	// 这不是任何人想要的行为。
	var extra yaml.Node
	if err := dec.Decode(&extra); err == nil {
		return Config{}, errors.New("配置文件不得包含多个 YAML 文档")
	} else if !errors.Is(err, io.EOF) {
		return Config{}, fmt.Errorf("解析失败: %w", err)
	}
	return cfg, nil
}

// Encode 以 YAML 输出配置，供 `openiotd config print` 使用。
//
// 能打印生效配置是必需的，不是便利功能：配置来自默认值、文件与 flags
// 三层叠加，出问题时第一个要回答的问题就是"到底生效的是什么"。
func (c *Config) Encode(w io.Writer) error {
	enc := yaml.NewEncoder(w)
	enc.SetIndent(2)
	if err := enc.Encode(c); err != nil {
		return fmt.Errorf("config: 编码失败: %w", err)
	}
	return enc.Close()
}
