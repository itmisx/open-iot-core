// Package consoleassets 嵌入控制台的前端产物。
//
// 单独成包是为了把 go:embed 的目标目录限制在一处：embed 指令只能
// 嵌入当前包所在目录及其子目录，而前端产物在 web/console/dist 下。
// 这里用一个软链接式的构建约定——产物由 `make console` 复制进来。
//
// 产物提交进仓库（PRD UI-039）：没有 Node 环境时 Go 侧构建不被阻塞。
// 这对一个以"单二进制、零额外依赖"为卖点的项目是必须的——
// 否则 `go install` 就需要先装 Node。
package consoleassets

import (
	"embed"
	"io/fs"
)

// dist 是前端构建产物。
//
// all: 前缀是必需的：默认的 embed 会跳过以点或下划线开头的文件，
// 而前端产物里可能有它们。
//
//go:embed all:dist
var dist embed.FS

// FS 返回以 dist 为根的文件系统。
func FS() (fs.FS, error) { return fs.Sub(dist, "dist") }

// Present 报告是否嵌入了可用的产物。
//
// 仅有一个占位文件时视为不可用：那说明构建没跑过，
// 此时应当给出一条明确的提示而不是一个白屏。
func Present() bool {
	f, err := dist.Open("dist/index.html")
	if err != nil {
		return false
	}
	_ = f.Close()
	return true
}
