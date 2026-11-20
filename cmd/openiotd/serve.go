package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/itmisx/open-iot-core/internal/config"
	"github.com/itmisx/open-iot-core/internal/node"
	"github.com/itmisx/open-iot-core/internal/server"
)

// runServe 启动节点。
func runServe(args []string) error {
	f := config.NewFlags("openiotd serve", os.Stderr)
	if err := f.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(f, os.LookupEnv)
	if err != nil {
		return err
	}
	log := newLogger(&cfg)

	dd, err := node.Open(cfg.Node.DataDir)
	if err != nil {
		return err
	}
	// AllowCreate 为假：serve 不新建集群（见 node.IdentityRequest 的说明）
	mf, err := node.Identify(dd, node.IdentityRequest{
		NodeID:     cfg.Node.ID,
		ShardCount: cfg.Cluster.ShardCount,
	})
	if err != nil {
		return err
	}

	s, err := server.New(server.Options{
		Config: cfg, DataDir: dd, Manifest: mf,
		Logger: log,
	})
	if err != nil {
		return err
	}

	// SIGINT 与 SIGTERM 触发优雅关闭（PRD NFR-064）。
	// 第二次信号强制退出：一个卡在关闭流程里、按 Ctrl-C 也不退的进程
	// 会被运维直接 kill -9，那样连落盘都来不及。
	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Error("收到第二次终止信号，立即退出")
		os.Exit(130)
	}()

	return s.Run(ctx)
}
