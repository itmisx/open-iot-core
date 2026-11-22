package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadSessionTicketKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ticket.keys")
	a := strings.Repeat("01", 32)
	b := strings.Repeat("a5", 32)
	if err := os.WriteFile(path, []byte("# current then previous\n"+a+"\n"+b+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	keys, err := LoadSessionTicketKeys(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || keys[0][0] != 0x01 || keys[1][0] != 0xa5 {
		t.Fatalf("密钥环解析错误: %x", keys)
	}
}

func TestTLSListenerTicketKeyErrorNamesListenerAndPath(t *testing.T) {
	r := newTestRig(t, nil)
	s, err := New(Options{Config: r.cfg, DataDir: r.dd, Manifest: r.mf})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	missing := filepath.Join(t.TempDir(), "missing-ticket.keys")
	l := r.cfg.Listeners.MQTTTLS
	l.SessionTicketKeysFile = missing
	err = s.startTLSListener(context.Background(), "mqtt_tls", &l)
	if err == nil {
		t.Fatal("ticket 密钥文件不存在时监听器不应启动")
	}
	if !strings.Contains(err.Error(), "mqtt_tls") || !strings.Contains(err.Error(), missing) {
		t.Fatalf("错误应同时包含监听器名与文件路径: %v", err)
	}
}

func TestLoadSessionTicketKeysRejectsInvalidRings(t *testing.T) {
	valid := strings.Repeat("11", 32)
	var tooMany strings.Builder
	for i := 0; i < 9; i++ {
		fmt.Fprintf(&tooMany, "%064x\n", i+1)
	}
	for _, tc := range []struct {
		name string
		body string
	}{
		{"empty", "# only comment\n"},
		{"short", "abcd\n"},
		{"not-hex", strings.Repeat("zz", 32) + "\n"},
		{"duplicate", valid + "\n" + valid + "\n"},
		{"too-many", tooMany.String()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "ticket.keys")
			if err := os.WriteFile(path, []byte(tc.body), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadSessionTicketKeys(path); err == nil {
				t.Fatal("无效 ticket 密钥环被接受")
			}
		})
	}
}
