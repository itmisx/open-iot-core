//go:build linux

package main

import (
	"net"
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

func TestConfigureSourcePortReuseLinux(t *testing.T) {
	d := &net.Dialer{}
	configureSourcePortReuse(d, net.ParseIP("127.0.0.2"))
	if d.Control == nil {
		t.Fatal("IPv4 source did not install IP_BIND_ADDRESS_NO_PORT control")
	}
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, unix.IPPROTO_TCP)
	if err != nil {
		t.Fatal(err)
	}
	f := os.NewFile(uintptr(fd), "mqttbench-source-port-test")
	defer f.Close()
	raw, err := f.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Control("tcp4", "", raw); err != nil {
		t.Fatalf("set IP_BIND_ADDRESS_NO_PORT: %v", err)
	}
	var got int
	var sockErr error
	if err := raw.Control(func(fd uintptr) {
		got, sockErr = unix.GetsockoptInt(int(fd), unix.IPPROTO_IP, unix.IP_BIND_ADDRESS_NO_PORT)
	}); err != nil {
		t.Fatal(err)
	}
	if sockErr != nil {
		t.Fatal(sockErr)
	}
	if got != 1 {
		t.Fatalf("IP_BIND_ADDRESS_NO_PORT = %d, want 1", got)
	}
	v6 := &net.Dialer{}
	configureSourcePortReuse(v6, net.ParseIP("::1"))
	if v6.Control != nil {
		t.Fatal("IPv6 source unexpectedly installed IPv4 socket option")
	}
}
