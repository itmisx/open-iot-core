//go:build linux

package main

import (
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

// configureSourcePortReuse 延迟 IPv4 临时端口分配到 connect(2)。指定
// Dialer.LocalAddr 时，普通 bind(sourceIP, 0) 会先独占一个本地端口，导致
// 同一源端口不能安全复用于不同 broker 目的地址；多 broker 容量因此错误地
// 退化成 sourceIP 数 × 临时端口数。IP_BIND_ADDRESS_NO_PORT 让内核按完整
// 四元组选端口，正是 Linux 为大型连接发生器提供的语义。
func configureSourcePortReuse(d *net.Dialer, ip net.IP) {
	if ip.To4() == nil {
		return
	}
	d.Control = func(_, _ string, raw syscall.RawConn) error {
		var sockErr error
		if err := raw.Control(func(fd uintptr) {
			sockErr = unix.SetsockoptInt(int(fd), unix.IPPROTO_IP,
				unix.IP_BIND_ADDRESS_NO_PORT, 1)
		}); err != nil {
			return err
		}
		return sockErr
	}
}
