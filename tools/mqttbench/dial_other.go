//go:build !linux

package main

import "net"

// 非 Linux 没有 IP_BIND_ADDRESS_NO_PORT；容量预检不得假设源端口能跨目的地址复用。
func configureSourcePortReuse(_ *net.Dialer, _ net.IP) {}
