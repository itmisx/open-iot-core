// TLS session ticket 的按设备索引。

package main

import (
	"crypto/tls"
	"sync"
)

const deviceTLSCacheShards = 64

type deviceTLSSessionKey struct {
	device, server string
}

type deviceTLSSessionShard struct {
	sync.Mutex
	entries map[deviceTLSSessionKey]*tls.ClientSessionState
}

type deviceTLSSessionCache struct {
	shards [deviceTLSCacheShards]deviceTLSSessionShard
}

type perDeviceTLSSessionCache struct {
	all    *deviceTLSSessionCache
	device string
}

func (c *deviceTLSSessionCache) forDevice(device string) tls.ClientSessionCache {
	return &perDeviceTLSSessionCache{all: c, device: device}
}

func (c *deviceTLSSessionCache) shard(device string) *deviceTLSSessionShard {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(device); i++ {
		h ^= uint64(device[i])
		h *= 1099511628211
	}
	return &c.shards[h&(deviceTLSCacheShards-1)]
}

func (c *perDeviceTLSSessionCache) Get(server string) (*tls.ClientSessionState, bool) {
	shard := c.all.shard(c.device)
	shard.Lock()
	state, ok := shard.entries[deviceTLSSessionKey{device: c.device, server: server}]
	shard.Unlock()
	return state, ok
}

func (c *perDeviceTLSSessionCache) Put(server string, state *tls.ClientSessionState) {
	shard := c.all.shard(c.device)
	key := deviceTLSSessionKey{device: c.device, server: server}
	shard.Lock()
	if state == nil {
		delete(shard.entries, key)
	} else {
		if shard.entries == nil {
			shard.entries = make(map[deviceTLSSessionKey]*tls.ClientSessionState)
		}
		shard.entries[key] = state
	}
	shard.Unlock()
}
