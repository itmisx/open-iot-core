package topic

import "bytes"

// Class 决定一个主题的路由策略，是契约 03 的落地。
//
// 本分类是本项目相对通用 broker 的主要性能杠杆：设备只订阅自己的主题，
// 而只有后端应用订阅通配符，且后端连接数是数十而非数百万。因此设备域主题
// 的归属节点可以直接由 scope key 哈希算出，完全不必进入全局订阅表——
// 只有通配符订阅需要跨节点复制。
type Class uint8

const (
	// ClassDeviceScoped 是 $aws/things/{thingName}/... 形式的主题。
	// 其归属节点由 thingName 哈希决定，不占用全局订阅表。
	ClassDeviceScoped Class = iota
	// ClassReservedExact 是其余 $ 保留主题。它们或由平台直投请求方，
	// 或只被后端的精确订阅命中，不需要按设备分区。
	ClassReservedExact
	// ClassGeneral 是普通业务主题，需要兴趣传播。
	ClassGeneral
)

var className = [...]string{"DeviceScoped", "ReservedExact", "General"}

func (c Class) String() string {
	if int(c) >= len(className) {
		return "Unknown"
	}
	return className[c]
}

var thingsPrefix = []byte("$aws/things/")

// Classify 对一个主题名分类。
//
// 对 ClassDeviceScoped，scope 返回其 thingName，引用 name 不做复制；
// 其余分类下 scope 为 nil。
//
// 入参应为主题名而非过滤器：共享订阅前缀与通配符不在此处处理。
func Classify(name []byte) (c Class, scope []byte) {
	if len(name) == 0 || name[0] != '$' {
		return ClassGeneral, nil
	}
	if !bytes.HasPrefix(name, thingsPrefix) {
		return ClassReservedExact, nil
	}
	tail := name[len(thingsPrefix):]
	i := bytes.IndexByte(tail, '/')
	if i <= 0 {
		// $aws/things/ 之后没有 thingName，或 thingName 为空：
		// 不是可路由的设备域主题。
		return ClassReservedExact, nil
	}
	thing := tail[:i]
	// thingName 不得含通配符，否则这是个过滤器而非主题名。
	if bytes.IndexByte(thing, '+') >= 0 || bytes.IndexByte(thing, '#') >= 0 {
		return ClassReservedExact, nil
	}
	return ClassDeviceScoped, thing
}

// ScopeOfFilter 在过滤器可以静态确定其设备域归属时返回该 thingName。
//
// 这用于集群路由：若一个订阅的 thingName 是确定的（例如设备订阅自己的
// $aws/things/dev-001/shadow/update/delta），该订阅就不必进入全局订阅表。
// 含通配符 thingName 的过滤器（后端应用常用）返回 ok 为 false，
// 必须参与兴趣传播。
func ScopeOfFilter(filter []byte) (scope []byte, ok bool) {
	if _, rest, isShared := ParseShared(filter); isShared {
		filter = rest
	}
	c, scope := Classify(filter)
	if c != ClassDeviceScoped {
		return nil, false
	}
	return scope, true
}
