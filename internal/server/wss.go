package server

import (
	"encoding/json"
	"fmt"
	"os"
)

// accessKey 是一把设备 access key，用于 WebSocket 接入（AUTH-076）。
//
// 形状是 `{"id","secret","thing"}`：一把钥匙代表一台设备，权限来自这台
// 设备已附加的 IoT Policy，所以没有角色可言。
//
// 控制面的 access key 不在这里。它们已经搬进复制的元数据、secret 加密
// 落盘、经管理面发放（AUTH-086，见 awskeys.go）。设备钥匙没跟着搬，
// 因为它在连接路径上，而那条路径不许有磁盘 IO（AUTH-010）——搬进来
// 要先在热视图里给它一个位置。两边不共用一个文件：这里的 `role` 字段
// 一旦出现就是把控制面的钥匙填错了地方，直接报出来。
type accessKey struct {
	ID     string `json:"id"`
	Secret string `json:"secret"`
	Thing  string `json:"thing"`
	Role   string `json:"role"`
}

const maxAccessKeysBytes = 1 << 20

// loadDeviceKeys 读取 WebSocket 接入用的密钥文件。文件在启动时读一次。
func loadDeviceKeys(path string) (map[string]accessKey, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("打开密钥文件 %s：%w", path, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("读取密钥文件 %s：%w", path, err)
	}
	if info.Size() == 0 || info.Size() > maxAccessKeysBytes {
		return nil, fmt.Errorf("密钥文件 %s 的大小必须在 1 字节到 1MiB 之间", path)
	}
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	var list []accessKey
	if err := dec.Decode(&list); err != nil {
		return nil, fmt.Errorf("解析密钥文件 %s：%w", path, err)
	}
	if len(list) == 0 {
		return nil, fmt.Errorf("密钥文件 %s 里没有 access key", path)
	}
	out := make(map[string]accessKey, len(list))
	for i, k := range list {
		if err := checkDeviceKey(k, i+1); err != nil {
			return nil, err
		}
		if _, ok := out[k.ID]; ok {
			return nil, fmt.Errorf("access key %s 重复", k.ID)
		}
		out[k.ID] = k
	}
	return out, nil
}

func checkDeviceKey(k accessKey, line int) error {
	if k.ID == "" || k.Secret == "" {
		return fmt.Errorf("密钥文件第 %d 条缺少 id 或 secret", line)
	}
	if k.Thing == "" {
		return fmt.Errorf("密钥文件第 %d 条缺少 thing", line)
	}
	// 带 role 的是控制面的钥匙，填错地方了。
	//
	// 宽松接受的话，这把钥匙会静静地加载成功，然后以一台不存在的设备
	// 的身份去找策略、找不到、拒绝 CONNECT——而报错里不会有任何一个字
	// 指向真正填错的那一行。
	if k.Role != "" {
		return fmt.Errorf("密钥文件第 %d 条不该有 role：WebSocket 上的权限来自这台设备已附加的策略；"+
			"控制面的 access key 经管理面 /api/admin/v1/awskeys 发放，不写在配置里", line)
	}
	return nil
}
