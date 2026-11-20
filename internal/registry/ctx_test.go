package registry

import "context"

// ctx 是测试里用的背景上下文。
//
// 控制面的写入现在都要 ctx：冷层记录随命令走复制日志（CLU-065），
// 而提交一条命令本来就是可能阻塞、可能失败的操作。包级一个够用——
// 已经有自己的 ctx 的测试函数里，局部那个会遮蔽它。
var ctx = context.Background()
