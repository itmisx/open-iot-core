package shadow

// delta 计算 desired 相对 reported 的差异（PRD SHDW-027）。
//
// 语义：delta 回答"设备还需要做什么改变才能达到 desired"。因此
//   - desired 有而 reported 没有的键 → 进入 delta；
//   - 两侧都有但值不同 → 取 desired 的值；
//   - 两侧都是对象 → 逐层递归，只有真有差异的子树才进入 delta；
//   - reported 有而 desired 没有的键 → **不**进入 delta。
//     这一条常被写错：desired 没提到的字段不代表要清空它。
//
// 数组按整体比较：逐元素 delta 无法表达"删除第 2 个元素"，
// 而数组在影子里本就是整体替换的（见 merge.go 规则 5）。
func delta(desired, reported Object) Object {
	if len(desired) == 0 {
		return nil
	}
	out := make(Object)
	for k, dv := range desired {
		rv, existed := reported[k]
		if !existed {
			out[k] = Clone(dv)
			continue
		}
		dobj, dIsObj := dv.(map[string]any)
		robj, rIsObj := rv.(map[string]any)
		if dIsObj && rIsObj {
			if sub := delta(dobj, robj); len(sub) > 0 {
				out[k] = sub
			}
			continue
		}
		if !Equal(dv, rv) {
			out[k] = Clone(dv)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// deltaMetadata 从完整元数据中摘出 delta 涉及的那些叶子。
//
// delta 消息自带 metadata，其中只应包含 delta 中出现的字段——
// 把整份 desired 的元数据塞进去会让设备以为有更多字段需要处理。
func deltaMetadata(d Object, desiredMeta Object) Object {
	if len(d) == 0 || len(desiredMeta) == 0 {
		return nil
	}
	out := make(Object)
	for k, dv := range d {
		mv, ok := desiredMeta[k]
		if !ok {
			continue
		}
		if dobj, isObj := dv.(map[string]any); isObj {
			if mobj, ok := mv.(map[string]any); ok {
				if sub := deltaMetadata(dobj, mobj); len(sub) > 0 {
					out[k] = sub
				}
				continue
			}
		}
		out[k] = Clone(mv)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
