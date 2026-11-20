package shadow

// 本文件实现 PRD SHDW-021 至 SHDW-023 的合并与元数据语义。
//
// 这是整个模块最容易做错的地方，规则逐条列在此处，每条都有对应测试：
//
//  1. 更新是深度合并，不是整体替换（SHDW-022）。
//  2. 显式 null 删除该键**及其元数据**，任意深度（SHDW-023）。
//  3. 被 null 删空的**嵌套**对象保留为 `{}`，其元数据同为 `{}`。
//  4. 但**顶层** desired/reported 段若为空则整段不保留。
//     规则 3 与 4 的这处不对称是真实的 AWS 行为，不是笔误。
//  5. 数组整体替换，不逐元素合并；其元数据是逐元素的时间戳数组。
//  6. 出现在更新中的每个叶子都获得本次更新的时间戳，
//     即使它的值没有变化。
//
// 时间戳单位是 Unix **秒**，与 AWS 一致。用毫秒会让所有对照测试偏三个数量级。

// metaTimestampKey 是元数据叶子中时间戳的键名。
const metaTimestampKey = "timestamp"

// mergeState 把 patch 深度合并进 state，并同步维护 meta。
//
// state 与 meta 会被就地修改；调用方须先 Clone。
// 返回 state 是否发生了实际变化（用于判断是否需要发布 delta）。
func mergeState(state, meta Object, patch Object, nowSec int64) bool {
	return mergeInto(state, meta, patch, nowSec)
}

// mergeInto 是递归主体。
func mergeInto(state, meta Object, patch Object, nowSec int64) bool {
	changed := false
	for k, pv := range patch {
		// 规则 2：显式 null 删除键与其元数据
		if pv == nil {
			if _, existed := state[k]; existed {
				delete(state, k)
				changed = true
			}
			delete(meta, k)
			continue
		}

		cur, existed := state[k]

		// 规则 1：两侧都是对象时递归合并
		if pobj, ok := pv.(map[string]any); ok {
			cobj, curIsObj := cur.(map[string]any)
			// replaced 表示状态子树是新建的，而不是在原有对象上合并。
			replaced := !curIsObj
			if replaced {
				// 当前值不是对象（或不存在）：整体替换为新对象。
				// 不能在非对象上递归合并——那会把标量悄悄变成对象。
				cobj = make(map[string]any, len(pobj))
				state[k] = cobj
				changed = true
			}

			// 元数据树必须由**状态树**驱动，不能靠它自己的 Go 类型判断。
			//
			// 这里曾有一个真实缺陷：标量的元数据是 {"timestamp": N}，
			// 它在 Go 里同样是 map[string]any。于是当一个标量被对象替换时，
			// 旧的元数据叶子被当成"镜像对象"复用，残留的 timestamp 键
			// 与新字段混在一起，产出
			//   {"a":{"timestamp":N},"timestamp":N}
			// 这种既不是叶子也不是镜像的畸形元数据。
			// 判据只能是"状态子树是否被替换"，因为元数据自身的类型
			// 无法区分「标量的元数据」与「对象的元数据」。
			mobj, metaIsObj := meta[k].(map[string]any)
			if replaced || !metaIsObj {
				mobj = make(map[string]any, len(pobj))
			}
			meta[k] = mobj

			if mergeInto(cobj, mobj, pobj, nowSec) {
				changed = true
			}
			// 规则 3：删空后保留为 {}，元数据同为 {}。
			// 这里什么都不做即可——空 map 就留在原处。
			continue
		}

		// 规则 5、6：标量与数组整体替换
		if !existed || !Equal(cur, pv) {
			changed = true
		}
		state[k] = Clone(pv)
		meta[k] = metaFor(pv, nowSec)
	}
	return changed
}

// metaFor 为一个新设置的值构造元数据。
//
// 结构与值同形：对象对应对象，数组对应逐元素的元数据数组，
// 标量对应 {"timestamp": N}。空对象对应 {}。
func metaFor(v any, nowSec int64) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, child := range t {
			// null 叶子在 update/accepted 的元数据里也带时间戳
			// （规则 6），但它在已存储文档中会被删除。
			out[k] = metaFor(child, nowSec)
		}
		return out
	case []any:
		// 规则 5：数组的元数据是逐元素的
		out := make([]any, len(t))
		for i, child := range t {
			out[i] = metaFor(child, nowSec)
		}
		return out
	default:
		return map[string]any{metaTimestampKey: nowSec}
	}
}

// pruneEmptySection 实现规则 4：顶层段为空则不保留。
//
// 与规则 3 的不对称是有意的：嵌套的空对象是用户数据的一部分
// （"这个子对象存在但没有字段"是有意义的状态），
// 而一个空的 desired 或 reported 段只是"没有这个段"的另一种写法。
func pruneEmptySection(o Object) Object {
	if len(o) == 0 {
		return nil
	}
	return o
}
