package meta

import "sync"

// attrStore 存放被策略引用的设备属性。
//
// thingHot 只持有一个 int32 引用而非内嵌 []string：切片头即使为 nil
// 也占 24 字节，百万设备规模下仅此一项就是数十 MB。而「策略引用了设备属性」
// 是少数情形——多数部署的策略只用 ${iot:ClientId} 或设备名，
// 此时整个侧表是空的（PRD CLU-021）。
type attrStore struct {
	mu   sync.RWMutex
	rows [][]string
	free []int32
}

func newAttrStore() *attrStore { return &attrStore{} }

// put 存入一组属性，返回其引用。属性为空时返回 noRef 且不占用槽位。
func (a *attrStore) put(attrs []string) int32 {
	if len(attrs) == 0 {
		return noRef
	}
	row := make([]string, len(attrs))
	copy(row, attrs)
	a.mu.Lock()
	defer a.mu.Unlock()
	if n := len(a.free); n > 0 {
		r := a.free[n-1]
		a.free = a.free[:n-1]
		a.rows[r] = row
		return r
	}
	a.rows = append(a.rows, row)
	return int32(len(a.rows) - 1)
}

// get 取出一组属性。引用无效时返回 nil。
func (a *attrStore) get(r int32) []string {
	if r == noRef {
		return nil
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if r < 0 || int(r) >= len(a.rows) {
		return nil
	}
	return a.rows[r]
}

// release 归还一个槽位。
func (a *attrStore) release(r int32) {
	if r == noRef {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if r < 0 || int(r) >= len(a.rows) || a.rows[r] == nil {
		return
	}
	a.rows[r] = nil
	a.free = append(a.free, r)
}

// Len 返回在用的属性行数。
func (a *attrStore) Len() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.rows) - len(a.free)
}
