package cluster

import (
	"bufio"
	"io"
	"strings"
	"sync"
)

// testFSM 是用例用的状态机：按序记下每条已提交命令。
//
// 它同时承担 applied index 的"持久化"（CLU-109）：换库之后状态机必须
// 自己记得应用到哪一条，而用例里那份记忆就是这个结构体——重建一个新的
// 就等于换了一块空盘。
type testFSM struct {
	mu      sync.Mutex
	entries map[uint64][]byte
	order   []uint64
	applied uint64
}

func newTestFSM() *testFSM { return &testFSM{entries: map[uint64][]byte{}} }

func (f *testFSM) apply(index uint64, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, dup := f.entries[index]; !dup {
		f.order = append(f.order, index)
	}
	f.entries[index] = append([]byte(nil), data...)
	if index > f.applied {
		f.applied = index
	}
	return nil
}

func (f *testFSM) appliedIndex() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.applied
}

func (f *testFSM) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.entries)
}

func (f *testFSM) payloads() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.order))
	for _, idx := range f.order {
		out = append(out, string(f.entries[idx]))
	}
	return out
}

func (f *testFSM) has(payload string) bool {
	for _, p := range f.payloads() {
		if p == payload {
			return true
		}
	}
	return false
}

// snapshotTo 与 restoreFrom 是最朴素的一行一条，够用例用。
func (f *testFSM) snapshotTo(w io.Writer) error {
	for _, p := range f.payloads() {
		if _, err := io.WriteString(w, p+"\n"); err != nil {
			return err
		}
	}
	return nil
}

func (f *testFSM) restoreFrom(r io.Reader) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries = map[uint64][]byte{}
	f.order = nil
	sc := bufio.NewScanner(r)
	var idx uint64
	for sc.Scan() {
		line := strings.TrimRight(sc.Text(), "\r")
		if line == "" {
			continue
		}
		idx++
		f.entries[idx] = []byte(line)
		f.order = append(f.order, idx)
	}
	return sc.Err()
}
