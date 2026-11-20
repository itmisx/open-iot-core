package meta

import (
	"context"
	"errors"
)

// stubCompile 是测试用的占位编译器：把文档原样当成「编译产物」。
//
// 本包视编译产物为不透明值（PolicySet.Compiled 是 any），所以测试
// 不需要真的 authz 编译器——用它还会把 meta 的测试绑在策略语法上。
func stubCompile(_ string, doc []byte) (any, error) { return string(doc), nil }

// errBadDoc 是测试里编译器返回的失败。
var errBadDoc = errors.New("meta: 测试用的编译失败")

// ctx 返回测试用的背景上下文。
func ctx() context.Context { return context.Background() }

// fakeCold 是测试用的冷层：只记在内存里。
type fakeCold struct {
	vals map[string][]byte
	// failOn 非空时，写到这个键就失败。
	failOn string
}

func (f *fakeCold) Set(key, value []byte, _ bool) error {
	if f.failOn != "" && string(key) == f.failOn {
		return errBadDoc
	}
	f.vals[string(key)] = append([]byte(nil), value...)
	return nil
}

func (f *fakeCold) Delete(key []byte, _ bool) error {
	if f.failOn != "" && string(key) == f.failOn {
		return errBadDoc
	}
	delete(f.vals, string(key))
	return nil
}
