package meta

// 本文件是热视图的写入面，只允许 Store 的 apply 循环调用。
//
// 读取面（view.go）对外暴露且签名无 ctx 无 error；写入面不导出，
// 因此调用方不可能绕过复制日志直接改动视图——这是契约 05 读写分离的
// 结构性保证，而非约定。

// putCert 写入或覆盖一条证书记录，返回其先前的状态与设备绑定。
//
// 两者都要返回，因为 Change.RequiresDisconnect 要判断两类转换：
// 「从可连接变为不可连接」（吊销，PKI-030），以及「设备绑定被改动
// 或清除」（PKI-109）。后者同样会让活连接继续持有一个已经不属于
// 它的身份。
func (v *View) putCert(c *CertCmd) (CertStatus, string) {
	thing := noRef
	if c.Thing != "" {
		thing = v.names.intern(c.Thing)
	}
	s := &v.certs[certShardOf(c.Fingerprint)]
	s.mu.Lock()
	prev, prevThing := CertUnknown, noRef
	if old, ok := s.m[c.Fingerprint]; ok {
		prev, prevThing = old.status, old.thing
	}
	s.m[c.Fingerprint] = certHot{
		status:    c.Status,
		policySet: c.PolicySet,
		thing:     thing,
	}
	s.mu.Unlock()
	return prev, v.names.at(prevThing)
}

// deleteCert 删除一条证书记录，返回其先前的状态。
func (v *View) deleteCert(fp CertFingerprint) CertStatus {
	s := &v.certs[certShardOf(fp)]
	s.mu.Lock()
	prev := CertUnknown
	if old, ok := s.m[fp]; ok {
		prev = old.status
		delete(s.m, fp)
	}
	s.mu.Unlock()
	return prev
}

// putThing 写入或覆盖一条设备记录。
func (v *View) putThing(t *ThingCmd) {
	ref := v.names.intern(t.Name)
	s := &v.things[thingShardOf(ref)]
	s.mu.Lock()
	// 覆盖写时先释放旧的属性行，避免侧表泄漏。
	if old, ok := s.m[ref]; ok {
		v.attrs.release(old.attrsRef)
	}
	s.m[ref] = thingHot{policySet: t.PolicySet, attrsRef: v.attrs.put(t.Attrs)}
	s.mu.Unlock()
}

// deleteThing 删除一条设备记录。
//
// 名字本身留在驻留表中不回收：设备名的基数有限且删除罕见，
// 回收会使 nameRef 失效并需要整表重写，代价远大于收益。
func (v *View) deleteThing(name string) {
	ref, ok := v.names.find(name)
	if !ok {
		return
	}
	s := &v.things[thingShardOf(ref)]
	s.mu.Lock()
	if old, ok := s.m[ref]; ok {
		v.attrs.release(old.attrsRef)
	}
	delete(s.m, ref)
	s.mu.Unlock()
}

// putPolicySet 写入或覆盖一个已编译的策略集。
func (v *View) putPolicySet(rev Revision, p *PolicyCmd, compiled any) {
	v.policyMu.Lock()
	v.policies[p.ID] = PolicySet{
		ID: p.ID, Name: p.Name, Revision: rev, Compiled: compiled,
	}
	v.policyMu.Unlock()
}

// deletePolicySet 删除一个策略集。
func (v *View) deletePolicySet(id PolicySetID) {
	v.policyMu.Lock()
	delete(v.policies, id)
	v.policyMu.Unlock()
}

// setAttrSlots 设定被策略引用的属性名及其槽位顺序。
//
// 槽位顺序变化会使既有 Thing.attrs 的对应关系失效，因此调用方
// （策略编译器）在变更槽位后必须重放全部 SetThing 命令。
func (v *View) setAttrSlots(slots []string) {
	cp := make([]string, len(slots))
	copy(cp, slots)
	v.policyMu.Lock()
	v.attrSlots = cp
	v.policyMu.Unlock()
}
