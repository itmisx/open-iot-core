package meta

import "errors"

// CmdKind 是复制日志中一条命令的类型。
type CmdKind uint8

const (
	// CmdNop 是占位命令，用于推进 revision 与确认活性。
	CmdNop CmdKind = iota
	CmdSetCert
	CmdDeleteCert
	CmdSetThing
	CmdDeleteThing
	CmdSetPolicySet
	CmdDeletePolicySet
	// CmdSetAttrSlots 设定被策略引用的属性名清单。
	// 热视图只物化这些属性，其余留在冷层（见 thingHot）。
	CmdSetAttrSlots
	// CmdSetThings 在一条日志里原子地写入一批设备。它只用于有界的控制面
	// 批量开通，避免百万设备产生百万条 Raft 日志。
	CmdSetThings
)

var cmdKindName = [...]string{
	"Nop", "SetCert", "DeleteCert", "SetThing", "DeleteThing",
	"SetPolicySet", "DeletePolicySet", "SetAttrSlots",
	"SetThings",
}

// MaxThingsPerCommand 限制一条复制日志里的设备数，防止管理面用超大请求
// 阻塞 Raft apply 与快照。512 台时日志仍是亚 MB 量级。
const MaxThingsPerCommand = 512

// MaxSetThingsCommandBytes 是批量设备命令的线上编码上限。条数上限挡不住
// 设备属性很大的批次；把日志限制在 2 MiB 内，避免单条提案长期占用
// Raft 复制与 apply 循环。
const MaxSetThingsCommandBytes = 2 << 20

func (k CmdKind) String() string {
	if int(k) >= len(cmdKindName) {
		return "Unknown"
	}
	return cmdKindName[k]
}

// CertCmd 是证书相关命令的载荷。
type CertCmd struct {
	Fingerprint    CertFingerprint
	Status         CertStatus
	PolicySet      PolicySetID
	Thing          string
	CA             string
	NotAfterUnixMs int64
	// Reason 记录吊销原因，进入审计事件（PRD PKI-034）。
	Reason string
}

// ThingCmd 是设备相关命令的载荷。
type ThingCmd struct {
	Name      string
	PolicySet PolicySetID
	// Attrs 按当前的属性槽位顺序排列。长度可以短于槽位数，
	// 缺失的槽位视为空值。
	Attrs []string
}

// PolicyCmd 是策略集相关命令的载荷。
type PolicyCmd struct {
	ID   PolicySetID
	Name string
	// Doc 是策略的原始文档，由 apply 阶段经注入的编译器编译。
	//
	// 这里传文档而不是编译产物，是因为日志要跨节点复制：编译产物是
	// 宿主语言里的一棵匹配器（PolicySet.Compiled 那个 any），序列化之后
	// 在另一个节点上还原不出来。各节点拿同一份文档各自编译，结果一致，
	// 而本包仍然不需要认识编译器（CLU-064）。
	//
	// 单机不走另一条路：它同样经 apply 编译（ARCH-057——单机是节点数为一
	// 的集群）。代价是控制面写入时编译两次，一次在前置校验里只为拿到
	// 「这份文档编译不过」的错误。那次校验必须留着：非法策略不得进日志
	// （CLU-046，日志提交后无法回滚）。
	Doc []byte
}

// RecordOp 是随命令一起复制的一条冷层记录。
//
// 本包不解释 Key 与 Value 的内容（与 PolicyCmd.Doc 交给编译器同一模式）：
// 键与正文都由控制面产出，meta 只保证它和热视图的改动落在**同一条已提交
// 日志**里，并按「先热视图、后冷层」的顺序应用（REG-110、REG-111）。
//
// 冷层必须进日志，否则 follower 上只有热视图：鉴权能过，而
// `GetThing`、证书正文、策略正文全是空的——空白节点靠快照追平之后
// （CLU-059）看起来是追上了，控制面读出来却什么都没有（CLU-065）。
type RecordOp struct {
	// Key 是 store.Key 产出的完整键。
	Key []byte
	// Value 是记录正文。Delete 为真时忽略它。
	Value []byte
	// Delete 为真表示删除该键。
	//
	// 不用「Value 为 nil 即删除」来表达：那会让一次把正文写成空字节的
	// 失误静默变成删除。
	Delete bool
}

// Command 是复制日志中的一条命令。
//
// 状态机对相同 ID 幂等（PRD CLU-036）：同一条命令被重复投递时，
// 第二次及以后的应用不改变状态，并返回首次应用得到的 revision。
// 这使客户端重试与至少一次投递语义的变更流都安全。
type Command struct {
	// ID 是命令的幂等键。为空时不做去重，仅适用于 CmdNop。
	ID   string
	Kind CmdKind
	// RequestedRevision 非 0 时启用乐观并发：
	// 当前 revision 与之不符则拒绝，返回 ErrRevisionConflict。
	RequestedRevision Revision

	Cert      *CertCmd
	Thing     *ThingCmd
	Things    []ThingCmd
	Policy    *PolicyCmd
	AttrSlots []string

	// Records 是与本命令一起提交的冷层记录。
	//
	// 可以单独出现在 CmdNop 上：角色别名、已注册 CA、检索开关这几样
	// 不进热视图，但在集群里同样必须复制，否则它们只存在于收到请求的
	// 那一个节点上。
	Records []RecordOp

	// Shadow 是一条影子命令的编码，本包不解释它（与 Records 同一模式）。
	//
	// 影子走同一条日志而不是另开一个组：各节点在 apply 阶段用同一份
	// 状态机算出同样的结果（SHDW-029、CLU-082）。不复制的话，设备连
	// A 更新影子，控制台在 B 上读到的是空。
	Shadow []byte
}

// Validate 检查命令的载荷是否与其类型匹配。
//
// 状态机在应用前校验，避免把非法命令写进日志——日志一旦提交就无法回滚，
// 只能以新命令补偿（PRD CLU-046）。
func (c *Command) Validate() error {
	for i := range c.Records {
		if len(c.Records[i].Key) == 0 {
			return errors.New("meta: 冷层记录的键为空")
		}
	}
	if len(c.Shadow) > 0 && c.ID == "" {
		// 影子命令要靠 ID 把结果交回发起方（shadow.ReplicatedStore 的
		// pending 表）。没有 ID 的话命令会被应用，而发起方永远拿不到
		// 结果——那是一次"看起来超时了但其实成功了"的写入。
		return errors.New("meta: 带影子命令的日志条目缺少 ID")
	}
	switch c.Kind {
	case CmdNop:
		return nil
	case CmdSetCert:
		if c.Cert == nil {
			return errors.New("meta: SetCert 缺少 Cert 载荷")
		}
		if c.Cert.Fingerprint == (CertFingerprint{}) {
			return errors.New("meta: SetCert 的证书指纹为空")
		}
		return nil
	case CmdDeleteCert:
		if c.Cert == nil || c.Cert.Fingerprint == (CertFingerprint{}) {
			return errors.New("meta: DeleteCert 缺少证书指纹")
		}
		return nil
	case CmdSetThing:
		if c.Thing == nil || c.Thing.Name == "" {
			return errors.New("meta: SetThing 缺少设备名")
		}
		return nil
	case CmdSetThings:
		if len(c.Things) == 0 {
			return errors.New("meta: SetThings 缺少设备")
		}
		if len(c.Things) > MaxThingsPerCommand {
			return errors.New("meta: SetThings 设备数超出上限")
		}
		seen := make(map[string]struct{}, len(c.Things))
		for i := range c.Things {
			name := c.Things[i].Name
			if name == "" {
				return errors.New("meta: SetThings 含空设备名")
			}
			if _, ok := seen[name]; ok {
				return errors.New("meta: SetThings 含重复设备名")
			}
			seen[name] = struct{}{}
		}
		raw, err := EncodeCommand(*c)
		if err != nil {
			return err
		}
		if len(raw) > MaxSetThingsCommandBytes {
			return errors.New("meta: SetThings 编码大小超出上限")
		}
		return nil
	case CmdDeleteThing:
		if c.Thing == nil || c.Thing.Name == "" {
			return errors.New("meta: DeleteThing 缺少设备名")
		}
		return nil
	case CmdSetPolicySet:
		if c.Policy == nil {
			return errors.New("meta: SetPolicySet 缺少 Policy 载荷")
		}
		if c.Policy.ID == NoPolicySet {
			return errors.New("meta: SetPolicySet 的 ID 非法")
		}
		if len(c.Policy.Doc) == 0 {
			// 没有文档的策略集在别的节点上编译不出匹配器，而它会被
			// 当成「已编译、但什么都不放行」装进热视图——设备连不上，
			// 日志里看不出是这条命令缺了正文。
			return errors.New("meta: SetPolicySet 缺少策略文档")
		}
		return nil
	case CmdDeletePolicySet:
		if c.Policy == nil || c.Policy.ID == NoPolicySet {
			return errors.New("meta: DeletePolicySet 的 ID 非法")
		}
		return nil
	case CmdSetAttrSlots:
		return nil
	default:
		return errors.New("meta: 未知命令类型 " + c.Kind.String())
	}
}

// Change 是一次已应用的元数据变更，经事件总线扇出（契约 11）。
//
// 吊销后立即踢连接就走这条链路：各节点在自己的 apply 循环里更新视图并
// 发出 Change，再由本节点的处理器断开受影响的连接。
// 广播由日志复制本身完成，不需要额外的跨节点失效通知机制（PRD CLU-019）。
type Change struct {
	Revision Revision
	Kind     CmdKind
	// Fingerprint 在证书类变更上非零。
	Fingerprint CertFingerprint
	// PrevStatus 与 Status 使消费者能判断是否发生了「变为不可连接」的转换，
	// 从而只在必要时去踢连接。
	PrevStatus CertStatus
	Status     CertStatus
	// Thing 在设备类变更上非空；证书类变更上是该证书绑定的设备。
	Thing string
	// PrevThing 是证书类变更前的设备绑定。
	//
	// 与 PrevStatus 同一用意：使消费者能判断绑定是否被改动或清除，
	// 从而只在必要时去踢连接。
	PrevThing string
	// PolicySet 在策略类变更上非 NoPolicySet。
	PolicySet PolicySetID
	Reason    string
}

// RequiresDisconnect 报告该变更是否要求断开相关连接。
//
// 从可连接状态转为不可连接即须立即断开所有在用该证书的活连接——
// 仅改状态不断连视为未完成吊销（PRD PKI-030）。
func (c *Change) RequiresDisconnect() bool {
	switch c.Kind {
	case CmdSetCert:
		if !c.PrevStatus.CanConnect() {
			// 此前就不可连接，没有活连接需要处理（PKI-111）。
			return false
		}
		if !c.Status.CanConnect() {
			return true
		}
		// 设备绑定被改动或清除同样须断连（PKI-109）。
		//
		// 连接的 ${iot:Connection.Thing.ThingName} 在握手时解析并固定。
		// 把证书改绑到另一台设备之后，旧连接仍以旧设备的身份收发——
		// 凭据已经移走，权限却没有跟着走。运维做的是"把这张证书给
		// 另一台设备"，不会预期旧设备还能继续用它。
		//
		// 原绑定为空时不断连（PKI-110）：那样的连接本来就解析不出设备变量，
		// 新增绑定不会让它多拿到任何东西（AuthzCtx 在握手时已固定）。
		return c.PrevThing != "" && c.PrevThing != c.Thing
	case CmdDeleteCert:
		return c.PrevStatus.CanConnect()
	default:
		return false
	}
}

// needsPersist 报告这条命令是否要往冷层或影子写东西。
//
// 单机写入只有这一类才进合批：不带记录也不带影子的命令只重建热视图，
// 对它们开写事务既没有收益，又会在启动期重放冷层时撞上 bbolt 的 mmaplock
// （见 LocalStore.Apply 的说明）。
func (c Command) needsPersist() bool {
	return len(c.Records) > 0 || len(c.Shadow) > 0
}
