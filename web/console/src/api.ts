/*
 * Admin API 客户端。
 *
 * 全部请求指向 /api/admin/v1（PRD UI-002、API-003）：控制台绝不触碰
 * AWS 兼容面——那一面是冻结契约，给 SDK 用的，控制台用它会让任何
 * 界面需求都变成兼容性破坏。
 */

export const API_BASE = 'api/admin/v1'

const TOKEN_KEY = 'oiot.token'

/*
 * 令牌存在 sessionStorage 而非 localStorage。
 *
 * 浏览器导航无法携带 Authorization 头，所以静态外壳不鉴权、
 * 由界面拿令牌去调 API。那么令牌放在哪里就是一个安全选择：
 * sessionStorage 随标签页关闭即失效，localStorage 会一直留在磁盘上。
 * 对一个能吊销证书的令牌，前者是正确的默认。
 */
export function loadToken(): string {
  try {
    return sessionStorage.getItem(TOKEN_KEY) ?? ''
  } catch {
    return ''
  }
}

export function saveToken(t: string) {
  try {
    sessionStorage.setItem(TOKEN_KEY, t)
  } catch {
    // 存不下仍可在本次渲染中使用，由调用方持有
  }
}

export function clearToken() {
  try {
    sessionStorage.removeItem(TOKEN_KEY)
  } catch {
    /* 忽略 */
  }
}

/** ApiError 携带状态码，使调用方能区分 401 与 404。 */
export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
    readonly hint?: string,
  ) {
    super(message)
    this.name = 'ApiError'
  }
}

type Options = {
  method?: string
  body?: unknown
  /** raw 为真表示 body 已是字符串，直接发送（策略文档用）。 */
  raw?: boolean
  signal?: AbortSignal
}

export async function call<T>(token: string, path: string, opt: Options = {}): Promise<T> {
  const headers: Record<string, string> = { Authorization: `Bearer ${token}` }
  const init: RequestInit = {
    method: opt.method ?? 'GET',
    headers,
  }
  if (opt.signal) init.signal = opt.signal
  if (opt.body !== undefined) {
    init.body = opt.raw ? String(opt.body) : JSON.stringify(opt.body)
    ;(init.headers as Record<string, string>)['Content-Type'] = 'application/json'
  }

  const resp = await fetch(`${API_BASE}/${path}`, init)
  if (resp.status === 204) return undefined as T
  const text = await resp.text()
  if (!resp.ok) {
    // 服务端的错误形状是 {code, message, hint}。message 已是面向
    // 调用方的文案（它带着"谁在引用"这类信息），原样呈现而不是
    // 替换成通用提示。
    try {
      const e = JSON.parse(text) as { message?: string; hint?: string }
      throw new ApiError(resp.status, e.message ?? resp.statusText, e.hint)
    } catch (err) {
      if (err instanceof ApiError) throw err
      throw new ApiError(resp.status, text || resp.statusText)
    }
  }
  return text ? (JSON.parse(text) as T) : (undefined as T)
}

/* ---------- 类型 ---------- */

export type TimelineEvent = {
  kind: string
  seq: number
  at: number
  clientId?: string
  thing?: string
  conn?: { result?: string; reasonText?: string }
  meta?: { reason?: string }
  authz?: { action?: string; decision?: string; resource?: string }
  quota?: { name?: string }
}

export type NodeInfo = {
  nodeId: string
  clusterId: string
  shardCount: number
  mode: string
  ready: boolean
  connections: number
  shadows: number
  sessions: number
  retained: number
}

export type QuotaStat = {
  name: string
  scope: string
  kind: string
  limit: number
  allowed: number
  rejected: number
  current: number
  prdRef?: string
}

export type OnlinePoint = { t: number; n: number }

export type Overview = {
  node: NodeInfo
  quotas: QuotaStat[]
  events: {
    emitted: Record<string, number>
    dropped: number
    subscribers: number
    seq: number
  }
  online: OnlinePoint[]
}

export type Thing = {
  name: string
  attributes?: Record<string, string>
  policyName?: string
  createdAt: number
  updatedAt: number
}

export type ThingPage = { things: Thing[]; nextAfter?: string }

export type ThingDetail = { thing: Thing; certificates: string[] }

export type Certificate = {
  fingerprint: string
  status: string
  thing?: string
  policyName?: string
  subject?: string
  issuer?: string
  notAfter?: number
  revokeReason?: string
  createdAt: number
  updatedAt: number
}

export type CertPage = { certificates: Certificate[]; nextAfter?: string }

export type IssueCertRequest = {
  thing: string
  policyName: string
  commonName?: string
  validitySeconds?: number
}

export type IssuedCert = {
  fingerprint: string
  certificatePem: string
  chainPem: string
  notAfter: number
  thing: string
  policyName: string
  /**
   * 私钥正文，只出现这一次。
   *
   * 不是可选的：控制台只调 /certificates/issue（UI-083），而该端点必然
   * 回传私钥。声明成可选会在结果面板上留一条永不执行的分支，让"私钥
   * 可能没有"看起来是一种正常情形。
   */
  privateKeyPem: string
  warning?: string
}

export type Connection = {
  clientId: string
  thing?: string
  certFingerprint?: string
  protoVer: number
  remote: string
  connectedAt: number
  keepAliveMs: number
  tlsVersion?: number
  alpn?: string
  authMethod: string
  epoch: number
}

export type ConnSub = { filter: string; share?: string; qos: number }
export type ConnInflight = {
  packetId: number
  qos: number
  dup?: boolean
  state: string
  topic: string
}
export type ConnectionDetail = Connection & {
  subscriptions: ConnSub[]
  inflight: ConnInflight[]
}

export type ConnPage = { connections: Connection[]; nextAfter: string }

export type PolicyDetail = {
  name: string
  document: unknown
  createdAt: number
  updatedAt: number
}

export type ShadowDoc = {
  state: { desired?: unknown; reported?: unknown; delta?: unknown }
  metadata: { desired?: unknown; reported?: unknown }
  version: number
  timestamp: number
}

export type SimulateRequest = {
  certFingerprint?: string
  thing?: string
  policyNames?: string[]
  clientId?: string
  action: string
  resource: string
  certSubjectCommonName?: string
  certIssuerCommonName?: string
  certSerialNumber?: string
}

export type SimulateResult = {
  allowed: boolean
  decision: string
  policyName?: string
  statementIndex: number
  matchedResource?: string
  variables: { name: string; value: string; resolved: boolean }[]
  variablesTruncated?: boolean
  evaluatedPolicies: string[]
  advice?: string[]
}

/** AWSKey 是一把兼容面的 access key。列表里永远不含密钥材料。 */
export type AWSKey = {
  id: string
  role: string
  note?: string
  created: number
  /** Unix 毫秒，缺省或 0 表示永不过期。 */
  expires?: number
  disabled: boolean
}

export type StreamEvent = {
  /** 为真表示这是订阅时回填的历史事件，不是刚发生的。 */
  backlog?: boolean
  kind: string
  node: string
  seq: number
  at: number
  clientId?: string
  thing?: string
  conn?: {
    result: string
    reasonCode: number
    reasonText?: string
    certFingerprint?: string
    certCommonName?: string
    authMethod?: string
    protoVer?: number
    remote?: string
    tlsVersion?: number
    alpn?: string
    handshakeMs?: number
    durationMs?: number
  }
  sub?: { filter: string; share?: string; qos: number; unsubscribe?: boolean; granted: number }
  authz?: {
    action: string
    resource: string
    decision: string
    policyName?: string
    statementIndex: number
    variables?: { name: string; value: string }[]
  }
  meta?: {
    revision: number
    certFingerprint?: string
    thing?: string
    reason?: string
    requiresDisconnect?: boolean
  }
  quota?: { name: string; subject?: string; limit: number }
  audit?: { action: string; target?: string; actor?: string; remote?: string; method?: string; path?: string }
}

/* ---------- 端点 ---------- */

export const api = {
  overview: (t: string, signal?: AbortSignal) =>
    call<Overview>(t, 'overview', signal ? { signal } : {}),
  node: (t: string) => call<NodeInfo>(t, 'node'),

  listThings: (t: string, q: { prefix?: string; policy?: string; after?: string; limit?: number }) =>
    call<ThingPage>(t, `things${qs(q)}`),
  getThing: (t: string, name: string) => call<ThingDetail>(t, `things/${enc(name)}`),
  putThing: (t: string, name: string, body: { policyName?: string; attributes?: Record<string, string> }) =>
    call<void>(t, `things/${enc(name)}`, { method: 'PUT', body }),
  deleteThing: (t: string, name: string) =>
    call<void>(t, `things/${enc(name)}`, { method: 'DELETE' }),

  getShadow: (t: string, name: string, shadow?: string) =>
    call<ShadowDoc>(t, `things/${enc(name)}/shadow${shadow ? `?name=${enc(shadow)}` : ''}`),
  updateShadow: (t: string, name: string, body: unknown, shadow?: string) =>
    call<unknown>(t, `things/${enc(name)}/shadow${shadow ? `?name=${enc(shadow)}` : ''}`, {
      method: 'PUT',
      body,
    }),
  deleteShadow: (t: string, name: string, shadow?: string) =>
    call<unknown>(t, `things/${enc(name)}/shadow${shadow ? `?name=${enc(shadow)}` : ''}`, {
      method: 'DELETE',
    }),
  listShadows: (t: string, name: string) =>
    call<{ results: string[] }>(t, `things/${enc(name)}/shadows`),

  listCerts: (t: string, q: { status?: string; thing?: string; after?: string; limit?: number }) =>
    call<CertPage>(t, `certificates${qs(q)}`),
  getCert: (t: string, fp: string) => call<Certificate>(t, `certificates/${enc(fp)}`),
  setCertStatus: (t: string, fp: string, status: string, reason: string) =>
    call<void>(t, `certificates/${enc(fp)}/status`, { method: 'PUT', body: { status, reason } }),
  deleteCert: (t: string, fp: string) =>
    call<void>(t, `certificates/${enc(fp)}`, { method: 'DELETE' }),
  /**
   * 签发证书，服务端生成密钥对。
   *
   * 响应里的 privateKeyPem 只出现这一次，服务端不留存（PKI-010）。
   * 调用方必须当场交给用户保存，不能指望之后再取。
   *
   * 这是控制台唯一的签发路径（UI-083）。CSR 签发仍在服务端
   * （/certificates/sign-csr，PKI-105），但浏览器里没有办法生成并保管
   * 设备私钥，所以它只经命令行与 API 使用，这里不再包一层。
   */
  issueCert: (t: string, body: IssueCertRequest) =>
    call<IssuedCert>(t, 'certificates/issue', { method: 'POST', body }),

  listConns: (t: string, q: { prefix?: string; thing?: string; after?: string; limit?: number }) =>
    call<ConnPage>(t, `connections${qs(q)}`),
  getConn: (t: string, id: string) => call<ConnectionDetail>(t, `connections/${enc(id)}`),
  kickConn: (t: string, id: string) =>
    call<void>(t, `connections/${enc(id)}`, { method: 'DELETE' }),

  listPolicies: (t: string) => call<{ policies: string[] }>(t, 'policies'),
  getPolicy: (t: string, name: string) => call<PolicyDetail>(t, `policies/${enc(name)}`),
  putPolicy: (t: string, name: string, doc: string) =>
    call<void>(t, `policies/${enc(name)}`, { method: 'PUT', body: doc, raw: true }),
  deletePolicy: (t: string, name: string) =>
    call<void>(t, `policies/${enc(name)}`, { method: 'DELETE' }),

  simulate: (t: string, req: SimulateRequest) =>
    call<SimulateResult>(t, 'authz/simulate', { method: 'POST', body: req }),

  session: (t: string) => call<{ name: string; role: string }>(t, 'session'),
  listUsers: (t: string) => call<{ users: { name: string; role: string }[] }>(t, 'users'),
  putUser: (t: string, name: string, role: string) =>
    call<{ name: string; role: string; token: string }>(t, `users/${enc(name)}`, {
      method: 'PUT',
      body: { role },
    }),
  deleteUser: (t: string, name: string) => call<void>(t, `users/${enc(name)}`, { method: 'DELETE' }),
  listAWSKeys: (t: string) => call<{ keys: AWSKey[] }>(t, 'awskeys'),
  // 返回里的 secret 只有这一次。服务端存的是密文，之后任何接口都取不到明文。
  createAWSKey: (t: string, role: string, note: string, expires: number) =>
    call<{ id: string; secret: string; role: string }>(t, 'awskeys', {
      method: 'POST',
      body: { role, note, expires },
    }),
  setAWSKeyDisabled: (t: string, id: string, disabled: boolean) =>
    call<AWSKey>(t, `awskeys/${enc(id)}`, { method: 'PATCH', body: { disabled } }),
  deleteAWSKey: (t: string, id: string) =>
    call<void>(t, `awskeys/${enc(id)}`, { method: 'DELETE' }),
  timeline: (t: string, name: string) =>
    call<{ events: TimelineEvent[] }>(t, `things/${enc(name)}/timeline`),
}

function enc(s: string) {
  return encodeURIComponent(s)
}

function qs(o: Record<string, string | number | undefined>) {
  const p = new URLSearchParams()
  for (const [k, v] of Object.entries(o)) {
    if (v !== undefined && v !== '' && v !== 0) p.set(k, String(v))
  }
  const s = p.toString()
  return s ? `?${s}` : ''
}

/*
 * eventStreamURL 构造 SSE 地址。
 *
 * EventSource 不支持自定义请求头。粘贴的 Bearer 令牌因此走查询参数。
 */
export function eventStreamURL(token: string, kinds: string[]): string {
  const p = new URLSearchParams()
  p.set('token', token)
  if (kinds.length > 0) p.set('kinds', kinds.join(','))
  const q = p.toString()
  return `${API_BASE}/events${q ? `?${q}` : ''}`
}

/*
 * mqttSocketURL 构造测试客户端的 WebSocket 地址。
 *
 * 与事件流一样，浏览器设不了 Authorization，令牌走查询参数。
 * 地址相对当前页面解析，这样控制台挂在子路径下时仍指向管理面。
 */
export function mqttSocketURL(token: string): string {
  return socketURL(token, 'mqtt')
}

/** captureSocketURL 是主题抓包的 WebSocket。 */
export function captureSocketURL(token: string): string {
  return socketURL(token, 'capture')
}

function socketURL(token: string, path: 'mqtt' | 'capture'): string {
  const p = new URLSearchParams()
  p.set('token', token)
  const q = p.toString()
  const u = new URL(`${API_BASE}/${path}${q ? `?${q}` : ''}`, window.location.href)
  u.protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  return u.toString()
}
