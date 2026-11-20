/* 共用组件与小钩子。手写而不引组件库：避免设计锁定与打包体积。 */
import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type ReactNode,
  type RefObject,
} from 'react'
import { ApiError } from './api'
import type { T } from './i18n'
import { Icon, type IconName } from './icons'

/* ---------- 异步数据 ---------- */

export type Async<T> = {
  data: T | null
  error: Error | null
  loading: boolean
  reload: () => void
}

/**
 * useAsync 加载一次数据并提供重载。
 *
 * 取消在途请求是必需的而不是优化：视图切换时未取消的请求会在
 * 组件卸载后 setState，而更糟的是先发的慢请求可能后到，
 * 把新视图的数据覆盖成旧的。
 */
export function useAsync<D>(fn: (signal: AbortSignal) => Promise<D>, deps: unknown[]): Async<D> {
  const [data, setData] = useState<D | null>(null)
  const [error, setError] = useState<Error | null>(null)
  const [loading, setLoading] = useState(true)
  const [tick, setTick] = useState(0)
  const fnRef = useRef(fn)
  fnRef.current = fn

  useEffect(() => {
    const ac = new AbortController()
    let live = true
    setLoading(true)
    setError(null)
    fnRef
      .current(ac.signal)
      .then((d) => {
        if (live) {
          setData(d)
          setLoading(false)
        }
      })
      .catch((e: unknown) => {
        if (!live || ac.signal.aborted) return
        setError(e instanceof Error ? e : new Error(String(e)))
        setLoading(false)
      })
    return () => {
      live = false
      ac.abort()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, tick])

  const reload = useCallback(() => setTick((n) => n + 1), [])
  return { data, error, loading, reload }
}

/* ---------- 基础元素 ---------- */

export function Button(props: {
  children: ReactNode
  onClick?: (() => void) | undefined
  variant?: 'primary' | 'danger' | undefined
  size?: 'sm' | undefined
  disabled?: boolean | undefined
  title?: string | undefined
  type?: 'button' | 'submit' | undefined
  /*
   * 按钮图标。同一个动词在每一页都要用同一个图标——图标的价值在于
   * "扫一眼就认出来"，每页各配各的反而要人先学一遍。所以它作为
   * Button 的属性而不是让调用方自己摆 <Icon>：属性只能传一个名字，
   * 想不一致都难。
   */
  icon?: IconName | undefined
  /*
   * 无障碍名称。只有图标的按钮必须给，否则读屏软件念到的是"按钮"。
   *
   * 它必须在这里显式列出并转发：TypeScript **不检查带连字符的 JSX 属性**
   * （aria-*、data-* 都按可透传处理），所以调用方写了 aria-label 而这个
   * 包装组件没转发时，类型检查照样通过，标签静默消失——这比类型报错难
   * 发现得多，因为界面看起来完全正常。
   */
  'aria-label'?: string | undefined
}) {
  return (
    <button
      className="btn"
      type={props.type ?? 'button'}
      data-variant={props.variant}
      data-size={props.size}
      disabled={props.disabled}
      title={props.title}
      aria-label={props['aria-label']}
      onClick={props.onClick}
    >
      {props.icon ? <Icon name={props.icon} /> : null}
      {props.children}
    </button>
  )
}

export type Tone = 'ok' | 'warn' | 'danger' | 'info' | 'muted'

export function Badge(props: { tone: Tone; children: ReactNode; title?: string | undefined }) {
  return (
    <span className="badge" data-tone={props.tone} title={props.title}>
      <span className="badge-dot" aria-hidden="true" />
      {props.children}
    </span>
  )
}

export function Banner(props: { tone: Tone; title?: string | undefined; children: ReactNode }) {
  return (
    <div className="banner" data-tone={props.tone} role={props.tone === 'danger' ? 'alert' : undefined}>
      <div className="banner-body">
        {props.title ? <strong>{props.title}</strong> : null}
        {props.children}
      </div>
    </div>
  )
}

export function Field(props: {
  label: string
  hint?: string | undefined
  error?: string | undefined
  children: ReactNode
}) {
  return (
    <div className="field">
      <label>{props.label}</label>
      {props.children}
      {props.error ? (
        <span className="field-error">{props.error}</span>
      ) : props.hint ? (
        <span className="field-hint">{props.hint}</span>
      ) : null}
    </div>
  )
}

export function Card(props: { title?: string | undefined; children: ReactNode }) {
  return (
    <section className="card">
      {props.title ? <h2 className="card-title">{props.title}</h2> : null}
      {props.children}
    </section>
  )
}

export function Stat(props: { label: string; value: ReactNode; sub?: ReactNode | undefined }) {
  return (
    <div className="stat">
      <div className="stat-label">{props.label}</div>
      <div className="stat-value">{props.value}</div>
      {props.sub ? <div className="stat-sub">{props.sub}</div> : null}
    </div>
  )
}

/* ---------- 状态包装 ---------- */

/**
 * AsyncView 统一处理加载、错误与空态。
 *
 * 401 单独处理：它意味着令牌失效，继续重试没有意义，
 * 应当让用户重新输入而不是反复看到同一条错误。
 */
export function AsyncView<D>(props: {
  state: Async<D>
  t: T
  onUnauthorized?: (() => void) | undefined
  children: (data: D) => ReactNode
}) {
  const { state, t } = props
  useEffect(() => {
    if (state.error instanceof ApiError && state.error.status === 401) {
      props.onUnauthorized?.()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [state.error])

  if (state.error) {
    const hint = state.error instanceof ApiError ? state.error.hint : undefined
    return (
      <Banner tone="danger" title={t('common.error')}>
        <div>{state.error.message}</div>
        {hint ? <div className="faint">{hint}</div> : null}
        <div style={{ marginTop: 8 }}>
          <Button size="sm" onClick={state.reload}>
            {t('common.retry')}
          </Button>
        </div>
      </Banner>
    )
  }
  if (state.data === null) {
    return <div className="empty">{t('common.loading')}</div>
  }
  return <>{props.children(state.data)}</>
}

/**
 * Empty 是列表与流的空状态（PRD UI-030…032）。
 *
 * 一行"没有数据"不合格：空面板与坏掉的面板在界面上长得一模一样，
 * 而用户的第一反应一定是后者。于是每处空状态都要说清三件事——
 * 通道是通的、此刻确实没有数据、怎么造出一条来。前两件由 title 与
 * body 承担，第三件要么是一条可直接粘贴的命令（cmd），要么是一个
 * 就在眼前的按钮（action）；只说"去建一个"而不给路径，等于把问题
 * 原样推回给用户。
 *
 * 被筛选滤空是另一回事（UI-032）：那时处置办法是去掉筛选，与排查
 * 故障完全相反，所以调用方须分开两种说法，不能共用一句。
 */
export function Empty(props: {
  title: string
  body?: ReactNode
  cmd?: string | undefined
  note?: ReactNode
  action?: ReactNode
}) {
  return (
    <div className="empty">
      <p className="empty-title">{props.title}</p>
      {props.body ? <p className="empty-body">{props.body}</p> : null}
      {props.cmd ? <pre className="empty-cmd mono">{props.cmd}</pre> : null}
      {props.note ? <p className="empty-note">{props.note}</p> : null}
      {props.action ? <div className="empty-actions">{props.action}</div> : null}
    </div>
  )
}

/**
 * EmptyFiltered 是"筛选把结果滤空了"的统一说法（UI-032）。
 *
 * 三个带筛选的列表页说的是同一件事，分开写只会让措辞各漂一点。
 */
export function EmptyFiltered(props: { t: T; onClear: () => void }) {
  return (
    <Empty
      title={props.t('common.filteredTitle')}
      body={props.t('common.filteredBody')}
      action={
        <Button size="sm" onClick={props.onClear}>
          {props.t('common.clearFilter')}
        </Button>
      }
    />
  )
}

/* ---------- 对话框 ---------- */

/**
 * Confirm 是破坏性操作的确认框。
 *
 * 吊销证书与踢连接都会立刻影响在线设备，不该靠"点错了就点错了"。
 * 焦点在打开时移到取消按钮上：危险操作的默认焦点不应是确认。
 */
export function Confirm(props: {
  title: string
  body: ReactNode
  confirmLabel: string
  tone?: 'danger' | undefined
  busy?: boolean | undefined
  t: T
  onConfirm: () => void
  onCancel: () => void
}) {
  const cancelRef = useRef<HTMLDivElement>(null)
  useEffect(() => {
    cancelRef.current?.querySelector('button')?.focus()
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') props.onCancel()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  return (
    <div
      style={{
        position: 'fixed',
        inset: 0,
        background: 'rgb(0 0 0 / 45%)',
        display: 'grid',
        placeItems: 'center',
        padding: 24,
        zIndex: 50,
      }}
      onClick={(e) => {
        if (e.target === e.currentTarget) props.onCancel()
      }}
    >
      <div
        className="card"
        role="dialog"
        aria-modal="true"
        aria-label={props.title}
        style={{ width: 'min(460px, 100%)', boxShadow: 'var(--shadow)' }}
      >
        <h2 style={{ margin: '0 0 10px', fontSize: 16 }}>{props.title}</h2>
        <div style={{ marginBottom: 16, color: 'var(--text-dim)' }}>{props.body}</div>
        <div className="row" style={{ justifyContent: 'flex-end' }} ref={cancelRef}>
          <Button onClick={props.onCancel} disabled={props.busy}>
            {props.t('common.cancel')}
          </Button>
          <Button
            variant={props.tone === 'danger' ? 'danger' : 'primary'}
            onClick={props.onConfirm}
            disabled={props.busy}
          >
            {props.confirmLabel}
          </Button>
        </div>
      </div>
    </div>
  )
}

/**
 * Drawer 从右侧滑出，用来放"读一读就走"的内容（使用说明之类）。
 *
 * 不做成 Modal：说明是**对照着用**的——用户要一边看主题一边改上面的
 * 表单。居中的模态框把下面的内容整个盖住，只能看完关掉再操作，等于
 * 逼人记忆。抽屉贴边，主区还露着。
 *
 * 也不做成新页面：离开当前设备再回来，上下文就断了。
 *
 * 与 Modal 共享三条约定：Esc 关闭、点遮罩关闭、role=dialog。
 */
export function Drawer(props: { title: string; onClose: () => void; children: ReactNode }) {
  const panelRef = useRef<HTMLDivElement>(null)
  useEffect(() => {
    // 焦点移进抽屉，否则 Tab 还在后面的页面上走，读屏软件也留在原处
    panelRef.current?.focus()
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') props.onClose()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])
  return (
    <div className="drawer-scrim" onClick={(e) => { if (e.target === e.currentTarget) props.onClose() }}>
      <div
        className="drawer"
        role="dialog"
        aria-modal="true"
        aria-label={props.title}
        tabIndex={-1}
        ref={panelRef}
      >
        <div className="drawer-head">
          <h2>{props.title}</h2>
          <Button size="sm" onClick={props.onClose} aria-label={props.title}>
            ✕
          </Button>
        </div>
        <div className="drawer-body">{props.children}</div>
      </div>
    </div>
  )
}

export function Modal(props: { title: string; onClose: () => void; children: ReactNode }) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') props.onClose()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])
  return (
    <div
      style={{
        position: 'fixed',
        inset: 0,
        background: 'rgb(0 0 0 / 45%)',
        display: 'grid',
        placeItems: 'center',
        padding: 24,
        zIndex: 50,
        overflowY: 'auto',
      }}
      onClick={(e) => {
        if (e.target === e.currentTarget) props.onClose()
      }}
    >
      <div
        className="card"
        role="dialog"
        aria-modal="true"
        aria-label={props.title}
        style={{ width: 'min(680px, 100%)', boxShadow: 'var(--shadow)' }}
      >
        <div className="row" style={{ justifyContent: 'space-between', marginBottom: 14 }}>
          <h2 style={{ margin: 0, fontSize: 16 }}>{props.title}</h2>
          <Button size="sm" onClick={props.onClose}>
            ✕
          </Button>
        </div>
        {props.children}
      </div>
    </div>
  )
}

/* ---------- 格式化 ---------- */

/** 指纹在表格里只显示前 16 位，完整值放 title。 */
export function shortFP(fp: string): string {
  return fp.length > 16 ? `${fp.slice(0, 16)}…` : fp
}

export function fmtTime(ms: number, lang: string): string {
  if (!ms) return '—'
  return new Date(ms).toLocaleString(lang === 'zh' ? 'zh-CN' : 'en-US', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  })
}

export function fmtClock(ms: number): string {
  const d = new Date(ms)
  return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}:${String(
    d.getSeconds(),
  ).padStart(2, '0')}`
}

/** fmtDuration 把毫秒差显示为人类尺度，用于"已连接多久"。 */
export function fmtDuration(ms: number): string {
  if (ms < 0) return '—'
  const s = Math.floor(ms / 1000)
  if (s < 60) return `${s}s`
  const m = Math.floor(s / 60)
  if (m < 60) return `${m}m ${s % 60}s`
  const h = Math.floor(m / 60)
  if (h < 24) return `${h}h ${m % 60}m`
  return `${Math.floor(h / 24)}d ${h % 24}h`
}

export function fmtNum(n: number): string {
  return n.toLocaleString('en-US')
}

export function tlsName(v?: number): string {
  switch (v) {
    case 0x0303:
      return 'TLS 1.2'
    case 0x0304:
      return 'TLS 1.3'
    case undefined:
    case 0:
      return '—'
    default:
      return `0x${v.toString(16)}`
  }
}

export function certTone(status: string): Tone {
  switch (status) {
    case 'ACTIVE':
      return 'ok'
    case 'REVOKED':
      return 'danger'
    case 'INACTIVE':
      return 'muted'
    case 'PENDING_ACTIVATION':
      return 'warn'
    default:
      return 'muted'
  }
}

/** CopyButton 把长值（指纹、节点编号）一键复制。 */
export function CopyButton(props: { value: string; t: T }) {
  const [done, setDone] = useState(false)
  return (
    <Button
      size="sm"
      icon="copy"
      title={props.value}
      onClick={() => {
        navigator.clipboard?.writeText(props.value).then(
          () => {
            setDone(true)
            window.setTimeout(() => setDone(false), 1200)
          },
          () => {
            /* 剪贴板不可用时静默：title 里仍有完整值可手动复制 */
          },
        )
      }}
    >
      {done ? props.t('common.copied') : props.t('common.copy')}
    </Button>
  )
}

/** Pager 是服务端分页的翻页控件。 */
export function Pager(props: {
  t: T
  canPrev: boolean
  canNext: boolean
  onPrev: () => void
  onNext: () => void
  summary: ReactNode
}) {
  return (
    <div className="table-foot">
      <span>{props.summary}</span>
      <span className="row">
        <Button size="sm" disabled={!props.canPrev} onClick={props.onPrev}>
          {props.t('common.prev')}
        </Button>
        <Button size="sm" disabled={!props.canNext} onClick={props.onNext}>
          {props.t('common.next')}
        </Button>
      </span>
    </div>
  )
}

// Window 是一张长表里当前该渲染的行。
//
// 服务端每页最多 50 条，但百万行的要求是前端不要把整页以外的数据画进 DOM。
// 这里按滚动位置切一截，上下用空行撑起总高度。不引入新的前端依赖。
export type Window<T> = {
  ref: RefObject<HTMLDivElement>
  slice: T[]
  padTop: number
  padBottom: number
}

export function useWindow<T>(items: T[], rowHeight = 41): Window<T> {
  const ref = useRef<HTMLDivElement>(null)
  const [scrollTop, setScrollTop] = useState(0)
  const [height, setHeight] = useState(520)
  useEffect(() => {
    const el = ref.current
    if (!el) return
    const onScroll = () => setScrollTop(el.scrollTop)
    el.addEventListener('scroll', onScroll, { passive: true })
    const ro = new ResizeObserver(() => setHeight(el.clientHeight || 520))
    ro.observe(el)
    setHeight(el.clientHeight || 520)
    return () => {
      el.removeEventListener('scroll', onScroll)
      ro.disconnect()
    }
  }, [items.length])
  const overscan = 6
  const start = Math.max(0, Math.floor(scrollTop / rowHeight) - overscan)
  const count = Math.ceil(height / rowHeight) + overscan * 2
  const slice = items.slice(start, Math.min(items.length, start + count))
  return {
    ref,
    slice,
    padTop: start * rowHeight,
    padBottom: Math.max(0, (items.length - start - slice.length) * rowHeight),
  }
}

export function PadRow(props: { span: number; height: number }) {
  if (props.height <= 0) return null
  return (
    <tr aria-hidden="true">
      <td colSpan={props.span} style={{ height: props.height, padding: 0, border: 0 }} />
    </tr>
  )
}
