/*
 * 实时事件流（PRD UI-003、API-051）。
 *
 * 走 SSE 而非轮询：连接建立、授权拒绝、吊销生效这些都是瞬时事件，
 * 轮询会把它们漏掉。UI-003 把轮询明确定为降级手段。
 */
import { Fragment, useEffect, useRef, useState } from 'react'
import { eventStreamURL, type StreamEvent } from '../api'
import type { T } from '../i18n'
import { Badge, Button, Card, Empty, fmtClock, shortFP, type Tone } from '../ui'
import { href } from '../router'

const KINDS = ['Conn', 'Subscription', 'Authz', 'Meta', 'Quota', 'Cluster', 'Audit']

/** 环形上限：事件流可以持续数小时，无上限的数组会把标签页吃光。 */
const MAX_ROWS = 500

export function EventsView(props: { token: string; t: T; filterThing?: string | undefined }) {
  const { token, t } = props
  const [kinds, setKinds] = useState<string[]>([])
  const [paused, setPaused] = useState(false)
  const [rows, setRows] = useState<StreamEvent[]>([])
  const [state, setState] = useState<'connecting' | 'open' | 'closed'>('connecting')
  const [total, setTotal] = useState(0)
  // synced 为真表示回填已推完，后续都是实时事件。
  const [synced, setSynced] = useState(false)
  const pausedRef = useRef(paused)
  pausedRef.current = paused

  useEffect(() => {
    const es = new EventSource(eventStreamURL(token, kinds))
    setState('connecting')
    setRows([])
    setTotal(0)
    es.onopen = () => setState('open')
    es.onerror = () => {
      // EventSource 自带重连，这里只反映当前状态而不手动重连——
      // 手动重连会与它自带的退避叠加成请求风暴
      setState('closed')
    }

    const onEvent = (m: MessageEvent<string>) => {
      setTotal((n) => n + 1)
      if (pausedRef.current) return
      try {
        const ev = JSON.parse(m.data) as StreamEvent
        setRows((prev) => {
          const next = [ev, ...prev]
          return next.length > MAX_ROWS ? next.slice(0, MAX_ROWS) : next
        })
      } catch {
        /* 单条解析失败不该中断整条流 */
      }
    }

    // 必须逐个类型挂监听，不能只用 onmessage。
    //
    // 服务端给每条事件都写了 event 字段（event: Conn 等），而按 SSE
    // 规范 onmessage 只接收类型为 "message" 的事件，也就是没有 event
    // 字段的那些。只挂 onmessage 的话，一条都收不到——事件流会永远
    // 是空的，且因为连接本身是成功的，状态灯还显示"实时"。
    for (const k of KINDS) es.addEventListener(k, onEvent as EventListener)
    // 回填结束的标记，用来画"以上为历史"的分隔线。
    es.addEventListener('sync', () => setSynced(true))
    // 兜底：若将来服务端发出未命名事件，不要静默丢弃。
    es.onmessage = onEvent

    return () => {
      for (const k of KINDS) es.removeEventListener(k, onEvent as EventListener)
      es.close()
    }
  }, [token, kinds])

  const shown = props.filterThing
    ? rows.filter((r) => r.thing === props.filterThing)
    : rows

  return (
    <>
      <div className="main-head">
        <div>
          <h1>{t('events.title')}</h1>
          <p>{t('events.subtitle')}</p>
        </div>
        <div className="main-actions">
          <Badge tone={state === 'open' ? 'ok' : state === 'connecting' ? 'warn' : 'danger'}>
            {state === 'open'
              ? paused
                ? t('events.paused')
                : t('events.live')
              : state === 'connecting'
                ? t('common.loading')
                : t('events.disconnected')}
          </Badge>
          <Button onClick={() => setPaused((p) => !p)}>
            {paused ? t('events.resume') : t('events.pause')}
          </Button>
          <Button onClick={() => setRows([])}>{t('events.clear')}</Button>
        </div>
      </div>

      <div className="toolbar">
        <span className="faint" style={{ fontSize: 12 }}>
          {t('events.kinds')}:
        </span>
        {KINDS.map((k) => {
          const on = kinds.length === 0 || kinds.includes(k)
          return (
            <Button
              key={k}
              size="sm"
              variant={kinds.includes(k) ? 'primary' : undefined}
              onClick={() =>
                setKinds((cur) =>
                  cur.includes(k) ? cur.filter((x) => x !== k) : [...cur, k],
                )
              }
              title={on ? undefined : t('common.filter')}
            >
              {k}
            </Button>
          )
        })}
        <span className="grow" />
        <span className="faint mono" style={{ fontSize: 12 }}>
          {t('events.count', String(total))}
        </span>
      </div>

      <Card>
        {shown.length === 0 ? (
          <EmptyFeed t={t} synced={synced} filtered={kinds.length > 0 || !!props.filterThing} />
        ) : (
          <div className="feed">
            {shown.map((ev, i) => (
              <Fragment key={`${ev.node}-${ev.seq}`}>
                {/* 历史与实时的分界。回填是倒序渲染的，所以分隔线画在
                    "上一条是实时、这一条是历史"的位置上。 */}
                {ev.backlog && !shown[i - 1]?.backlog && i > 0 ? (
                  <div className="feed-divider">{t('events.backlogAbove')}</div>
                ) : null}
                <EventRow ev={ev} t={t} />
              </Fragment>
            ))}
          </div>
        )}
      </Card>
    </>
  )
}

/**
 * 空的事件流。
 *
 * 三件事（UI-030）：流是通的、现在确实没有活动、怎么造出一条来。
 * 回填推完之前只显示加载中（UI-033），否则首屏会先闪一下"没有活动"
 * 再被历史事件填满。
 */
function EmptyFeed({ t, synced, filtered }: { t: T; synced: boolean; filtered: boolean }) {
  if (!synced) return <div className="empty">{t('common.loading')}</div>
  if (filtered) {
    return <Empty title={t('events.emptyTitle')} body={t('events.emptyFiltered')} />
  }
  return (
    <Empty
      title={t('events.emptyTitle')}
      body={t('events.emptyHint')}
      cmd={t('common.connectCmd')}
    />
  )
}

function EventRow({ ev, t }: { ev: StreamEvent; t: T }) {
  const [tone, label, detail] = describe(ev, t)
  return (
    <div className="feed-row">
      <span className="feed-time">{fmtClock(ev.at)}</span>
      <span>
        <Badge tone={tone}>{label}</Badge>
      </span>
      <span className="feed-detail">{detail}</span>
    </div>
  )
}

/**
 * describe 把一条事件变成可读的一行。
 *
 * 色调按"是否需要人关注"而不是按事件类型分配：被拒的连接、
 * 授权拒绝、吊销生效都是 danger，正常连接是 muted。
 * 按类型上色会让一屏全是花的，反而看不出异常。
 */
function describe(ev: StreamEvent, t: T): [Tone, string, React.ReactNode] {
  const who = ev.thing ? (
    <a href={href({ view: 'thing', name: ev.thing })}>{ev.thing}</a>
  ) : (
    <span className="mono">{ev.clientId || '—'}</span>
  )

  if (ev.conn) {
    const bad = ev.conn.result !== 'Accepted'
    return [
      bad ? 'danger' : ev.conn.durationMs ? 'muted' : 'ok',
      ev.conn.durationMs ? 'disconnect' : 'connect',
      <>
        {who}
        {ev.conn.remote ? <span className="faint mono"> {ev.conn.remote}</span> : null}
        {ev.conn.reasonText ? (
          <span className={bad ? '' : 'faint'}> · {ev.conn.reasonText}</span>
        ) : null}
        {ev.conn.handshakeMs ? (
          <span className="faint"> · {ev.conn.handshakeMs.toFixed(1)}ms</span>
        ) : null}
        {ev.conn.durationMs ? (
          <span className="faint"> · {Math.round(ev.conn.durationMs / 1000)}s</span>
        ) : null}
      </>,
    ]
  }
  if (ev.sub) {
    return [
      ev.sub.granted >= 0x80 ? 'danger' : 'info',
      ev.sub.unsubscribe ? 'unsubscribe' : 'subscribe',
      <>
        {who} <span className="mono">{ev.sub.filter}</span>
        <span className="faint"> QoS{ev.sub.qos}</span>
      </>,
    ]
  }
  if (ev.authz) {
    const denied = ev.authz.decision !== 'Allow'
    return [
      denied ? 'danger' : 'ok',
      ev.authz.action.toLowerCase(),
      <>
        {who} <span className="mono">{ev.authz.resource}</span>
        <span className="faint"> · {ev.authz.decision}</span>
        {ev.authz.policyName ? (
          <span className="faint">
            {' '}
            · {ev.authz.policyName}[{ev.authz.statementIndex}]
          </span>
        ) : null}
        {denied && ev.authz.variables && ev.authz.variables.length > 0 ? (
          <span className="faint mono">
            {' '}
            · {ev.authz.variables.map((v) => `${v.name}=${v.value}`).join(' ')}
          </span>
        ) : null}
      </>,
    ]
  }
  if (ev.meta) {
    return [
      ev.meta.requiresDisconnect ? 'danger' : 'info',
      ev.meta.requiresDisconnect ? 'revoked' : 'meta',
      <>
        {ev.meta.certFingerprint ? (
          <span className="mono" title={ev.meta.certFingerprint}>
            {shortFP(ev.meta.certFingerprint)}
          </span>
        ) : (
          who
        )}
        {ev.meta.reason ? <span> · {ev.meta.reason}</span> : null}
        <span className="faint"> · rev {ev.meta.revision}</span>
      </>,
    ]
  }
  if (ev.quota) {
    return [
      'warn',
      'quota',
      <>
        <span className="mono">{ev.quota.name}</span>
        {ev.quota.subject ? <span className="faint"> {ev.quota.subject}</span> : null}
        <span className="faint"> · {t('overview.limit')} {ev.quota.limit}</span>
      </>,
    ]
  }
  if (ev.audit) {
    return [
      'info',
      ev.audit.action,
      <>
        <span className="mono">{ev.audit.target || '—'}</span>
        {ev.audit.actor ? <span className="faint"> · {ev.audit.actor}</span> : null}
        {ev.audit.remote ? <span className="faint mono"> {ev.audit.remote}</span> : null}
      </>,
    ]
  }
  return ['muted', ev.kind.toLowerCase(), who]
}
