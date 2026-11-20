/* 实时连接（PRD UI-012）。 */
import { useEffect, useState } from 'react'
import { api, type Connection, type ConnectionDetail, type OnlinePoint } from '../api'
import type { T } from '../i18n'
import {
  AsyncView, Badge, Button, Confirm, Empty, EmptyFiltered, Modal, PadRow, Pager,
  fmtDuration, fmtTime, shortFP, tlsName, useAsync, useWindow,
} from '../ui'
import { href, navigate } from '../router'

export function ConnectionsView(props: {
  token: string
  t: T
  lang: string
  onUnauthorized: () => void
}) {
  const { token, t } = props
  const [prefix, setPrefix] = useState('')
  const [thing, setThing] = useState('')
  const [cursors, setCursors] = useState<string[]>([''])
  const [kick, setKick] = useState<Connection | null>(null)
  const [detail, setDetail] = useState<ConnectionDetail | null>(null)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)

  const after = cursors[cursors.length - 1] ?? ''
  // 筛选与分页都在服务端完成（UI-004 的同类约束）：
  // 十万连接下把全量拉到前端再过滤是一次几十 MB 的响应。
  const state = useAsync(
    () => api.listConns(token, { prefix, thing, after, limit: 50 }),
    [token, prefix, thing, after],
  )
  const win = useWindow(state.data?.connections ?? [])

  const resetPaging = () => setCursors([''])
  // 连接页的空表最容易被当成故障：它本来就常常是空的（设备断开即消失），
  // 所以要把"这是实时快照"说出来，并区分筛选滤空（UI-030、UI-032）。
  const filtered = prefix !== '' || thing !== ''
  const clearFilters = () => {
    setPrefix('')
    setThing('')
    resetPaging()
  }

  return (
    <>
      <div className="main-head">
        <div>
          <h1>{t('conns.title')}</h1>
          <p>{t('conns.subtitle')}</p>
        </div>
        <div className="main-actions">
          <Button onClick={state.reload} icon="refresh">{t('common.refresh')}</Button>
        </div>
      </div>

      <OnlineCurve token={token} t={t} />

      {err ? (
        <div className="banner" data-tone="danger" role="alert">
          <div className="banner-body">{err}</div>
        </div>
      ) : null}

      <div className="toolbar">
        <input
          className="grow"
          placeholder={t('conns.clientId')}
          value={prefix}
          onChange={(e) => {
            setPrefix(e.target.value)
            resetPaging()
          }}
        />
        <input
          className="grow"
          placeholder={t('certs.thing')}
          value={thing}
          onChange={(e) => {
            setThing(e.target.value)
            resetPaging()
          }}
        />
      </div>

      <AsyncView state={state} t={t} onUnauthorized={props.onUnauthorized}>
        {(page) => (
          <div className="table-wrap" ref={win.ref} style={{ maxHeight: 520, overflow: 'auto' }}>
            <table>
              <thead>
                <tr>
                  <th>{t('conns.clientId')}</th>
                  <th>{t('certs.thing')}</th>
                  <th>{t('conns.remote')}</th>
                  <th>{t('conns.proto')}</th>
                  <th>{t('conns.tls')}</th>
                  <th>{t('conns.auth')}</th>
                  <th className="num">{t('conns.since')}</th>
                  <th>{t('common.actions')}</th>
                </tr>
              </thead>
              <tbody>
                {page.connections.length === 0 ? (
                  <tr>
                    <td colSpan={8}>
                      {filtered ? (
                        <EmptyFiltered t={t} onClear={clearFilters} />
                      ) : (
                        <Empty
                          title={t('conns.emptyTitle')}
                          body={t('conns.emptyBody')}
                          cmd={t('common.connectCmd')}
                          note={t('conns.emptyDenied')}
                          action={
                            <>
                              <a className="btn" data-size="sm" href={href({ view: 'events' })}>
                                {t('nav.events')}
                              </a>
                              <a className="btn" data-size="sm" href={href({ view: 'debugger' })}>
                                {t('nav.debugger')}
                              </a>
                            </>
                          }
                        />
                      )}
                    </td>
                  </tr>
                ) : (
                  <>
                    <PadRow span={8} height={win.padTop} />
                    {win.slice.map((c) => (
                    <tr key={c.clientId}>
                      <td className="mono">{c.clientId}</td>
                      <td>
                        {c.thing ? (
                          <a href={href({ view: 'thing', name: c.thing })}>{c.thing}</a>
                        ) : (
                          <span className="faint">—</span>
                        )}
                      </td>
                      <td className="mono">{c.remote}</td>
                      <td>
                        <Badge tone="muted">MQTT {c.protoVer === 5 ? '5.0' : '3.1.1'}</Badge>
                      </td>
                      <td className="nowrap">
                        {tlsName(c.tlsVersion)}
                        {c.alpn ? <span className="faint"> · {c.alpn}</span> : null}
                      </td>
                      <td>
                        <span
                          title={
                            c.certFingerprint
                              ? `${t('certs.fingerprint')}: ${c.certFingerprint}`
                              : undefined
                          }
                        >
                          {c.authMethod}
                          {c.certFingerprint ? (
                            <span className="faint mono"> {shortFP(c.certFingerprint)}</span>
                          ) : null}
                        </span>
                      </td>
                      <td className="num nowrap" title={fmtTime(c.connectedAt, props.lang)}>
                        {fmtDuration(Date.now() - c.connectedAt)}
                      </td>
                      <td className="row">
                        <Button
                          size="sm"
                          onClick={() => {
                            setErr(null)
                            api
                              .getConn(token, c.clientId)
                              .then(setDetail)
                              .catch((e: Error) => setErr(e.message))
                          }}
                        >
                          {t('conns.detail')}
                        </Button>
                        <Button size="sm" variant="danger" onClick={() => setKick(c)} icon="kick">
                          {t('conns.kick')}
                        </Button>
                      </td>
                    </tr>
                  ))}
                    <PadRow span={8} height={win.padBottom} />
                  </>
                )}
              </tbody>
            </table>
            <Pager
              t={t}
              canPrev={cursors.length > 1}
              canNext={!!page.nextAfter}
              onPrev={() => setCursors((c) => c.slice(0, -1))}
              onNext={() => setCursors((c) => [...c, page.nextAfter])}
              summary={`${page.connections.length} / ${t('nav.connections')}`}
            />
          </div>
        )}
      </AsyncView>

      {kick ? (
        <Confirm
          t={t}
          tone="danger"
          busy={busy}
          title={t('conns.kick')}
          body={t('conns.kickConfirm', kick.clientId)}
          confirmLabel={t('conns.kick')}
          onCancel={() => setKick(null)}
          onConfirm={() => {
            setBusy(true)
            setErr(null)
            api
              .kickConn(token, kick.clientId)
              .then(() => {
                setKick(null)
                state.reload()
              })
              .catch((e: Error) => setErr(e.message))
              .finally(() => setBusy(false))
          }}
        />
      ) : null}
      {detail ? (
        <Modal title={detail.clientId} onClose={() => setDetail(null)}>
          <h3 style={{ margin: '0 0 8px', fontSize: 14 }}>{t('conns.subs')}</h3>
          {detail.subscriptions.length === 0 ? (
            <Empty title={t('conns.subsEmptyTitle')} body={t('conns.subsEmptyBody')} />
          ) : (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>{t('conns.subs')}</th>
                    <th className="num">QoS</th>
                  </tr>
                </thead>
                <tbody>
                  {detail.subscriptions.map((s) => (
                    <tr key={`${s.share ?? ''}/${s.filter}`}>
                      <td className="mono">
                        {s.share ? `$share/${s.share}/${s.filter}` : s.filter}
                      </td>
                      <td className="num">{s.qos}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          <h3 style={{ margin: '16px 0 8px', fontSize: 14 }}>{t('conns.inflight')}</h3>
          {detail.inflight.length === 0 ? (
            <p className="faint">{t('conns.inflightEmpty')}</p>
          ) : (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>{t('mqtt.topic')}</th>
                    <th className="num">QoS</th>
                    <th>{t('conns.state')}</th>
                  </tr>
                </thead>
                <tbody>
                  {detail.inflight.map((m) => (
                    <tr key={m.packetId}>
                      <td className="mono">{m.topic}</td>
                      <td className="num">{m.qos}</td>
                      <td>{m.state}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Modal>
      ) : null}
    </>
  )
}

function OnlineCurve(props: { token: string; t: T }) {
  const state = useAsync(
    (signal) => api.overview(props.token, signal),
    [props.token],
  )
  useEffect(() => {
    const id = setInterval(() => state.reload(), 5000)
    return () => clearInterval(id)
  }, [state.reload])
  const pts = state.data?.online ?? []
  return (
    <div className="curve card">
      <h2 className="card-title">{props.t('overview.curve')}</h2>
      {pts.length < 2 ? (
        <p className="faint">{props.t('overview.curveWait')}</p>
      ) : (
        <Curve pts={pts} />
      )}
    </div>
  )
}

function Curve(props: { pts: OnlinePoint[] }) {
  const pts = props.pts
  const w = 480
  const h = 72
  let max = 1
  for (const p of pts) {
    if (p.n > max) max = p.n
  }
  const d = pts.map((p, i) => {
    const x = (i / (pts.length - 1)) * w
    const y = h - 4 - (p.n / max) * (h - 8)
    return `${i === 0 ? 'M' : 'L'}${x.toFixed(1)},${y.toFixed(1)}`
  }).join(' ')
  return (
    <svg viewBox={`0 0 ${w} ${h}`} role="img">
      <path d={d} fill="none" stroke="var(--accent)" strokeWidth="2" />
    </svg>
  )
}

export { navigate }
