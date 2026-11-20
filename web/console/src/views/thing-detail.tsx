/*
 * 单设备聚合页（PRD UI-021 的"一屏"）。
 *
 * 属性、证书、策略、影子和时间线集中在一处。时间线读的是事件日志，
 * 不是另一份为界面准备的表。
 */
import { useState } from 'react'
import { api, type ShadowDoc, type TimelineEvent } from '../api'
import type { T } from '../i18n'
import { IssueCertDialog } from './issue-cert'
import { ShadowPanel } from './shadow'
import {
  AsyncView, Badge, Button, Card, Empty,
  certTone, fmtTime, shortFP, useAsync,
} from '../ui'
import { href } from '../router'

export function ThingDetailView(props: {
  token: string
  name: string
  t: T
  lang: string
  onUnauthorized: () => void
}) {
  const [issuing, setIssuing] = useState(false)
  const { token, t, name } = props
  const detail = useAsync(() => api.getThing(token, name), [token, name])
  const shadow = useAsync(
    () =>
      api.getShadow(token, name).catch(() => null as ShadowDoc | null),
    [token, name],
  )

  return (
    <>
      <div className="main-head">
        <div>
          <div className="row faint" style={{ fontSize: 12, marginBottom: 4 }}>
            <a href={href({ view: 'things' })}>{t('nav.things')}</a>
            <span>/</span>
          </div>
          <h1 className="mono" style={{ fontSize: 20 }}>
            {name}
          </h1>
        </div>
        <div className="main-actions">
          <Button variant="primary" onClick={() => setIssuing(true)} icon="plus">
            {t('issue.open')}
          </Button>
          <Button
            onClick={() => {
              detail.reload()
              shadow.reload()
            }}
           icon="refresh">
            {t('common.refresh')}
          </Button>
        </div>
      </div>

      <AsyncView state={detail} t={t} onUnauthorized={props.onUnauthorized}>
        {(d) => (
          <div className="col">
            <div className="spread">
              <Card title={t('common.detail')}>
                <dl className="kv">
                  <dt>{t('things.policy')}</dt>
                  <dd>
                    {d.thing.policyName ? (
                      <span className="row">
                        <Badge tone="info">{d.thing.policyName}</Badge>
                        <a
                          href={href({ view: 'debugger', policy: d.thing.policyName })}
                          className="faint"
                          style={{ fontSize: 12 }}
                        >
                          {t('policies.debug')}
                        </a>
                      </span>
                    ) : (
                      <span className="faint">{t('common.none')}</span>
                    )}
                  </dd>
                  <dt>{t('things.created')}</dt>
                  <dd>{fmtTime(d.thing.createdAt, props.lang)}</dd>
                  <dt>{t('things.attrs')}</dt>
                  <dd>
                    {d.thing.attributes && Object.keys(d.thing.attributes).length > 0
                      ? Object.entries(d.thing.attributes).map(([k, v]) => (
                          <div key={k}>
                            {k} = {v}
                          </div>
                        ))
                      : '—'}
                  </dd>
                </dl>
              </Card>

              <Card title={`${t('things.certs')} (${d.certificates.length})`}>
                {d.certificates.length === 0 ? (
                  <div className="faint">{t('common.none')}</div>
                ) : (
                  <div className="col" style={{ gap: 8 }}>
                    {d.certificates.map((fp) => (
                      <CertRow key={fp} token={token} fp={fp} t={t} />
                    ))}
                  </div>
                )}
              </Card>
            </div>

            <ShadowPanel
              token={token}
              name={name}
              t={t}
              state={shadow}
              onChanged={shadow.reload}
            />
            <TimelinePanel
              token={token}
              name={name}
              t={t}
              lang={props.lang}
              onUnauthorized={props.onUnauthorized}
            />
          </div>
        )}
      </AsyncView>
      {issuing ? (
        <IssueCertDialog
          token={token}
          t={t}
          thing={name}
          onClose={() => setIssuing(false)}
          onIssued={detail.reload}
        />
      ) : null}
    </>
  )
}

/** CertRow 在设备页里内联展示一张证书的状态。 */
function CertRow({ token, fp, t }: { token: string; fp: string; t: T }) {
  const state = useAsync(() => api.getCert(token, fp), [token, fp])
  if (!state.data) {
    return (
      <div className="mono faint" title={fp}>
        {shortFP(fp)}
      </div>
    )
  }
  const c = state.data
  return (
    <div className="row" style={{ justifyContent: 'space-between' }}>
      <span className="mono" title={fp}>
        {shortFP(fp)}
      </span>
      <span className="row">
        <Badge tone={certTone(c.status)} title={c.revokeReason}>
          {c.status}
        </Badge>
        <a href={href({ view: 'certificates' })} className="faint" style={{ fontSize: 12 }}>
          {t('common.detail')}
        </a>
      </span>
    </div>
  )
}

function TimelinePanel(props: {
  token: string
  name: string
  t: T
  lang: string
  onUnauthorized: () => void
}) {
  const state = useAsync(() => api.timeline(props.token, props.name), [props.token, props.name])
  return (
    <Card title={props.t('timeline.title')}>
      <AsyncView state={state} t={props.t} onUnauthorized={props.onUnauthorized}>
        {(page) =>
          page.events.length === 0 ? (
            <Empty
              title={props.t('timeline.emptyTitle')}
              body={props.t('timeline.emptyBody')}
              cmd={props.t('common.connectCmd')}
            />
          ) : (
            <div className="col">
              {page.events.map((ev) => (
                <div key={ev.seq} className="mqtt-row">
                  <div className="row">
                    <Badge tone="muted">{ev.kind}</Badge>
                    <span className="faint">{fmtTime(ev.at, props.lang)}</span>
                    {ev.clientId ? <span className="mono">{ev.clientId}</span> : null}
                  </div>
                  <div className="faint">{timelineText(ev)}</div>
                </div>
              ))}
            </div>
          )
        }
      </AsyncView>
    </Card>
  )
}

function timelineText(ev: TimelineEvent): string {
  if (ev.conn?.reasonText) return ev.conn.reasonText
  if (ev.conn?.result) return ev.conn.result
  if (ev.meta?.reason) return ev.meta.reason
  if (ev.authz) return `${ev.authz.decision ?? ''} ${ev.authz.action ?? ''} ${ev.authz.resource ?? ''}`.trim()
  if (ev.quota?.name) return ev.quota.name
  return ''
}
