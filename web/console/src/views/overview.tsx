/* 舰队总览（PRD UI-020）。 */
import { Fragment } from 'react'
import { api, type Overview, type QuotaStat } from '../api'
import type { T } from '../i18n'
import {
  AsyncView, Badge, Button, Card, CopyButton, Empty, Stat,
  fmtNum, useAsync,
} from '../ui'

export function OverviewView(props: { token: string; t: T; onUnauthorized: () => void }) {
  const { token, t } = props
  // 轮询 5 秒。实时指标曲线应走 SSE（UI-003），而这些是累计计数，
  // 轮询对它们足够且更省事。
  const state = useAsync((signal) => api.overview(token, signal), [token])

  return (
    <>
      <div className="main-head">
        <div>
          <h1>{t('overview.title')}</h1>
          <p>{t('overview.subtitle')}</p>
        </div>
        <div className="main-actions">
          <Button onClick={state.reload} icon="refresh">{t('common.refresh')}</Button>
        </div>
      </div>
      <AsyncView state={state} t={t} onUnauthorized={props.onUnauthorized}>
        {(d) => <OverviewBody d={d} t={t} />}
      </AsyncView>
    </>
  )
}

function OverviewBody({ d, t }: { d: Overview; t: T }) {
  const dropped = d.events.dropped
  return (
    <>
      {dropped > 0 ? (
        <div className="banner" data-tone="warn">
          <div className="banner-body">
            <strong>{t('overview.droppedWarn')}</strong>
            <span className="mono">{t('overview.dropped')}: {fmtNum(dropped)}</span>
          </div>
        </div>
      ) : null}

      <div className="stat-grid">
        <Stat label={t('overview.conns')} value={fmtNum(d.node.connections)} />
        <Stat label={t('overview.shadows')} value={fmtNum(d.node.shadows)} />
        <Stat label={t('overview.sessions')} value={fmtNum(d.node.sessions)} />
        <Stat label={t('overview.retained')} value={fmtNum(d.node.retained)} />
      </div>

      <div className="spread" style={{ marginBottom: 16 }}>
        <Card title={t('overview.node')}>
          <dl className="kv">
            <dt>node_id</dt>
            <dd className="row">
              <span className="truncate" title={d.node.nodeId}>{d.node.nodeId}</span>
              <CopyButton value={d.node.nodeId} t={t} />
            </dd>
            <dt>cluster_id</dt>
            <dd className="truncate" title={d.node.clusterId}>{d.node.clusterId}</dd>
            <dt>mode</dt>
            <dd>{d.node.mode}</dd>
            <dt>shard_count</dt>
            <dd>{d.node.shardCount}</dd>
            <dt>{t('common.status')}</dt>
            <dd>
              <Badge tone={d.node.ready ? 'ok' : 'warn'}>
                {d.node.ready ? t('overview.ready') : t('overview.notReady')}
              </Badge>
            </dd>
          </dl>
        </Card>

        <Card title={t('overview.events')}>
          <dl className="kv">
            {Object.entries(d.events.emitted).length === 0 ? (
              <>
                {/* 一个光秃秃的 0 读不出"总线是通的还是没接上"（UI-030）。 */}
                <dt>{t('overview.emitted')}</dt>
                <dd>
                  0 <span className="faint">· {t('overview.emittedNone')}</span>
                </dd>
              </>
            ) : (
              // key 必须挂在 Fragment 上：挂在子元素上 React 仍会告警，
              // 因为被遍历的是 Fragment 本身
              Object.entries(d.events.emitted).map(([k, v]) => (
                <Fragment key={k}>
                  <dt>{k}</dt>
                  <dd>{fmtNum(v)}</dd>
                </Fragment>
              ))
            )}
            <dt>{t('overview.dropped')}</dt>
            <dd>{fmtNum(dropped)}</dd>
            <dt>{t('overview.subscribers')}</dt>
            <dd>{d.events.subscribers}</dd>
          </dl>
        </Card>
      </div>

      <Card title={t('overview.quotas')}>
        {d.quotas.length === 0 ? (
          // 空配额表是健康状态，但它和"统计坏了"在界面上一样，所以要说出来
          // 这是正常的、以及什么时候会有东西出现（UI-030）。
          // 配额要有设备连上来才会被检查，所以"怎么不空"就是连一台
          // 设备——与连接页、事件流同一条命令（UI-030 的标准对全部
          // 同类生效，这两处是此前漏掉的）。
          <Empty
            title={t('overview.quotaNone')}
            body={t('overview.quotaNoneBody')}
            cmd={t('common.connectCmd')}
          />
        ) : (
          <QuotaTable rows={d.quotas} t={t} />
        )}
      </Card>
    </>
  )
}

/**
 * QuotaTable 按拒绝数降序排，尚未产生统计的定义留在表内。
 *
 * 异常排行比平均值有用（UI-020）：运维要的是"哪一项正在被打满"，
 * 而按名字排序会把它埋在三十多项里。未统计不等于零：零表示检查过但
 * 没有拒绝，破折号表示这项自启动以来尚未走到（UI-085）。
 */
function QuotaTable({ rows, t }: { rows: QuotaStat[]; t: T }) {
  const sorted = [...rows].sort((a, b) => b.rejected - a.rejected || b.allowed - a.allowed)
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>{t('overview.quotaName')}</th>
            <th>{t('overview.quotaScope')}</th>
            <th className="num">{t('overview.limit')}</th>
            <th className="num">{t('overview.allowed')}</th>
            <th className="num">{t('overview.rejected')}</th>
            <th className="num">{t('overview.current')}</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((q) => {
            const observed = q.allowed !== 0 || q.rejected !== 0 || q.current !== 0
            return (
              <tr key={q.name}>
                <td className="mono" title={q.prdRef ? `PRD ${q.prdRef}` : undefined}>
                  {q.name}
                </td>
                <td>
                  <Badge tone={q.scope === 'global' ? 'info' : 'muted'}>{q.scope}</Badge>
                </td>
                <td className="num">{q.limit === 0 ? '∞' : fmtNum(q.limit)}</td>
                <td className="num">{observed ? fmtNum(q.allowed) : '—'}</td>
                <td className="num">
                  {!observed ? (
                    '—'
                  ) : q.rejected > 0 ? (
                    <span style={{ color: 'var(--danger)', fontWeight: 600 }}>
                      {fmtNum(q.rejected)}
                    </span>
                  ) : (
                    '0'
                  )}
                </td>
                <td className="num">
                  {observed && q.kind === 'count' ? fmtNum(q.current) : '—'}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}
