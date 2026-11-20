/* 证书管理（PRD UI-021、PKI-030 的界面入口）。 */
import { useState } from 'react'
import { api, type Certificate } from '../api'
import type { T } from '../i18n'
import { IssueCertDialog } from './issue-cert'
import {
  AsyncView, Badge, Button, Confirm, Empty, EmptyFiltered, Field, Modal, PadRow, Pager,
  certTone, fmtTime, shortFP, useAsync, useWindow,
} from '../ui'
import { href } from '../router'

const STATUSES = ['ACTIVE', 'INACTIVE', 'REVOKED', 'PENDING_ACTIVATION']

export function CertificatesView(props: {
  token: string
  t: T
  lang: string
  onUnauthorized: () => void
}) {
  const [issuing, setIssuing] = useState(false)
  const { token, t } = props
  const [status, setStatus] = useState('')
  const [thing, setThing] = useState('')
  const [cursors, setCursors] = useState<string[]>([''])
  const [revoking, setRevoking] = useState<Certificate | null>(null)
  const [removing, setRemoving] = useState<Certificate | null>(null)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)

  const after = cursors[cursors.length - 1] ?? ''
  const state = useAsync(
    () => api.listCerts(token, { status, thing, after, limit: 50 }),
    [token, status, thing, after],
  )
  const win = useWindow(state.data?.certificates ?? [])
  const resetPaging = () => setCursors([''])
  // 被筛选滤空与一张证书都没有，处置办法完全不同（UI-032）。
  const filtered = status !== '' || thing !== ''
  const clearFilters = () => {
    setStatus('')
    setThing('')
    resetPaging()
  }

  const setStatusTo = (c: Certificate, next: string) => {
    setBusy(true)
    setErr(null)
    api
      .setCertStatus(token, c.fingerprint, next, '')
      .then(state.reload)
      .catch((e: Error) => setErr(e.message))
      .finally(() => setBusy(false))
  }

  return (
    <>
      <div className="main-head">
        <div>
          <h1>{t('certs.title')}</h1>
          <p>{t('certs.subtitle')}</p>
        </div>
        <div className="main-actions">
          <Button variant="primary" onClick={() => setIssuing(true)} icon="plus">
            {t('issue.open')}
          </Button>
          <Button onClick={state.reload} icon="refresh">{t('common.refresh')}</Button>
        </div>
      </div>

      {err ? (
        <div className="banner" data-tone="danger" role="alert">
          <div className="banner-body">{err}</div>
        </div>
      ) : null}

      <div className="toolbar">
        <select
          style={{ width: 'auto', minWidth: 180 }}
          value={status}
          onChange={(e) => {
            setStatus(e.target.value)
            resetPaging()
          }}
        >
          <option value="">{t('common.status')}：{t('common.all')}</option>
          {STATUSES.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
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
                  <th>{t('certs.fingerprint')}</th>
                  <th>{t('common.status')}</th>
                  <th>{t('certs.thing')}</th>
                  <th>{t('things.policy')}</th>
                  <th>{t('certs.notAfter')}</th>
                  <th>{t('common.actions')}</th>
                </tr>
              </thead>
              <tbody>
                {page.certificates.length === 0 ? (
                  <tr>
                    <td colSpan={6}>
                      {filtered ? (
                        <EmptyFiltered t={t} onClear={clearFilters} />
                      ) : (
                        <Empty
                          title={t('certs.emptyTitle')}
                          body={t('certs.emptyBody')}
                          cmd={t('certs.emptyCmd')}
                          action={
                            <Button variant="primary" size="sm" onClick={() => setIssuing(true)} icon="plus">
                              {t('issue.open')}
                            </Button>
                          }
                        />
                      )}
                    </td>
                  </tr>
                ) : (
                  <>
                    <PadRow span={6} height={win.padTop} />
                    {win.slice.map((c) => (
                    <tr key={c.fingerprint}>
                      <td className="mono" title={c.fingerprint}>
                        {shortFP(c.fingerprint)}
                      </td>
                      <td>
                        <Badge tone={certTone(c.status)} title={c.revokeReason}>
                          {c.status}
                        </Badge>
                      </td>
                      <td>
                        {c.thing ? (
                          <a href={href({ view: 'thing', name: c.thing })}>{c.thing}</a>
                        ) : (
                          <span className="faint">—</span>
                        )}
                      </td>
                      <td>
                        {c.policyName ? (
                          <Badge tone="info">{c.policyName}</Badge>
                        ) : (
                          <span className="faint">—</span>
                        )}
                      </td>
                      <td className="nowrap faint">
                        <ExpiryCell notAfter={c.notAfter} t={t} lang={props.lang} />
                      </td>
                      <td>
                        <span className="row">
                          {c.status === 'ACTIVE' ? (
                            <Button
                              size="sm"
                              disabled={busy}
                              onClick={() => setStatusTo(c, 'INACTIVE')}
                            >
                              {t('certs.deactivate')}
                            </Button>
                          ) : c.status === 'INACTIVE' || c.status === 'PENDING_ACTIVATION' ? (
                            <Button
                              size="sm"
                              disabled={busy}
                              onClick={() => setStatusTo(c, 'ACTIVE')}
                            >
                              {t('certs.activate')}
                            </Button>
                          ) : null}
                          {c.status !== 'REVOKED' ? (
                            <Button size="sm" variant="danger" onClick={() => setRevoking(c)} icon="trash">
                              {t('certs.revoke')}
                            </Button>
                          ) : null}
                          <Button size="sm" variant="danger" onClick={() => setRemoving(c)} icon="trash">
                            {t('common.delete')}
                          </Button>
                        </span>
                      </td>
                    </tr>
                  ))}
                    <PadRow span={6} height={win.padBottom} />
                  </>
                )}
              </tbody>
            </table>
            <Pager
              t={t}
              canPrev={cursors.length > 1}
              canNext={!!page.nextAfter}
              onPrev={() => setCursors((c) => c.slice(0, -1))}
              onNext={() => setCursors((c) => [...c, page.nextAfter ?? ''])}
              summary={`${page.certificates.length} / ${t('nav.certificates')}`}
            />
          </div>
        )}
      </AsyncView>

      {revoking ? (
        <RevokeDialog
          t={t}
          cert={revoking}
          onCancel={() => setRevoking(null)}
          onConfirm={(reason) => {
            setBusy(true)
            setErr(null)
            api
              .setCertStatus(token, revoking.fingerprint, 'REVOKED', reason)
              .then(() => {
                setRevoking(null)
                state.reload()
              })
              .catch((e: Error) => {
                setErr(e.message)
                setRevoking(null)
              })
              .finally(() => setBusy(false))
          }}
          busy={busy}
        />
      ) : null}

      {removing ? (
        <Confirm
          t={t}
          tone="danger"
          busy={busy}
          title={t('common.delete')}
          body={t('certs.deleteConfirm', shortFP(removing.fingerprint))}
          confirmLabel={t('common.delete')}
          onCancel={() => setRemoving(null)}
          onConfirm={() => {
            setBusy(true)
            setErr(null)
            api
              .deleteCert(token, removing.fingerprint)
              .then(() => {
                setRemoving(null)
                state.reload()
              })
              .catch((e: Error) => {
                setErr(e.message)
                setRemoving(null)
              })
              .finally(() => setBusy(false))
          }}
        />
      ) : null}
      {issuing ? (
        <IssueCertDialog
          token={props.token}
          t={t}
          onClose={() => setIssuing(false)}
          onIssued={state.reload}
        />
      ) : null}
    </>
  )
}

/**
 * ExpiryCell 把到期时间按紧迫程度着色。
 *
 * 到期是 IoT 部署里最常见的长期隐患（PRD PKI-080 的由来），
 * 一个纯文本的日期列无法让人在扫视时发现它。
 */
function ExpiryCell({ notAfter, t, lang }: { notAfter?: number | undefined; t: T; lang: string }) {
  if (!notAfter) return <span>—</span>
  const days = Math.floor((notAfter - Date.now()) / 86_400_000)
  if (days < 0) {
    return <Badge tone="danger">{t('certs.notAfter')} · {days}d</Badge>
  }
  if (days < 30) {
    return (
      <Badge tone="warn" title={fmtTime(notAfter, lang)}>
        {t('certs.expiringSoon')} · {days}d
      </Badge>
    )
  }
  return <span title={fmtTime(notAfter, lang)}>{days}d</span>
}

/**
 * RevokeDialog 要求填写原因。
 *
 * 原因会进审计，而"为什么吊销了这张证书"是事后复盘必问的问题。
 * 不强制填写会让多数记录是空的。
 */
function RevokeDialog(props: {
  t: T
  cert: Certificate
  busy: boolean
  onCancel: () => void
  onConfirm: (reason: string) => void
}) {
  const { t } = props
  const [reason, setReason] = useState('')
  return (
    <Modal title={t('certs.revokeTitle')} onClose={props.onCancel}>
      <div className="col">
        <div className="banner" data-tone="danger">
          <div className="banner-body">{t('certs.revokeDesc')}</div>
        </div>
        <dl className="kv">
          <dt>{t('certs.fingerprint')}</dt>
          <dd>{props.cert.fingerprint}</dd>
          {props.cert.thing ? (
            <>
              <dt>{t('certs.thing')}</dt>
              <dd>{props.cert.thing}</dd>
            </>
          ) : null}
        </dl>
        <Field label={t('certs.revokeReason')}>
          <input
            value={reason}
            onChange={(e) => setReason(e.target.value)}
            placeholder={t('certs.reasonPlaceholder')}
          />
        </Field>
        <div className="row" style={{ justifyContent: 'flex-end' }}>
          <Button onClick={props.onCancel} disabled={props.busy}>
            {t('common.cancel')}
          </Button>
          <Button
            variant="danger"
            disabled={props.busy || !reason.trim()}
            onClick={() => props.onConfirm(reason.trim())}
           icon="trash">
            {t('certs.revoke')}
          </Button>
        </div>
      </div>
    </Modal>
  )
}
