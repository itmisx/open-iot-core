/* 设备管理（PRD UI-021）。 */
import { useState } from 'react'
import { api, type Thing } from '../api'
import type { T } from '../i18n'
import {
  AsyncView, Badge, Button, Confirm, Empty, EmptyFiltered, Field, Modal, PadRow, Pager,
  fmtTime, useAsync, useWindow,
} from '../ui'
import { Icon } from '../icons'
import { href } from '../router'

export function ThingsView(props: {
  token: string
  t: T
  lang: string
  onUnauthorized: () => void
}) {
  const { token, t } = props
  const [prefix, setPrefix] = useState('')
  const [policy, setPolicy] = useState('')
  const [cursors, setCursors] = useState<string[]>([''])
  const [editing, setEditing] = useState<Thing | 'new' | null>(null)
  const [removing, setRemoving] = useState<Thing | null>(null)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)

  const after = cursors[cursors.length - 1] ?? ''
  const state = useAsync(
    () => api.listThings(token, { prefix, policy, after, limit: 50 }),
    [token, prefix, policy, after],
  )
  const win = useWindow(state.data?.things ?? [])
  const policies = useAsync(() => api.listPolicies(token), [token])

  const resetPaging = () => setCursors([''])
  // 空表要分两种说法（UI-032）：被筛选滤空的处置办法是去掉筛选，
  // 与"一台设备都还没有"完全不是一回事。
  const filtered = prefix !== '' || policy !== ''
  const clearFilters = () => {
    setPrefix('')
    setPolicy('')
    resetPaging()
  }

  return (
    <>
      <div className="main-head">
        <div>
          <h1>{t('things.title')}</h1>
          <p>{t('things.subtitle')}</p>
        </div>
        <div className="main-actions">
          <Button onClick={state.reload} icon="refresh">{t('common.refresh')}</Button>
          <Button variant="primary" onClick={() => setEditing('new')} icon="plus">
            {t('common.create')}
          </Button>
        </div>
      </div>

      {err ? (
        <div className="banner" data-tone="danger" role="alert">
          <div className="banner-body">{err}</div>
        </div>
      ) : null}

      <div className="toolbar">
        <input
          className="grow"
          placeholder={t('things.prefix')}
          value={prefix}
          onChange={(e) => {
            setPrefix(e.target.value)
            resetPaging()
          }}
        />
        <select
          style={{ width: 'auto', minWidth: 160 }}
          value={policy}
          onChange={(e) => {
            setPolicy(e.target.value)
            resetPaging()
          }}
        >
          <option value="">{t('things.policy')}：{t('common.all')}</option>
          {(policies.data?.policies ?? []).map((p) => (
            <option key={p} value={p}>
              {p}
            </option>
          ))}
        </select>
      </div>

      <AsyncView state={state} t={t} onUnauthorized={props.onUnauthorized}>
        {(page) => (
          <div className="table-wrap" ref={win.ref} style={{ maxHeight: 520, overflow: 'auto' }}>
            <table>
              <thead>
                <tr>
                  <th>{t('common.name')}</th>
                  <th>{t('things.policy')}</th>
                  <th>{t('things.attrs')}</th>
                  <th>{t('things.created')}</th>
                  <th>{t('common.actions')}</th>
                </tr>
              </thead>
              <tbody>
                {page.things.length === 0 ? (
                  <tr>
                    <td colSpan={5}>
                      {filtered ? (
                        <EmptyFiltered t={t} onClear={clearFilters} />
                      ) : (
                        <Empty
                          title={t('things.emptyTitle')}
                          body={t('things.emptyBody')}
                          cmd={t('things.emptyCmd')}
                          action={
                            <Button variant="primary" size="sm" onClick={() => setEditing('new')}>
                              {t('things.newTitle')}
                            </Button>
                          }
                        />
                      )}
                    </td>
                  </tr>
                ) : (
                  <>
                    <PadRow span={5} height={win.padTop} />
                    {win.slice.map((th) => (
                    <tr key={th.name}>
                      <td className="mono">
                        <a href={href({ view: 'thing', name: th.name })}>{th.name}</a>
                      </td>
                      <td>
                        {th.policyName ? (
                          <Badge tone="info">{th.policyName}</Badge>
                        ) : (
                          <span className="faint">{t('common.none')}</span>
                        )}
                      </td>
                      <td className="mono faint">
                        {th.attributes && Object.keys(th.attributes).length > 0
                          ? Object.entries(th.attributes)
                              .map(([k, v]) => `${k}=${v}`)
                              .join(' ')
                          : '—'}
                      </td>
                      <td className="nowrap faint">{fmtTime(th.createdAt, props.lang)}</td>
                      <td>
                        <span className="row">
                          {/* 影子放在列表上，而不是只藏在设备详情页里：
                              不点进去就看不出这个产品有影子功能（UI-069）。 */}
                          <a
                            className="btn"
                            data-size="sm"
                            href={href({ view: 'shadow', name: th.name })}
                            onClick={(e) => e.stopPropagation()}
                          >
                            <Icon name="shadow" />
                            {t('things.shadow')}
                          </a>
                          <Button size="sm" onClick={() => setEditing(th)} icon="edit">
                            {t('common.edit')}
                          </Button>
                          <Button size="sm" variant="danger" onClick={() => setRemoving(th)} icon="trash">
                            {t('common.delete')}
                          </Button>
                        </span>
                      </td>
                    </tr>
                  ))}
                    <PadRow span={5} height={win.padBottom} />
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
              summary={`${page.things.length} / ${t('nav.things')}`}
            />
          </div>
        )}
      </AsyncView>

      {editing ? (
        <ThingEditor
          t={t}
          token={token}
          thing={editing === 'new' ? null : editing}
          policies={policies.data?.policies ?? []}
          onClose={() => setEditing(null)}
          onSaved={() => {
            setEditing(null)
            state.reload()
          }}
        />
      ) : null}

      {removing ? (
        <Confirm
          t={t}
          tone="danger"
          busy={busy}
          title={t('common.delete')}
          body={t('things.deleteConfirm', removing.name)}
          confirmLabel={t('common.delete')}
          onCancel={() => setRemoving(null)}
          onConfirm={() => {
            setBusy(true)
            setErr(null)
            api
              .deleteThing(token, removing.name)
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
    </>
  )
}

function ThingEditor(props: {
  t: T
  token: string
  thing: Thing | null
  policies: string[]
  onClose: () => void
  onSaved: () => void
}) {
  const { t } = props
  const [name, setName] = useState(props.thing?.name ?? '')
  const [policy, setPolicy] = useState(props.thing?.policyName ?? '')
  const [attrs, setAttrs] = useState(
    Object.entries(props.thing?.attributes ?? {})
      .map(([k, v]) => `${k}=${v}`)
      .join('\n'),
  )
  const [err, setErr] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  const save = () => {
    const parsed: Record<string, string> = {}
    for (const line of attrs.split('\n')) {
      const s = line.trim()
      if (!s) continue
      const i = s.indexOf('=')
      if (i <= 0) {
        setErr(`${t('things.attrsHint')}: ${s}`)
        return
      }
      parsed[s.slice(0, i).trim()] = s.slice(i + 1).trim()
    }
    setBusy(true)
    setErr(null)
    api
      .putThing(props.token, name, {
        ...(policy ? { policyName: policy } : {}),
        attributes: parsed,
      })
      .then(props.onSaved)
      .catch((e: Error) => setErr(e.message))
      .finally(() => setBusy(false))
  }

  return (
    <Modal
      title={props.thing ? t('things.editTitle') : t('things.newTitle')}
      onClose={props.onClose}
    >
      <div className="col">
        {err ? (
          <div className="banner" data-tone="danger" role="alert">
            <div className="banner-body">{err}</div>
          </div>
        ) : null}
        <Field label={t('common.name')} hint={t('things.nameHint')}>
          <input
            value={name}
            disabled={!!props.thing}
            onChange={(e) => setName(e.target.value)}
            placeholder="dev-001"
          />
        </Field>
        <Field label={t('things.policy')}>
          <select value={policy} onChange={(e) => setPolicy(e.target.value)}>
            <option value="">{t('common.none')}</option>
            {props.policies.map((p) => (
              <option key={p} value={p}>
                {p}
              </option>
            ))}
          </select>
        </Field>
        <Field label={t('things.attrs')} hint={t('things.attrsHint')}>
          <textarea rows={4} value={attrs} onChange={(e) => setAttrs(e.target.value)} />
        </Field>
        <div className="row" style={{ justifyContent: 'flex-end' }}>
          <Button onClick={props.onClose} disabled={busy}>
            {t('common.cancel')}
          </Button>
          <Button variant="primary" onClick={save} disabled={busy || !name}>
            {t('common.save')}
          </Button>
        </div>
      </div>
    </Modal>
  )
}
