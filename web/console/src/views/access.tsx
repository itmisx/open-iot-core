/*
 * 访问控制：控制台用户（UI-024、UI-067）与兼容面的 access key（UI-075）。
 *
 * 两块放同一页，因为它们回答的是同一个问题的两半——「谁能动这套系统」。
 * 分成两页的话，发 access key 这件事会藏在一个没人想到去找的地方；
 * 而它们都只给 admin，权限边界也是同一条。但两块必须分开讲清楚：
 * 控制台用户登录界面，access key 给 SDK 用，角色名相同而作用的面不同。
 *
 * 角色不复用设备的 IoT Policy：设备的权限说的是"能发哪些主题"，
 * 控制台的权限说的是"能点哪些按钮"，混成一套会让其中一边被迫迁就
 * 另一边的粒度。
 *
 * 令牌只在创建时出现一次，服务端只留哈希。所以"刚建好的那一次"是
 * 唯一能拿到它的时机——它必须显示在一个要用户主动关掉的对话框里，
 * 而不是页面上一条会被下一次操作顶掉的横幅。
 */
import { useState } from 'react'
import { api, type AWSKey } from '../api'
import type { T } from '../i18n'
import {
  AsyncView, Badge, Button, Card, Confirm, CopyButton, Empty, Field, fmtTime, Modal, useAsync,
} from '../ui'

type User = { name: string; role: string }

const ROLES = ['reader', 'operator', 'admin']

export function AccessView(props: {
  token: string
  t: T
  lang: string
  self: string
  onUnauthorized: () => void
}) {
  const { token, t } = props
  const users = useAsync(() => api.listUsers(token), [token])
  const [creating, setCreating] = useState(false)
  const [removing, setRemoving] = useState<User | null>(null)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)

  return (
    <>
      {/* 这一页有两块，所以各自装在带标题的卡片里，动作也放各自的工具条上。
          页头的 main-actions 留空：一组按钮悬在两块之上，分不清它刷新的
          是哪一张表。两块的形状必须一样——同类里只给一块配标题就是双标。 */}
      <div className="main-head">
        <div>
          <h1>{t('nav.access')}</h1>
          <p className="faint">{t('access.lead')}</p>
        </div>
      </div>

      <Card title={t('access.usersTitle')}>
        {err ? (
          <div className="banner" data-tone="danger" role="alert">
            <div className="banner-body">{err}</div>
          </div>
        ) : null}
        <div className="toolbar">
          <span className="grow" />
          <Button size="sm" onClick={() => users.reload()} icon="refresh">
            {t('common.refresh')}
          </Button>
          <Button size="sm" variant="primary" onClick={() => setCreating(true)} icon="plus">
            {t('access.create')}
          </Button>
        </div>

        <AsyncView state={users} t={t} onUnauthorized={props.onUnauthorized}>
          {(page) =>
            page.users.length === 0 ? (
              // 这一页几乎总是空的：多数部署只用引导令牌。所以空状态要说清
              // "这是正常的"，否则看起来像列表没加载出来（UI-030）。
              //
              // 第三件事（怎么造出一条来）这里给的是按钮不是命令：命令行
              // 没有建控制台用户的子命令，印一条 openiotd 开头的假命令比
              // 不印更糟——用户会照着敲，然后得到 unknown command。
              <Empty
                title={t('access.emptyTitle')}
                body={t('access.emptyBody')}
                action={
                  <Button size="sm" variant="primary" onClick={() => setCreating(true)} icon="plus">
                    {t('access.create')}
                  </Button>
                }
              />
            ) : (
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>{t('common.name')}</th>
                      <th>{t('access.role')}</th>
                      <th className="num">{t('common.actions')}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {page.users.map((u) => (
                      <tr key={u.name}>
                        <td className="mono">
                          {u.name}
                          {/* 标出自己：删掉自己之后手里的令牌立刻失效，
                            而界面上不会有任何提示，下一次点击才报 401 */}
                          {u.name === props.self ? (
                            <span className="faint"> {t('access.selfTag')}</span>
                          ) : null}
                        </td>
                        <td>
                          <Badge tone={u.role === 'admin' ? 'warn' : 'muted'}>{u.role}</Badge>
                        </td>
                        <td className="num">
                          <Button
                            size="sm"
                            variant="danger"
                            onClick={() => setRemoving(u)}
                            icon="trash"
                          >
                            {t('common.delete')}
                          </Button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                <div className="table-foot">
                  <span>{t('access.count', String(page.users.length))}</span>
                </div>
              </div>
            )
          }
        </AsyncView>
      </Card>

      {creating ? (
        <CreateUser
          t={t}
          token={token}
          onClose={() => setCreating(false)}
          onCreated={() => users.reload()}
        />
      ) : null}

      <AWSKeys token={token} t={t} lang={props.lang} onUnauthorized={props.onUnauthorized} />

      {removing ? (
        <Confirm
          t={t}
          tone="danger"
          busy={busy}
          title={t('access.deleteTitle')}
          body={
            removing.name === props.self
              ? t('access.deleteSelfBody', removing.name)
              : t('access.deleteBody', removing.name)
          }
          confirmLabel={t('common.delete')}
          onCancel={() => setRemoving(null)}
          onConfirm={() => {
            setBusy(true)
            setErr(null)
            api
              .deleteUser(token, removing.name)
              .then(() => {
                setRemoving(null)
                users.reload()
              })
              .catch((e: Error) => setErr(e.message))
              .finally(() => setBusy(false))
          }}
        />
      ) : null}
    </>
  )
}

/**
 * CreateUser 是新建用户对话框。
 *
 * 它有两幕：先填表，建成之后换成"这是令牌，只显示这一次"。不在表单
 * 旁边顺手把令牌印出来——那条信息一旦被下一次操作顶掉就永远拿不回来，
 * 只能把这个用户删了重建。换成第二幕之后，关闭是用户自己的动作，
 * 他总得先看见。
 */
function CreateUser(props: { t: T; token: string; onClose: () => void; onCreated: () => void }) {
  const { t } = props
  const [name, setName] = useState('')
  const [role, setRole] = useState('reader')
  const [issued, setIssued] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)

  if (issued) {
    return (
      <Modal title={t('access.createdTitle')} onClose={props.onClose}>
        <div className="col">
          <div className="banner" data-tone="warn" role="status">
            <div className="banner-body">
              <strong>{t('access.tokenOnce')}</strong>
              {t('access.tokenOnceBody')}
            </div>
          </div>
          <Field label={t('access.name')}>
            <input readOnly value={name} />
          </Field>
          <pre className="json">{issued}</pre>
          <div className="row" style={{ justifyContent: 'flex-end' }}>
            <CopyButton value={issued} t={t} />
            <Button variant="primary" onClick={props.onClose}>
              {t('common.close')}
            </Button>
          </div>
        </div>
      </Modal>
    )
  }

  return (
    <Modal title={t('access.create')} onClose={props.onClose}>
      <form
        className="col"
        onSubmit={(e) => {
          e.preventDefault()
          setBusy(true)
          setErr(null)
          api
            .putUser(props.token, name.trim(), role)
            .then((out) => {
              setIssued(out.token)
              props.onCreated()
            })
            .catch((e2: Error) => setErr(e2.message))
            .finally(() => setBusy(false))
        }}
      >
        <Field label={t('access.name')} hint={t('access.nameHint')} error={err ?? undefined}>
          <input autoFocus value={name} onChange={(e) => setName(e.target.value)} />
        </Field>
        <Field label={t('access.role')} hint={t(`access.roleHint.${role}`)}>
          <select value={role} onChange={(e) => setRole(e.target.value)}>
            {ROLES.map((r) => (
              <option key={r} value={r}>
                {r}
              </option>
            ))}
          </select>
        </Field>
        <div className="row" style={{ justifyContent: 'flex-end' }}>
          <Button onClick={props.onClose} disabled={busy}>
            {t('common.cancel')}
          </Button>
          <Button type="submit" variant="primary" disabled={busy || !name.trim()}>
            {busy ? t('common.loading') : t('access.create')}
          </Button>
        </div>
      </form>
    </Modal>
  )
}

/*
 * AWSKeys 是兼容面 access key 的那一块（UI-075）。
 *
 * 与用户那块同一个形状：列表加「新建」对话框，密钥材料只在创建后的
 * 对话框里出现一次。不一致的话，人会以为其中一处还能再取回 secret。
 *
 * 比用户多一个「停用」：怀疑泄露时要的是立刻停掉**并且留下痕迹**，
 * 而删掉之后审计日志里的 `ak:AKIA…` 再也对不上人。两个动作都给，
 * 删除标红、停用不标——不可撤销的那个才配得上危险色。
 */
function AWSKeys(props: { token: string; t: T; lang: string; onUnauthorized: () => void }) {
  const { token, t, lang } = props
  const keys = useAsync(() => api.listAWSKeys(token), [token])
  const [creating, setCreating] = useState(false)
  const [removing, setRemoving] = useState<AWSKey | null>(null)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)

  const toggle = (k: AWSKey) => {
    setErr(null)
    api
      .setAWSKeyDisabled(token, k.id, !k.disabled)
      .then(() => keys.reload())
      .catch((e: Error) => setErr(e.message))
  }

  return (
    <Card title={t('akey.title')}>
      <p className="faint">{t('akey.lead')}</p>
      {err ? (
        <div className="banner" data-tone="danger" role="alert">
          <div className="banner-body">{err}</div>
        </div>
      ) : null}
      <div className="toolbar">
        <span className="grow" />
        <Button size="sm" onClick={() => keys.reload()} icon="refresh">
          {t('common.refresh')}
        </Button>
        <Button size="sm" variant="primary" onClick={() => setCreating(true)} icon="plus">
          {t('akey.create')}
        </Button>
      </div>

      <AsyncView state={keys} t={t} onUnauthorized={props.onUnauthorized}>
        {(page) =>
          page.keys.length === 0 ? (
            <Empty
              title={t('akey.emptyTitle')}
              body={t('akey.emptyBody')}
              action={
                <Button size="sm" variant="primary" onClick={() => setCreating(true)} icon="plus">
                  {t('akey.create')}
                </Button>
              }
            />
          ) : (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>{t('akey.id')}</th>
                    <th>{t('access.role')}</th>
                    <th>{t('common.status')}</th>
                    <th>{t('akey.note')}</th>
                    <th className="num">{t('common.actions')}</th>
                  </tr>
                </thead>
                <tbody>
                  {page.keys.map((k) => (
                    <tr key={k.id}>
                      <td className="mono">{k.id}</td>
                      <td>
                        <Badge tone={k.role === 'admin' ? 'warn' : 'muted'}>{k.role}</Badge>
                      </td>
                      <td>
                        <KeyStatus k={k} t={t} lang={lang} />
                      </td>
                      <td className="faint">{k.note ?? ''}</td>
                      <td className="num">
                        <Button size="sm" onClick={() => toggle(k)} icon="power">
                          {k.disabled ? t('akey.enable') : t('akey.disable')}
                        </Button>
                        <Button
                          size="sm"
                          variant="danger"
                          onClick={() => setRemoving(k)}
                          icon="trash"
                        >
                          {t('common.delete')}
                        </Button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              <div className="table-foot">
                <span>{t('akey.count', String(page.keys.length))}</span>
              </div>
            </div>
          )
        }
      </AsyncView>

      {creating ? (
        <CreateAWSKey
          t={t}
          token={token}
          onClose={() => setCreating(false)}
          onCreated={() => keys.reload()}
        />
      ) : null}

      {removing ? (
        <Confirm
          t={t}
          tone="danger"
          busy={busy}
          title={t('akey.deleteTitle')}
          body={t('akey.deleteBody', removing.id)}
          confirmLabel={t('common.delete')}
          onCancel={() => setRemoving(null)}
          onConfirm={() => {
            setBusy(true)
            setErr(null)
            api
              .deleteAWSKey(token, removing.id)
              .then(() => {
                setRemoving(null)
                keys.reload()
              })
              .catch((e: Error) => setErr(e.message))
              .finally(() => setBusy(false))
          }}
        />
      ) : null}
    </Card>
  )
}

/*
 * KeyStatus 把三种状态说成文字加颜色，不只给颜色（UI-061）。
 *
 * 「已过期」要算出来而不是等服务端标记：记录里只有到期时刻，
 * 而一把过期的钥匙在列表上看起来和可用的一模一样——人会以为它还能用，
 * 直到 SDK 报 401。
 */
function KeyStatus(props: { k: AWSKey; t: T; lang: string }) {
  const { k, t, lang } = props
  if (k.disabled) return <Badge tone="danger">{t('akey.disabled')}</Badge>
  if (k.expires && k.expires <= Date.now()) {
    return (
      <Badge tone="warn" title={fmtTime(k.expires, lang)}>
        {t('akey.expired')}
      </Badge>
    )
  }
  return (
    <Badge tone="ok" title={k.expires ? fmtTime(k.expires, lang) : undefined}>
      {k.expires ? t('akey.expiresAt', fmtTime(k.expires, lang)) : t('akey.active')}
    </Badge>
  )
}

/*
 * CreateAWSKey 与 CreateUser 一样有两幕：先填表，建成之后换成
 * 「这是 id 和 secret，只显示这一次」。
 */
function CreateAWSKey(props: { t: T; token: string; onClose: () => void; onCreated: () => void }) {
  const { t } = props
  const [role, setRole] = useState('reader')
  const [note, setNote] = useState('')
  const [days, setDays] = useState('0')
  const [issued, setIssued] = useState<{ id: string; secret: string } | null>(null)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)

  if (issued) {
    return (
      <Modal title={t('akey.createdTitle')} onClose={props.onClose}>
        <div className="col">
          <div className="banner" data-tone="warn" role="status">
            <div className="banner-body">
              <strong>{t('akey.secretOnce')}</strong>
              {t('akey.secretOnceBody')}
            </div>
          </div>
          <Field label={t('akey.id')}>
            <input readOnly value={issued.id} />
          </Field>
          <Field label={t('akey.secret')}>
            <input readOnly value={issued.secret} />
          </Field>
          <div className="row" style={{ justifyContent: 'flex-end' }}>
            <CopyButton value={issued.secret} t={t} />
            <Button variant="primary" onClick={props.onClose}>
              {t('common.close')}
            </Button>
          </div>
        </div>
      </Modal>
    )
  }

  return (
    <Modal title={t('akey.create')} onClose={props.onClose}>
      <form
        className="col"
        onSubmit={(e) => {
          e.preventDefault()
          setBusy(true)
          setErr(null)
          // 有效期在界面上是天数，接口收的是到期时刻：天数会随"什么时候
          // 建的"漂移，而到期时刻是确定的。换算放在这里而不是服务端，
          // 是因为只有这里知道用户看到的是哪一天。
          const n = Number(days)
          const expires = n > 0 ? Date.now() + n * 86400000 : 0
          api
            .createAWSKey(props.token, role, note.trim(), expires)
            .then((out) => {
              setIssued({ id: out.id, secret: out.secret })
              props.onCreated()
            })
            .catch((e2: Error) => setErr(e2.message))
            .finally(() => setBusy(false))
        }}
      >
        <Field label={t('access.role')} hint={t(`akey.roleHint.${role}`)} error={err ?? undefined}>
          <select autoFocus value={role} onChange={(e) => setRole(e.target.value)}>
            {ROLES.map((r) => (
              <option key={r} value={r}>
                {r}
              </option>
            ))}
          </select>
        </Field>
        <Field label={t('akey.note')} hint={t('akey.noteHint')}>
          <input value={note} onChange={(e) => setNote(e.target.value)} />
        </Field>
        <Field label={t('akey.expires')}>
          <select value={days} onChange={(e) => setDays(e.target.value)}>
            <option value="0">{t('akey.expiresNever')}</option>
            <option value="30">{t('akey.expires30')}</option>
            <option value="90">{t('akey.expires90')}</option>
          </select>
        </Field>
        <div className="row" style={{ justifyContent: 'flex-end' }}>
          <Button onClick={props.onClose} disabled={busy}>
            {t('common.cancel')}
          </Button>
          <Button type="submit" variant="primary" disabled={busy}>
            {busy ? t('common.loading') : t('akey.create')}
          </Button>
        </div>
      </form>
    </Modal>
  )
}
