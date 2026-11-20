/* 策略管理（PRD UI-021）。 */
import { useState } from 'react'
import { api } from '../api'
import type { T } from '../i18n'
import { AsyncView, Button, Confirm, Drawer, Empty, Field, Modal, useAsync } from '../ui'
import { Icon } from '../icons'
import { href } from '../router'

export function PoliciesView(props: { token: string; t: T; onUnauthorized: () => void }) {
  const [helping, setHelping] = useState(false)
  const { token, t } = props
  const state = useAsync(() => api.listPolicies(token), [token])
  const [editing, setEditing] = useState<string | 'new' | null>(null)
  const [removing, setRemoving] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)

  return (
    <>
      {helping ? <PolicyHelp t={t} onClose={() => setHelping(false)} /> : null}

      <div className="main-head">
        <div>
          <h1>{t('policies.title')}</h1>
          <p>{t('policies.subtitle')}</p>
        </div>
        <div className="main-actions">
          <Button onClick={() => setHelping(true)} icon="help">{t('policies.help')}</Button>
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

      <AsyncView state={state} t={t} onUnauthorized={props.onUnauthorized}>
        {(d) => (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>{t('common.name')}</th>
                  <th>{t('common.actions')}</th>
                </tr>
              </thead>
              <tbody>
                {d.policies.length === 0 ? (
                  <tr>
                    <td colSpan={2}>
                      {/* 策略页没有筛选，空表只有一种可能，不必分支（UI-032）。 */}
                      <Empty
                        title={t('policies.emptyTitle')}
                        body={t('policies.emptyBody')}
                        action={
                          <Button variant="primary" size="sm" onClick={() => setEditing('new')}>
                            {t('policies.newTitle')}
                          </Button>
                        }
                      />
                    </td>
                  </tr>
                ) : (
                  d.policies.map((name) => (
                    <tr key={name}>
                      <td className="mono">{name}</td>
                      <td>
                        <span className="row">
                          <Button size="sm" onClick={() => setEditing(name)} icon="edit">
                            {t('common.edit')}
                          </Button>
                          <a className="btn" data-size="sm" href={href({ view: 'debugger', policy: name })}>
                            <Icon name="debugger" />
                            {t('policies.debug')}
                          </a>
                          <Button size="sm" variant="danger" onClick={() => setRemoving(name)} icon="trash">
                            {t('common.delete')}
                          </Button>
                        </span>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        )}
      </AsyncView>

      {editing ? (
        <PolicyEditor
          t={t}
          token={token}
          name={editing === 'new' ? null : editing}
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
          body={t('policies.deleteConfirm', removing)}
          confirmLabel={t('common.delete')}
          onCancel={() => setRemoving(null)}
          onConfirm={() => {
            setBusy(true)
            setErr(null)
            api
              .deletePolicy(token, removing)
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

const TEMPLATE = `{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "iot:Connect",
      "Resource": "arn:aws:iot:*:*:client/\${iot:Connection.Thing.ThingName}"
    },
    {
      "Effect": "Allow",
      "Action": ["iot:Publish", "iot:Receive"],
      "Resource": "arn:aws:iot:*:*:topic/\$aws/things/\${iot:Connection.Thing.ThingName}/*"
    },
    {
      "Effect": "Allow",
      "Action": "iot:Subscribe",
      "Resource": "arn:aws:iot:*:*:topicfilter/\$aws/things/\${iot:Connection.Thing.ThingName}/*"
    }
  ]
}`

function PolicyEditor(props: {
  t: T
  token: string
  name: string | null
  onClose: () => void
  onSaved: () => void
}) {
  const { t } = props
  const existing = useAsync(
    () => (props.name ? api.getPolicy(props.token, props.name) : Promise.resolve(null)),
    [props.token, props.name],
  )
  const [name, setName] = useState(props.name ?? '')
  const [doc, setDoc] = useState<string | null>(null)
  const [err, setErr] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  // 已有策略的原文在加载完成后填入；新建时给一份可用模板，
  // 空白编辑器对初次使用者等于一道门槛
  const text = doc ?? (existing.data ? JSON.stringify(existing.data.document, null, 2) : props.name ? '' : TEMPLATE)

  return (
    <Modal title={props.name ? t('policies.editTitle') : t('policies.newTitle')} onClose={props.onClose}>
      <div className="col">
        {err ? (
          <div className="banner" data-tone="danger" role="alert">
            <div className="banner-body">{err}</div>
          </div>
        ) : null}
        <Field label={t('common.name')} hint={t('policies.nameHint')}>
          <input
            value={name}
            disabled={!!props.name}
            onChange={(e) => setName(e.target.value)}
            placeholder="device-scoped"
          />
        </Field>
        <Field label={t('policies.document')}>
          <textarea rows={16} value={text} onChange={(e) => setDoc(e.target.value)} />
        </Field>
        <div className="row" style={{ justifyContent: 'flex-end' }}>
          <Button onClick={props.onClose} disabled={busy}>
            {t('common.cancel')}
          </Button>
          <Button
            variant="primary"
            disabled={busy || !name}
            onClick={() => {
              // 先在本地解析一次 JSON：语法错误不必往返一次服务端。
              // 语义错误（未知 Effect、不支持的 Action）仍由服务端的
              // 编译器判定——那是唯一的权威。
              try {
                JSON.parse(text)
              } catch (e) {
                setErr(e instanceof Error ? e.message : String(e))
                return
              }
              setBusy(true)
              setErr(null)
              api
                .putPolicy(props.token, name, text)
                .then(props.onSaved)
                .catch((e: Error) => setErr(e.message))
                .finally(() => setBusy(false))
            }}
          >
            {t('common.save')}
          </Button>
        </div>
      </div>
    </Modal>
  )
}

/**
 * PolicyHelp 是 IoT Policy 的使用说明。
 *
 * 这一页的文档比影子那页更值得写：策略写错**不会报错**，只会让设备
 * 在运行时被拒，而 NotAuthorized 不告诉你是哪一条语句、哪个变量没求出
 * 值。下面这几条都是实测被绊过的地方，不是对 AWS 文档的转述。
 *
 * 动作名、ARN 与 JSON 样例是协议字面量，不进词典——翻译它们就是写错。
 */
function PolicyHelp(props: { t: T; onClose: () => void }) {
  const { t } = props
  const actions: { act: string; res: string; note: string }[] = [
    { act: 'iot:Connect', res: 'client/<clientId>', note: t('policies.help.connect') },
    { act: 'iot:Publish', res: 'topic/<主题>', note: t('policies.help.publish') },
    { act: 'iot:Subscribe', res: 'topicfilter/<过滤器>', note: t('policies.help.subscribe') },
    { act: 'iot:Receive', res: 'topic/<主题>', note: t('policies.help.receive') },
    { act: 'iot:RetainPublish', res: 'topic/<主题>', note: t('policies.help.retain') },
  ]
  const vars = [
    'iot:ClientId',
    'iot:Connection.Thing.ThingName',
    'iot:Connection.Thing.IsAttached',
    'iot:Connection.Thing.Attributes[<属性名>]',
    'iot:Certificate.SerialNumber',
    'iot:Certificate.Subject.CommonName',
    'iot:Certificate.Issuer.CommonName',
  ]

  return (
    <Drawer title={t('policies.help')} onClose={props.onClose}>
      <h3>{t('policies.help.shapeTitle')}</h3>
      <p>{t('policies.help.shapeBody')}</p>
      <pre className="json">{`{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "iot:Connect",
      "Resource": "arn:aws:iot:*:*:client/\${iot:Connection.Thing.ThingName}"
    },
    {
      "Effect": "Allow",
      "Action": ["iot:Publish", "iot:Receive"],
      "Resource": "arn:aws:iot:*:*:topic/$aws/things/\${iot:Connection.Thing.ThingName}/*"
    }
  ]
}`}</pre>

      <h3>{t('policies.help.actionsTitle')}</h3>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Action</th>
              <th>{t('policies.help.resType')}</th>
              <th>{t('policies.help.note')}</th>
            </tr>
          </thead>
          <tbody>
            {actions.map((a) => (
              <tr key={a.act}>
                <td className="mono">{a.act}</td>
                <td className="mono">{a.res}</td>
                <td>{a.note}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="faint">{t('policies.help.subVsRecv')}</p>

      <h3>{t('policies.help.varsTitle')}</h3>
      <p>{t('policies.help.varsBody')}</p>
      <ul>
        {vars.map((v) => (
          <li key={v}>
            <code>{'${' + v + '}'}</code>
          </li>
        ))}
      </ul>

      <h3>{t('policies.help.pitfallsTitle')}</h3>
      <ul>
        <li>{t('policies.help.pitResType')}</li>
        <li>{t('policies.help.pitDeny')}</li>
        <li>{t('policies.help.pitVarFail')}</li>
        <li>{t('policies.help.pitGlob')}</li>
        <li>{t('policies.help.pitClientId')}</li>
        <li>{t('policies.help.pitDebug')}</li>
      </ul>
      {/* 抽屉是当前上下文的清单，教程是完整路径（UI-081）。重叠的事实
          （深度合并、null 删键、version）权威在教程，这里只留清单并链
          过去——写两份的结局是只改一份，而两份都看起来像权威。 */}
      <p>
        <a href={href({ view: 'guide', chapter: 'policy', page: 'basics' })}>{t('guide.link')}</a>
      </p>
    </Drawer>
  )
}
