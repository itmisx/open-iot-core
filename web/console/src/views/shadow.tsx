/*
 * 影子页与影子面板。
 *
 * 面板同时服务两处：设备详情页的"一屏"（UI-021）与这里的独立影子页
 * （UI-069）。两处共用一个组件而不是各写一份——影子的下发是真正的写
 * 操作（保存后 shadow.Service 产出 /update/delta 投给设备），两份实现
 * 迟早会在某一边漏掉版本冲突或删除确认。
 *
 * 独立页存在的理由是**发现性**：影子此前只在"点进某台设备再往下翻"
 * 之后才出现，设备列表上看不出这个功能存在。它同时是命名影子唯一的
 * 入口——聚合页只显示经典影子，一台用命名影子的设备在那里看不全。
 */
import { useState } from 'react'
import { api, type ShadowDoc } from '../api'
import type { T } from '../i18n'
import { Icon } from '../icons'
import { href } from '../router'
import {
  AsyncView, Badge, Button, Card, Confirm, Drawer, Field, Modal, useAsync,
} from '../ui'

/** CLASSIC 代表经典（未命名）影子。空串正是接口里"不带 name"的表示。 */
const CLASSIC = ''

export function ShadowView(props: {
  token: string
  name: string
  t: T
  onUnauthorized: () => void
}) {
  const { token, name, t } = props
  const [picked, setPicked] = useState<string>(CLASSIC)
  const [helping, setHelping] = useState(false)

  // 命名影子清单（SHDW-043）。取不到就只显示经典那一个——
  // 列不出来不该让整页变成错误页。
  const names = useAsync(
    () => api.listShadows(token, name).catch(() => ({ results: [] as string[] })),
    [token, name],
  )
  const shadow = useAsync(
    () => api.getShadow(token, name, picked || undefined).catch(() => null as ShadowDoc | null),
    [token, name, picked],
  )

  const list = names.data?.results ?? []

  return (
    <>
      <div className="main-head">
        <div>
          <h1>{t('shadow.title')}</h1>
          <p className="faint">{t('shadow.subtitle')}</p>
        </div>
        <div className="main-actions">
          <Button onClick={() => setHelping(true)} icon="help">{t('shadow.help')}</Button>
          <a className="btn" href={href({ view: 'thing', name })}>
            <Icon name="back" />
            {t('shadow.backToThing')}
          </a>
          <Button onClick={() => { names.reload(); shadow.reload() }} icon="refresh">
            {t('common.refresh')}
          </Button>
        </div>
      </div>

      <Card title={t('certs.thing')}>
        <div className="row" style={{ flexWrap: 'wrap' }}>
          <span className="mono">{name}</span>
          {/* 只有真的存在命名影子时才显示选择器：多数设备只有经典影子，
              给它们摆一个只有一项的下拉是噪声。 */}
          {list.length > 0 ? (
            <>
              <span className="faint">{t('shadow.which')}</span>
              <select
                value={picked}
                onChange={(e) => setPicked(e.target.value)}
                style={{ width: 'auto' }}
              >
                <option value={CLASSIC}>{t('shadow.classic')}</option>
                {list.map((n) => (
                  <option key={n} value={n}>
                    {n}
                  </option>
                ))}
              </select>
            </>
          ) : (
            <Badge tone="muted">{t('shadow.classic')}</Badge>
          )}
        </div>
      </Card>

      {helping ? (
        <ShadowHelp t={t} name={name} shadow={picked} onClose={() => setHelping(false)} />
      ) : null}

      <div style={{ marginTop: 16 }}>
        <AsyncView state={shadow} t={t} onUnauthorized={props.onUnauthorized}>
          {() => (
            <ShadowPanel
              token={token}
              name={name}
              t={t}
              shadow={picked || undefined}
              state={shadow}
              onChanged={shadow.reload}
            />
          )}
        </AsyncView>
      </div>
    </>
  )
}

/**
 * ShadowPanel 展示并编辑影子。
 *
 * 下发 desired 是真正的写操作：保存后 shadow.Service 会产出
 * /update/delta 与 /update/documents，由 broker 投递给设备
 * （PRD SHDW-045）。这就是控制台"下发配置"的实际路径，
 * 而不是只改了一行数据库。
 */
export function ShadowPanel(props: {
  token: string
  name: string
  t: T
  /* 影子名。空串是经典（未命名）影子，与设备侧 $aws/things/<名>/shadow
     对应；给了名字就是 $aws/things/<名>/shadow/name/<影子名>。 */
  shadow?: string | undefined
  state: ReturnType<typeof useAsync<ShadowDoc | null>>
  onChanged: () => void
}) {
  const { t } = props
  const [editing, setEditing] = useState(false)
  const [removing, setRemoving] = useState(false)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const doc = props.state.data

  return (
    <Card title={t('things.shadow')}>
      {err ? (
        <div className="banner" data-tone="danger" role="alert">
          <div className="banner-body">{err}</div>
        </div>
      ) : null}

      {!doc ? (
        <div className="col">
          <div className="faint">{t('things.noShadow')}</div>
          <div>
            <Button size="sm" variant="primary" onClick={() => setEditing(true)}>
              {t('things.pushDesired')}
            </Button>
          </div>
        </div>
      ) : (
        <div className="col">
          <div className="row" style={{ justifyContent: 'space-between' }}>
            <span className="row">
              <Badge tone="muted">
                {t('things.shadowVersion')} {doc.version}
              </Badge>
              {doc.state.delta ? <Badge tone="warn">delta</Badge> : null}
            </span>
            <span className="row">
              <Button size="sm" onClick={() => setEditing(true)}>
                {t('things.pushDesired')}
              </Button>
              <Button size="sm" variant="danger" onClick={() => setRemoving(true)} icon="trash">
                {t('things.deleteShadow')}
              </Button>
            </span>
          </div>
          <div className="spread">
            <div>
              <h3 className="card-title">{t('things.shadowDesired')}</h3>
              <pre className="json">{JSON.stringify(doc.state.desired ?? {}, null, 2)}</pre>
            </div>
            <div>
              <h3 className="card-title">{t('things.shadowReported')}</h3>
              <pre className="json">{JSON.stringify(doc.state.reported ?? {}, null, 2)}</pre>
            </div>
            {doc.state.delta ? (
              <div>
                <h3 className="card-title">{t('things.shadowDelta')}</h3>
                <pre className="json">{JSON.stringify(doc.state.delta, null, 2)}</pre>
              </div>
            ) : null}
          </div>
        </div>
      )}

      {editing ? (
        <Modal title={t('things.pushDesired')} onClose={() => setEditing(false)}>
          <DesiredEditor
            t={t}
            initial={doc?.state.desired ?? {}}
            busy={busy}
            onCancel={() => setEditing(false)}
            onSave={(desired) => {
              setBusy(true)
              setErr(null)
              api
                .updateShadow(props.token, props.name, { state: { desired } }, props.shadow)
                .then(() => {
                  setEditing(false)
                  props.onChanged()
                })
                .catch((e: Error) => setErr(e.message))
                .finally(() => setBusy(false))
            }}
          />
        </Modal>
      ) : null}

      {removing ? (
        <Confirm
          t={t}
          tone="danger"
          busy={busy}
          title={t('things.deleteShadow')}
          body={t('things.deleteShadowConfirm', props.name)}
          confirmLabel={t('common.delete')}
          onCancel={() => setRemoving(false)}
          onConfirm={() => {
            setBusy(true)
            setErr(null)
            api
              .deleteShadow(props.token, props.name, props.shadow)
              .then(() => {
                setRemoving(false)
                props.onChanged()
              })
              .catch((e: Error) => {
                setErr(e.message)
                setRemoving(false)
              })
              .finally(() => setBusy(false))
          }}
        />
      ) : null}
    </Card>
  )
}

function DesiredEditor(props: {
  t: T
  initial: unknown
  busy: boolean
  onCancel: () => void
  onSave: (desired: unknown) => void
}) {
  const { t } = props
  const [text, setText] = useState(JSON.stringify(props.initial, null, 2))
  const [err, setErr] = useState<string | null>(null)

  return (
    <div className="col">
      <Field label={t('things.shadowDesired')} hint={t('things.pushHint')} error={err ?? undefined}>
        <textarea rows={12} value={text} onChange={(e) => setText(e.target.value)} />
      </Field>
      <div className="row" style={{ justifyContent: 'flex-end' }}>
        <Button onClick={props.onCancel} disabled={props.busy}>
          {t('common.cancel')}
        </Button>
        <Button
          variant="primary"
          disabled={props.busy}
          onClick={() => {
            try {
              // 在本地先解析一次：把 JSON 语法错误挡在发请求之前，
              // 否则用户看到的是服务端返回的 400，反馈慢且不精确
              props.onSave(JSON.parse(text))
              setErr(null)
            } catch (e) {
              setErr(e instanceof Error ? e.message : String(e))
            }
          }}
        >
          {t('common.save')}
        </Button>
      </div>
    </div>
  )
}

/**
 * ShadowHelp 是影子的使用说明。
 *
 * 主题按**当前这台设备**渲染，不用 <thingName> 占位符：照着占位符敲的
 * 人总要再替换一次，而替换错了的症状是"订阅成功但永远收不到"——主题
 * 写错不会报错，只是永远不匹配。
 *
 * 主题串与 JSON 样例不进词典：它们是协议字面量，翻译它们就是写错。
 * 需要翻译的只有标题与说明句。
 */
function ShadowHelp(props: { t: T; name: string; shadow: string; onClose: () => void }) {
  const { t } = props
  // 选中命名影子时，说明里的主题也跟着变——否则用户照着经典影子的
  // 主题去操作一个命名影子，同样是静默不匹配。
  const base = props.shadow
    ? `$aws/things/${props.name}/shadow/name/${props.shadow}`
    : `$aws/things/${props.name}/shadow`

  const topics: { use: string; topic: string }[] = [
    { use: t('shadow.help.get'), topic: `${base}/get` },
    { use: t('shadow.help.getOk'), topic: `${base}/get/accepted` },
    { use: t('shadow.help.getErr'), topic: `${base}/get/rejected` },
    { use: t('shadow.help.update'), topic: `${base}/update` },
    { use: t('shadow.help.updateOk'), topic: `${base}/update/accepted` },
    { use: t('shadow.help.updateErr'), topic: `${base}/update/rejected` },
    { use: t('shadow.help.delta'), topic: `${base}/update/delta` },
    { use: t('shadow.help.documents'), topic: `${base}/update/documents` },
    { use: t('shadow.help.del'), topic: `${base}/delete` },
  ]

  return (
    <Drawer title={t('shadow.help')} onClose={props.onClose}>
      <h3>{t('shadow.help.topicsTitle')}</h3>
      <p>{t('shadow.help.topicsBody')}</p>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>{t('shadow.help.use')}</th>
              <th>{t('shadow.help.topic')}</th>
            </tr>
          </thead>
          <tbody>
            {topics.map((r) => (
              <tr key={r.topic}>
                <td>{r.use}</td>
                <td className="mono">{r.topic}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="faint">{t('shadow.help.namedHint')}</p>

      <h3>{t('shadow.help.updateTitle')}</h3>
      <p>{t('shadow.help.updateBody')}</p>
      <pre className="json">{`{
  "state": {
    "desired":  { "interval": 30 },
    "reported": { "interval": 60 }
  },
  "clientToken": "abc-123",
  "version": 7
}`}</pre>

      <h3>{t('shadow.help.acceptedTitle')}</h3>
      <p>{t('shadow.help.acceptedBody')}</p>
      <pre className="json">{`{
  "state":    { "desired": { "interval": 30 } },
  "metadata": { "desired": { "interval": { "timestamp": 1730000000000 } } },
  "version":   8,
  "timestamp": 1730000000000,
  "clientToken": "abc-123"
}`}</pre>

      <h3>{t('shadow.help.rejectedTitle')}</h3>
      <pre className="json">{`{ "code": 409, "message": "version conflict", "clientToken": "abc-123" }`}</pre>

      <h3>{t('shadow.help.rulesTitle')}</h3>
      <ul>
        <li>{t('shadow.help.ruleMerge')}</li>
        <li>{t('shadow.help.ruleNull')}</li>
        <li>{t('shadow.help.ruleVersion')}</li>
        <li>{t('shadow.help.ruleReset')}</li>
        <li>{t('shadow.help.ruleLimits')}</li>
        <li>{t('shadow.help.ruleToken')}</li>
        <li>{t('shadow.help.rulePolicy')}</li>
      </ul>
      {/* 抽屉是当前上下文的清单，教程是完整路径（UI-081）。重叠的事实
          （深度合并、null 删键、version）权威在教程，这里只留清单并链
          过去——写两份的结局是只改一份，而两份都看起来像权威。 */}
      <p>
        <a href={href({ view: 'guide', chapter: 'shadow', page: 'merge' })}>{t('guide.link')}</a>
      </p>
    </Drawer>
  )
}
