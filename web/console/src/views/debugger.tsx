/*
 * 策略调试器（PRD UI-010）。
 *
 * 这是本项目相对 AWS 的头号差异化能力。AWS 的策略调试几乎不可能：
 * 它不告诉你每个 ${iot:...} 变量求到了什么值，而那正是
 * "策略看起来写对了却仍被拒"的根源。
 *
 * 这里把 authz.Trace 完整呈现——判定结果、命中语句的下标、
 * 每个变量的求值结果，以及让它放行需要做什么。
 */
import { useEffect, useState } from 'react'
import { api, type SimulateRequest, type SimulateResult } from '../api'
import type { T } from '../i18n'
import { Badge, Button, Card, Field, useAsync } from '../ui'

const ACTIONS = [
  'iot:Connect',
  'iot:Publish',
  'iot:Subscribe',
  'iot:Receive',
  'iot:RetainPublish',
]

export function DebuggerView(props: {
  token: string
  t: T
  initialPolicy?: string | undefined
  onUnauthorized: () => void
}) {
  const { token, t } = props
  const policies = useAsync(() => api.listPolicies(token), [token])

  const [thing, setThing] = useState('')
  const [policy, setPolicy] = useState(props.initialPolicy ?? '')
  const [clientId, setClientId] = useState('')
  const [action, setAction] = useState('iot:Publish')
  const [resource, setResource] = useState('')
  const [showCertOverride, setShowCertOverride] = useState(false)
  const [subjectCN, setSubjectCN] = useState('')
  const [issuerCN, setIssuerCN] = useState('')
  const [serial, setSerial] = useState('')

  const [result, setResult] = useState<SimulateResult | null>(null)
  const [err, setErr] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  // 没指定策略时默认选中第一条：这一页只按策略判定，而"哪条策略"
  // 是必填的——让它一进来就有一个可用的选择，比摆一个「—」等用户去
  // 点一下强。用户当然可以换成别的。
  useEffect(() => {
    const first = (policies.data?.policies ?? [])[0]
    if (!policy && first) setPolicy(first)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [policies.data])

  const resourceHint =
    action === 'iot:Connect'
      ? t('dbg.resourceHintConnect')
      : action === 'iot:Subscribe'
        ? t('dbg.resourceHintSub')
        : t('dbg.resourceHintTopic')

  const run = () => {
    const req: SimulateRequest = { action, resource, policyNames: [policy] }
    // 设备名可选：填了 Thing 类变量才有值可求。它不参与"用哪条策略"
    // 的判断——那已经由上面的选择定死了。
    if (thing) req.thing = thing
    if (clientId) req.clientId = clientId
    if (subjectCN) req.certSubjectCommonName = subjectCN
    if (issuerCN) req.certIssuerCommonName = issuerCN
    if (serial) req.certSerialNumber = serial

    setBusy(true)
    setErr(null)
    api
      .simulate(token, req)
      .then(setResult)
      .catch((e: Error) => {
        setErr(e.message)
        setResult(null)
      })
      .finally(() => setBusy(false))
  }

  const canRun = !!resource && !!policy

  return (
    <>
      <div className="main-head">
        <div>
          <h1>{t('dbg.title')}</h1>
          <p>{t('dbg.subtitle')}</p>
        </div>
      </div>

      <div className="spread" style={{ alignItems: 'flex-start' }}>
        <Card title={t('dbg.subject')}>
          <div className="col">
            {/* 只按策略判定（UI-068）。
                此前还有"按证书指纹"和"按设备"两种，用三个按钮切换——
                而三者判的是同一件事：某份策略文档对某个请求怎么判。
                前两种只是先把主体解析成它绑定的策略，多出来的一步让
                用户在还没开始调试之前就得先回答一个不相干的问题。
                要按主体反查用哪条策略，设备页和证书页本来就直接写着。
                接口 POST /authz/simulate 仍然接受三种主体，脚本不受影响。 */}
            <Field label={t('things.policy')} hint={t('dbg.policyHint')}>
              <select value={policy} onChange={(e) => setPolicy(e.target.value)}>
                {(policies.data?.policies ?? []).length === 0 ? (
                  <option value="">{t('dbg.noPolicies')}</option>
                ) : null}
                {(policies.data?.policies ?? []).map((p) => (
                  <option key={p} value={p}>
                    {p}
                  </option>
                ))}
              </select>
            </Field>
            <Field label={t('certs.thing')} hint={t('dbg.thingHint')}>
              <input value={thing} onChange={(e) => setThing(e.target.value.trim())} />
            </Field>

            <Field label={t('dbg.clientId')} hint={t('dbg.clientIdHint')}>
              <input value={clientId} onChange={(e) => setClientId(e.target.value.trim())} />
            </Field>

            <Button size="sm" onClick={() => setShowCertOverride((v) => !v)}>
              {showCertOverride ? '▾' : '▸'} {t('dbg.certOverride')}
            </Button>
            {showCertOverride ? (
              <>
                <Field label={t('dbg.subjectCN')}>
                  <input value={subjectCN} onChange={(e) => setSubjectCN(e.target.value)} />
                </Field>
                <Field label={t('dbg.issuerCN')}>
                  <input value={issuerCN} onChange={(e) => setIssuerCN(e.target.value)} />
                </Field>
                <Field label={t('dbg.serial')}>
                  <input value={serial} onChange={(e) => setSerial(e.target.value)} />
                </Field>
              </>
            ) : null}
          </div>
        </Card>

        <Card title={t('dbg.action')}>
          <div className="col">
            <Field label={t('dbg.action')}>
              <select value={action} onChange={(e) => setAction(e.target.value)}>
                {ACTIONS.map((a) => (
                  <option key={a} value={a}>
                    {a}
                  </option>
                ))}
              </select>
            </Field>
            <Field label={t('dbg.resource')} hint={resourceHint}>
              <input
                value={resource}
                onChange={(e) => setResource(e.target.value)}
                placeholder={
                  action === 'iot:Connect'
                    ? 'dev-001'
                    : action === 'iot:Subscribe'
                      ? '$aws/things/dev-001/shadow/#'
                      : '$aws/things/dev-001/shadow/update'
                }
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && canRun) run()
                }}
              />
            </Field>
            <div>
              <Button variant="primary" disabled={busy || !canRun} onClick={run} icon="run">
                {t('dbg.run')}
              </Button>
            </div>
          </div>
        </Card>
      </div>

      {err ? (
        <div className="banner" data-tone="danger" role="alert" style={{ marginTop: 16 }}>
          <div className="banner-body">{err}</div>
        </div>
      ) : null}

      {result ? <ResultPanel r={result} t={t} /> : null}
    </>
  )
}

function ResultPanel({ r, t }: { r: SimulateResult; t: T }) {
  return (
    <div style={{ marginTop: 16 }} className="col">
      <div className="banner" data-tone={r.allowed ? 'ok' : 'danger'}>
        <div className="banner-body">
          <strong>{r.allowed ? t('dbg.allowed') : t('dbg.denied')}</strong>
          <span className="mono">{r.decision}</span>
        </div>
      </div>

      <div className="spread" style={{ alignItems: 'flex-start' }}>
        <Card title={t('dbg.matched')}>
          {r.statementIndex >= 0 ? (
            <dl className="kv">
              <dt>{t('things.policy')}</dt>
              <dd>{r.policyName}</dd>
              <dt>Statement</dt>
              <dd>[{r.statementIndex}]</dd>
              <dt>{t('dbg.matchedResource')}</dt>
              <dd>{r.matchedResource}</dd>
            </dl>
          ) : (
            <div className="faint">{t('dbg.noStatement')}</div>
          )}
          <div style={{ marginTop: 12 }}>
            <div className="card-title">{t('dbg.evaluated')}</div>
            <div className="row" style={{ flexWrap: 'wrap' }}>
              {r.evaluatedPolicies.map((p) => (
                <Badge key={p} tone="muted">
                  {p}
                </Badge>
              ))}
            </div>
          </div>
        </Card>

        <Card title={t('dbg.variables')}>
          {r.variables.length === 0 ? (
            <div className="faint">{t('common.none')}</div>
          ) : (
            <div className="table-wrap" style={{ border: 'none' }}>
              <table>
                <tbody>
                  {r.variables.map((v) => (
                    <tr key={v.name}>
                      <td className="mono">${'{'}
                        {v.name}
                        {'}'}
                      </td>
                      <td>
                        {v.resolved ? (
                          <span className="mono">{v.value || '""'}</span>
                        ) : (
                          /* 无法求值的变量是"策略写对了却仍被拒"的头号原因，
                             因此单独标出来而不是显示空值 */
                          <Badge tone="danger">{t('dbg.unresolved')}</Badge>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          {r.variablesTruncated ? (
            <div className="faint" style={{ marginTop: 8 }}>
              …
            </div>
          ) : null}
        </Card>
      </div>

      {r.advice && r.advice.length > 0 ? (
        <Card title={t('dbg.advice')}>
          <ul style={{ margin: 0, paddingLeft: 20 }}>
            {r.advice.map((a, i) => (
              <li key={i} style={{ marginBottom: 6 }}>
                {a}
              </li>
            ))}
          </ul>
        </Card>
      ) : null}
    </div>
  )
}
