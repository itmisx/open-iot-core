/*
 * 证书签发（PRD PKI-010、UI-083）。
 *
 * 控制台只提供「服务端生成密钥对」这一条路径。CSR 签发仍然存在
 * （PKI-011、PKI-105 的 `/certificates/sign-csr`），但不进控制台：浏览器
 * 里没有办法生成并保管设备私钥，所以那条路径的表单只能承担"把 CSR 贴
 * 进来"这后一半——而用户在命令行上 openssl req 完之后，接着用
 * `openiotd cert issue` 签完比切回浏览器更短。留一个半截的表单，等于把
 * 工具链最难的那一半留给用户自己拼，却让人以为控制台能全程代办。
 *
 * 签出的私钥只在那一次响应里出现，服务端不留存。因此结果面板必须
 * 让人当场拿走——给下载、给复制、并明确说清关掉就没了（UI-052）。
 *
 * 管理端点绑在非回环地址时，服务端会拒绝本路径（PKI-045，403），提示里
 * 指向 sign-csr 与 acknowledge_plaintext_key_export。那条提示由 hint 区
 * 单独呈现（UI-053）——它是此时唯一能告诉用户出路的文字，拼进错误里会
 * 被当成"又一句看不懂的报错"。
 */
import { useEffect, useState } from 'react'
import { api, type IssuedCert } from '../api'
import type { T } from '../i18n'
import { Button, Field, Modal } from '../ui'

export function IssueCertDialog(props: {
  token: string
  t: T
  /** 预填的设备名。从设备详情页进入时带上，证书页进入时为空。 */
  thing?: string | undefined
  onClose: () => void
  onIssued: () => void
}) {
  const { t, token } = props
  const [thing, setThing] = useState(props.thing ?? '')
  const [policy, setPolicy] = useState('')
  const [policies, setPolicies] = useState<string[]>([])
  const [days, setDays] = useState('')
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState('')
  const [hint, setHint] = useState('')
  const [issued, setIssued] = useState<IssuedCert | null>(null)

  useEffect(() => {
    api
      .listPolicies(token)
      .then((r) => {
        setPolicies(r.policies)
        // 只有一份策略时直接选上：这是单机起步的常见情形，
        // 让用户再点一次没有意义。
        if (r.policies.length === 1 && r.policies[0]) setPolicy(r.policies[0])
      })
      .catch((e: unknown) => setErr(String(e)))
  }, [token])

  async function submit() {
    setBusy(true)
    setErr('')
    setHint('')
    try {
      const res = await api.issueCert(token, {
        thing: thing.trim(),
        policyName: policy,
        ...(days.trim() ? { validitySeconds: Number(days) * 86400 } : {}),
      })
      setIssued(res)
      props.onIssued()
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e)
      setErr(msg)
      // 服务端的 hint 往往比错误本身有用（非回环地址下的 403 那一类），
      // 单独呈现而不是拼进错误里。
      const h = (e as { hint?: string }).hint
      if (h) setHint(h)
    } finally {
      setBusy(false)
    }
  }

  if (issued) {
    return <IssuedPanel t={t} issued={issued} onClose={props.onClose} />
  }

  const ready = thing.trim() !== '' && policy !== ''

  return (
    <Modal title={t('issue.title')} onClose={props.onClose}>
      <div className="col">
        <div className="banner" data-tone="warn">
          <div className="banner-body">{t('issue.keypairDesc')}</div>
        </div>

        <Field label={t('issue.thing')} hint={t('issue.thingHint')}>
          <input
            value={thing}
            onChange={(e) => setThing(e.target.value)}
            placeholder="sensor-01"
            autoFocus={!props.thing}
          />
        </Field>

        <Field label={t('issue.policy')} hint={t('issue.policyHint')}>
          <select value={policy} onChange={(e) => setPolicy(e.target.value)}>
            <option value="">{t('issue.pickPolicy')}</option>
            {policies.map((p) => (
              <option key={p} value={p}>
                {p}
              </option>
            ))}
          </select>
        </Field>

        <Field label={t('issue.validity')} hint={t('issue.validityHint')}>
          <input
            value={days}
            onChange={(e) => setDays(e.target.value.replace(/\D/g, ''))}
            placeholder={t('issue.validityDefault')}
            inputMode="numeric"
          />
        </Field>

        {err ? (
          <div className="banner" data-tone="danger">
            <div className="banner-body">
              <div>{err}</div>
              {hint ? (
                <div style={{ marginTop: 6, opacity: 0.85 }}>{hint}</div>
              ) : null}
            </div>
          </div>
        ) : null}

        <div className="row" style={{ justifyContent: 'flex-end' }}>
          <Button onClick={props.onClose} disabled={busy}>
            {t('common.cancel')}
          </Button>
          <Button variant="primary" disabled={busy || !ready} onClick={submit}>
            {busy ? t('common.loading') : t('issue.submit')}
          </Button>
        </div>
      </div>
    </Modal>
  )
}

/**
 * 签发结果。
 *
 * 私钥不可再次获取，所以这个面板不提供"稍后再说"：关闭按钮的文案
 * 明确说明关掉之后私钥就取不回来了。
 */
function IssuedPanel(props: { t: T; issued: IssuedCert; onClose: () => void }) {
  const { t, issued } = props
  const [saved, setSaved] = useState(false)

  return (
    <Modal title={t('issue.doneTitle')} onClose={props.onClose}>
      <div className="col">
        <div className="banner" data-tone="warn">
          <div className="banner-body">{issued.warning ?? t('issue.keyOnce')}</div>
        </div>

        <dl className="kv">
          <dt>{t('certs.fingerprint')}</dt>
          <dd className="mono">{issued.fingerprint}</dd>
          <dt>{t('certs.thing')}</dt>
          <dd>{issued.thing}</dd>
          <dt>{t('issue.policy')}</dt>
          <dd>{issued.policyName}</dd>
          <dt>{t('certs.notAfter')}</dt>
          <dd>{new Date(issued.notAfter).toLocaleString()}</dd>
        </dl>

        <PemBlock
          t={t}
          label={t('issue.certPem')}
          filename={`${issued.thing}.pem`}
          text={issued.certificatePem}
        />
        <PemBlock
          t={t}
          label={t('issue.keyPem')}
          filename={`${issued.thing}.key`}
          text={issued.privateKeyPem}
          tone="warn"
          onTaken={() => setSaved(true)}
        />
        <PemBlock
          t={t}
          label={t('issue.chainPem')}
          filename="ca-chain.pem"
          text={issued.chainPem}
        />

        <div className="banner">
          <div className="banner-body">{t('issue.nextSteps')}</div>
        </div>

        <div className="row" style={{ justifyContent: 'flex-end' }}>
          <Button variant={saved ? 'primary' : undefined} onClick={props.onClose}>
            {saved ? t('common.close') : t('issue.closeWithoutSaving')}
          </Button>
        </div>
      </div>
    </Modal>
  )
}

/** PemBlock 展示一段 PEM，并提供复制与下载。 */
function PemBlock(props: {
  t: T
  label: string
  filename: string
  text: string
  tone?: 'warn' | undefined
  onTaken?: (() => void) | undefined
}) {
  const { t } = props
  const [copied, setCopied] = useState(false)

  function copy() {
    void navigator.clipboard.writeText(props.text).then(() => {
      setCopied(true)
      props.onTaken?.()
      window.setTimeout(() => setCopied(false), 1500)
    })
  }

  function download() {
    // 用 Blob 而不是 data: URI：PEM 可能有几 KB，而 data: URI 在
    // 部分浏览器上有长度上限。
    const url = URL.createObjectURL(new Blob([props.text], { type: 'application/x-pem-file' }))
    const a = document.createElement('a')
    a.href = url
    a.download = props.filename
    a.click()
    URL.revokeObjectURL(url)
    props.onTaken?.()
  }

  return (
    <div className="field">
      <label>
        {props.label}
        {props.tone === 'warn' ? ' ⚠' : ''}
      </label>
      <pre
        className="mono"
        style={{
          maxHeight: 120,
          overflow: 'auto',
          padding: '8px 10px',
          background: 'var(--surface-2)',
          borderRadius: 6,
          fontSize: 11,
          margin: 0,
        }}
      >
        {props.text}
      </pre>
      <div className="row" style={{ marginTop: 6 }}>
        <Button size="sm" onClick={copy}>
          {copied ? t('issue.copied') : t('issue.copy')}
        </Button>
        <Button size="sm" onClick={download}>
          {t('issue.download')}
        </Button>
        <span className="grow" />
        <span className="faint mono" style={{ fontSize: 11 }}>
          {props.filename}
        </span>
      </div>
    </div>
  )
}
