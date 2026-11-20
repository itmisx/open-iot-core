/*
 * MQTT 测试客户端（UI-013）。
 *
 * 走管理面的 WebSocket，不走设备的 /mqtt。设备那条要 SigV4，
 * 而浏览器的 WebSocket 设不了 Authorization。令牌在查询参数里，
 * 与事件流同一条理由。
 *
 * 录制和回放都在浏览器里：导出是一份 JSON，导入只是把那份记录
 * 铺回消息带，不会再发给 broker。
 */
import { useEffect, useRef, useState } from 'react'
import { captureSocketURL, mqttSocketURL } from '../api'
import { base64ToBytes, bytesToBase64, present, type DecodedKind } from '../decode'
import type { T } from '../i18n'
import { Badge, Button, Card, Empty, EmptyFiltered, Field, type Tone } from '../ui'

const MAX_ROWS = 500

type Status = 'connecting' | 'open' | 'closed'

type Sub = { filter: string; qos: number }

type Msg = {
  id: number
  topic: string
  payload: Uint8Array
  qos: number
  retain: boolean
  contentType?: string
  payloadFormat?: number
  responseTopic?: string
  correlation?: string
  userProperties?: [string, string][]
  messageExpiry?: number
  playback: boolean
}

type ServerFrame = {
  op?: string
  error?: string
  filter?: string
  topic?: string
  qos?: number
  payload?: string
  retain?: boolean
  dropped?: number
  contentType?: string
  payloadFormat?: number
  responseTopic?: string
  correlationData?: string
  userProperties?: [string, string][]
  messageExpiry?: number
}

/*
 * 两种模式共用这一页（UI-070）。
 *
 *   订阅：以一个真实订阅者接入（/mqtt）。能发布，会真的加入共享订阅组。
 *   旁观：看经过本节点的发布（/capture）。不建立订阅，所以不占订阅额度、
 *         不从共享组里抢消息，而且**连一个订阅者都没有的发布也看得到**
 *         ——「设备明明发了，为什么没人收到」只有这个模式能回答。
 *
 * 做成一页两档而不是两页：两页看起来几乎一样（一个主题框加一片消息流），
 * 使用者分不清该开哪个；而两者的差别不在界面上，在"要不要进路由表"。
 * 并成一个开关之后，这个差别从"两个入口"变成"一次明确的选择"。
 *
 * 同一时刻只开一条套接字。旁观模式下不给发布面板：发布是客户端的角色，
 * 而旁观的全部意义是不以客户端身份介入。
 */
type Mode = 'client' | 'watch'

export function MqttView(props: { token: string; t: T }) {
  const { token, t } = props
  const [mode, setMode] = useState<Mode>('client')
  const [status, setStatus] = useState<Status>('connecting')
  const [subs, setSubs] = useState<Sub[]>([])
  const [rows, setRows] = useState<Msg[]>([])
  const [err, setErr] = useState<string | null>(null)
  const [dropped, setDropped] = useState(0)
  const [needle, setNeedle] = useState('')

  const [filter, setFilter] = useState('$aws/things/sensor-01/#')
  const [subQos, setSubQos] = useState(1)
  const [topic, setTopic] = useState('$aws/things/sensor-01/telemetry')
  const [body, setBody] = useState('21')
  const [bodyIsB64, setBodyIsB64] = useState(false)
  const [pubQos, setPubQos] = useState(0)
  const [retain, setRetain] = useState(false)
  const [contentType, setContentType] = useState('')
  const [payloadFormat, setPayloadFormat] = useState('')
  const [responseTopic, setResponseTopic] = useState('')
  const [correlation, setCorrelation] = useState('')
  const [userProps, setUserProps] = useState('')
  const [expiry, setExpiry] = useState('')

  const wsRef = useRef<WebSocket | null>(null)
  const subsRef = useRef(subs)
  subsRef.current = subs
  const tRef = useRef(t)
  tRef.current = t
  const seq = useRef(0)
  const fileRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    let stop = false
    let sock: WebSocket | null = null
    let timer = 0

    const connect = () => {
      if (stop) return
      setStatus('connecting')
      const ws = new WebSocket(mode === 'watch' ? captureSocketURL(token) : mqttSocketURL(token))
      sock = ws
      wsRef.current = ws
      ws.onmessage = (ev) => {
        if (typeof ev.data !== 'string') return
        let frame: ServerFrame
        try {
          frame = JSON.parse(ev.data) as ServerFrame
        } catch {
          return
        }
        if (frame.op === 'ready') {
          setStatus('open')
          setErr(null)
          // 断线重连后把原来的订阅/旁观过滤器补回去
          for (const s of subsRef.current) {
            ws.send(
              JSON.stringify(
                mode === 'watch'
                  ? { op: 'watch', filter: s.filter }
                  : { op: 'subscribe', filter: s.filter, qos: s.qos },
              ),
            )
          }
          return
        }
        if (frame.op === 'suback' && frame.filter) {
          const qos = frame.qos ?? 0
          setSubs((prev) => {
            const rest = prev.filter((s) => s.filter !== frame.filter)
            return [...rest, { filter: frame.filter ?? '', qos }]
          })
          return
        }
        if (frame.op === 'unsuback' && frame.filter) {
          setSubs((prev) => prev.filter((s) => s.filter !== frame.filter))
          return
        }
        // 旁观模式的两条应答与订阅模式同义，只是没有 QoS 可言
        if (frame.op === 'watching' && frame.filter) {
          setSubs((prev) => {
            const rest = prev.filter((s) => s.filter !== frame.filter)
            return [...rest, { filter: frame.filter ?? '', qos: 0 }]
          })
          setErr(null)
          return
        }
        if (frame.op === 'unwatched' && frame.filter) {
          setSubs((prev) => prev.filter((s) => s.filter !== frame.filter))
          return
        }
        if (frame.op === 'error') {
          setErr(frame.error ?? tRef.current('mqtt.badFrame'))
          return
        }
        if (frame.op === 'message' && frame.topic) {
          const payload = base64ToBytes(frame.payload ?? '') ?? new Uint8Array()
          const msg: Msg = {
            id: ++seq.current,
            topic: frame.topic,
            payload,
            qos: frame.qos ?? 0,
            retain: Boolean(frame.retain),
            playback: false,
          }
          if (frame.contentType) msg.contentType = frame.contentType
          if (typeof frame.payloadFormat === 'number') msg.payloadFormat = frame.payloadFormat
          if (frame.responseTopic) msg.responseTopic = frame.responseTopic
          if (frame.correlationData) msg.correlation = frame.correlationData
          if (frame.userProperties && frame.userProperties.length > 0) {
            msg.userProperties = frame.userProperties
          }
          if (typeof frame.messageExpiry === 'number') msg.messageExpiry = frame.messageExpiry
          if (typeof frame.dropped === 'number') setDropped(frame.dropped)
          setRows((prev) => {
            const next = [msg, ...prev]
            return next.length > MAX_ROWS ? next.slice(0, MAX_ROWS) : next
          })
        }
      }
      ws.onclose = () => {
        if (wsRef.current === ws) wsRef.current = null
        setStatus('closed')
        if (!stop) timer = window.setTimeout(connect, 1000)
      }
    }

    connect()
    return () => {
      stop = true
      window.clearTimeout(timer)
      sock?.close()
    }
  }, [token, mode])

  const send = (v: unknown) => {
    const ws = wsRef.current
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      setErr(t('mqtt.notOpen'))
      return false
    }
    ws.send(JSON.stringify(v))
    return true
  }

  const subscribe = () => {
    const f = filter.trim()
    if (!f) return
    send(mode === 'watch' ? { op: 'watch', filter: f } : { op: 'subscribe', filter: f, qos: subQos })
  }

  // 切模式时清空：订阅列表与消息流都属于刚关掉的那条套接字，
  // 留着会让人以为旁观模式下还订着东西。
  const switchMode = (next: Mode) => {
    if (next === mode) return
    setSubs([])
    setRows([])
    setErr(null)
    setMode(next)
  }

  const publish = () => {
    const name = topic.trim()
    if (!name) return
    let payload = ''
    if (bodyIsB64) {
      if (body !== '' && base64ToBytes(body) == null) {
        setErr(t('mqtt.badB64'))
        return
      }
      payload = body.replace(/\s/g, '')
    } else if (body !== '') {
      payload = bytesToBase64(new TextEncoder().encode(body))
    }
    const cmd: Record<string, unknown> = {
      op: 'publish',
      topic: name,
      payload,
      qos: pubQos,
      retain,
    }
    if (contentType.trim()) cmd.contentType = contentType.trim()
    if (payloadFormat !== '') cmd.payloadFormat = Number(payloadFormat)
    if (responseTopic.trim()) cmd.responseTopic = responseTopic.trim()
    if (correlation !== '') cmd.correlationData = bytesToBase64(new TextEncoder().encode(correlation))
    const pairs = parseUserProps(userProps)
    if (pairs == null) {
      setErr(t('mqtt.badUserProps'))
      return
    }
    if (pairs.length > 0) cmd.userProperties = pairs
    if (expiry.trim() !== '') {
      const n = Number(expiry)
      if (!Number.isInteger(n) || n < 0) {
        setErr(t('mqtt.badExpiry'))
        return
      }
      cmd.messageExpiry = n
    }
    if (send(cmd)) setErr(null)
  }

  const shown = needle.trim()
    ? rows.filter((m) => m.topic.includes(needle.trim()))
    : rows

  const exportLog = () => {
    const doc = rows
      .slice()
      .reverse()
      .map((m) => ({
        topic: m.topic,
        payload: bytesToBase64(m.payload),
        qos: m.qos,
        retain: m.retain,
        ...(m.contentType ? { contentType: m.contentType } : {}),
        ...(m.payloadFormat !== undefined ? { payloadFormat: m.payloadFormat } : {}),
        ...(m.responseTopic ? { responseTopic: m.responseTopic } : {}),
        ...(m.correlation ? { correlationData: m.correlation } : {}),
        ...(m.userProperties ? { userProperties: m.userProperties } : {}),
        ...(m.messageExpiry !== undefined ? { messageExpiry: m.messageExpiry } : {}),
      }))
    const blob = new Blob([JSON.stringify(doc, null, 2)], { type: 'application/json' })
    const a = document.createElement('a')
    a.href = URL.createObjectURL(blob)
    a.download = 'mqtt-log.json'
    a.click()
    URL.revokeObjectURL(a.href)
  }

  const importLog = (file: File) => {
    file
      .text()
      .then((text) => {
        const parsed = JSON.parse(text) as unknown
        if (!Array.isArray(parsed)) throw new Error('shape')
        const loaded: Msg[] = []
        for (const item of parsed) {
          if (!item || typeof item !== 'object') continue
          const rec = item as Record<string, unknown>
          if (typeof rec.topic !== 'string') continue
          const payload =
            typeof rec.payload === 'string' ? (base64ToBytes(rec.payload) ?? new Uint8Array()) : new Uint8Array()
          const msg: Msg = {
            id: ++seq.current,
            topic: rec.topic,
            payload,
            qos: typeof rec.qos === 'number' ? rec.qos : 0,
            retain: Boolean(rec.retain),
            playback: true,
          }
          if (typeof rec.contentType === 'string') msg.contentType = rec.contentType
          if (typeof rec.payloadFormat === 'number') msg.payloadFormat = rec.payloadFormat
          if (typeof rec.responseTopic === 'string') msg.responseTopic = rec.responseTopic
          if (typeof rec.correlationData === 'string') msg.correlation = rec.correlationData
          if (typeof rec.messageExpiry === 'number') msg.messageExpiry = rec.messageExpiry
          loaded.push(msg)
        }
        setRows((prev) => {
          const next = [...loaded.reverse(), ...prev]
          return next.length > MAX_ROWS ? next.slice(0, MAX_ROWS) : next
        })
        setErr(null)
      })
      .catch(() => setErr(t('mqtt.badLog')))
  }

  return (
    <>
      <div className="main-head">
        <div>
          <div className="row">
            <h1>{t('mqtt.title')}</h1>
            <Badge tone={statusTone(status)}>{t(`mqtt.status.${status}`)}</Badge>
          </div>
          {/* 说明随模式换：两档做的事不同，一句通用的介绍等于都没说 */}
          <p>{mode === 'watch' ? t('capture.lead') : t('mqtt.lead')}</p>
        </div>
        <div className="main-actions" role="group" aria-label={t('mqtt.mode')}>
          <Button
            variant={mode === 'client' ? 'primary' : undefined}
            onClick={() => switchMode('client')}
          >
            {t('mqtt.modeClient')}
          </Button>
          <Button
            variant={mode === 'watch' ? 'primary' : undefined}
            onClick={() => switchMode('watch')}
          >
            {t('mqtt.modeWatch')}
          </Button>
        </div>
      </div>

      {err ? (
        <div className="banner" data-tone="danger" role="alert">
          <div className="banner-body">{err}</div>
        </div>
      ) : null}
      {dropped > 0 ? (
        <div className="banner" data-tone="warn">
          <div className="banner-body">{t('mqtt.dropped', String(dropped))}</div>
        </div>
      ) : null}

      <div className="spread">
        <div className="col">
          <Card title={mode === 'watch' ? t('capture.watch') : t('mqtt.subscribe')}>
            <div className="col">
              <Field
                label={t('mqtt.filter')}
                hint={mode === 'watch' ? t('capture.filterHint') : t('mqtt.filterHint')}
              >
                <input value={filter} onChange={(e) => setFilter(e.target.value)} spellCheck={false} />
              </Field>
              <div className="row row-controls">
                {/* 旁观不建立订阅，没有 QoS 可言 */}
                {mode === 'client' ? (
                  <Field label={t('mqtt.qos')}>
                    <select value={subQos} onChange={(e) => setSubQos(Number(e.target.value))}>
                      <option value={0}>0</option>
                      <option value={1}>1</option>
                      <option value={2}>2</option>
                    </select>
                  </Field>
                ) : null}
                <Button variant="primary" onClick={subscribe} disabled={status !== 'open'}>
                  {mode === 'watch' ? t('capture.watchAction') : t('mqtt.subscribe')}
                </Button>
              </div>
              {subs.length > 0 ? (
                <ul className="mqtt-subs">
                  {subs.map((s) => (
                    <li key={s.filter}>
                      <span className="mono">{s.filter}</span>
                      {mode === 'client' ? <Badge tone="muted">QoS {s.qos}</Badge> : null}
                      <Button
                        size="sm"
                        onClick={() =>
                          send(
                            mode === 'watch'
                              ? { op: 'unwatch', filter: s.filter }
                              : { op: 'unsubscribe', filter: s.filter },
                          )
                        }
                        disabled={status !== 'open'}
                      >
                        {mode === 'watch' ? t('common.delete') : t('mqtt.unsubscribe')}
                      </Button>
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="dim">{t('mqtt.noSubs')}</p>
              )}
            </div>
          </Card>

          {/* 旁观模式不给发布面板：发布是客户端的角色，而旁观的全部
              意义就是不以客户端身份介入。想发一条就切回「订阅」。 */}
          {mode === 'watch' ? (
            <Card title={t('mqtt.publish')}>
              <p className="dim">{t('mqtt.publishInWatch')}</p>
            </Card>
          ) : null}

          {mode === 'client' ? (
          <Card title={t('mqtt.publish')}>
            <div className="col">
              <Field label={t('mqtt.topic')}>
                <input value={topic} onChange={(e) => setTopic(e.target.value)} spellCheck={false} />
              </Field>
              <Field label={t('mqtt.payload')}>
                <textarea rows={4} value={body} onChange={(e) => setBody(e.target.value)} spellCheck={false} />
              </Field>
              <label className="row">
                <input type="checkbox" checked={bodyIsB64} onChange={(e) => setBodyIsB64(e.target.checked)} />
                <span>{t('mqtt.payloadB64')}</span>
              </label>
              <div className="row row-controls">
                <Field label={t('mqtt.qos')}>
                  <select value={pubQos} onChange={(e) => setPubQos(Number(e.target.value))}>
                    <option value={0}>0</option>
                    <option value={1}>1</option>
                    <option value={2}>2</option>
                  </select>
                </Field>
                <label className="row">
                  <input type="checkbox" checked={retain} onChange={(e) => setRetain(e.target.checked)} />
                  <span>{t('mqtt.retain')}</span>
                </label>
                <Button variant="primary" onClick={publish} disabled={status !== 'open'}>
                  {t('mqtt.publish')}
                </Button>
              </div>
              <details>
                <summary>{t('mqtt.props')}</summary>
                <div className="col mqtt-props">
                  <Field label={t('mqtt.contentType')}>
                    <input value={contentType} onChange={(e) => setContentType(e.target.value)} spellCheck={false} />
                  </Field>
                  <Field label={t('mqtt.payloadFormat')} hint={t('mqtt.payloadFormatHint')}>
                    <select value={payloadFormat} onChange={(e) => setPayloadFormat(e.target.value)}>
                      <option value="">{t('common.none')}</option>
                      <option value="0">0</option>
                      <option value="1">1</option>
                    </select>
                  </Field>
                  <Field label={t('mqtt.responseTopic')}>
                    <input
                      value={responseTopic}
                      onChange={(e) => setResponseTopic(e.target.value)}
                      spellCheck={false}
                    />
                  </Field>
                  <Field label={t('mqtt.correlation')}>
                    <input value={correlation} onChange={(e) => setCorrelation(e.target.value)} spellCheck={false} />
                  </Field>
                  <Field label={t('mqtt.userProps')} hint={t('mqtt.userPropsHint')}>
                    <textarea
                      rows={3}
                      value={userProps}
                      onChange={(e) => setUserProps(e.target.value)}
                      spellCheck={false}
                    />
                  </Field>
                  <Field label={t('mqtt.expiry')} hint={t('mqtt.expiryHint')}>
                    <input
                      value={expiry}
                      onChange={(e) => setExpiry(e.target.value)}
                      inputMode="numeric"
                      spellCheck={false}
                    />
                  </Field>
                </div>
              </details>
            </div>
          </Card>
          ) : null}
        </div>

        <Card title={t('mqtt.tape')}>
          <div className="toolbar">
            <input
              className="grow"
              value={needle}
              placeholder={t('mqtt.find')}
              onChange={(e) => setNeedle(e.target.value)}
              spellCheck={false}
            />
            <Button size="sm" onClick={exportLog} disabled={rows.length === 0}>
              {t('mqtt.export')}
            </Button>
            <Button size="sm" onClick={() => fileRef.current?.click()}>
              {t('mqtt.import')}
            </Button>
            <Button size="sm" onClick={() => setRows([])} disabled={rows.length === 0}>
              {t('mqtt.clear')}
            </Button>
            <input
              ref={fileRef}
              type="file"
              accept="application/json,.json"
              hidden
              onChange={(e) => {
                const f = e.target.files?.[0]
                if (f) importLog(f)
                e.target.value = ''
              }}
            />
          </div>
          {status !== 'open' && rows.length === 0 ? (
            <p className="dim">{t('mqtt.connecting')}</p>
          ) : rows.length === 0 ? (
            <Empty
              title={t('mqtt.emptyTitle')}
              body={mode === 'watch' ? t('capture.emptyBody') : t('mqtt.emptyBody')}
              cmd={mode === 'watch' ? t('capture.emptyCmd') : t('mqtt.emptyCmd')}
            />
          ) : shown.length === 0 ? (
            <EmptyFiltered t={t} onClear={() => setNeedle('')} />
          ) : (
            <div className="mqtt-tape">
              {shown.map((m) => (
                <MessageRow key={m.id} m={m} t={t} />
              ))}
            </div>
          )}
        </Card>
      </div>
    </>
  )
}

function MessageRow(props: { m: Msg; t: T }) {
  const { m, t } = props
  const decoded = present(m.payload, m.contentType, m.payloadFormat)
  return (
    <article className="mqtt-row">
      <div className="row">
        <span className="mono">{m.topic}</span>
        <Badge tone="muted">QoS {m.qos}</Badge>
        {m.retain ? <Badge tone="info">{t('mqtt.retain')}</Badge> : null}
        {m.playback ? <Badge tone="warn">{t('mqtt.playback')}</Badge> : null}
        <Badge tone={kindTone(decoded.kind)}>{t(`mqtt.kind.${decoded.kind}`)}</Badge>
        {m.contentType ? <span className="faint mono">{m.contentType}</span> : null}
      </div>
      <pre className="json">{decoded.text || t('mqtt.emptyPayload')}</pre>
      {m.responseTopic || m.correlation || m.userProperties || m.messageExpiry !== undefined ? (
        <p className="faint mono mqtt-meta">
          {m.responseTopic ? `${t('mqtt.responseTopic')} ${m.responseTopic}  ` : ''}
          {m.correlation ? `${t('mqtt.correlation')} ${m.correlation}  ` : ''}
          {m.messageExpiry !== undefined ? `${t('mqtt.expiry')} ${m.messageExpiry}s  ` : ''}
          {m.userProperties?.map(([k, v]) => `${k}=${v}`).join(' ')}
        </p>
      ) : null}
    </article>
  )
}

function parseUserProps(raw: string): [string, string][] | null {
  const out: [string, string][] = []
  for (const line of raw.split('\n')) {
    const trimmed = line.trim()
    if (!trimmed) continue
    const i = trimmed.indexOf('=')
    if (i <= 0) return null
    out.push([trimmed.slice(0, i), trimmed.slice(i + 1)])
  }
  return out
}

function statusTone(s: Status): Tone {
  if (s === 'open') return 'ok'
  if (s === 'connecting') return 'warn'
  return 'danger'
}

function kindTone(k: DecodedKind): Tone {
  if (k === 'json' || k === 'cbor' || k === 'protobuf') return 'info'
  return 'muted'
}
