/*
 * 测试客户端的负载解码。只做 JSON、定长 CBOR 和没有 schema 的 protobuf
 * 线格式。不引库：这三种够看一条设备消息，而一个解码库会进主包。
 */

export type DecodedKind = 'json' | 'cbor' | 'protobuf' | 'text' | 'hex'

export type Decoded = { kind: DecodedKind; text: string }

const MAX_TEXT = 8 * 1024

export function present(
  payload: Uint8Array,
  contentType: string | undefined,
  payloadFormat: number | undefined,
): Decoded {
  const ct = (contentType ?? '').toLowerCase()
  const text = decodeUtf8(payload)

  if (ct.includes('json') || ct.endsWith('+json')) {
    return text != null && looksLikeJSON(text)
      ? { kind: 'json', text: prettyJSON(text) }
      : { kind: 'hex', text: clip(toHex(payload)) }
  }
  if (ct.includes('cbor')) {
    const v = decodeCbor(payload)
    return v.ok
      ? { kind: 'cbor', text: clip(JSON.stringify(v.value, null, 2)) }
      : { kind: 'hex', text: clip(toHex(payload)) }
  }
  if (ct.includes('protobuf') || ct.includes('proto')) {
    const fields = decodeProtobuf(payload)
    return fields
      ? { kind: 'protobuf', text: clip(formatProtobuf(fields, 0)) }
      : { kind: 'hex', text: clip(toHex(payload)) }
  }

  if (text != null && looksLikeJSON(text)) {
    return { kind: 'json', text: prettyJSON(text) }
  }
  const cbor = decodeCbor(payload)
  if (cbor.ok && cbor.structured) {
    return { kind: 'cbor', text: clip(JSON.stringify(cbor.value, null, 2)) }
  }
  if (text != null && (payloadFormat === 1 || printable(text))) {
    return { kind: 'text', text: clip(text) }
  }
  const fields = decodeProtobuf(payload)
  if (fields) return { kind: 'protobuf', text: clip(formatProtobuf(fields, 0)) }
  if (cbor.ok) return { kind: 'cbor', text: clip(JSON.stringify(cbor.value, null, 2)) }
  return { kind: 'hex', text: clip(toHex(payload)) }
}

function clip(s: string): string {
  if (s.length <= MAX_TEXT) return s
  return s.slice(0, MAX_TEXT) + '\n…'
}

function prettyJSON(s: string): string {
  try {
    return clip(JSON.stringify(JSON.parse(s), null, 2))
  } catch {
    return clip(s)
  }
}

function looksLikeJSON(s: string): boolean {
  const t = s.trimStart()
  if (!t.startsWith('{') && !t.startsWith('[')) return false
  try {
    JSON.parse(s)
    return true
  } catch {
    return false
  }
}

function printable(s: string): boolean {
  if (s.length === 0) return true
  let ok = 0
  for (const ch of s) {
    const c = ch.codePointAt(0) ?? 0
    if (c === 9 || c === 10 || c === 13 || (c >= 32 && c !== 127)) ok++
  }
  return ok / [...s].length > 0.9
}

function decodeUtf8(b: Uint8Array): string | null {
  try {
    return new TextDecoder('utf-8', { fatal: true }).decode(b)
  } catch {
    return null
  }
}

export function toHex(b: Uint8Array): string {
  let s = ''
  for (const x of b) s += x.toString(16).padStart(2, '0')
  return s
}

export function bytesToBase64(b: Uint8Array): string {
  let bin = ''
  const chunk = 0x8000
  for (let i = 0; i < b.length; i += chunk) {
    bin += String.fromCharCode(...b.subarray(i, i + chunk))
  }
  return btoa(bin)
}

export function base64ToBytes(s: string): Uint8Array | null {
  try {
    const bin = atob(s)
    const out = new Uint8Array(bin.length)
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i)
    return out
  } catch {
    return null
  }
}

/* ---------- CBOR（只接受定长） ---------- */

type CborResult = { ok: true; structured: boolean; value: unknown } | { ok: false }

class Cursor {
  i = 0
  readonly view: DataView
  constructor(readonly b: Uint8Array) {
    this.view = new DataView(b.buffer, b.byteOffset, b.byteLength)
  }
  need(n: number) {
    if (this.i + n > this.b.length) throw new Error('short')
  }
  u8(): number {
    this.need(1)
    return this.b[this.i++] ?? 0
  }
  u16(): number {
    this.need(2)
    const n = this.view.getUint16(this.i)
    this.i += 2
    return n
  }
  u32(): number {
    this.need(4)
    const n = this.view.getUint32(this.i)
    this.i += 4
    return n
  }
  f32(): number {
    this.need(4)
    const n = this.view.getFloat32(this.i)
    this.i += 4
    return n
  }
  f64(): number {
    this.need(8)
    const n = this.view.getFloat64(this.i)
    this.i += 8
    return n
  }
  take(n: number): Uint8Array {
    this.need(n)
    const s = this.b.subarray(this.i, this.i + n)
    this.i += n
    return s
  }
}

function decodeCbor(b: Uint8Array): CborResult {
  if (b.length === 0) return { ok: false }
  try {
    const c = new Cursor(b)
    const value = readCbor(c)
    if (c.i !== b.length) return { ok: false }
    return { ok: true, structured: isStructured(value), value }
  } catch {
    return { ok: false }
  }
}

function isStructured(v: unknown): boolean {
  if (Array.isArray(v)) return true
  if (typeof v !== 'object' || v === null) return false
  if ('tag' in v) return true
  if (Object.keys(v).length === 1 && 'bytes' in v) return false
  return true
}

function readLen(c: Cursor, ai: number): number {
  if (ai < 24) return ai
  if (ai === 24) return c.u8()
  if (ai === 25) return c.u16()
  if (ai === 26) return c.u32()
  if (ai === 27) {
    const hi = c.u32()
    const lo = c.u32()
    const n = hi * 0x100000000 + lo
    if (!Number.isSafeInteger(n)) throw new Error('big')
    return n
  }
  throw new Error('indefinite')
}

function readCbor(c: Cursor): unknown {
  const head = c.u8()
  const major = head >> 5
  const ai = head & 0x1f
  if (ai === 31) throw new Error('indefinite')
  switch (major) {
    case 0:
      return readLen(c, ai)
    case 1:
      return -1 - readLen(c, ai)
    case 2: {
      const raw = c.take(readLen(c, ai))
      return { bytes: toHex(raw) }
    }
    case 3: {
      const raw = c.take(readLen(c, ai))
      const s = decodeUtf8(raw)
      if (s == null) throw new Error('utf8')
      return s
    }
    case 4: {
      const n = readLen(c, ai)
      const arr: unknown[] = []
      for (let i = 0; i < n; i++) arr.push(readCbor(c))
      return arr
    }
    case 5: {
      const n = readLen(c, ai)
      const obj: Record<string, unknown> = {}
      const pairs: [unknown, unknown][] = []
      let allString = true
      for (let i = 0; i < n; i++) {
        const k = readCbor(c)
        const v = readCbor(c)
        pairs.push([k, v])
        if (typeof k !== 'string') allString = false
      }
      if (!allString) return pairs
      for (const [k, v] of pairs) obj[k as string] = v
      return obj
    }
    case 6:
      return { tag: readLen(c, ai), value: readCbor(c) }
    case 7:
      return readSimple(c, ai)
    default:
      throw new Error('major')
  }
}

function readSimple(c: Cursor, ai: number): unknown {
  if (ai < 20) return ai
  if (ai === 20) return false
  if (ai === 21) return true
  if (ai === 22) return null
  if (ai === 23) return null
  if (ai === 25) return halfToFloat(c.u16())
  if (ai === 26) return c.f32()
  if (ai === 27) return c.f64()
  throw new Error('simple')
}

function halfToFloat(n: number): number {
  const sign = n & 0x8000 ? -1 : 1
  const exp = (n >> 10) & 0x1f
  const frac = n & 0x3ff
  if (exp === 0) return sign * 2 ** -14 * (frac / 1024)
  if (exp === 31) return frac ? Number.NaN : sign * Number.POSITIVE_INFINITY
  return sign * 2 ** (exp - 15) * (1 + frac / 1024)
}

/* ---------- protobuf 线格式（没有字段名） ---------- */

type ProtoField = { n: number; text: string; nested?: ProtoField[] }

function decodeProtobuf(b: Uint8Array): ProtoField[] | null {
  if (b.length === 0) return null
  try {
    const fields = walkProto(b, 0)
    return fields && fields.length > 0 ? fields : null
  } catch {
    return null
  }
}

function walkProto(b: Uint8Array, depth: number): ProtoField[] | null {
  if (depth > 6 || b.length === 0) return null
  const c = new Cursor(b)
  const out: ProtoField[] = []
  try {
    while (c.i < b.length) {
      const key = readVarint(c)
      const n = Number(key >> 3n)
      const w = Number(key & 7n)
      if (!Number.isSafeInteger(n) || n <= 0 || n > 100_000) return null
      if (w === 0) {
        out.push({ n, text: readVarint(c).toString() })
      } else if (w === 1) {
        out.push({ n, text: toHex(c.take(8)) })
      } else if (w === 5) {
        out.push({ n, text: toHex(c.take(4)) })
      } else if (w === 2) {
        const len = readVarint(c)
        if (len > 1_000_000n) return null
        const raw = c.take(Number(len))
        const utf = decodeUtf8(raw)
        const field: ProtoField = { n, text: utf ?? toHex(raw) }
        const nested = walkProto(raw, depth + 1)
        if (nested && (utf == null || !printable(utf))) field.nested = nested
        out.push(field)
      } else {
        return null
      }
    }
  } catch {
    return null
  }
  if (c.i !== b.length) return null
  return out
}

function readVarint(c: Cursor): bigint {
  let n = 0n
  let shift = 0n
  for (let i = 0; i < 10; i++) {
    const b = c.u8()
    n |= BigInt(b & 0x7f) << shift
    if ((b & 0x80) === 0) return n
    shift += 7n
  }
  throw new Error('varint')
}

function formatProtobuf(fields: ProtoField[], depth: number): string {
  const pad = '  '.repeat(depth)
  return fields
    .map((f) => {
      if (f.nested && f.nested.length > 0) {
        return `${pad}${f.n} {\n${formatProtobuf(f.nested, depth + 1)}\n${pad}}`
      }
      return `${pad}${f.n}: ${f.text}`
    })
    .join('\n')
}
