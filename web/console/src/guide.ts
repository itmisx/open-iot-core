/*
 * 使用教程的内容源与解析（UI-078）。
 *
 * 正文是 docs/tutorial/{zh,en}/ 下的多级目录 markdown，vite 以 ?raw 把它们
 * 原样打进产物。为什么不引 markdown 库：控制台的运行时依赖锁在 react 与
 * react-dom 两个（LANG-019、TST-133），而产物是**提交进仓库**的（UI-039），
 * 一个解析器加高亮器就是几十到几百 KB 进 git 历史。这里要的子集只有八种
 * 块级节点，手写一百多行就够。
 *
 * 子集之外的语法不静默丢弃，而是渲染成一条显眼的错误：静默丢弃的后果是
 * 作者在编辑器里看着对，而界面上少一段。门禁（console_guide_test.go）会在
 * 合并前把这类错误全部拦下，所以界面上这条错误只会出现在本地改坏的那一刻。
 */

import type { Lang } from './i18n'

export type GuideSpan = { t: string; s?: 'code' | 'strong'; href?: string }

export type GuideNode =
  | { k: 'h2' | 'h3'; id: string; text: string }
  | { k: 'p' | 'quote'; spans: GuideSpan[] }
  | { k: 'ul' | 'ol'; items: GuideSpan[][] }
  | { k: 'code'; lang: string; text: string; file?: string }
  | { k: 'table'; head: string[]; rows: GuideSpan[][][] }
  | { k: 'error'; text: string }

export type GuideToc = { id: string; text: string; level: 2 | 3 }

export type GuidePage = { slug: string; title: string; toc: GuideToc[]; nodes: GuideNode[] }

export type GuideChapter = {
  slug: string
  title: string
  intro: GuideSpan[]
  pages: GuidePage[]
}

// 两棵语言树与示例代码，构建期原样取入。
//
// glob 的键是相对本文件的路径，形如 ../../../docs/tutorial/zh/01-sdk/…，
// 下面统一归一化成 zh/01-sdk/… 再用。
const MD = import.meta.glob('../../../docs/tutorial/**/*.md', {
  query: '?raw',
  import: 'default',
  eager: true,
}) as Record<string, string>

const SNIPPETS = import.meta.glob('../../../examples/**/*.{go,py,java,xml}', {
  query: '?raw',
  import: 'default',
  eager: true,
}) as Record<string, string>

/** 去掉 glob 键上的相对前缀，留下仓库根起算的路径。 */
function repoPath(key: string): string {
  return key.replace(/^(\.\.\/)+/, '')
}

const EXT_LANG: Record<string, string> = {
  go: 'go',
  py: 'python',
  java: 'java',
  xml: 'xml',
  json: 'json',
  sh: 'bash',
  yaml: 'yaml',
  yml: 'yaml',
}

/**
 * snippet 取示例文件里两个标记之间的那一段（UI-080）。
 *
 * 示例不写在 markdown 里，而是来自真实可编译的文件：文档里的代码会腐坏而
 * 没有任何东西会发现——改了接口形状，markdown 里那段照样好好地印在页面上。
 * 标记写成注释里的 tutorial:begin / tutorial:end，于是 Go、Python、Java 与
 * XML 共用一套写法，而示例本身照样能编译。
 *
 * 取不到时返回一条错误而不是空串：空串会让这一节看起来只是"短了点"。
 */
function snippet(spec: string): GuideNode {
  const [path, anchor] = spec.split('#')
  if (!path || !anchor) {
    return { k: 'error', text: `include needs path#anchor, got: ${spec}` }
  }
  const key = Object.keys(SNIPPETS).find((k) => repoPath(k) === path)
  if (key === undefined) {
    return { k: 'error', text: `include target not found: ${path}` }
  }
  const lines = SNIPPETS[key]!.replace(/\r\n/g, '\n').split('\n')
  const begin = `tutorial:begin ${anchor}`
  const end = `tutorial:end ${anchor}`
  let open = false
  const body: string[] = []
  for (const line of lines) {
    if (line.includes(begin)) {
      open = true
      continue
    }
    if (line.includes(end)) {
      if (!open) return { k: 'error', text: `anchor ${anchor} ends before it begins in ${path}` }
      if (body.length === 0) return { k: 'error', text: `anchor ${anchor} is empty in ${path}` }
      const ext = path.slice(path.lastIndexOf('.') + 1)
      return { k: 'code', lang: EXT_LANG[ext] ?? '', text: dedent(body), file: path }
    }
    if (open) body.push(line)
  }
  return { k: 'error', text: `anchor ${anchor} not found in ${path}` }
}

/** 去掉整段共同的缩进：片段多在函数体内，照原样贴出来每行都多四格。 */
function dedent(lines: string[]): string {
  let indent = -1
  for (const l of lines) {
    if (l.trim() === '') continue
    const n = l.length - l.replace(/^[ \t]+/, '').length
    if (indent < 0 || n < indent) indent = n
  }
  const cut = indent > 0 ? lines.map((l) => l.slice(indent)) : lines
  while (cut.length > 0 && cut[0]!.trim() === '') cut.shift()
  while (cut.length > 0 && cut[cut.length - 1]!.trim() === '') cut.pop()
  return cut.join('\n')
}

const CJK = /[　-〿぀-ヿ㐀-䶿一-鿿＀-￯]/

/**
 * 软换行的拼接：中文之间直接相连，其余补一个空格。
 *
 * markdown 的软换行在英文里是空格，而在中文里会多出一个可见的空隙。
 * 两棵树用同一个解析器，这条必须按两种语言各自正确。
 */
function joinLines(parts: string[]): string {
  return parts.reduce((acc, cur) => {
    if (acc === '') return cur
    const left = acc[acc.length - 1]!
    const right = cur[0] ?? ''
    return CJK.test(left) && CJK.test(right) ? acc + cur : `${acc} ${cur}`
  }, '')
}

/** 行内语法：`代码`、**强调**、[文字](地址)。未闭合的标记原样留着当文字。 */
function inline(s: string): GuideSpan[] {
  const out: GuideSpan[] = []
  let lit = ''
  const flush = () => {
    if (lit !== '') {
      out.push({ t: lit })
      lit = ''
    }
  }
  let i = 0
  while (i < s.length) {
    const rest = s.slice(i)
    if (s[i] === '`') {
      const j = rest.indexOf('`', 1)
      if (j > 0) {
        flush()
        out.push({ t: rest.slice(1, j), s: 'code' })
        i += j + 1
        continue
      }
    } else if (rest.startsWith('**')) {
      const j = rest.indexOf('**', 2)
      if (j > 0) {
        flush()
        out.push({ t: rest.slice(2, j), s: 'strong' })
        i += j + 2
        continue
      }
    } else if (s[i] === '[') {
      const close = rest.indexOf(']')
      if (close > 0 && rest[close + 1] === '(') {
        const end = rest.indexOf(')', close)
        if (end > 0) {
          flush()
          out.push({ t: rest.slice(1, close), href: rest.slice(close + 2, end) })
          i += end + 1
          continue
        }
      }
    }
    lit += s[i]
    i += 1
  }
  flush()
  return out
}

function isOrdered(line: string): boolean {
  return /^\d+\. /.test(line)
}

function isBlockStart(line: string): boolean {
  return (
    line.startsWith('#') ||
    line.startsWith('```') ||
    line.startsWith('- ') ||
    line.startsWith('|') ||
    line.startsWith('> ') ||
    line.startsWith('<!-- include:') ||
    isOrdered(line)
  )
}

/** 按 | 切格，但不切行内代码里的 |（策略与主题的例子里会出现管道符）。 */
function cells(row: string): string[] {
  const s = row.trim().replace(/^\|/, '').replace(/\|$/, '')
  const out: string[] = []
  let cur = ''
  let inCode = false
  for (const ch of s) {
    if (ch === '`') {
      inCode = !inCode
      cur += ch
    } else if (ch === '|' && !inCode) {
      out.push(cur)
      cur = ''
    } else {
      cur += ch
    }
  }
  out.push(cur)
  return out.map((c) => c.trim())
}

type Parsed = { title: string; nodes: GuideNode[] }

/** 解析一份教程 markdown。第一个 # 是页标题，二三级标题进页内目录。 */
export function parseMarkdown(src: string): Parsed {
  const lines = src.replace(/\r\n/g, '\n').split('\n').map((l) => l.replace(/[ \t]+$/, ''))
  const nodes: GuideNode[] = []
  let title = ''
  let secs = 0
  let i = 0

  while (i < lines.length) {
    const line = lines[i]!
    if (line.trim() === '') {
      i += 1
      continue
    }

    if (line.startsWith('<!-- include:')) {
      nodes.push(snippet(line.replace('<!-- include:', '').replace('-->', '').trim()))
      i += 1
      continue
    }

    if (line.startsWith('#')) {
      const level = line.length - line.replace(/^#+/, '').length
      const text = line.slice(level).trim()
      if (level === 1 && title === '') {
        title = text
      } else if (level === 2 || level === 3) {
        secs += 1
        // 锚点按序号而不是标题文字：中文标题 slug 化之后是空的，而两棵树
        // 的同一节要是同一个锚点，链接在切换语言之后才还有效。
        nodes.push({ k: level === 2 ? 'h2' : 'h3', id: `sec-${secs}`, text })
      } else {
        nodes.push({ k: 'error', text: `unsupported heading level ${level}: ${text}` })
      }
      i += 1
      continue
    }

    if (line.startsWith('```')) {
      const lang = line.slice(3).trim()
      const body: string[] = []
      i += 1
      let closed = false
      while (i < lines.length) {
        if (lines[i]!.trim().startsWith('```')) {
          closed = true
          i += 1
          break
        }
        body.push(lines[i]!)
        i += 1
      }
      if (!closed) nodes.push({ k: 'error', text: 'unterminated code fence' })
      else nodes.push({ k: 'code', lang, text: body.join('\n') })
      continue
    }

    if (line.startsWith('- ') || isOrdered(line)) {
      const kind = line.startsWith('- ') ? 'ul' : 'ol'
      const items: GuideSpan[][] = []
      while (i < lines.length) {
        const cur = lines[i]!
        if (kind === 'ul' && cur.startsWith('- ')) {
          items.push(inline(cur.slice(2)))
        } else if (kind === 'ol' && isOrdered(cur)) {
          items.push(inline(cur.slice(cur.indexOf('. ') + 2)))
        } else if (cur.startsWith('  ') && cur.trim() !== '' && items.length > 0) {
          // 缩进两格是上一条的续行
          const prev = items[items.length - 1]!
          items[items.length - 1] = prev.concat(inline(` ${cur.trim()}`))
        } else {
          break
        }
        i += 1
      }
      nodes.push({ k: kind, items })
      continue
    }

    if (line.startsWith('|')) {
      const head = cells(line)
      const sep = lines[i + 1] ?? ''
      if (!/^\|[-:| \t]+\|?$/.test(sep.trim())) {
        nodes.push({ k: 'error', text: `table head needs a |---| row: ${line}` })
        i += 1
        continue
      }
      i += 2
      const rows: GuideSpan[][][] = []
      while (i < lines.length && lines[i]!.trim().startsWith('|')) {
        rows.push(cells(lines[i]!).map(inline))
        i += 1
      }
      nodes.push({ k: 'table', head, rows })
      continue
    }

    if (line.startsWith('> ')) {
      const parts: string[] = []
      while (i < lines.length && lines[i]!.startsWith('> ')) {
        parts.push(lines[i]!.slice(2).trim())
        i += 1
      }
      nodes.push({ k: 'quote', spans: inline(joinLines(parts)) })
      continue
    }

    const parts: string[] = []
    while (i < lines.length && lines[i]!.trim() !== '' && !isBlockStart(lines[i]!)) {
      parts.push(lines[i]!.trim())
      i += 1
    }
    nodes.push({ k: 'p', spans: inline(joinLines(parts)) })
  }

  return { title, nodes }
}

/** 去掉 NN- 前缀。顺序由数字前缀决定，所以不另立清单（UI-078）。 */
function slugOf(name: string): string {
  return name.replace(/\.md$/, '').replace(/^\d+-/, '')
}

function orderOf(name: string): number {
  const m = /^(\d+)-/.exec(name)
  return m ? Number(m[1]) : 0
}

const cache = new Map<Lang, GuideChapter[]>()

/**
 * loadGuide 组出一种语言的章节树。
 *
 * 结果缓存：解析是纯函数，而切一次语言或翻一页都会再问一次。
 */
export function loadGuide(lang: Lang): GuideChapter[] {
  const hit = cache.get(lang)
  if (hit) return hit

  const prefix = `docs/tutorial/${lang}/`
  const byChapter = new Map<string, { file: string; src: string }[]>()
  for (const key of Object.keys(MD)) {
    const path = repoPath(key)
    if (!path.startsWith(prefix)) continue
    const rest = path.slice(prefix.length)
    const [dir, file] = rest.split('/')
    if (!dir || !file) continue
    const list = byChapter.get(dir) ?? []
    list.push({ file, src: MD[key]! })
    byChapter.set(dir, list)
  }

  const chapters: GuideChapter[] = []
  for (const dir of [...byChapter.keys()].sort((a, b) => orderOf(a) - orderOf(b))) {
    const files = byChapter.get(dir)!
    const meta = files.find((f) => f.file === '_chapter.md')
    const parsedMeta = meta ? parseMarkdown(meta.src) : { title: dir, nodes: [] }
    const introNode = parsedMeta.nodes.find((n) => n.k === 'p')
    const pages = files
      .filter((f) => f.file !== '_chapter.md')
      .sort((a, b) => orderOf(a.file) - orderOf(b.file))
      .map(({ file, src }) => {
        const { title, nodes } = parseMarkdown(src)
        const toc: GuideToc[] = nodes
          .filter((n): n is Extract<GuideNode, { k: 'h2' | 'h3' }> => n.k === 'h2' || n.k === 'h3')
          .map((n) => ({ id: n.id, text: n.text, level: n.k === 'h2' ? 2 : 3 }))
        return { slug: slugOf(file), title, toc, nodes }
      })
    chapters.push({
      slug: slugOf(dir),
      title: parsedMeta.title || dir,
      intro: introNode && introNode.k === 'p' ? introNode.spans : [],
      pages,
    })
  }
  cache.set(lang, chapters)
  return chapters
}
