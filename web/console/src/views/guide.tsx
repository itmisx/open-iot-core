/*
 * 使用教程（UI-076、UI-077）。
 *
 * 入口在侧栏最顶部：第一次打开控制台的人是这一页唯一的读者，而他们不知道
 * 该找什么词——排在「设备」「证书」之后，等于只有已经会用的人才看得到。
 *
 * 章与页都能深链接（#/guide/sdk/golang）：教程的主要传播方式是把某一页贴给
 * 别人，只能跳到首页的教程在这件事上等于没有链接。
 */
import type { T } from '../i18n'
import type { Lang } from '../i18n'
import { loadGuide, type GuideChapter, type GuideNode, type GuideSpan } from '../guide'
import { href } from '../router'
import { Card, CopyButton } from '../ui'
import { Icon } from '../icons'

export function GuideView(props: { t: T; lang: Lang; chapter?: string; page?: string }) {
  const { t, lang } = props
  const chapters = loadGuide(lang)

  if (!props.chapter) return <GuideHome t={t} chapters={chapters} />

  const chapter = chapters.find((c) => c.slug === props.chapter)
  const page = chapter?.pages.find((p) => p.slug === (props.page ?? chapter.pages[0]?.slug))
  if (!chapter || !page) {
    return (
      <>
        <div className="main-head">
          <div>
            <h1>{t('guide.title')}</h1>
            <p>{t('guide.notFound')}</p>
          </div>
        </div>
        <Card>
          <a className="btn" href={href({ view: 'guide' })}>
            <Icon name="back" />
            <span>{t('guide.backHome')}</span>
          </a>
        </Card>
      </>
    )
  }

  // 同一章里的前后页。跨章不接：翻到章末停住，比把人悄悄带进另一个主题好。
  const idx = chapter.pages.indexOf(page)
  const prev = idx > 0 ? chapter.pages[idx - 1] : undefined
  const next = idx < chapter.pages.length - 1 ? chapter.pages[idx + 1] : undefined

  return (
    <>
      <div className="main-head">
        <div>
          <h1>{page.title}</h1>
          <p>{chapter.title}</p>
        </div>
      </div>

      <div className="guide">
        <nav className="guide-nav" aria-label={t('guide.contents')}>
          {chapters.map((c) => (
            <div className="guide-nav-group" key={c.slug}>
              <div className="nav-label">{c.title}</div>
              {c.pages.map((p) => (
                <a
                  key={p.slug}
                  className="nav-item"
                  href={href({ view: 'guide', chapter: c.slug, page: p.slug })}
                  aria-current={c.slug === chapter.slug && p.slug === page.slug ? 'page' : undefined}
                >
                  <span className="nav-text">{p.title}</span>
                </a>
              ))}
            </div>
          ))}
        </nav>

        <article className="guide-body">
          {page.nodes.map((n, i) => (
            <NodeView key={i} node={n} t={t} />
          ))}

          <div className="guide-pager">
            {prev ? (
              <a className="btn" href={href({ view: 'guide', chapter: chapter.slug, page: prev.slug })}>
                <Icon name="back" />
                <span>{prev.title}</span>
              </a>
            ) : (
              <span />
            )}
            {next ? (
              <a className="btn" href={href({ view: 'guide', chapter: chapter.slug, page: next.slug })}>
                <span>{next.title}</span>
              </a>
            ) : null}
          </div>
        </article>

        {/* 页内目录。只在有两节以上时出现：一条目录的目录是噪声。 */}
        {page.toc.length > 1 ? (
          <aside className="guide-toc" aria-label={t('guide.onThisPage')}>
            <div className="nav-label">{t('guide.onThisPage')}</div>
            {page.toc.map((s) => (
              <a key={s.id} href={`#${s.id}`} data-level={s.level}>
                {s.text}
              </a>
            ))}
          </aside>
        ) : null}
      </div>
    </>
  )
}

function GuideHome(props: { t: T; chapters: GuideChapter[] }) {
  const { t, chapters } = props
  return (
    <>
      <div className="main-head">
        <div>
          <h1>{t('guide.title')}</h1>
          <p>{t('guide.subtitle')}</p>
        </div>
      </div>

      <div className="spread">
        {chapters.map((c) => (
          <Card key={c.slug} title={c.title}>
            <p className="dim">
              <Spans spans={c.intro} />
            </p>
            <div className="col guide-home-pages">
              {c.pages.map((p) => (
                <a key={p.slug} href={href({ view: 'guide', chapter: c.slug, page: p.slug })}>
                  {p.title}
                </a>
              ))}
            </div>
          </Card>
        ))}
      </div>
    </>
  )
}

function NodeView(props: { node: GuideNode; t: T }) {
  const { node, t } = props
  switch (node.k) {
    case 'h2':
      // 锚点挂在标题自身的 id 上，于是 #sec-3 这样的页内链接直接可用。
      return <h2 id={node.id}>{node.text}</h2>
    case 'h3':
      return <h3 id={node.id}>{node.text}</h3>
    case 'p':
      return (
        <p>
          <Spans spans={node.spans} />
        </p>
      )
    case 'quote':
      return (
        <blockquote>
          <Spans spans={node.spans} />
        </blockquote>
      )
    case 'ul':
      return (
        <ul>
          {node.items.map((it, i) => (
            <li key={i}>
              <Spans spans={it} />
            </li>
          ))}
        </ul>
      )
    case 'ol':
      return (
        <ol>
          {node.items.map((it, i) => (
            <li key={i}>
              <Spans spans={it} />
            </li>
          ))}
        </ol>
      )
    case 'code':
      return (
        <div className="guide-code">
          <div className="guide-code-head">
            {/* 片段来自哪个文件要写出来：照着教程改不通的时候，下一步是去
                看那个真实示例，而不是再读一遍同一段。 */}
            <span className="mono faint">{node.file ?? node.lang}</span>
            <CopyButton value={node.text} t={t} />
          </div>
          <pre>
            <code>{node.text}</code>
          </pre>
        </div>
      )
    case 'table':
      return (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                {node.head.map((h, i) => (
                  <th key={i}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {node.rows.map((row, i) => (
                <tr key={i}>
                  {row.map((cell, j) => (
                    <td key={j}>
                      <Spans spans={cell} />
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )
    case 'error':
      // 解析或 include 出错时给一条显眼的、带路径的提示，而不是少渲染一段。
      // 门禁会在合并前拦下这类错误（TST-142），所以它只出现在本地改坏的那
      // 一刻——那时要的正是"哪一个锚点没找到"。
      return (
        <div className="banner" data-tone="danger" role="alert">
          <div className="banner-body">
            <strong>{t('guide.badSource')}</strong>
            <span className="mono">{node.text}</span>
          </div>
        </div>
      )
  }
}

function Spans(props: { spans: GuideSpan[] }) {
  return (
    <>
      {props.spans.map((s, i) => {
        if (s.href !== undefined) {
          return (
            <a key={i} href={s.href}>
              {s.t}
            </a>
          )
        }
        if (s.s === 'code') return <code key={i}>{s.t}</code>
        if (s.s === 'strong') return <strong key={i}>{s.t}</strong>
        return <span key={i}>{s.t}</span>
      })}
    </>
  )
}
