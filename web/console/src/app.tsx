/* 控制台外壳：令牌闸、标题栏、分组导航、主题与语言。 */
import { useCallback, useEffect, useState } from 'react'
import { api, clearToken, loadToken, saveToken } from './api'
import { detectLang, makeT, saveLang, type Lang, type T } from './i18n'
import { Icon, Logo, type IconName } from './icons'
import { href, parseRoute, type Route } from './router'
import { Badge, Button, Card, Field } from './ui'
import { GuideView } from './views/guide'
import { OverviewView } from './views/overview'
import { ThingsView } from './views/things'
import { ThingDetailView } from './views/thing-detail'
import { ShadowView } from './views/shadow'
import { CertificatesView } from './views/certificates'
import { ConnectionsView } from './views/connections'
import { PoliciesView } from './views/policies'
import { DebuggerView } from './views/debugger'
import { EventsView } from './views/events'
import { MqttView } from './views/mqtt'
import { AccessView } from './views/access'

type Theme = 'system' | 'light' | 'dark'

const THEME_KEY = 'oiot.theme'

function loadTheme(): Theme {
  try {
    const v = localStorage.getItem(THEME_KEY)
    if (v === 'light' || v === 'dark' || v === 'system') return v
  } catch {
    /* 隐私模式下访问本身会抛，不能让它拖垮渲染 */
  }
  return 'system'
}

export function App() {
  const [token, setToken] = useState(loadToken)
  const [lang, setLang] = useState<Lang>(detectLang)
  const [theme, setTheme] = useState<Theme>(loadTheme)
  const [route, setRoute] = useState<Route>(() => parseRoute(window.location.hash))
  const [role, setRole] = useState('')
  // 当前登录名：访问控制页要标出哪一条是你自己
  const [self, setSelf] = useState('')
  const t = makeT(lang)

  useEffect(() => {
    const onHash = () => setRoute(parseRoute(window.location.hash))
    window.addEventListener('hashchange', onHash)
    return () => window.removeEventListener('hashchange', onHash)
  }, [])

  useEffect(() => {
    // system 时移除标记，让 prefers-color-scheme 生效；
    // 显式选择时打标记，使它压过系统设置
    if (theme === 'system') {
      document.documentElement.removeAttribute('data-theme')
    } else {
      document.documentElement.setAttribute('data-theme', theme)
    }
    try {
      localStorage.setItem(THEME_KEY, theme)
    } catch {
      /* 存不下就只在本次会话生效 */
    }
  }, [theme])

  useEffect(() => {
    document.documentElement.lang = lang === 'zh' ? 'zh-CN' : 'en'
  }, [lang])

  useEffect(() => {
    if (!token) {
      setRole('')
      setSelf('')
      return
    }
    let live = true
    api.session(token).then((s) => {
      if (!live) return
      setRole(s.role)
      setSelf(s.name)
    }).catch(() => {})
    return () => {
      live = false
    }
  }, [token])

  // 退出就是本地忘掉令牌。
  //
  // 令牌登录没有服务端会话可销毁——此前那次 DELETE /session 销的是
  // OIDC 签发的 cookie 会话，而那条路径已经整条拿掉了（AUTH-080）。
  const signOut = useCallback(() => {
    clearToken()
    setToken('')
  }, [])

  const onLang = useCallback((l: Lang) => {
    setLang(l)
    saveLang(l)
  }, [])

  if (!token) {
    return (
      <Gate
        t={t}
        lang={lang}
        theme={theme}
        onLang={onLang}
        onTheme={setTheme}
        onToken={(tk) => { saveToken(tk); setToken(tk) }}
      />
    )
  }

  return (
    <div className="shell">
      <Topbar
        t={t}
        lang={lang}
        theme={theme}
        token={token}
        role={role}
        route={route}
        onLang={onLang}
        onTheme={setTheme}
        onSignOut={signOut}
      />
      <Sidebar t={t} route={route} token={token} role={role} />
      <main className="main">
        <Views
          route={route}
          token={token}
          role={role}
          self={self}
          t={t}
          lang={lang}
          onUnauthorized={signOut}
        />
      </main>
    </div>
  )
}

function Views(props: {
  route: Route
  token: string
  role: string
  self: string
  t: T
  lang: Lang
  onUnauthorized: () => void
}) {
  const { route, token, t, lang } = props
  switch (route.view) {
    case 'guide':
      return (
        <GuideView
          t={t}
          lang={lang}
          {...(route.chapter ? { chapter: route.chapter } : {})}
          {...(route.page ? { page: route.page } : {})}
        />
      )
    case 'things':
      return <ThingsView token={token} t={t} lang={lang} onUnauthorized={props.onUnauthorized} />
    case 'thing':
      return (
        <ThingDetailView
          token={token}
          name={route.name}
          t={t}
          lang={lang}
          onUnauthorized={props.onUnauthorized}
        />
      )
    case 'shadow':
      return (
        <ShadowView
          token={token}
          name={route.name}
          t={t}
          onUnauthorized={props.onUnauthorized}
        />
      )
    case 'certificates':
      return (
        <CertificatesView token={token} t={t} lang={lang} onUnauthorized={props.onUnauthorized} />
      )
    case 'connections':
      return (
        <ConnectionsView token={token} t={t} lang={lang} onUnauthorized={props.onUnauthorized} />
      )
    case 'policies':
      return <PoliciesView token={token} t={t} onUnauthorized={props.onUnauthorized} />
    case 'debugger':
      return (
        <DebuggerView
          token={token}
          t={t}
          initialPolicy={route.policy}
          onUnauthorized={props.onUnauthorized}
        />
      )
    case 'events':
      return <EventsView token={token} t={t} />
    case 'mqtt':
      return <MqttView token={token} t={t} />
    case 'access':
      return (
        <AccessView
          token={token}
          t={t}
          lang={lang}
          self={props.self}
          onUnauthorized={props.onUnauthorized}
        />
      )
    default:
      return <OverviewView token={token} t={t} onUnauthorized={props.onUnauthorized} />
  }
}

type NavItem = { view: Route['view']; key: string; icon: IconName; adminOnly?: boolean }

/*
 * 导航分十项分四组。
 *
 * 平铺的问题不是"长"，是相邻项读起来一样：「策略」与「策略调试器」
 * 挨在一起，而一个是写、一个是试；「MQTT 测试」与「主题抓包」都是出事
 * 才会点开的工具，却混在日常页面里。
 *
 * 分组的判据是"你在做什么"，不是对象类型：
 *
 *   - 资源：注册进来、会落盘、重启还在的东西（设备、证书）
 *   - 授权：谁能做什么（策略、试一次求值、控制台自己的角色）
 *   - 运行时：此刻的状态，重启就没了（连接、事件流）
 *   - 诊断：出了事才会打开的工具，两者都要开一条 WebSocket
 *
 * 「连接」归运行时而不是资源，正是因为这条分界：它随进程消失，而设备
 * 与证书不会。把它放进资源会让人以为连接也是登记出来的。
 *
 * 总览不进任何组：它是落地页，加个组标题等于给一个单项配一行废话。
 *
 * 教程**不在这里**：它在标题栏（UI-076 修订，见 Topbar）。侧栏列的是
 * 这套系统管理的对象，教程不是其中之一；给它单开一个「上手」组，是为了
 * 一个孤项配一行组标题，而那一行在侧栏里只说明"它和下面那些不是一类"。
 */
const NAV_GROUPS: { key: string | null; items: NavItem[] }[] = [
  { key: null, items: [{ view: 'overview', key: 'nav.overview', icon: 'overview' }] },
  {
    key: 'navgroup.resources',
    items: [
      { view: 'things', key: 'nav.things', icon: 'things' },
      { view: 'certificates', key: 'nav.certificates', icon: 'certificates' },
    ],
  },
  {
    key: 'navgroup.authz',
    items: [
      { view: 'policies', key: 'nav.policies', icon: 'policies' },
      { view: 'debugger', key: 'nav.debugger', icon: 'debugger' },
      { view: 'access', key: 'nav.access', icon: 'access', adminOnly: true },
    ],
  },
  {
    key: 'navgroup.runtime',
    items: [
      { view: 'connections', key: 'nav.connections', icon: 'connections' },
      { view: 'events', key: 'nav.events', icon: 'events' },
    ],
  },
  {
    key: 'navgroup.diagnostics',
    items: [
      // 主题抓包并进了测试客户端的「旁观」模式（UI-070），不再单列一页
      { view: 'mqtt', key: 'nav.mqtt', icon: 'mqtt' },
    ],
  },
]

/**
 * Topbar 是全局标题栏。
 *
 * 它不重复当前页标题——那个 h1 在各视图自己的 main-head 里，一页两个
 * 同名标题会让读屏软件念两遍，也让"这页叫什么"有两个来源。标题栏承担
 * 的是另外三件事：身份（这是哪套系统、我是什么角色）、节点状态、
 * 以及全局动作（语言、主题、教程、退出）。
 *
 * 退出放右上角而不是侧栏底部：侧栏在窄屏下会变成横向滚动条，底部那组
 * 控件会被滚出可视区——"怎么退出登录"于是要靠横向拖动才能找到。
 *
 * 使用教程也在这一组（UI-076 修订）。它和语言、主题、退出是同一类东西：
 * 与当前页无关、任何时候都能点、跟着外壳走——而侧栏列的是这套系统管理
 * 的对象（设备、证书、策略、连接），教程不是其中之一。放标题栏还顺带
 * 解掉窄屏那条：侧栏在窄屏下会横向滚动，而第一次打开控制台的人正是最
 * 不会去横向拖动找入口的那批人。
 */
function Topbar(props: {
  t: T
  lang: Lang
  theme: Theme
  token: string
  role: string
  route: Route
  onLang: (l: Lang) => void
  onTheme: (th: Theme) => void
  onSignOut: () => void
}) {
  const { t } = props
  const [ready, setReady] = useState<boolean | null>(null)

  // 节点就绪每 10 秒看一次。失败按"未就绪"处理：拿不到答案和拿到
  // 否定的答案，对运维的意义是一样的——都得去看服务端。
  useEffect(() => {
    let live = true
    const tick = () => {
      api
        .node(props.token)
        .then((n) => {
          if (live) setReady(n.ready)
        })
        .catch(() => {
          if (live) setReady(false)
        })
    }
    tick()
    const id = window.setInterval(tick, 10_000)
    return () => {
      live = false
      window.clearInterval(id)
    }
  }, [props.token])

  return (
    <header className="topbar">
      <a className="topbar-brand" href={href({ view: 'overview' })}>
        <Logo />
        <span className="topbar-name">open-iot-core</span>
      </a>

      {/* 就绪状态带文字，不只靠圆点的颜色（UI-061）。
          只有颜色的话，色觉差异的用户与黑白截图里都读不出来，
          而这正是出事时第一个要看的信号。 */}
      {ready === null ? null : (
        <Badge tone={ready ? 'ok' : 'danger'}>
          {ready ? t('topbar.ready') : t('topbar.notReady')}
        </Badge>
      )}

      <div className="topbar-right">
        {/* 角色一直显示：reader 与 operator 会看到按下去才 403 的按钮，
            先告诉他们自己是什么角色，比事后解释一个 403 便宜。 */}
        {props.role ? (
          <Badge tone="muted" title={t('topbar.roleTip')}>
            {t('topbar.role')} {props.role}
          </Badge>
        ) : null}
        {/* 教程入口（UI-076 修订）。
            用 <a> 而不是 Button：它是导航而不是动作，新标签页打开、
            复制链接这些行为是浏览器在 <a> 上免费给的，换成 button
            就得自己接一遍——而把某一页贴给别人正是教程的主要用法。
            aria-current 要给：没有它，当前就在教程页时这个入口看起来
            和别处一样，读屏软件也念不出"你已经在这儿了"。 */}
        <a
          className="btn topbar-guide"
          data-size="sm"
          href={href({ view: 'guide' })}
          aria-current={props.route.view === 'guide' ? 'page' : undefined}
          aria-label={t('nav.guide')}
          title={t('nav.guide')}
        >
          <Icon name="guide" />
          <span className="btn-text">{t('nav.guide')}</span>
        </a>
        <ShellControls
          t={t}
          lang={props.lang}
          theme={props.theme}
          onLang={props.onLang}
          onTheme={props.onTheme}
        />
        {/* 带 aria-label：窄屏下 .btn-text 被 display:none 藏起来，
            而 display:none 的文字不在无障碍树里——只剩图标的按钮就没有
            名字了。靠可见文字当名字的按钮，一旦文字会被藏就必须补。 */}
        <Button size="sm" onClick={props.onSignOut} aria-label={t('common.signout')}>
          <Icon name="signout" />
          <span className="btn-text">{t('common.signout')}</span>
        </Button>
      </div>
    </header>
  )
}

/**
 * ShellControls 是语言与主题这两个开关。
 *
 * 抽成组件只为一件事：登录页也要有它（UI-062）。此前语言开关只在登录
 * 之后的侧栏里，而登录页是用户见到的**第一屏**——浏览器语言判错时，
 * 他要先用一种读不懂的语言完成登录，才能找到切换的地方。
 */
function ShellControls(props: {
  t: T
  lang: Lang
  theme: Theme
  onLang: (l: Lang) => void
  onTheme: (th: Theme) => void
}) {
  const { t } = props
  const nextTheme: Theme =
    props.theme === 'system' ? 'light' : props.theme === 'light' ? 'dark' : 'system'
  const themeIcon = props.theme === 'system' ? 'system' : props.theme === 'light' ? 'sun' : 'moon'

  return (
    <>
      <Button
        size="sm"
        onClick={() => props.onLang(props.lang === 'zh' ? 'en' : 'zh')}
        // aria-label 要说出当前值。三态循环按钮只给一个图标时，读屏
        // 软件念到的是"按钮"，而看得见的人也无法确定"◐"是哪一态。
        aria-label={`${t('common.lang')}: ${t('common.langName')}`}
        title={t('common.lang')}
      >
        <Icon name="lang" />
        <span className="btn-text">{props.lang === 'zh' ? 'EN' : '中文'}</span>
      </Button>
      <Button
        size="sm"
        onClick={() => props.onTheme(nextTheme)}
        aria-label={`${t('common.theme')}: ${t(`theme.${props.theme}`)}`}
        title={`${t('common.theme')}: ${t(`theme.${props.theme}`)}`}
      >
        <Icon name={themeIcon} />
      </Button>
    </>
  )
}

function Sidebar(props: { t: T; route: Route; token: string; role: string }) {
  const { t } = props
  const [conns, setConns] = useState<number | null>(null)

  // 在线连接数每 10 秒刷一次：它是运维最常看的一个数字，放在导航上
  // 免去来回切页。失败时静默——侧栏不该因为一次轮询失败而变成错误页。
  useEffect(() => {
    let live = true
    const tick = () => {
      api
        .node(props.token)
        .then((n) => {
          if (live) setConns(n.connections)
        })
        .catch(() => {})
    }
    tick()
    const id = window.setInterval(tick, 10_000)
    return () => {
      live = false
      window.clearInterval(id)
    }
  }, [props.token])

  return (
    <nav className="sidebar" aria-label={t('nav.label')}>
      {NAV_GROUPS.map((group) => {
        // 角色不够的项整条不显示；一组里的项全被滤掉时，组标题也不能留，
        // 否则 reader 会看到一个空的"诊断"标题，像是页面坏了。
        const items = group.items.filter((n) => !n.adminOnly || props.role === 'admin')
        if (items.length === 0) return null
        return (
          <div className="nav-group" key={group.key ?? 'root'}>
            {group.key ? <div className="nav-label">{t(group.key)}</div> : null}
            {items.map((n) => {
              const active =
                props.route.view === n.view ||
                (n.view === 'things' &&
                  (props.route.view === 'thing' || props.route.view === 'shadow'))
              return (
                <a
                  key={n.view}
                  className="nav-item"
                  href={href({ view: n.view } as Route)}
                  aria-current={active ? 'page' : undefined}
                >
                  <Icon name={n.icon} />
                  <span className="nav-text">{t(n.key)}</span>
                  {n.view === 'connections' && conns !== null ? (
                    <span className="nav-count" aria-label={t('nav.connCount', String(conns))}>
                      {conns}
                    </span>
                  ) : null}
                </a>
              )
            })}
          </div>
        )
      })}
    </nav>
  )
}

/**
 * Gate 收集管理面令牌。
 *
 * 浏览器导航无法携带 Authorization 头，所以静态外壳不鉴权——
 * 它不含任何数据，只是一段通用 JS。全部数据都要令牌。
 * 这里先用它试一次 /node：立刻给出"令牌对不对"的反馈，
 * 而不是让用户进去之后每个页面都报错。
 *
 * 用真 <form> 而不是给输入框挂 onKeyDown：回车提交、密码管理器识别、
 * 手机输入法上的"前往"键都是浏览器在表单上免费给的行为，自己接一遍
 * 只会漏掉其中两个。
 */
function Gate(props: {
  t: T
  lang: Lang
  theme: Theme
  onLang: (l: Lang) => void
  onTheme: (th: Theme) => void
  onToken: (t: string) => void
}) {
  const { t } = props
  const [value, setValue] = useState('')
  const [err, setErr] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  const submit = () => {
    const tk = value.trim()
    if (!tk) return
    setBusy(true)
    setErr(null)
    api
      .node(tk)
      .then(() => props.onToken(tk))
      .catch(() => setErr(t('gate.bad')))
      .finally(() => setBusy(false))
  }

  return (
    <div className="gate">
      {/* 语言与主题放在登录页右上角：这是第一屏，浏览器语言判错时
          用户得先能把界面切回自己读得懂的语言（UI-062）。 */}
      <div className="gate-controls">
        <ShellControls
          t={t}
          lang={props.lang}
          theme={props.theme}
          onLang={props.onLang}
          onTheme={props.onTheme}
        />
      </div>

      <div className="gate-card">
        <div className="gate-head">
          <Logo size={34} />
          <div>
            <div className="gate-brand">open-iot-core</div>
            <div className="gate-tagline">{t('gate.tagline')}</div>
          </div>
        </div>

        <Card>
          <div className="col">
            <h1>{t('gate.title')}</h1>
            <p className="dim gate-desc">{t('gate.desc')}</p>

            <form
              className="col"
              onSubmit={(e) => {
                e.preventDefault()
                submit()
              }}
            >
              <Field
                label={t('gate.placeholder')}
                hint={t('gate.hint')}
                error={err ?? undefined}
              >
                <input
                  type="password"
                  name="admin-token"
                  autoComplete="current-password"
                  autoFocus
                  value={value}
                  onChange={(e) => setValue(e.target.value)}
                />
              </Field>
              <Button type="submit" variant="primary" disabled={busy || !value.trim()}>
                {busy ? t('common.loading') : t('gate.submit')}
              </Button>
            </form>
          </div>
        </Card>

        <p className="gate-foot faint">{t('gate.stored')}</p>
      </div>
    </div>
  )
}
