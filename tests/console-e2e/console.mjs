/*
 * 在真实 Chromium 里把控制台走一遍。
 *
 * 断言的取向：只断言「在浏览器里才会错」的东西。接口形状由 Go 侧的
 * TestConsoleCriticalFlows 钉住，这里不重复；这里管的是渲染、窗口切片、
 * SSE 的事件分发、WebSocket 往返、语言切换，以及——最重要的——
 * 整个过程里浏览器控制台不许出现一条错误。React 的渲染错误与未捕获的
 * Promise 拒绝在 Go 测试里完全不可见，而它们会让页面白屏。
 */
import { chromium } from 'playwright'

const BASE = process.env.BASE
const TOKEN = process.env.TOKEN
const SHOTS = process.env.SHOTS

const results = []
let browser

function record(name, ok, detail) {
  results.push({ name, ok, detail })
  const tag = ok ? '\x1b[1;32mPASS\x1b[0m' : '\x1b[1;31mFAIL\x1b[0m'
  console.log(`${tag}  ${name.padEnd(42)} ${detail ?? ''}`)
}

async function check(name, fn) {
  try {
    const detail = await fn()
    record(name, true, detail)
  } catch (err) {
    record(name, false, err.message)
  }
}

function must(cond, msg) {
  if (!cond) throw new Error(msg)
}

// pageErrors 收集整轮运行里浏览器自己报的错。
const pageErrors = []

async function shot(page, name) {
  if (SHOTS) await page.screenshot({ path: `${SHOTS}/${name}.png`, fullPage: true })
}

async function main() {
  browser = await chromium.launch()
  // locale 固定成 zh-CN：detectLang 读 navigator.language，不固定的话
  // 同一套断言在不同机器上面对的是不同语言的界面（第一次跑就撞上了：
  // Chromium 默认 en-US，于是所有中文断言都在找不存在的文案）。
  // 英文那一半在最后一条用切换按钮验。
  const ctx = await browser.newContext({
    viewport: { width: 1440, height: 900 },
    locale: 'zh-CN',
  })
  const page = await ctx.newPage()

  page.on('console', (msg) => {
    if (msg.type() !== 'error') return
    const text = msg.text()
    // 令牌闸在进入前会故意拿一个坏令牌试一次，那两条 401 是预期的
    if (/401|Failed to load resource/.test(text)) return
    pageErrors.push(`console.error: ${text}`)
  })
  page.on('pageerror', (err) => pageErrors.push(`pageerror: ${err.message}`))

  await check('控制台在 / 上渲染出登录闸', async () => {
    await page.goto(BASE, { waitUntil: 'networkidle' })
    await page.waitForSelector('.gate', { timeout: 10000 })
    const title = await page.textContent('.gate h1')
    must(title && title.includes('令牌'), `登录闸标题 = ${title}`)
    must(await page.isVisible('.gate-controls'), '登录页没有语言/主题控件')
    await shot(page, '01-gate')
    return title.trim()
  })

  // 登录页的语言开关（UI-062）。它曾经只在登录之后才有：浏览器语言判错
  // 的用户得先用一种读不懂的语言完成登录，才能找到切换的地方。
  await check('登录页就能切换语言', async () => {
    await page.click('.gate-controls button:has-text("EN")')
    await page.waitForFunction(() => document.documentElement.lang === 'en', { timeout: 5000 })
    const en = await page.textContent('.gate h1')
    must(/token/i.test(en), `切英文后登录闸标题 = ${en}`)
    await shot(page, '01b-gate-en')
    // 切回中文，后面的断言都是中文文案
    await page.click('.gate-controls button:has-text("中文")')
    await page.waitForFunction(() => document.documentElement.lang === 'zh-CN', { timeout: 5000 })
    const zh = await page.textContent('.gate h1')
    must(zh.includes('令牌'), `切回中文后 = ${zh}`)
    return `英文为 ${en.trim()}，可切回中文`
  })

  await check('错误令牌被拒且给出提示', async () => {
    await page.fill('.gate input', 'not-a-real-token-but-long-enough-32xx')
    await page.click('.gate button[type="submit"]')
    const err = await page.waitForSelector('text=令牌无效', { timeout: 10000 })
    must(await err.isVisible(), '错误提示不可见')
    return '显示「令牌无效或管理面未启用」'
  })

  await check('正确令牌进入控制台', async () => {
    await page.fill('.gate input', TOKEN)
    await page.click('.gate button[type="submit"]')
    await page.waitForSelector('.shell', { timeout: 10000 })
    const nav = await page.textContent('.shell')
    must(nav.includes('总览'), '侧栏没有渲染出导航')
    await shot(page, '02-overview')
    return '侧栏与主区已渲染'
  })

  await check('标题栏给出身份、角色与节点状态', async () => {
    await page.waitForSelector('.topbar', { timeout: 10000 })
    const bar = await page.textContent('.topbar')
    must(bar.includes('open-iot-core'), '标题栏没有品牌名')
    // 角色是异步取回来的，等它出现
    await page.waitForFunction(
      () => /admin/.test(document.querySelector('.topbar')?.textContent ?? ''),
      { timeout: 10000 })
    const withRole = await page.textContent('.topbar')
    must(/角色/.test(withRole), '标题栏没有显示角色')
    // 就绪状态必须有文字，不能只是一个有颜色的点（UI-061）
    must(/就绪/.test(withRole), `标题栏没有就绪文字：${withRole}`)
    // 退出在右侧那一组里
    const signout = await page.getAttribute('.topbar-right button:last-of-type', 'aria-label')
    must(signout === '退出', `右上角最后一个按钮的无障碍名 = ${signout}`)
    return '品牌、角色 admin、就绪文字、右上角退出都在'
  })

  await check('侧栏按组呈现且每项带图标', async () => {
    const labels = await page.$$eval('.sidebar .nav-label', (ns) => ns.map((n) => n.textContent))
    // 四组按生命周期分。断言的是集合而不是个数：个数会随加页漂移，
    // 而那一位数字对不对不说明任何事。
    for (const want of ['资源', '授权', '运行时', '诊断']) {
      must(labels.some((l) => l.includes(want)), `缺少「${want}」分组：${labels}`)
    }
    // 教程不在侧栏（UI-076 修订）：它在标题栏那一组全局动作里。
    const navText = await page.textContent('.sidebar')
    must(!/使用教程/.test(navText ?? ''), `侧栏里还有教程入口：${navText}`)
    // 每个导航项都要有一个图标。少一个的表现是那一行文字左边空一块，
    // 整列对不齐——而这在 Go 侧完全看不见。
    const counts = await page.$$eval('.sidebar .nav-item', (ns) =>
      ns.map((n) => n.querySelectorAll('svg.icon').length))
    // 主题抓包并进了测试客户端（UI-070）；教程挪去了标题栏（UI-076 修订）
    must(counts.length >= 9, `导航项数 = ${counts.length}`)
    must(counts.every((c) => c === 1), `有导航项没有图标：${counts}`)
    await shot(page, '02b-nav')
    return `${counts.length} 项、${labels.length} 组，图标齐备`
  })

  await check('总览页显示节点与计数', async () => {
    await page.goto(`${BASE}/#/`, { waitUntil: 'networkidle' })
    await page.waitForSelector('h1', { timeout: 10000 })
    const body = await page.textContent('.main')
    must(body.includes('运行概览'), '标题不对')
    must(/节点/.test(body), '没有节点区块')
    must(/在线连接/.test(body), '没有连接计数')
    return '标题、节点与计数都在'
  })

  await check('设备页按窗口渲染（UI-059）', async () => {
    await page.goto(`${BASE}/#/things`, { waitUntil: 'networkidle' })
    await page.waitForSelector('table tbody tr', { timeout: 10000 })
    // 服务端一页 50 条。窗口渲染意味着 DOM 里的数据行远少于 50，
    // 其余高度由两个 aria-hidden 的撑高行顶起来。
    const rows = await page.$$eval('table tbody tr', (trs) =>
      trs.filter((tr) => tr.getAttribute('aria-hidden') !== 'true').length)
    const pads = await page.$$eval('table tbody tr[aria-hidden="true"]', (trs) => trs.length)
    must(rows > 0, 'DOM 里没有数据行')
    must(rows < 50, `DOM 里有 ${rows} 行数据，窗口渲染没有生效（服务端一页 50 条）`)
    must(pads > 0, '没有撑高行，滚动条高度会不对')
    const first = await page.textContent('table tbody tr:not([aria-hidden]) td')
    await shot(page, '03-things')
    return `渲染 ${rows} 行 + ${pads} 个撑高行，首行 ${first.trim()}`
  })

  await check('设备页前缀筛选把列表收窄', async () => {
    const input = await page.waitForSelector('input', { timeout: 5000 })
    await input.fill('lamp-01')
    await page.waitForTimeout(600)
    const names = await page.$$eval('table tbody tr:not([aria-hidden]) td:first-child',
      (tds) => tds.map((td) => td.textContent.trim()))
    must(names.length > 0, '筛选之后一行都没有')
    must(names.every((n) => n.startsWith('lamp-01')), `筛选结果含无关项: ${names.join(',')}`)
    await input.fill('')
    await page.waitForTimeout(400)
    return `前缀 lamp-01 命中 ${names.length} 台`
  })

  await check('点进设备详情看到影子', async () => {
    await page.goto(`${BASE}/#/things/lamp-001`, { waitUntil: 'networkidle' })
    await page.waitForSelector('h1', { timeout: 10000 })
    const body = await page.textContent('.main')
    must(body.includes('lamp-001'), '详情页没有设备名')
    must(/影子/.test(body), '没有影子区块')
    must(/interval/.test(body), '影子正文没渲染出来')
    await shot(page, '04-thing-detail')
    return '设备名、影子区块与 desired 正文都在'
  })

  // 影子页（UI-069）。它解决两件事：影子功能此前要"点进设备再往下翻"
  // 才看得见；命名影子在界面上完全没有入口。
  await check('设备列表的操作列能直达影子页', async () => {
    await page.goto(`${BASE}/#/things`, { waitUntil: 'networkidle' })
    await page.waitForSelector('table tbody tr', { timeout: 10000 })
    await page.click('table tbody tr:has-text("lamp-001") a:has-text("影子")')
    await page.waitForFunction(() => /#\/shadow\//.test(location.hash), { timeout: 5000 })
    await page.waitForSelector('h1', { timeout: 10000 })
    const body = await page.textContent('.main')
    must(/设备影子/.test(body), `影子页标题不对：${body.slice(0, 60)}`)
    must(/lamp-001/.test(body), '影子页没显示设备名')
    must(/interval/.test(body), '经典影子的 desired 没渲染')
    await shot(page, '04b-shadow')
    return '从列表一步到影子页'
  })

  await check('影子页的使用说明抽屉按当前设备给出主题', async () => {
    must(!(await page.isVisible('[role="dialog"]')), '没点就弹出了抽屉')
    await page.click('.main-actions button:has-text("使用说明")')
    await page.waitForSelector('.drawer', { timeout: 5000 })
    const d = await page.textContent('.drawer')
    // 主题要按这台设备渲染出来，不是 <thingName> 占位符——照着占位符
    // 敲的人总要再替换一次，而替换错了只是静默不匹配
    must(d.includes('$aws/things/lamp-001/shadow/update/delta'),
      `抽屉里没有按设备渲染的主题：${d.slice(0, 120)}`)
    must(!/thingName|<设备名>/.test(d), '抽屉里还留着占位符')
    must(/clientToken/.test(d), '没有报文样例')
    must(/409/.test(d), '没有说明版本冲突')
    await shot(page, '04d-shadow-help')
    // Esc 关掉
    await page.keyboard.press('Escape')
    await page.waitForSelector('.drawer', { state: 'detached', timeout: 5000 })
    return '主题按 lamp-001 渲染，Esc 可关'
  })

  await check('影子页能切到命名影子', async () => {
    // 有命名影子时才出选择器：多数设备只有经典影子
    const opts = await page.$$eval('.main select option', (os) =>
      os.map((o) => o.value))
    must(opts.includes('firmware'), `选择器里没有命名影子：${opts}`)
    await page.selectOption('.main select', 'firmware')
    await page.waitForFunction(
      () => /channel/.test(document.querySelector('.main')?.textContent ?? ''),
      { timeout: 10000 })
    const body = await page.textContent('.main')
    must(/stable/.test(body), '命名影子的正文没渲染')
    must(!/interval/.test(body), '切过去之后还留着经典影子的内容')
    // 说明里的主题也要跟着换：照着经典影子的主题去操作命名影子，
    // 同样是静默不匹配
    await page.click('.main-actions button:has-text("使用说明")')
    await page.waitForSelector('.drawer', { timeout: 5000 })
    const help = await page.textContent('.drawer')
    must(help.includes('$aws/things/lamp-001/shadow/name/firmware/update'),
      `命名影子的说明没换主题：${help.slice(0, 140)}`)
    await page.keyboard.press('Escape')
    await page.waitForSelector('.drawer', { state: 'detached', timeout: 5000 })
    await shot(page, '04c-shadow-named')
    return '切到 firmware 看到它自己的 desired，说明里的主题也跟着换'
  })

  await check('证书页列出已签发的证书', async () => {
    await page.goto(`${BASE}/#/certificates`, { waitUntil: 'networkidle' })
    await page.waitForSelector('table tbody tr', { timeout: 10000 })
    const body = await page.textContent('.main')
    must(/lamp-00[12]/.test(body), '证书表里没有签出来的那两张')
    await shot(page, '05-certs')
    return '两张证书在表里'
  })

  // 签发对话框只剩一条路径（UI-083）。这一条是浏览器里才证得了的：
  // Go 侧看不见渲染，而上一版那个表单被整段重写过——模式切换与 CSR
  // 文本域一起去掉，漏一处的表现是白屏或者一个按不下去的"签发"。
  await check('签发对话框只给服务端生成密钥这一条路径（UI-083）', async () => {
    await page.goto(`${BASE}/#/certificates`, { waitUntil: 'networkidle' })
    await page.waitForSelector('table tbody tr', { timeout: 10000 })
    must(!(await page.isVisible('[role="dialog"]')), '没点就弹出了对话框')
    await page.click('.main-actions button:has-text("签发证书")')
    await page.waitForSelector('[role="dialog"]', { timeout: 5000 })
    const form = await page.textContent('[role="dialog"]')
    // 模式切换与 CSR 文本域都不该再出现
    must(!/用 CSR 签发/.test(form), '模式切换还在：CSR 那一档没去掉')
    must(!/CSR（PEM）/.test(form), `对话框里还有 CSR 字段：${form.slice(0, 160)}`)
    const areas = await page.$$eval('[role="dialog"] textarea', (els) => els.length)
    must(areas === 0, `对话框里还有 ${areas} 个文本域，CSR 输入没去掉`)
    // 不能按文案判"服务端生成密钥"那个按钮还在不在：它是说明文案
    // 「服务端生成密钥对…」的前缀，照文本搜必然命中。按钮要按元素数。
    const modeBtns = await page.$$eval('[role="dialog"] button',
      (els) => els.map((e) => e.textContent.trim())
        .filter((x) => x === '服务端生成密钥').length)
    must(modeBtns === 0, '模式切换的按钮还在')
    // 说明文案须预先给出非回环部署下的出路（UI-084）
    must(/openiotd cert issue/.test(form) && /sign-csr/.test(form),
      '说明里没写非回环部署下的两条出路')
    await shot(page, '05b-issue-form')

    // 真的签一张出来：这一步同时证明表单的必填判定没把按钮永久锁死
    await page.fill('[role="dialog"] input[placeholder="sensor-01"]', 'lamp-001')
    await page.selectOption('[role="dialog"] select', 'e2e-scoped')
    await page.click('[role="dialog"] button:has-text("签发")')
    await page.waitForFunction(
      () => /签发完成/.test(document.querySelector('[role="dialog"]')?.textContent ?? ''),
      { timeout: 15000 })
    const done = await page.textContent('[role="dialog"]')
    must(/BEGIN CERTIFICATE/.test(done), '结果面板里没有证书正文')
    must(/PRIVATE KEY/.test(done), '结果面板里没有私钥')
    // 取走之前，关闭按钮须明说私钥会丢（UI-052）
    must(/仍然关闭/.test(done), `关闭按钮没有警告文案：${done.slice(-160)}`)
    await shot(page, '05c-issue-done')
    await page.keyboard.press('Escape')
    await page.waitForSelector('[role="dialog"]', { state: 'detached', timeout: 5000 })
    return '只有一条路径，签出的证书与私钥都在，关闭按钮带警告'
  })

  await check('连接页有在线率曲线与空状态（UI-060）', async () => {
    await page.goto(`${BASE}/#/connections`, { waitUntil: 'networkidle' })
    await page.waitForSelector('h1', { timeout: 10000 })
    const body = await page.textContent('.main')
    must(body.includes('实时连接'), '标题不对')
    // 此刻没有设备连着，空状态必须出现（UI-030 的标准）
    must(/此刻没有在线连接/.test(body), '没有空状态')
    // 曲线是 svg role=img，取样点不足时显示提示文案，两者有一个就算有
    const svgs = await page.$$eval('svg[role="img"]', (els) => els.length)
    must(svgs > 0 || /再刷新一次就有曲线/.test(body), '既没有曲线也没有取样提示')
    await shot(page, '06-connections')
    return `空状态在，曲线元素 ${svgs} 个`
  })

  await check('策略页列出策略', async () => {
    await page.goto(`${BASE}/#/policies`, { waitUntil: 'networkidle' })
    await page.waitForSelector('h1', { timeout: 10000 })
    const body = await page.textContent('.main')
    must(body.includes('e2e-scoped'), '策略表里没有预置的那份')
    await shot(page, '07-policies')
    return 'e2e-scoped 在表里'
  })

  await check('策略页的使用说明列出动作与资源类型', async () => {
    await page.goto(`${BASE}/#/policies`, { waitUntil: 'networkidle' })
    await page.waitForSelector('h1', { timeout: 10000 })
    await page.click('.main-actions button:has-text("使用说明")')
    await page.waitForSelector('.drawer', { timeout: 5000 })
    const d = await page.textContent('.drawer')
    // 五个动作与各自的资源类型是这页的核心：订阅判 topicfilter、
    // 接收判 topic，混为一谈就是"订得上却收不到"
    for (const a of ['iot:Connect', 'iot:Publish', 'iot:Subscribe', 'iot:Receive', 'iot:RetainPublish']) {
      must(d.includes(a), `说明里缺动作 ${a}`)
    }
    must(/topicfilter/.test(d), '没有区分 topicfilter')
    must(/iot:Connection.Thing.ThingName/.test(d), '没有列出策略变量')
    must(/不接受通配/.test(d), '没有提资源类型不可通配这条坑')
    await shot(page, '07b-policy-help')
    await page.keyboard.press('Escape')
    await page.waitForSelector('.drawer', { state: 'detached', timeout: 5000 })
    return '五个动作、资源类型、变量与坑都在'
  })

  await check('策略调试器给出放行判定与变量求值', async () => {
    await page.goto(`${BASE}/#/debugger`, { waitUntil: 'networkidle' })
    await page.waitForSelector('h1', { timeout: 10000 })
    // 只按策略判定（UI-068）：没有主体类型可选，策略下拉默认选中第一条
    must(
      !(await page.isVisible('.main button:has-text("按设备")')),
      '调试器上还有「按设备」这种主体切换',
    )
    const picked = await page.inputValue('.main select')
    must(picked.length > 0, '策略下拉没有默认选中任何一条')
    // 设备名可选，填上让 Thing 类变量有值
    const inputs = await page.$$('.main input')
    await inputs[0].fill('lamp-001')
    // 资源栏：Publish 填主题名
    const selects = await page.$$('.main select')
    for (const sel of selects) {
      const opts = await sel.$$eval('option', (os) => os.map((o) => o.value))
      if (opts.includes('iot:Publish')) await sel.selectOption('iot:Publish')
    }
    const all = await page.$$('.main input')
    await all[all.length - 1].fill('$aws/things/lamp-001/telemetry')
    await page.click('.main button:has-text("判定")')
    await page.waitForTimeout(1200)
    const body = await page.textContent('.main')
    must(/放行|拒绝/.test(body), `点了判定但没有结果，页面：${body.replace(/\s+/g, ' ').slice(0, 400)}`)
    must(/放行/.test(body), `判定不是放行，页面：${body.replace(/\s+/g, ' ').slice(0, 400)}`)
    must(/变量求值|ThingName/.test(body), '没有变量求值轨迹')
    await shot(page, '08-debugger-allow')
    return '判定为放行，且带变量求值'
  })

  await check('策略调试器给出拒绝判定与「如何放行」', async () => {
    const all = await page.$$('.main input')
    await all[all.length - 1].fill('sensors/temp')
    await page.click('.main button:has-text("判定")')
    await page.waitForTimeout(800)
    const body = await page.textContent('.main')
    must(/拒绝/.test(body), `越权资源没有判成拒绝，页面：${body.replace(/\s+/g, ' ').slice(0, 400)}`)
    must(/如何放行|没有任何语句匹配/.test(body), '拒绝时没有给出建议')
    await shot(page, '09-debugger-deny')
    return '越权主题判为拒绝，并给出建议'
  })

  await check('事件流经 SSE 连上', async () => {
    await page.goto(`${BASE}/#/events`, { waitUntil: 'networkidle' })
    await page.waitForSelector('h1', { timeout: 10000 })
    await page.waitForFunction(
      () => /实时|已连接|事件流已连通/.test(document.querySelector('.main')?.textContent ?? ''),
      { timeout: 15000 })
    const body = await page.textContent('.main')
    must(/实时|事件流已连通/.test(body), 'SSE 没有连上')
    await shot(page, '10-events')
    return 'SSE 已连通'
  })

  await check('MQTT 测试客户端订阅并收到自己发布的消息', async () => {
    await page.goto(`${BASE}/#/mqtt`, { waitUntil: 'networkidle' })
    await page.waitForSelector('h1', { timeout: 10000 })
    await page.waitForFunction(
      () => /已连接/.test(document.querySelector('.main')?.textContent ?? ''),
      { timeout: 15000 })

    // 订阅 $aws/things/lamp-001/#，再往 $aws/things/lamp-001/telemetry 发一条
    const subInput = await page.waitForSelector('.main input')
    await subInput.fill('$aws/things/lamp-001/#')
    await page.click('button:has-text("订阅")')
    await page.waitForTimeout(500)

    const inputs = await page.$$('.main input')
    // 发布区的主题与负载是订阅框之后的两个输入框
    await inputs[1].fill('$aws/things/lamp-001/telemetry')
    const areas = await page.$$('.main textarea')
    const payload = `e2e-${Date.now()}`
    if (areas.length > 0) await areas[0].fill(payload)
    else await inputs[2].fill(payload)
    await page.click('button:has-text("发布")')

    await page.waitForFunction(
      (p) => (document.querySelector('.main')?.textContent ?? '').includes(p),
      payload, { timeout: 15000 })
    await shot(page, '11-mqtt')
    return `订阅后收到自己发布的 ${payload}`
  })

  await check('旁观模式看得到无人订阅的发布', async () => {
    // 这是旁观模式存在的唯一理由，也是订阅模式做不到的事：
    // 影子更新由 Admin API 触发，此刻没有任何客户端订阅 /update/accepted，
    // 订阅模式下不订就收不到，而旁观在 Route() 之前取样，照样看得见。
    await page.goto(`${BASE}/#/mqtt`, { waitUntil: 'networkidle' })
    await page.waitForSelector('h1', { timeout: 10000 })
    await page.click('.main-actions button:has-text("旁观")')
    await page.waitForFunction(
      () => /已连接/.test(document.querySelector('.main')?.textContent ?? ''),
      { timeout: 10000 })
    // 旁观模式不该有发布面板
    const body0 = await page.textContent('.main')
    must(/只看不发/.test(body0), '旁观模式仍然摆着发布面板')

    const f = await page.$('.main input')
    await f.fill('$aws/things/lamp-001/shadow/#')
    // 用 .spread 把范围限定在卡片里：页头的模式按钮也叫「旁观」，
    // 不限定的话点到的是它，过滤器根本没提交
    await page.click('.spread button:has-text("开始旁观")')
    await page.waitForFunction(
      () => /shadow/.test(document.querySelector('.mqtt-subs')?.textContent ?? ''),
      { timeout: 10000 })

    // 从页面里直接调 Admin API 改影子，触发一条没人订阅的发布
    await page.evaluate(async (tok) => {
      await fetch('/api/admin/v1/things/lamp-001/shadow', {
        method: 'PUT',
        headers: { Authorization: `Bearer ${tok}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ state: { desired: { watched: Date.now() } } }),
      })
    }, TOKEN)

    await page.waitForFunction(
      () => /update\/accepted|update\/documents/.test(
        document.querySelector('.main')?.textContent ?? ''),
      { timeout: 10000 })
    await shot(page, '12-watch')
    return '无人订阅的影子发布被旁观捕获'
  })

  await check('切回订阅模式后发布面板回来', async () => {
    await page.click('.main-actions button:has-text("订阅")')
    await page.waitForFunction(
      () => !/只看不发/.test(document.querySelector('.main')?.textContent ?? ''),
      { timeout: 5000 })
    const body = await page.textContent('.main')
    must(/发布/.test(body), '切回订阅后没有发布面板')
    // 切模式要清掉上一条套接字留下的过滤器
    const subs = await page.textContent('.mqtt-subs').catch(() => '')
    must(!/shadow/.test(subs), `切模式后还留着旁观的过滤器：${subs}`)
    return '发布面板回来，旁观的过滤器已清掉'
  })

  await check('访问控制页先给空状态，再从对话框建出用户', async () => {
    await page.goto(`${BASE}/#/access`, { waitUntil: 'networkidle' })
    await page.waitForSelector('h1', { timeout: 10000 })
    // 多数部署只有引导令牌，这一页默认是空的——空状态要说清这是正常的
    const empty = await page.textContent('.main')
    must(/还没有额外的用户/.test(empty), `空状态文案不对：${empty.slice(0, 80)}`)
    await shot(page, '13-access-empty')

    // 新建用户是对话框，不是常驻在页面上的表单
    must(!(await page.isVisible('[role="dialog"]')), '没点就弹出了对话框')
    // 两块各有自己的工具条（UI-075），所以要指名道姓是哪一块的按钮
    await page.locator('.card:has-text("控制台用户") button:has-text("新建用户")').first().click()
    await page.waitForSelector('[role="dialog"]', { timeout: 5000 })
    await page.fill('[role="dialog"] input', 'e2e-reader')
    await page.selectOption('[role="dialog"] select', 'reader')
    await shot(page, '13b-access-create')
    await page.click('[role="dialog"] button[type="submit"]')

    // 令牌只显示这一次：它必须出现在一个要人主动关掉的对话框里，
    // 而不是一条会被下一次操作顶掉的横幅
    await page.waitForSelector('[role="dialog"] pre', { timeout: 10000 })
    const shown = await page.textContent('[role="dialog"]')
    must(/只显示这一次/.test(shown), `没有提示令牌只出现一次：${shown.slice(0, 90)}`)
    const tok = (await page.textContent('[role="dialog"] pre')).trim()
    must(tok.length >= 24, `令牌看起来不对：${tok}`)
    await shot(page, '13c-access-token')
    await page.click('[role="dialog"] button:has-text("关闭")')
    await page.waitForSelector('[role="dialog"]', { state: 'detached', timeout: 5000 })

    // 关掉之后列表里要有这个用户，并标出哪一条是自己
    await page.waitForSelector('table tbody tr', { timeout: 10000 })
    const listed = await page.textContent('.main')
    must(/e2e-reader/.test(listed), '新建的用户不在列表里')
    must(/reader/.test(listed), '列表里没有角色')
    return `建出 e2e-reader，令牌 ${tok.slice(0, 6)}… 只显示一次`
  })

  await check('删除用户要先确认', async () => {
    await page.click('table tbody tr:has-text("e2e-reader") button:has-text("删除")')
    await page.waitForSelector('[role="dialog"]', { timeout: 5000 })
    const ask = await page.textContent('[role="dialog"]')
    must(/立刻失效/.test(ask), `确认框没说清后果：${ask.slice(0, 80)}`)
    // 先取消：确认框存在的意义就是取消真的能取消
    await page.click('[role="dialog"] button:has-text("取消")')
    await page.waitForSelector('[role="dialog"]', { state: 'detached', timeout: 5000 })
    must(/e2e-reader/.test(await page.textContent('.main')), '点了取消却把用户删了')

    await page.click('table tbody tr:has-text("e2e-reader") button:has-text("删除")')
    await page.waitForSelector('[role="dialog"]', { timeout: 5000 })
    await page.click('[role="dialog"] button:has-text("删除")')
    await page.waitForFunction(
      () => !/e2e-reader/.test(document.querySelector('.main')?.textContent ?? ''),
      { timeout: 10000 })
    await shot(page, '13d-access-deleted')
    return '取消不删，确认才删'
  })

  // access key 与用户同页（UI-075）。这一段要验的是两件 Go 侧看不见的事：
  // secret 真的显示出来了，以及停用按钮点下去之后状态真的变了。
  await check('访问控制页能发出 access key，secret 只显示一次', async () => {
    await page.goto(`${BASE}/#/access`, { waitUntil: 'networkidle' })
    await page.waitForSelector('h1', { timeout: 10000 })
    const card = page.locator('.card:has-text("AWS access key")')
    must(await card.count() > 0, '访问控制页上没有 access key 那一块')
    must(/还没有 access key/.test(await card.textContent()), 'access key 的空状态文案不对')

    await card.locator('button:has-text("新建 access key")').first().click()
    await page.waitForSelector('[role="dialog"]', { timeout: 5000 })
    await page.selectOption('[role="dialog"] select', 'operator')
    await page.click('[role="dialog"] button[type="submit"]')

    // 第二幕：id 与 secret 各一个只读框，外加"只显示这一次"的提示
    await page.waitForSelector('[role="dialog"] input[readonly]', { timeout: 10000 })
    const shown = await page.textContent('[role="dialog"]')
    must(/只显示这一次/.test(shown), `没有提示 secret 只出现一次：${shown.slice(0, 90)}`)
    const boxes = page.locator('[role="dialog"] input[readonly]')
    const keyId = await boxes.nth(0).inputValue()
    const secret = await boxes.nth(1).inputValue()
    must(/^AKIA[A-Z2-7]{16}$/.test(keyId), `access key id 形状不对：${keyId}`)
    must(secret.length === 40, `secret 长度 = ${secret.length}，期望 40`)
    await shot(page, '13e-awskey-created')
    await page.click('[role="dialog"] button:has-text("关闭")')
    await page.waitForSelector('[role="dialog"]', { state: 'detached', timeout: 5000 })

    const row = page.locator(`table tbody tr:has-text("${keyId}")`)
    await row.waitFor({ timeout: 10000 })
    must(/可用/.test(await row.textContent()), '新钥匙的状态不是「可用」')
    return `建出 ${keyId}，secret 只显示一次`
  })

  await check('停用 access key 之后状态跟着变', async () => {
    const row = page.locator('table tbody tr:has-text("AKIA")').first()
    const keyId = (await row.locator('td').first().textContent()).trim()
    await row.locator('button:has-text("停用")').click()
    // 状态要真的变成「已停用」。只改了按钮文字而列表没变的话，
    // 人会以为停掉了而那把钥匙还在用。
    await page.waitForFunction(
      (id) => {
        const tr = [...document.querySelectorAll('table tbody tr')]
          .find((x) => x.textContent.includes(id))
        return tr && /已停用/.test(tr.textContent)
      }, keyId, { timeout: 10000 })
    await shot(page, '13f-awskey-disabled')
    // 删除要先确认，和删用户同一个形状
    await page.locator(`table tbody tr:has-text("${keyId}") button:has-text("删除")`).click()
    await page.waitForSelector('[role="dialog"]', { timeout: 5000 })
    must(/立刻失效/.test(await page.textContent('[role="dialog"]')), '确认框没说清后果')
    await page.click('[role="dialog"] button:has-text("删除")')
    await page.waitForFunction(
      (id) => !document.querySelector('.main')?.textContent.includes(id),
      keyId, { timeout: 10000 })
    return `${keyId} 停用后状态可见，删除要确认`
  })

  // 登录页不该再出现 OIDC 入口（AUTH-080）
  await check('登录页只给令牌一条路', async () => {
    const gate = await page.evaluate(async (base) => {
      const r = await fetch(`${base}/api/admin/v1/oidc`)
      return r.status
    }, BASE.replace('/console', ''))
    must(gate === 404 || gate === 405, `/oidc 仍然应答：${gate}`)
    return `/oidc 已下线（${gate}）`
  })

  // 使用教程（UI-076 至 UI-081、TST-143）。
  //
  // 这一页是纯渲染的：正文是 docs/tutorial/ 下的 markdown，由前端在运行时
  // 解析成节点再渲染。Go 侧证明不了它渲染得出来——八种块级节点里任何一种
  // 写错，表现都是白屏或少一段，而两者在 Go 测试里完全不可见。
  await check('标题栏里就是教程入口', async () => {
    await page.goto(`${BASE}/#/`, { waitUntil: 'networkidle' })
    await page.waitForSelector('.topbar-guide', { timeout: 10000 })
    const label = await page.textContent('.topbar-guide')
    must(/使用教程/.test(label ?? ''), `标题栏入口的文案 = ${label}`)
    // 未在教程页时不点亮：aria-current 是"我在哪儿"的唯一来源，
    // 一直点亮等于没有这个信号。
    must(
      (await page.getAttribute('.topbar-guide', 'aria-current')) === null,
      '不在教程页时标题栏入口就点亮了')
    await page.click('.topbar-guide')
    await page.waitForSelector('.main-head h1', { timeout: 10000 })
    must(
      (await page.getAttribute('.topbar-guide', 'aria-current')) === 'page',
      '进了教程页标题栏入口却没点亮')
    const chapters = await page.$$eval('.card h2, .card h3', (els) => els.map((e) => e.textContent))
    must(chapters.length >= 3, `教程首页只有 ${chapters.length} 章`)
    await shot(page, '16-guide-home')
    return chapters.join(' / ')
  })

  await check('教程页深链接直达并渲染出代码块', async () => {
    await page.goto(`${BASE}/#/guide/sdk/golang`, { waitUntil: 'networkidle' })
    await page.waitForSelector('.guide-body', { timeout: 10000 })
    const h1 = await page.textContent('.main-head h1')
    must(/Go/.test(h1 ?? ''), `页标题 = ${h1}`)
    // include 注入的代码块：解析不到锚点时渲染的是一条红色的错误条，
    // 所以这里既要有代码块，也不能有错误条。
    const codes = await page.$$('.guide-code pre code')
    must(codes.length >= 4, `代码块只有 ${codes.length} 个`)
    const text = await page.textContent('.guide-body')
    must(/BaseEndpoint/.test(text ?? ''), '示例代码里没有 BaseEndpoint，include 可能没注入')
    must(!(await page.$('.guide-body .banner[data-tone="danger"]')), '页面上有解析错误')
    // 表格、页内目录、上下页都要在
    must((await page.$$('.guide-toc a')).length >= 2, '页内目录是空的')
    must(await page.isVisible('.guide-pager'), '没有上下页')
    await shot(page, '17-guide-page')
    return `${codes.length} 个代码块`
  })

  await check('代码块的复制按钮把片段放进剪贴板', async () => {
    await ctx.grantPermissions(['clipboard-read', 'clipboard-write'])
    await page.click('.guide-code:first-of-type .guide-code-head button')
    const copied = await page.evaluate(() => navigator.clipboard.readText())
    must(copied.length > 20, `剪贴板里只有 ${copied.length} 个字符`)
    return `${copied.split('\n').length} 行`
  })

  await check('教程切到英文后两棵树都渲染得出来', async () => {
    await page.click('.topbar button:has-text("EN")')
    await page.waitForFunction(() => document.documentElement.lang === 'en', { timeout: 5000 })
    await page.waitForSelector('.guide-body', { timeout: 10000 })
    const body = await page.textContent('.guide-body')
    must(/prerequisites/i.test(body ?? ''), '英文正文没渲染出来')
    must(!/先读这一页/.test(body ?? ''), '英文页里混着中文正文')
    must(!(await page.$('.guide-body .banner[data-tone="danger"]')), '英文页上有解析错误')
    // 代码块在两棵树里必须一样（UI-079：代码不翻译）
    must(/BaseEndpoint/.test(body ?? ''), '英文页的代码块变了')
    await shot(page, '18-guide-en')
    await page.click('.topbar button:has-text("中文")')
    await page.waitForFunction(() => document.documentElement.lang === 'zh-CN', { timeout: 5000 })
    return '中英各渲染一遍，代码一致'
  })

  // 使用说明抽屉链到教程（UI-081）：抽屉是当前上下文的清单，教程是完整
  // 路径。链接断掉的表现是点进去一个"没有这一页"。
  await check('策略页的使用说明链到教程对应页', async () => {
    await page.goto(`${BASE}/#/policies`, { waitUntil: 'networkidle' })
    await page.waitForSelector('.main-actions', { timeout: 10000 })
    await page.click('.main-actions button:has-text("使用说明")')
    await page.waitForSelector('[role="dialog"]', { timeout: 5000 })
    const link = await page.getAttribute('[role="dialog"] a[href^="#/guide"]', 'href')
    must(link === '#/guide/policy/basics', `抽屉里的教程链接 = ${link}`)
    await page.click('[role="dialog"] a[href^="#/guide"]')
    await page.waitForSelector('.guide-body', { timeout: 10000 })
    const body = await page.textContent('.guide-body')
    must(!/没有这一页/.test(body ?? ''), '抽屉链过来落到了空页')
    return link
  })

  await check('切到英文后界面真的变英文', async () => {
    await page.goto(`${BASE}/#/things`, { waitUntil: 'networkidle' })
    await page.waitForSelector('table tbody tr', { timeout: 10000 })
    await page.click('.topbar button:has-text("EN")')
    await page.waitForFunction(
      () => document.documentElement.lang === 'en', { timeout: 5000 })
    const shell = await page.textContent('.shell')
    must(/Things/.test(shell), '导航没有变成英文')
    must(!/总览/.test(shell), `切到英文后仍有中文：${shell.slice(0, 80)}`)
    await shot(page, '14-english')
    return 'lang=en，导航为英文且无中文残留'
  })

  // 退出是此前完全没被测过的一条路径：它要清掉令牌、调一次 logout，
  // 并且回到登录闸。做错的表现是界面留在原处而令牌已经没了，
  // 之后每个请求都 401。
  await check('标题栏右侧的退出能回到登录闸', async () => {
    await page.click('.topbar-right button:last-of-type')
    await page.waitForSelector('.gate', { timeout: 10000 })
    must(!(await page.isVisible('.shell')), '退出后外壳还在')
    const left = await page.evaluate(() => {
      try {
        return sessionStorage.length + localStorage.getItem('oiot.token')
      } catch {
        return 'n/a'
      }
    })
    must(!/[0-9a-f]{16}/.test(String(left)), `退出后仍有令牌痕迹：${left}`)
    await shot(page, '15-signed-out')
    return '回到登录闸，令牌已清除'
  })

  await check('整轮运行里浏览器没有报错', async () => {
    must(pageErrors.length === 0, `浏览器报了 ${pageErrors.length} 条：\n  ${pageErrors.join('\n  ')}`)
    return '没有 console.error，也没有未捕获异常'
  })
}

try {
  await main()
} catch (err) {
  record('运行本身失败', false, err.stack ?? err.message)
} finally {
  if (browser) await browser.close()
}

const failed = results.filter((r) => !r.ok)
console.log('')
console.log(`${results.length - failed.length}/${results.length} 通过`)
process.exit(failed.length === 0 ? 0 : 1)
