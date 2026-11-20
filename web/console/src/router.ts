/*
 * 极小的哈希路由。
 *
 * 不引 react-router：这些视图用不上嵌套路由与数据加载器，
 * 而它是 20 KB。这与项目只保三个 Go 模块的取向一致——
 * 依赖要为它的体积换来相称的东西。
 *
 * 用哈希而非 History API：控制台可能被挂在反向代理的子路径下
 * （UI-001 要求支持），哈希路由不需要服务端配合重写。
 */

export type Route =
  | { view: 'overview' }
  | { view: 'guide'; chapter?: string; page?: string }
  | { view: 'things' }
  | { view: 'thing'; name: string }
  | { view: 'shadow'; name: string }
  | { view: 'certificates' }
  | { view: 'connections' }
  | { view: 'policies' }
  | { view: 'debugger'; policy?: string }
  | { view: 'events' }
  | { view: 'mqtt' }
  | { view: 'access' }

export function parseRoute(hash: string): Route {
  const raw = hash.replace(/^#\/?/, '')
  const [head = '', ...rest] = raw.split('/')
  const arg = rest.length > 0 ? decodeURIComponent(rest.join('/')) : ''
  switch (head) {
    // 教程：#/guide 是目录，#/guide/<章>/<页> 直达一页。
    // 两段都要能深链接——把某一页贴给别人是它的主要传播方式。
    case 'guide': {
      const [chapter, page] = rest
      return {
        view: 'guide',
        ...(chapter ? { chapter: decodeURIComponent(chapter) } : {}),
        ...(page ? { page: decodeURIComponent(page) } : {}),
      }
    }
    case 'things':
      return arg ? { view: 'thing', name: arg } : { view: 'things' }
    // 影子是独立的一页而不是 things/<名>/shadow：things 下的参数
    // 会被整段当成设备名（rest.join('/')），再加一层就要改掉那条规则。
    case 'shadow':
      return arg ? { view: 'shadow', name: arg } : { view: 'things' }
    case 'certificates':
      return { view: 'certificates' }
    case 'connections':
      return { view: 'connections' }
    case 'policies':
      return { view: 'policies' }
    case 'debugger':
      return arg ? { view: 'debugger', policy: arg } : { view: 'debugger' }
    case 'events':
      return { view: 'events' }
    case 'mqtt':
      return { view: 'mqtt' }
    case 'access':
      return { view: 'access' }
    default:
      return { view: 'overview' }
  }
}

export function href(r: Route): string {
  switch (r.view) {
    case 'overview':
      return '#/'
    case 'thing':
      return `#/things/${encodeURIComponent(r.name)}`
    case 'shadow':
      return `#/shadow/${encodeURIComponent(r.name)}`
    case 'guide':
      if (!r.chapter) return '#/guide'
      return r.page
        ? `#/guide/${encodeURIComponent(r.chapter)}/${encodeURIComponent(r.page)}`
        : `#/guide/${encodeURIComponent(r.chapter)}`
    case 'debugger':
      return r.policy ? `#/debugger/${encodeURIComponent(r.policy)}` : '#/debugger'
    default:
      return `#/${r.view}`
  }
}

export function navigate(r: Route) {
  window.location.hash = href(r)
}
