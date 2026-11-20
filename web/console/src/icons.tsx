/*
 * 图标集：手写内联 SVG，不引图标库。
 *
 * 理由与控制台不引 react-router 是同一条（LANG-019）：lucide-react 这类
 * 包为了 1000 多个图标把整棵树搬进来，而这里要的是 15 个。按需引入依赖
 * tree-shaking 配对，配错了就是几百 KB 进包，而这个产物是**提交进仓库**
 * 的（UI-039），体积失控会直接进 git 历史。
 *
 * 全部用 currentColor 描边：图标的颜色由所在位置的文字色决定，于是
 * 深浅两套主题各自正确，不必为每个主题各出一套图标。
 *
 * viewBox 统一 24×24 而渲染为 16px：描边宽度 1.75 在 24 格里换算到 16px
 * 约等于 1.17 物理像素，视觉重量与 13px 的正文接近。写死成 16 格会让
 * 路径坐标全是小数，改一笔要重算一遍。
 */
import type { JSX } from 'react'

export type IconName =
  | 'guide'
  | 'overview'
  | 'things'
  | 'certificates'
  | 'connections'
  | 'policies'
  | 'debugger'
  | 'events'
  | 'mqtt'
  | 'capture'
  | 'access'
  | 'sun'
  | 'moon'
  | 'system'
  | 'lang'
  | 'signout'
  // 动作类：按钮上用。同一个动词在每一页都该是同一个图标，
  // 否则图标非但不帮忙，还要人先学一遍"这里的删除长什么样"。
  | 'refresh'
  | 'plus'
  | 'edit'
  | 'trash'
  | 'help'
  | 'copy'
  | 'back'
  | 'run'
  | 'kick'
  | 'shadow'
  | 'power'

/* 每个图标是一段路径。只存 d/几何，不存尺寸与颜色——那些由 Icon 统一给，
   否则一次改动要扫 15 处。 */
const PATHS: Record<IconName, JSX.Element> = {
  // 教程：一本摊开的书。书脊在中间，两页各一条线代表正文——
  // 两笔就够，再多的线在 16px 上会糊成一团。
  guide: (
    <>
      <path d="M12 6.5C10.5 5 8.5 4.5 4 4.5v13c4.5 0 6.5.5 8 2 1.5-1.5 3.5-2 8-2v-13c-4.5 0-6.5.5-8 2z" />
      <path d="M12 6.5v13" />
    </>
  ),
  // 总览：四格面板
  overview: (
    <>
      <rect x="3" y="3" width="7" height="7" rx="1.5" />
      <rect x="14" y="3" width="7" height="7" rx="1.5" />
      <rect x="3" y="14" width="7" height="7" rx="1.5" />
      <rect x="14" y="14" width="7" height="7" rx="1.5" />
    </>
  ),
  // 设备：芯片（带引脚），比"手机"更贴近 Thing 的含义
  things: (
    <>
      <rect x="7" y="7" width="10" height="10" rx="1.5" />
      <path d="M10 3v4M14 3v4M10 17v4M14 17v4M3 10h4M3 14h4M17 10h4M17 14h4" />
    </>
  ),
  // 证书：盾牌加对勾
  certificates: (
    <>
      <path d="M12 3l7 3v6c0 4-3 7.3-7 9-4-1.7-7-5-7-9V6l7-3z" />
      <path d="M9 12l2.2 2.2L15.5 10" />
    </>
  ),
  // 连接：插头对接
  connections: (
    <>
      <path d="M9.5 3v5M14.5 3v5" />
      <path d="M7 8h10v3a5 5 0 01-5 5 5 5 0 01-5-5V8z" />
      <path d="M12 16v5" />
    </>
  ),
  // 策略：文档加条目
  policies: (
    <>
      <path d="M6 3h8l4 4v14H6z" />
      <path d="M14 3v4h4" />
      <path d="M9 12h6M9 16h6" />
    </>
  ),
  // 策略调试器：放大镜（"看一次求值结果"）
  debugger: (
    <>
      <circle cx="11" cy="11" r="6" />
      <path d="M15.5 15.5L21 21" />
      <path d="M8.5 11h5" />
    </>
  ),
  // 事件流：心跳折线
  events: <path d="M3 12h4l2.5-6 4 12L16 12h5" />,
  // MQTT 测试：终端提示符
  mqtt: (
    <>
      <rect x="3" y="4" width="18" height="16" rx="2" />
      <path d="M7 10l2.5 2L7 14M12.5 15h4" />
    </>
  ),
  // 主题抓包：同心波（"在某个主题上听"）
  capture: (
    <>
      <circle cx="12" cy="12" r="2" />
      <path d="M8.5 8.5a5 5 0 000 7M15.5 8.5a5 5 0 010 7" />
      <path d="M5.5 5.5a9 9 0 000 13M18.5 5.5a9 9 0 010 13" />
    </>
  ),
  // 访问控制：钥匙
  access: (
    <>
      <circle cx="8" cy="8" r="4" />
      <path d="M11 11l9 9M17 14l-2.5 2.5M20 17l-2.5 2.5" />
    </>
  ),
  sun: (
    <>
      <circle cx="12" cy="12" r="4" />
      <path d="M12 2v2M12 20v2M2 12h2M20 12h2M5 5l1.5 1.5M17.5 17.5L19 19M19 5l-1.5 1.5M6.5 17.5L5 19" />
    </>
  ),
  moon: <path d="M20 14.5A8.5 8.5 0 019.5 4a8.5 8.5 0 1010.5 10.5z" />,
  // 跟随系统：半明半暗的显示器
  system: (
    <>
      <rect x="3" y="4" width="18" height="12" rx="2" />
      <path d="M8 20h8M12 16v4" />
      <path d="M12 4v12" />
    </>
  ),
  lang: (
    <>
      <circle cx="12" cy="12" r="9" />
      <path d="M3 12h18" />
      <path d="M12 3c2.5 2.6 2.5 15.4 0 18M12 3c-2.5 2.6-2.5 15.4 0 18" />
    </>
  ),
  signout: (
    <>
      <path d="M15 4h3a2 2 0 012 2v12a2 2 0 01-2 2h-3" />
      <path d="M10 8l-4 4 4 4M6 12h9" />
    </>
  ),

  refresh: (
    <>
      <path d="M20 12a8 8 0 01-13.7 5.7L4 15.5" />
      <path d="M4 12a8 8 0 0113.7-5.7L20 8.5" />
      <path d="M4 20v-4.5h4.5M20 4v4.5h-4.5" />
    </>
  ),
  plus: <path d="M12 5v14M5 12h14" />,
  edit: (
    <>
      <path d="M4 20h4L19 9a2.1 2.1 0 00-3-3L5 17v3z" />
      <path d="M15 6l3 3" />
    </>
  ),
  trash: (
    <>
      <path d="M4 7h16M10 7V5h4v2M6 7l1 13h10l1-13" />
      <path d="M10 11v6M14 11v6" />
    </>
  ),
  help: (
    <>
      <circle cx="12" cy="12" r="9" />
      <path d="M9.5 9.5a2.6 2.6 0 015 .9c0 1.7-2.5 2.1-2.5 3.6" />
      <path d="M12 17.2h.01" />
    </>
  ),
  copy: (
    <>
      <rect x="9" y="9" width="11" height="11" rx="2" />
      <path d="M5 15H4a1 1 0 01-1-1V5a2 2 0 012-2h9a1 1 0 011 1v1" />
    </>
  ),
  back: <path d="M19 12H5M11 6l-6 6 6 6" />,
  // 判定/运行：一个"执行"的三角
  run: <path d="M8 5.5l11 6.5-11 6.5z" />,
  // 踢掉连接：断开的插头
  kick: (
    <>
      <path d="M9.5 3v5M14.5 3v5" />
      <path d="M7 8h10v3a5 5 0 01-5 5 5 5 0 01-5-5V8z" />
      <path d="M4 20L20 4" />
    </>
  ),
  // 停用/恢复：电源符号。这一对是同一个开关的两个方向，所以共用一个
  // 图标，由按钮文字说清当前要做哪一边——给"停用"和"恢复"各配一个，
  // 反而要人去分辨两个长得差不多的三角。
  power: (
    <>
      <path d="M12 3v9" />
      <path d="M6.5 7a7.5 7.5 0 1011 0" />
    </>
  ),
  // 影子：实体加一层偏移的"投影"
  shadow: (
    <>
      <rect x="4" y="4" width="11" height="11" rx="2" />
      <path d="M9 19h8a2 2 0 002-2V9" />
    </>
  ),
}

/**
 * Icon 渲染一个 16px 的描边图标。
 *
 * 一律 aria-hidden：图标旁边必有文字（导航项、按钮标签或 aria-label）。
 * 给图标自己加标签会让读屏软件把同一件事念两遍。
 */
export function Icon(props: { name: IconName; size?: number }) {
  return (
    <svg
      className="icon"
      width={props.size ?? 16}
      height={props.size ?? 16}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={1.75}
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      focusable="false"
    >
      {PATHS[props.name]}
    </svg>
  )
}

/**
 * Logo 是品牌标记：一个节点连着三个设备。
 *
 * 不用文字首字母：那在 16px 下与正文混在一起认不出来。用 accent 实心
 * 填充，使它成为界面上唯一一处"品牌色块"，而导航与按钮的强调色都只是
 * 描边或浅底——层级因此清楚。
 */
export function Logo(props: { size?: number }) {
  const s = props.size ?? 22
  return (
    <svg
      className="logo"
      width={s}
      height={s}
      viewBox="0 0 24 24"
      fill="none"
      aria-hidden="true"
      focusable="false"
    >
      <rect width="24" height="24" rx="6" fill="url(#oiot-logo-grad)" />
      <defs>
        <linearGradient id="oiot-logo-grad" x1="0" y1="0" x2="24" y2="24">
          <stop stopColor="var(--accent)" />
          <stop offset="1" stopColor="var(--accent-2)" />
        </linearGradient>
      </defs>
      <g stroke="var(--accent-text)" strokeWidth="1.6" strokeLinecap="round">
        <path d="M12 12V6.5M12 12l-4.2 3M12 12l4.2 3" />
      </g>
      <g fill="var(--accent-text)">
        <circle cx="12" cy="5.4" r="1.9" />
        <circle cx="6.8" cy="15.8" r="1.9" />
        <circle cx="17.2" cy="15.8" r="1.9" />
      </g>
    </svg>
  )
}
