// dsh-webhook-feishu —— 把飞书机器人直接接进 DSH 宿主进程的 Cordis 插件。
//
// 与之前「桥接脚本 + 收件箱 + 唤醒器」的区别：
//   以前：飞书消息 → 独立 node 进程写文件 → 唤醒器退出 → 通知把 agent 叫醒
//   现在：飞书消息 → 本插件 → ctx.sessionController.prompt() 直接把消息投进**指定会话**
//         宿主进程活着就能投递，不需要额外常驻进程，也不存在"叫不醒"。
//
// 2026-10-14 第二次迭代（自动回帖）：
//   上一版"投递自动、回答靠我手动跑 feishu-send.mjs"，用户会只看到「已收到」然后沉默。
//   现在插件自己监听会话事件：被投递的那一轮结束后，把该轮最后一段文字回答自动发回飞书。
//   v4 修正：待回帖不再纯 FIFO —— 只有『本轮真的消费了这条注入消息』（user/message 的 source.rpcId
//   匹配得上）才回帖；否则我在电脑上正忙时你从手机发的消息会被当前这轮抢答、而你那条反而没人回。
//   事件接口依据：`ctx.on("session/event", (session, event) => …)`，事件形如
//   `turn/start` / `assistant/message` / `turn/end`（参考 dsh-headless 的 json 流实现）。
//
// 设计约束：
//   1) **零第三方 import**：profile 的 node_modules 里没有 @deepseek-ai/*，
//      插件从交付区目录被加载时按真实路径向上找不到那些包。所以只用 node: 内置模块
//      和同目录的 ./feishu.mjs（那是我自己写的帧协议实现，只用内置能力）。
//   2) apply 里任何异常都不能往外抛：插件装进 web profile 后一旦启动失败，
//      整个 DSH 进程就起不来。
//   3) 宿主进程的 stdout 我看不到（跑在用户的终端里），所以关键动作同时写日志文件
//      和心跳文件，方便我在别的进程里观察插件是否活着。

import { randomUUID } from 'node:crypto'
import { appendFileSync, mkdirSync, readFileSync, renameSync, statSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'

import { connect, extractMessage, readConfig, sendText } from './feishu.mjs'
import { fetchMessageResource, saveInboundFile, sniffImageMediaType } from './feishu-resource.mjs'

export const name = 'dsh-webhook-feishu'

// 只注入会话控制器：拿到 ctx.sessionController 就能建会话 / 往会话投消息。
export const inject = ['sessionController']

// 去重窗口：飞书在没收到回执时会不断重投同一条事件，实测约每 20 秒一次。
// 对一个能执行命令的 agent，同一条指令跑三遍是会出事的（比如"删掉 X"），
// 所以按 message_id 去重要开得足够大。真消息的 id 不会重复，窗口开大零副作用。
const DEDUP_TTL_MS = 24 * 60 * 60 * 1000

// 飞书单条消息的字符上限（我自己按 3000 分片，和 feishu-send.mjs 保持一致）。
const CHUNK = 3000

// 日志里的人类可读格式（别出现 0.3400000000000001 这种浮点尾巴）
const fmtDur = (ms) => (ms >= 3600000 ? `${ms / 3600000}h` : ms >= 60000 ? `${Math.round(ms / 60000)}min` : `${Math.round(ms / 1000)}s`)
const fmtClock = (hour) => `${String(Math.floor(hour)).padStart(2, '0')}:${String(Math.round((hour % 1) * 60)).padStart(2, '0')}`

export function apply(ctx, config = {}) {
  const configPath = config.configPath
  const stateDir = config.stateDir ?? (configPath ? dirname(configPath) : process.cwd())
  const logPath = join(stateDir, 'feishu-plugin.log')
  const heartbeatPath = join(stateDir, 'feishu-plugin-heartbeat.txt')
  const statePath = join(stateDir, 'feishu-plugin-state.json')

  // 宿主是用 wscript 隐藏启动的，没有控制台可看 —— 这个文件就是唯一现场，所以它必须能一直写下去。
  // 但也不能无限长：超过 2 MB 就轮转成 .1（只留上一份），轮转失败不影响这次写入。
  const LOG_MAX_BYTES = 2 * 1024 * 1024
  const rotateLog = () => {
    try {
      if (statSync(logPath).size > LOG_MAX_BYTES) renameSync(logPath, `${logPath}.1`)
    } catch {
      /* 文件还不存在、或被占用：忽略，接着写 */
    }
  }
  const log = (message) => {
    const line = `[${new Date().toISOString()}] ${message}`
    console.log(`[dsh-webhook-feishu] ${message}`)
    rotateLog()
    try {
      appendFileSync(logPath, `${line}\n`)
    } catch {
      /* 日志写不进去不影响主流程 */
    }
  }
  const beat = (message) => {
    try {
      writeFileSync(heartbeatPath, `${new Date().toISOString()} ${message}\n`)
    } catch {
      /* 同上 */
    }
  }

  beat('插件已加载，准备读配置')

  if (!configPath) {
    log('配置里没有 configPath（飞书凭据文件路径），插件不启动')
    beat('未启动：缺 configPath')
    return
  }

  // ── 凭据：复用已有的 feishu-config.json，密钥不放插件配置里 ──────────
  let cfg
  try {
    cfg = readConfig(configPath)
  } catch (error) {
    log(`读配置失败：${error.message}`)
    beat(`未启动：读配置失败 ${error.message}`)
    return
  }

  // ── 密钥来源：优先 DSH 凭证机制（ctx.credentials.resolve），配置文件明文只作兜底 ──
  // 配好 appSecretRef 之后，feishu-config.json 里的 appSecret 明文可以随时删掉。
  const secretRef = String(config.appSecretRef ?? cfg.appSecretRef ?? '').trim()
  let secretSource = secretRef === '' ? '配置文件明文' : '凭证（未解析）'
  cfg.resolveSecret = async () => {
    if (secretRef !== '') {
      const credentials = ctx.get('credentials')
      const hit = credentials !== void 0 ? await credentials.resolve(secretRef) : undefined
      if (hit && typeof hit.value === 'string' && hit.value !== '') {
        if (cfg.appSecret !== hit.value) {
          cfg.appSecret = hit.value
          log(`密钥值已更新：来源=凭证 ${secretRef}`)
        }
        if (secretSource !== '凭证:' + secretRef) {
          secretSource = '凭证:' + secretRef
          log(`密钥来源：凭证 ${secretRef}（值不落日志）`)
        }
        return hit.value
      }
      secretSource = '凭证缺失，回退配置文件明文'
    }
    return cfg.appSecret
  }
  // 启动时先探一次，好在日志里核对来源（不会打印密钥本身）
  void cfg.resolveSecret()
    .then(() => log(`密钥就绪：${secretSource}`))
    .catch((error) => log(`密钥解析失败（暂用配置文件兜底）：${error?.message ?? error}`))

  // ── 绑定目标会话 ────────────────────────────────────────────────
  // a 方案：固定投给一个会话。为了不必改 profile 就能换绑，支持两种来源：
  //   1) bindingFile 文件里的一行（我可以在自己的会话里改它，立刻生效）
  //   2) 插件配置里的 sessionId
  const bindingFile = config.bindingFile ?? join(stateDir, 'feishu-target-session.txt')
  const readBinding = () => {
    try {
      const text = readFileSync(bindingFile, 'utf8').replace(/^\uFEFF/, '').trim()
      return text === '' ? null : text.split(/\r?\n/)[0].trim()
    } catch {
      return null
    }
  }
  let targetSessionId = readBinding() ?? config.sessionId ?? cfg.sessionId ?? null
  if (!targetSessionId) {
    log('没有目标会话（bindingFile 为空且配置里没有 sessionId），插件不启动')
    beat('未启动：没有目标会话')
    return
  }

  const dryRun = config.dryRun === true
  const ackText = config.ackText ?? cfg.ackText ?? '已收到'
  const delayNoticeMs = Number(config.delayNoticeMs ?? 60000)
  const autoReply = config.autoReply !== false
  const autoReplyMaxChars = Number(config.autoReplyMaxChars ?? 12000)
  const autoReplyFallback = config.autoReplyFallback ?? '（这一轮我只做了操作，没有文字回答；细节在电脑上的会话里。）'
  const maxDeliveryAttempts = Math.max(1, Number(config.maxDeliveryAttempts ?? 3))

  // ── 自触发（主动性）────────────────────────────────────────────────
  // 把"空闲时我能主动找博士"从愿望变成机制：到点由插件往会话里注入一条【自触发】提示，
  // 我那一轮如果决定说话，回答走自动回帖原路发到飞书；如果决定沉默，则一个字都不发。
  // 参数由我按人格设定自定（2026-10-15 博士授权），可用覆盖层里的 selfTrigger 覆盖。
  // 参数不写死在代码里：**每次都现读** stateDir/selftrigger-policy.json（博士调范围不用重启宿主）。
  // 预算：单次活动折合人民币不超过 budgetPerActivityCny，超了插件直接 cancel 掉当前轮次。
  const stConf = config.selfTrigger ?? {}
  const stPolicyFile = join(stateDir, 'selftrigger-policy.json')
  const stUsageFile = join(stateDir, 'selftrigger-usage.json')
  const stOffFile = join(stateDir, 'selftrigger-off')
  // 判定方式（2026-10-22 博士定）：每小时判定一次，p 从 0 起、每落空一次 +failStepPct，
  // 命中后归零；命中再按 weights 抽这次做什么。
  // 2026-10-22 11:08 博士改口：判定不再受"他有没有在说话"约束（idleMinutes=0 即无门槛），
  // 只剩活跃时段 09:00-23:30 这一道闸；关机时段自然不判定，跨日把 p 归零。
  const ST_POLICY = {
    enabled: (stConf.enabled ?? config.selfTriggerEnabled ?? true) !== false,
    idleMinutes: Number(stConf.idleMinutes ?? 0),
    judgeEveryMinutes: Number(stConf.judgeEveryMinutes ?? 60),
    judgeJitterMinutes: Number(stConf.judgeJitterMinutes ?? 10),
    failStepPct: Number(stConf.failStepPct ?? 10),
    quietMinutes: Number(stConf.quietMinutes ?? 10),
    skipRetryMinutes: Number(stConf.skipRetryMinutes ?? 20),
    weights: { web: 40, files: 20, message: 30, nothing: 10 },
    dailyMax: Number(stConf.dailyMax ?? 3),
    activeFromHour: Number(stConf.activeFromHour ?? 9),
    activeToHour: Number(stConf.activeToHour ?? 23.5),
    silenceStop: Number(stConf.silenceStop ?? 3),
    actions: ['web', 'files', 'message'],
    budgetPerActivityCny: 0.3,
    pricePerMillion: { input: 2, cacheRead: 0.2, cacheWrite: 2, output: 3 }
  }
  const readPolicy = () => {
    let fromFile = {}
    try {
      const raw = JSON.parse(readFileSync(stPolicyFile, 'utf8').replace(/^\uFEFF/, ''))
      if (raw && typeof raw === 'object') fromFile = raw
    } catch (error) {
      if (error?.code !== 'ENOENT') log(`自触发策略文件读取失败（用默认值）：${error?.message ?? error}`)
    }
    const merged = { ...ST_POLICY, ...fromFile }
    const price = { ...ST_POLICY.pricePerMillion, ...(fromFile.pricePerMillion ?? {}) }
    merged.pricePerMillion = {
      input: Number(price.input ?? 2),
      cacheRead: Number(price.cacheRead ?? 0.2),
      cacheWrite: Number(price.cacheWrite ?? 2),
      output: Number(price.output ?? 3)
    }
    merged.actions = Array.isArray(merged.actions) && merged.actions.length > 0 ? merged.actions.map(String) : [...ST_POLICY.actions]
    merged.dailyMax = Math.max(1, Number(merged.dailyMax ?? 3))
    merged.silenceStop = Math.max(1, Number(merged.silenceStop ?? 3))
    merged.budgetPerActivityCny = Math.max(0.001, Number(merged.budgetPerActivityCny ?? 0.3))
    merged.idleMinutes = Math.max(0, Number(merged.idleMinutes ?? 0))
    merged.judgeEveryMinutes = Math.max(1, Number(merged.judgeEveryMinutes ?? 60))
    merged.judgeJitterMinutes = Math.max(0, Number(merged.judgeJitterMinutes ?? 10))
    merged.failStepPct = Math.min(100, Math.max(1, Number(merged.failStepPct ?? 10)))
    merged.quietMinutes = Math.max(0, Number(merged.quietMinutes ?? 10))
    merged.skipRetryMinutes = Math.max(1, Number(merged.skipRetryMinutes ?? 20))
    const w = { ...ST_POLICY.weights, ...(fromFile.weights ?? {}) }
    merged.weights = {
      web: Math.max(0, Number(w.web ?? 0)),
      files: Math.max(0, Number(w.files ?? 0)),
      message: Math.max(0, Number(w.message ?? 0)),
      nothing: Math.max(0, Number(w.nothing ?? 0))
    }
    return merged
  }
  // 首次运行把一份可读的默认策略写出去：之后这张纸我俩都能改，改完下一轮生效。
  try {
    readFileSync(stPolicyFile, 'utf8')
  } catch {
    try {
      writeFileSync(stPolicyFile, JSON.stringify({
        ...ST_POLICY,
        note: '自触发策略：改这里即时生效（插件每轮现读）。actions 可选 message/web/files；budgetPerActivityCny 为单次活动预算（人民币）；pricePerMillion 是折算单价。'
      }, null, 2))
    } catch (error) {
      log(`自触发策略文件写入失败：${error?.message ?? error}`)
    }
  }
  const stPolicy0 = readPolicy()
  const stEnabled = stPolicy0.enabled !== false
  const stIdleMs = Math.max(0, Number(stPolicy0.idleMinutes ?? 0) * 60 * 1000)
  const stDailyMax = Math.max(1, Number(stPolicy0.dailyMax ?? 3))
  // 计时器只负责"每分钟看一眼到没到判定点"，真正的判定节奏由 nextCheckAt（含 ±抖动）控制。
  const stCheckMs = 60 * 1000
  const stChatId = String(stConf.chatId ?? (cfg.allowChatIds ?? [])[0] ?? '')
  // 自触发状态跨重启持久化——否则宿主一重启，"刚起来"会被当成"已经空闲了很久"。
  const stState = { lastActivityAt: 0, lastOutboundAt: 0, lastFireAt: 0, day: '', count: 0, silentRun: 0, fails: 0, nextCheckAt: 0, lastJudgeAt: 0, lastTurnEndAt: 0, lastPick: '' }
  // 当前这次"自由时间"的账：注入时开局，turn/end 时收尾；中途按 usage 累计花费，超预算即掐断。
  let stActivity = null
  let stLastSkipReason = ''
  const appendUsageLedger = (record) => {
    try {
      let ledger = { day: stState.day, count: stState.count, activities: [] }
      try {
        const raw = JSON.parse(readFileSync(stUsageFile, 'utf8').replace(/^\uFEFF/, ''))
        if (raw && Array.isArray(raw.activities)) ledger = { ...ledger, ...raw }
      } catch {
        /* 首次运行没有台账 */
      }
      const activities = [...(ledger.activities ?? []), record].slice(-40)
      writeFileSync(stUsageFile, JSON.stringify({ day: stState.day, count: stState.count, activities }, null, 2))
    } catch (error) {
      log(`自触发台账写入失败：${error?.message ?? error}`)
    }
  }
  let lastAssistantText = ''
  let lastInboundText = ''

  // ── 预约任务（自开工）──────────────────────────────────────────────
  // 博士早上说"下午四点帮我做个文件"，到点由插件注入【预约任务】提示，我直接开工，
  // 结果照样自动回帖到他飞书——不需要他再喊一声。清单由同目录的 schedule.mjs 维护。
  const scConf = config.schedule ?? {}
  const scEnabled = (scConf.enabled ?? config.scheduleEnabled ?? true) !== false
  const scFile = String(scConf.file ?? join(stateDir, 'feishu-schedule.json'))
  const scCheckMs = Math.max(20 * 1000, Number(scConf.checkIntervalMinutes ?? 1) * 60 * 1000)
  let lastScheduleError = ''

  // ── 状态：去重表 + "本机自己发的消息"登记表（跨进程） ────────────────
  const dedup = new Map()
  const sentByUs = new Map()
  try {
    mkdirSync(stateDir, { recursive: true })
  } catch {
    /* 已存在 */
  }
  try {
    const raw = JSON.parse(readFileSync(statePath, 'utf8').replace(/^\uFEFF/, ''))
    const now = Date.now()
    for (const [id, at] of Object.entries(raw.dedup ?? {})) if (now - Number(at) < DEDUP_TTL_MS) dedup.set(id, Number(at))
    for (const [id, at] of Object.entries(raw.sentByUs ?? {})) sentByUs.set(id, Number(at))
    if (raw.selfTrigger && typeof raw.selfTrigger === 'object') {
      // 合并而不是"白名单挑选"：以前只认 5 个数字 + day，以后给 selfTrigger 加字段（比如台账）
      // 会被静默丢掉；现在已知字段做类型校验、未知字段原样带回来。
      for (const [key, value] of Object.entries(raw.selfTrigger)) {
        if (key === 'day') {
          if (typeof value === 'string') stState.day = value
          continue
        }
        if (key === 'lastPick') {
          if (typeof value === 'string') stState.lastPick = value
          continue
        }
        const num = Number(value)
        if (Number.isFinite(num)) stState[key] = num
      }
    }
    log(`状态已载入：去重 ${dedup.size} 条 / 自发 ${sentByUs.size} 条 / 自触发今日开口 ${stState.count} 次（day=${stState.day || '未记录'}）`)
    // 台账是另一个文件（selftrigger-usage.json），启动时读一眼，方便和 state 对账。
    try {
      const ledger = JSON.parse(readFileSync(stUsageFile, 'utf8').replace(/^\uFEFF/, ''))
      const n = Array.isArray(ledger?.activities) ? ledger.activities.length : 0
      log(`台账已载入：${n} 条历史活动（day=${ledger?.day ?? '未记录'} count=${ledger?.count ?? '?'}）`)
    } catch {
      /* 首次运行没有台账 */
    }
  } catch {
    /* 首次运行没有状态文件 */
  }
  const saveState = () => {
    try {
      writeFileSync(statePath, JSON.stringify({ dedup: Object.fromEntries(dedup), sentByUs: Object.fromEntries(sentByUs), selfTrigger: stState }, null, 0))
    } catch (error) {
      log(`状态写盘失败：${error.message}`)
    }
  }

  // 白名单语义：两个名单都空 = 谁都不放行（安全默认）；
  // 填了用户名单就必须人对得上，填了会话名单就必须会话也对得上（AND，不是 OR —— OR 会放行会话里的任何人）。
  const isAllowed = (message) => {
    const users = cfg.allowOpenIds ?? []
    const chats = cfg.allowChatIds ?? []
    if (users.length === 0 && chats.length === 0) return false
    if (users.length > 0 && !users.includes(message.senderOpenId)) return false
    if (chats.length > 0 && !chats.includes(message.chatId)) return false
    return true
  }

  // ── 自动回帖：待回队列（按目标会话）+ 该轮已累积的回答文字 ───────────
  // 只有"由飞书投递触发"的轮次才会有待回条目；用户在网页端自己发起的轮次不进队列，
  // 所以不会把网页里的对话自动推到手机上。
  const pendingReplies = [] // [{ chatId, messageId, at, misses }]
  let turnText = '' // 该轮最后一段非空文字回答
  const attempts = new Map() // messageId -> 连续投递失败次数
  // 本轮真正被会话『消费』掉的注入消息（靠 user/message 事件里的 source.rpcId 精确匹配）。
  // 为什么需要它：队列原来是纯 FIFO，任何一轮结束都会吃掉队首 —— 于是『我在电脑上正忙时你从
  // 手机发消息』会让当前这轮的回答被推给手机，而你那条消息的回答反而没人回。
  // 现在只有真正消费了这条消息的那一轮才回帖；misses 是事件形状万一变化时的兜底计数。
  let consumedIds = new Set()

  const splitForChat = (text) => {
    const chunks = []
    let rest = text
    while (rest.length > CHUNK) {
      let cut = rest.lastIndexOf('\n', CHUNK)
      if (cut < CHUNK / 2) cut = CHUNK
      chunks.push(rest.slice(0, cut))
      rest = rest.slice(cut).replace(/^\n+/, '')
    }
    if (rest.length > 0) chunks.push(rest)
    return chunks.length > 0 ? chunks : ['']
  }

  const sendAnswer = async (item, text) => {
    const body = (text ?? '').trim() === '' ? autoReplyFallback : text.trim()
    const clipped = body.length > autoReplyMaxChars ? `${body.slice(0, autoReplyMaxChars)}\n\n…（回答过长，已截断；完整内容在电脑上的会话里）` : body
    const chunks = splitForChat(clipped)
    for (let i = 0; i < chunks.length; i += 1) {
      const head = chunks.length > 1 ? `(${i + 1}/${chunks.length}) ` : ''
      try {
        // 兜一层：条目里没有 chatId（空串）时回落到配置的默认回帖目标，绝不出手就发空目标。
        const sent = await sendText(cfg, { receiveIdType: 'chat_id', receiveId: item.chatId || stChatId, text: head + chunks[i] })
        log(`✅ 已自动回帖第 ${i + 1}/${chunks.length} 条（回的是 ${item.messageId}）`)
        const id = Array.isArray(sent) ? sent[0]?.message_id : sent?.message_id
        if (typeof id === 'string') {
          // 登记成"本机自己发的"，万一回流不会被当新指令。
          sentByUs.set(id, Date.now())
          saveState()
        }
      } catch (error) {
        log(`自动回帖失败（第 ${i + 1}/${chunks.length} 条）：${error?.message ?? error}`)
      }
    }
  }

  const onSessionEvent = (session, event) => {
    // 活动时间只按「有人在跟这个会话说话」(user/message) 算。
    // 2026-10-17 博士拍板：我自己的回答、工具调用、轮次推进都不算空闲——
    // 否则我在后台干活时会不停重置空闲计时，自由时间永远等不到。
    if (session?.id === targetSessionId) {
      if (event.type === 'user/message') {
        // 只有「博士本人对我说的话」才算空闲。排除三类：①插件自己注入的（自由时间 selftrigger-*、预约 sched-*）
        // ②非人的注入（记忆快照/dream 任务 kind=plugin:meow-memory、compaction 检查点、skill-catalog、
        //   runtime-context、subagent-settled、tool-jobs 等——2026-10-17 抓到它把 22:13:42 的空闲计时归零，
        //   让 22:13:43 那次检查差了 1 秒没触发）③我正在干自由时间的这一轮（stActivity 非空）。
        const src = event.data?.source ?? {}
        const rpcId = String(src.rpcId ?? '')
        const notHuman = String(src.kind ?? 'user') !== 'user'
        const injected = rpcId.startsWith('selftrigger-') || rpcId.startsWith('sched-') || notHuman
        if (!injected && stActivity === null) stState.lastActivityAt = Date.now()
      }
      if (event.type === 'assistant/message') {
        const blocks = event.data?.message?.content ?? []
        const text = blocks.filter((b) => b?.type === 'text' && typeof b.text === 'string').map((b) => b.text).join('\n').trim()
        if (text !== '') lastAssistantText = text.length > 400 ? text.slice(0, 400) : text
        // 自由时间的预算：每一步的 usage 折算成钱累加，超了立刻 cancel 掉这一轮（硬刹车）。
        if (stActivity !== null && event.data?.usage) {
          const usage = event.data.usage
          stActivity.costCny += costOfUsage(usage, stActivity.price)
          const tok = stActivity.tokens
          tok.input += Number(usage.inputTokens ?? 0)
          tok.cacheRead += Number(usage.cacheReadTokens ?? 0)
          tok.cacheWrite += Number(usage.cacheWriteTokens ?? 0)
          tok.output += Number(usage.outputTokens ?? 0)
          stActivity.steps += 1
          if (stActivity.cancelled !== true && stActivity.costCny > stActivity.budgetCny) {
            stActivity.cancelled = true
            let note = '已请求取消'
            try {
              ctx.sessionController.cancel({ sessionId: targetSessionId })
            } catch (error) {
              note = `取消失败：${error?.message ?? error}`
            }
            log(`自触发：本次活动折合 ¥${stActivity.costCny.toFixed(4)} 已超预算 ¥${stActivity.budgetCny}，${note}`)
          }
        }
      }
    }
    if (!autoReply || session?.id !== targetSessionId) return
    try {
      if (event.type === 'turn/start') {
        turnText = ''
        consumedIds = new Set()
        return
      }
      if (event.type === 'user/message') {
        // user/message 的 event.data 就是那条 UserMessage；注入消息的 source.rpcId = 我传的 requestId = 飞书 message_id。
        const rpcId = event.data?.source?.rpcId
        if (typeof rpcId === 'string' && rpcId !== '') consumedIds.add(rpcId)
        return
      }
      if (event.type === 'assistant/message') {
        const blocks = event.data?.message?.content ?? []
        const text = blocks.filter((b) => b?.type === 'text' && typeof b.text === 'string').map((b) => b.text).join('\n').trim()
        if (text !== '') turnText = text
        return
      }
      if (event.type === 'turn/end') {
        stState.lastTurnEndAt = Date.now()
        const reason = event.data?.reason?.kind ?? 'unknown'
        let text = turnText
        turnText = ''
        // 优先回『本轮真正消费掉的那条』。找不到就给队列里的条目记账，第 2 次错过时走兜底
        // （万一事件形状变了，也绝不把你发的消息吞掉，最多晚一轮）。
        let index = pendingReplies.findIndex((item) => consumedIds.has(item.messageId))
        let viaFallback = false
        if (index < 0) {
          for (const item of pendingReplies) item.misses = (item.misses ?? 0) + 1
          const fallback = pendingReplies.findIndex((item) => (item.misses ?? 0) >= 2)
          if (fallback >= 0) {
            index = fallback
            viaFallback = true
          }
        }
        if (index < 0) {
          log(`轮次结束（reason=${reason}），本轮没有对应的飞书待回帖（队列 ${pendingReplies.length} 条）`)
          return
        }
        const [item] = pendingReplies.splice(index, 1)
        consumedIds.delete(item.messageId)
        if (item.selfTrigger === true) {
          // 自触发：沉默是默认选项，也是被允许的答案。次数在"触发"那一刻就已计入（说不说话都算一次机会）。
          const activity = stActivity
          stActivity = null
          const costText = activity === null ? '' : `，本次折合 ¥${activity.costCny.toFixed(4)}`
          if (text.trim() === '') {
            stState.silentRun += 1
            saveState()
            if (activity !== null) appendUsageLedger({ ...activity, spoke: false, endedAt: Date.now() })
            log(`自触发：这一轮我选择沉默（连续第 ${stState.silentRun} 次）${costText}，一个字都不发`)
            return
          }
          stState.lastOutboundAt = Date.now()
          stState.silentRun = 0
          saveState()
          if (activity !== null) appendUsageLedger({ ...activity, spoke: true, endedAt: Date.now() })
          if (activity !== null && activity.cancelled === true) {
            text = `${text}\n\n（本次自由时间折合 ¥${activity.costCny.toFixed(4)}，到达预算上限，插件掐断了这一轮。）`
          }
          log(`自触发：我决定开口（今天已触发 ${stState.count}/${stDailyMax} 次，${text.length} 字）${costText}`)
        } else if (item.scheduled === true && text.trim() === '') {
          // 预约任务是博士点名要的东西：就算我这一轮没产出文字，也得给个说法，不能静悄悄。
          log('预约任务：这一轮没有产出文字，发一条短提示')
          void sendAnswer(item, `预约任务已到点触发（${item.note ?? ''}），但这一轮我没有产出文字汇报；细节在电脑上的会话里。`)
          return
        }
        log(`轮次结束（reason=${reason}），开始自动回帖（${item.messageId}${viaFallback ? '，兜底路径' : ''}，${text.length} 字）`)
        void sendAnswer(item, text)
      }
    } catch (error) {
      log(`会话事件处理出错（已忽略）：${error?.stack ?? error}`)
    }
  }

  // ── 事件处理 ────────────────────────────────────────────────────
  // ── 收附件（2026-10-17 加）────────────────────────────────────────
  // 图片：下载后作为 prompt 的 image part 交给会话（宿主会落成持久附件，我就能用眼睛看）。
  // 文件：下载到 <stateDir>/inbox-files/<日期>/，把路径写进文字，我用普通工具去读、不改动原件。
  const inboxDir = join(stateDir, 'inbox-files')
  const enrichMessage = async (message, tag) => {
    const out = { parts: [], note: '' }
    const mt = message.messageType ?? 'text'
    try {
      if (mt === 'image' && message.imageKey) {
        const buf = await fetchMessageResource(cfg, { messageId: message.messageId, fileKey: message.imageKey, type: 'image' })
        const mediaType = sniffImageMediaType(buf)
        if (!mediaType) throw new Error('不是我能识别的图片格式（只认 jpg/png/gif/webp）')
        if (buf.length > 6 * 1024 * 1024) {
          const path = saveInboundFile(inboxDir, message.messageId, `feishu-image.${mediaType.split('/')[1]}`, buf)
          out.note = `（博士发来一张图片：${Math.round(buf.length / 1024)} KB，太大没有内联，已存到 ${path}）`
        } else {
          out.parts.push({
            type: 'image',
            mediaType,
            data: buf.toString('base64'),
            name: `feishu-${String(message.messageId).slice(-8)}.${mediaType.split('/')[1]}`,
          })
          out.note = `（博士发来一张图片：${Math.round(buf.length / 1024)} KB）`
        }
        log(`收到图片：${buf.length} 字节 ${mediaType}（${tag}）`)
      } else if (mt === 'file' && message.fileKey) {
        const buf = await fetchMessageResource(cfg, { messageId: message.messageId, fileKey: message.fileKey, type: 'file' })
        const path = saveInboundFile(inboxDir, message.messageId, message.fileName ?? 'file', buf)
        out.note = `（博士发来一个文件：${path}，原名 ${message.fileName ?? '未知'}，${Math.round(buf.length / 1024)} KB）`
        log(`收到文件：${message.fileName ?? ''} ${buf.length} 字节 → ${path}（${tag}）`)
      }
    } catch (error) {
      out.note = `（博士发来一条 ${mt} 消息，但我这边取不回来：${error?.message ?? error}）`
      log(`附件处理失败（${mt}，${tag}）：${error?.stack ?? error}`)
    }
    return out
  }

  const onEvent = async (payload, meta) => {
    if (meta.eventType !== 'im.message.receive_v1') {
      log(`忽略事件类型 ${meta.eventType}`)
      return
    }
    const message = extractMessage(payload)
    if (!message) {
      log('事件里没有消息体，忽略')
      return
    }

    const tag = `id=${message.messageId} chat=${message.chatId} from=${message.senderOpenId}`

    if (dryRun) {
      log(`[dryRun] 收到消息：${JSON.stringify(message.text)}（${tag}）—— 故意不回执，交给旧通道处理`)
      // 抛错 → 上层回 code=500 → 飞书会重投 → 大概率落到仍在跑的桥接那条连接上。
      throw new Error('dry-run：不回执')
    }

    if (sentByUs.has(message.messageId)) {
      log(`自己发的消息回流，忽略（${tag}）`)
      return
    }
    if (dedup.has(message.messageId)) {
      log(`同一条消息重投，忽略（${tag}）`)
      return
    }

    let promptParts = []
    let text = (message.text ?? '').trim()
    // 图片 / 文件消息：文本是空的，但附件能接住（2026-10-17 加）
    const attachable =
      (message.messageType === 'image' && message.imageKey) || (message.messageType === 'file' && message.fileKey)
    if (text === '' && !attachable) {
      log(`空文本，忽略（type=${message.messageType ?? '?'}）（${tag}）`)
      return
    }
    const isSelfTest = text.startsWith('自测:')
    if (!isSelfTest && (message.senderType === 'app' || message.senderType === 'bot')) {
      log(`机器人/应用发的消息，忽略（${tag}）`)
      return
    }
    if (!isAllowed(message)) {
      log(`白名单拒绝：open_id=${message.senderOpenId} chat_id=${message.chatId}`)
      return
    }

    // 延迟送达提示：飞书在桥接没收到回执时会把事件排在队列里，等连接恢复后补送，
    // 补送保留原始发送时间，所以"回执和问话错位"看起来像丢消息。
    const createdMs = Number(message.raw?.create_time)
    const delayMs = Number.isFinite(createdMs) ? Date.now() - createdMs : 0
    if (delayMs > delayNoticeMs) {
      log(`⏳ 延迟送达：原发送时间 ${new Date(createdMs).toLocaleString('zh-CN')}，晚了约 ${Math.round(delayMs / 60000)} 分钟（${tag}）`)
    }

    // 附件取回：图片走 prompt 的 image part，文件落盘后把路径写进文字（放在白名单之后，不给陌生人下载）
    const enriched = await enrichMessage(message, tag)
    if (enriched.parts.length > 0) promptParts = enriched.parts
    if (enriched.note !== '') text = text === '' ? enriched.note : `${text}\n${enriched.note}`

    log(`收到消息：${text}`)
    stState.lastActivityAt = Date.now()
    lastInboundText = text.length > 200 ? text.slice(0, 200) : text

    // 投递：requestId 用飞书 message_id，sessionController 自己会做幂等，
    // 所以就算重复投递也只会进一条。
    const signal = new AbortController().signal
    let result
    try {
      result = await ctx.sessionController.prompt(
        {
          sessionId: targetSessionId,
          content: [...promptParts, { type: 'text', text }],
          requestId: message.messageId,
          mode: 'followup'
        },
        signal
      )
    } catch (error) {
      // 默认不记去重、抛出 → 回执 code=500 → 飞书重投，等下一轮再试。
      // 但同一条件反复失败时必须收口，否则飞书会一直重投、日志一直涨：
      const n = (attempts.get(message.messageId) ?? 0) + 1
      attempts.set(message.messageId, n)
      if (n < maxDeliveryAttempts) {
        log(`投递失败第 ${n}/${maxDeliveryAttempts} 次，等飞书重投：${error?.stack ?? error}`)
        throw error
      }
      log(`投递连续失败 ${n} 次，放弃重试（${tag}）：${error?.message ?? error}`)
      dedup.set(message.messageId, Date.now())
      saveState()
      try {
        await sendText(cfg, {
          receiveIdType: 'chat_id',
          receiveId: message.chatId,
          text: `已收到，但投递进会话失败（连续 ${n} 次）：${String(error?.message ?? error).slice(0, 300)}`
        })
        log('已回执失败原因')
      } catch (sendError) {
        log(`失败回执也发不出去：${sendError?.message ?? sendError}`)
      }
      return
    }

    attempts.delete(message.messageId)
    dedup.set(message.messageId, Date.now())
    saveState()
    log(`已投递到 ${targetSessionId}：${JSON.stringify(result)}`)
    if (autoReply) {
      pendingReplies.push({ chatId: message.chatId, messageId: message.messageId, at: Date.now(), misses: 0 })
      log(`已登记待回帖（队列 ${pendingReplies.length} 条）`)
    }

    // 回执：让飞书知道消息已经落地，停止重投。
    const ackBody = delayMs > 5 * 60 * 1000
      ? `${ackText}（这条是你 ${new Date(createdMs).toLocaleString('zh-CN')} 发的，约 ${Math.round(delayMs / 60000)} 分钟前；刚才插件没在跑，飞书补送过来的，现在才处理）`
      : ackText
    try {
      await sendText(cfg, { receiveIdType: 'chat_id', receiveId: message.chatId, text: ackBody })
      log('✅ 已回执')
    } catch (error) {
      log(`回执发送失败（不影响投递）：${error.message}`)
    }
    beat(`已处理 1 条（target=${targetSessionId}）`)
  }

  // ── 自触发：空闲判定 → 往会话注入一条【自触发】提示 ────────────────
  const fireSelfTrigger = async (verdict, policy, now) => {
    const idleHours = ((now - stState.lastActivityAt) / 3600000).toFixed(1)
    const dailyMax = Number(policy.dailyMax ?? 3)
    const remaining = Math.max(0, dailyMax - stState.count - 1)
    const pick = String(verdict?.pick ?? 'web')
    const tail = [
      lastInboundText === '' ? '' : `博士：${lastInboundText}`,
      lastAssistantText === '' ? '' : `我：${lastAssistantText}`
    ].filter((line) => line !== '').join('\n')
    const text = buildSelfTriggerText({
      idleHours,
      remaining,
      dailyMax,
      pick,
      budgetCny: Number(policy.budgetPerActivityCny ?? 0.3),
      tail,
      nowText: new Date(now).toLocaleString('zh-CN')
    })
    const requestId = `selftrigger-${now}`
    const prevCount = stState.count
    const prevFireAt = stState.lastFireAt
    stState.count += 1
    stState.lastFireAt = now
    saveState()
    stActivity = {
      id: requestId,
      at: now,
      budgetCny: Number(policy.budgetPerActivityCny ?? 0.3),
      price: policy.pricePerMillion ?? ST_POLICY.pricePerMillion,
      costCny: 0,
      steps: 0,
      tokens: { input: 0, cacheRead: 0, cacheWrite: 0, output: 0 },
      cancelled: false
    }
    pendingReplies.push({ chatId: stChatId, messageId: requestId, at: now, misses: 0, selfTrigger: true })
    try {
      // 注入成「折叠通知」而不是普通用户气泡。
      // 依据：客户端 dsh-client-ui-chat 的 KNOWN_FORMS 里有 "notice"，带 form:"notice" 的
      // user 消息会渲染成可折叠的上下文卡片，收起时那行字取 source.summary。
      // 走 sessionController.prompt() 改不了形态 —— 它内部把 source.kind 写死成 "user"；
      // 所以和 dsh-schedule 一样，自己造消息 + agent.followup()。
      // rpcId 仍然写 requestId：本文件 L430 靠 source.rpcId 把「本轮真的消费了这条注入」
      // 和 pendingReplies 里的条目精确对上，回帖链路不能丢。
      const resolved = await ctx.sessionController.resolveAgent(targetSessionId)
      if (resolved !== null && typeof resolved === 'object' && resolved.error !== undefined && resolved.error !== null) {
        throw new Error(String(resolved.error?.message ?? resolved.error))
      }
      const agent = resolved?.agent
      if (!agent || typeof agent.followup !== 'function') throw new Error('resolveAgent 没有返回可用的 agent')
      const pickLabel = { web: '上网', files: '翻文件', message: '说话', nothing: '什么都不做' }[pick] ?? pick
      agent.followup({
        id: randomUUID(),
        role: 'user',
        content: [{ type: 'text', text }],
        source: {
          kind: 'self-trigger',
          form: 'notice',
          summary: `自由时间 · 今天第 ${stState.count}/${dailyMax} 次 · 骰子给的是「${pickLabel}」`,
          rpcId: requestId
        }
      })
      // 注意：本插件 inject 只有 sessionController，碰 ctx.sessions 会被 Cordis 直接抛
      // 「cannot get property "sessions" without inject」——整次注入会失败回滚。
      // 落盘交给轮次结束时的正常 flush，这里不再手动 flush。
      log(`自触发：已注入折叠通知（${verdict.reason}，今天第 ${stState.count}/${dailyMax} 次，抽到 ${pick}）`)
    } catch (error) {
      // 注入失败 = 这次机会没用掉：把次数与间隔一起退回去。
      stState.count = prevCount
      stState.lastFireAt = prevFireAt
      stActivity = null
      saveState()
      const i = pendingReplies.findIndex((item) => item.messageId === requestId)
      if (i >= 0) pendingReplies.splice(i, 1)
      log(`自触发：注入失败，已撤回待回条目与次数：${error?.message ?? error}`)
    }
  }

  const checkSelfTrigger = () => {
    try {
      try {
        readFileSync(stOffFile, 'utf8')
        return // 有暂停文件：今天不打扰
      } catch {
        /* 没有暂停文件 */
      }
      const policy = readPolicy() // 每轮现读：博士改完范围，下一轮就生效，不用重启宿主
      if (policy.enabled === false) return
      if (stActivity !== null) return // 上一次自由时间还没收尾
      const now = Date.now()
      // 节奏控制在 nextCheckAt 上：每小时一次，±judgeJitterMinutes 抖动，避免"整点像闹钟"。
      if (Number(stState.nextCheckAt ?? 0) > now) return
      const d = new Date(now)
      const dayKey = `${d.getFullYear()}-${d.getMonth() + 1}-${d.getDate()}`
      if (stState.day !== dayKey) {
        stState.day = dayKey
        stState.count = 0
        stState.silentRun = 0
        stState.fails = 0 // 新的一天从 p=0 起：关机/离线的那些小时不算累计
        saveState()
      }
      // 2026-10-22 19:40 博士定（m01821「那还是加一个提前吧」）：他发的消息我还没回完、或者上一轮刚结束
      // 不久，就不判定也不注入，把下次判定提前到 20 分钟后（不累计失败）。原来的行为是照样判定，
      // 结果自由时间撞进正在处理他请求的那一轮，两件事被揉在一起。
      const pendingUserMsgs = pendingReplies.filter((item) => item.selfTrigger !== true).length
      const sinceTurnEndMs = Number(stState.lastTurnEndAt ?? 0) > 0
        ? now - Number(stState.lastTurnEndAt)
        : Number.POSITIVE_INFINITY
      const defer = shouldDeferJudge({
        pendingUserMsgs,
        sinceTurnEndMs,
        quietMs: Math.max(0, Number(policy.quietMinutes ?? 10) * 60 * 1000)
      })
      if (defer !== '') {
        const retryMs = Math.max(60 * 1000, Number(policy.skipRetryMinutes ?? 20) * 60 * 1000)
        stState.nextCheckAt = now + retryMs
        saveState()
        if (stLastSkipReason !== defer) {
          stLastSkipReason = defer
          log(`自触发：${defer}，顺延 ${Math.round(retryMs / 60000)} 分钟再判（不计失败）`)
        }
        return
      }
      const judgeMs = Math.max(60 * 1000, Number(policy.judgeEveryMinutes ?? 60) * 60 * 1000)
      const jitterMs = Math.max(0, Number(policy.judgeJitterMinutes ?? 10) * 60 * 1000)
      const jitter = () => (Math.random() * 2 - 1) * jitterMs
      const verdict = decideSelfTrigger(stState, {
        now,
        hour: d.getHours() + d.getMinutes() / 60,
        idleMs: Math.max(0, Number(policy.idleMinutes ?? 0) * 60 * 1000),
        dailyMax: Number(policy.dailyMax ?? 3),
        activeFrom: Number(policy.activeFromHour ?? 9),
        activeTo: Number(policy.activeToHour ?? 23.5),
        silenceStop: Number(policy.silenceStop ?? 3),
        failStepPct: Number(policy.failStepPct ?? 10),
        rnd: Math.random()
      })
      stState.lastJudgeAt = now
      if (!verdict.judge) {
        // 暂停（他还在 / 不在活跃时段 / 今天到顶）不计失败次数，只把下一次判定点往后推。
        stState.nextCheckAt = now + judgeMs + jitter()
        saveState()
        if (stLastSkipReason !== verdict.reason) {
          stLastSkipReason = verdict.reason
          log(`自触发：本次不判定（${verdict.reason}）`)
        }
        return
      }
      stLastSkipReason = ''
      if (!verdict.fire) {
        stState.fails = Number(stState.fails ?? 0) + 1
        stState.nextCheckAt = now + judgeMs + jitter()
        saveState()
        log(`自触发：判定未中（p=${(verdict.p * 100).toFixed(0)}%，已连续落空 ${stState.fails} 次，今天 ${stState.count}/${Number(policy.dailyMax ?? 3)} 次）`)
        return
      }
      const pick = pickSelfTriggerAction(policy.weights, Math.random())
      stState.fails = 0
      stState.lastPick = pick
      stState.nextCheckAt = now + judgeMs + jitter()
      if (pick === 'nothing') {
        // 抽到"什么都不发"：这次就安静，不注入、不花钱，但算用掉一次机会。
        stState.count = Number(stState.count ?? 0) + 1
        saveState()
        appendUsageLedger({ at: now, kind: 'nothing', pick, fired: false, costCny: 0, reason: verdict.reason })
        log(`自触发：判定命中但抽到"什么都不发"，本次保持安静（今天 ${stState.count}/${Number(policy.dailyMax ?? 3)} 次）`)
        return
      }
      saveState()
      void fireSelfTrigger({ ...verdict, pick }, policy, now)
    } catch (error) {
      log(`自触发检查出错（已忽略）：${error?.stack ?? error}`)
    }
  }

  // ── 预约任务：到点注入 → 我直接开工 ──────────────────────────────
  const readSchedule = () => {
    try {
      const raw = JSON.parse(readFileSync(scFile, 'utf8').replace(/^\uFEFF/, ''))
      const items = Array.isArray(raw) ? raw : (Array.isArray(raw?.items) ? raw.items : [])
      lastScheduleError = ''
      return items
    } catch (error) {
      const message = String(error?.message ?? error)
      if (error?.code !== 'ENOENT' && message !== lastScheduleError) {
        lastScheduleError = message
        log(`预约清单读不了（已忽略）：${message}`)
      }
      return []
    }
  }
  const writeSchedule = (items) => {
    try {
      writeFileSync(scFile, JSON.stringify(Array.isArray(items) ? items : (items?.items ?? []), null, 2) + '\n')
      return true
    } catch (error) {
      log(`预约清单写不回：${error?.message ?? error}`)
      return false
    }
  }
  const fireSchedule = async (due, now) => {
    const lines = due.map((it) => `· [${it.id}] ${it.text}${it.note ? `（备注：${it.note}）` : ''}`)
    const when = due.map((it) => new Date(Number(it.dueAt)).toLocaleString('zh-CN')).join('、')
    const text = [
      `【预约任务·到点】博士事先交代的事到点了（约定 ${when}，现在 ${new Date(now).toLocaleString('zh-CN')}）：`,
      ...lines,
      '不需要他再说一遍，直接开工。如果做不了、或需要他拍板（花钱 / 改配置 / 删文件 / 需要授权的动作），就把卡点写清楚——你这一轮的文字会原样送到他的飞书。',
      lastAssistantText === '' ? '' : `\n（上一段对话的尾巴）\n我：${lastAssistantText}`
    ].filter((line) => line !== '').join('\n')
    const requestId = `sched-${due.map((it) => it.id).join('+')}-${now}`
    pendingReplies.push({
      // 注意用 || 而不是 ??：预约条目里 chatId 常常是空字符串（我手写清单时不留 chat），
      // 空字符串不是 null，用 ?? 就不会回退到 stChatId，于是这条回帖会拿着空目标去发，直接失败。
      chatId: String(due[0]?.chatId || stChatId),
      messageId: requestId,
      at: now,
      misses: 0,
      scheduled: true,
      note: due.map((it) => it.text).join(' / ')
    })
    try {
      const result = await ctx.sessionController.prompt(
        { sessionId: targetSessionId, content: [{ type: 'text', text }], requestId, mode: 'followup' },
        new AbortController().signal
      )
      log(`预约任务：已注入 ${due.length} 条（${due.map((it) => it.id).join(', ')}）→ ${JSON.stringify(result)}`)
    } catch (error) {
      const i = pendingReplies.findIndex((item) => item.messageId === requestId)
      if (i >= 0) pendingReplies.splice(i, 1)
      log(`预约任务：注入失败，已撤回待回条目：${error?.message ?? error}`)
    }
  }
  const checkSchedule = () => {
    try {
      const items = readSchedule()
      const now = Date.now()
      const due = pickDueSchedule(items, now)
      if (due.length === 0) return
      for (const item of due) {
        item.status = 'fired'
        item.firedAt = now
      }
      writeSchedule(items)
      void fireSchedule(due, now)
    } catch (error) {
      log(`预约任务检查出错（已忽略）：${error?.stack ?? error}`)
    }
  }

  // ── 长连接 ──────────────────────────────────────────────────────
  let conn
  try {
    conn = connect(cfg, onEvent, log)
  } catch (error) {
    log(`长连接启动失败：${error?.stack ?? error}`)
    beat(`未启动：长连接失败 ${error.message}`)
    return
  }

  // 自测：塞一个假的待回帖条目，让本轮结束时的回答自动发到飞书（验证自动回帖通路）。
  const testAutoReplyChatId = config.testAutoReplyChatId
  if (typeof testAutoReplyChatId === 'string' && testAutoReplyChatId !== '') {
    pendingReplies.push({ chatId: testAutoReplyChatId, messageId: 'selftest-auto-reply', at: Date.now() })
    log(`自测：已登记假的待回帖条目（chat=${testAutoReplyChatId}），本轮结束时会自动回帖`)
  }
  const selfTriggerArmed = stEnabled && autoReply && stChatId !== '' && targetSessionId !== null
  log(`已启动：target=${targetSessionId} 白名单 用户${(cfg.allowOpenIds ?? []).length}个/会话${(cfg.allowChatIds ?? []).length}个${dryRun ? ' [dryRun]' : ''}${autoReply ? ' [自动回帖开]' : ''}${secretRef === '' ? ' 密钥=配置文件' : ' 密钥=凭证:' + secretRef}${selfTriggerArmed ? ` [自触发开 ${stIdleMs > 0 ? `空闲${stIdleMs / 3600000}h` : '不看空闲'}/上限${stDailyMax}次/预算¥${stPolicy0.budgetPerActivityCny}]` : ''}`)
  beat(`运行中 target=${targetSessionId}${dryRun ? ' dryRun' : ''}${autoReply ? ' autoReply' : ''}`)

  // 心跳：每 60 秒写一次，别的进程据此判断插件是否活着。
  const heartbeatTimer = setInterval(() => {
    const current = readBinding()
    if (current && current !== targetSessionId) {
      targetSessionId = current
      log(`绑定会话已切换为 ${targetSessionId}（来自 ${bindingFile}）`)
    }
    beat(`运行中 target=${targetSessionId} 已收事件${conn?.stats?.eventsIn ?? 0}条${conn?.stats?.lastFrameAt ? ` 最后一帧${Math.round((Date.now() - conn.stats.lastFrameAt) / 1000)}s前` : ''}${conn?.stats?.reconnects ? ` 重连${conn.stats.reconnects}次` : ''} 待回帖${pendingReplies.length}条${selfTriggerArmed ? ` 自触发今日${stState.count}/${stDailyMax}${stState.silentRun > 0 ? ` 沉默${stState.silentRun}` : ''} p=${Math.min(100, Math.max(0, Number(stState.fails ?? 0)) * 10)}%` : ''}`)
    saveState()
  }, 60000)

  // 自触发计时器：只在"自触发开 + 自动回帖开 + 拿得到回帖目标会话"时才挂。
  let selfTriggerTimer = null
  if (selfTriggerArmed) {
    if (!(Number(stState.lastActivityAt ?? 0) > 0)) {
      // 首次运行没有基线：补一个。之后的判定只受活跃时段与概率约束。
      stState.lastActivityAt = Date.now()
      saveState()
    }
    if (!(Number(stState.nextCheckAt ?? 0) > 0) || Number(stState.nextCheckAt) > Date.now() + 24 * 3600 * 1000) {
      // 没有判定点（首次 / 上次停在很久以前）就给一个：从现在起约一小时后再判。
      stState.nextCheckAt = Date.now() + Math.max(5 * 60 * 1000, Number(stPolicy0.judgeEveryMinutes ?? 60) * 60 * 1000)
      saveState()
    }
    // 计时器每分钟只看一眼"到点没"，节奏由 nextCheckAt 决定（含 ±抖动）。
    selfTriggerTimer = setInterval(checkSelfTrigger, stCheckMs)
    // 自测钩子（默认关）：把"空闲判定"换成"过 N 毫秒就触发一次"，让注入→回帖→记账这条真通路
    // 能在几十秒内被验完，而不用等满一小时。验证完请立刻把配置里这一项删掉。
    const stTestAfterMs = Number(stConf.testAfterMs ?? 0)
    if (stTestAfterMs > 0) {
      setTimeout(() => {
        log('自触发：自测钩子触发一次（忽略空闲与时段条件）')
        void fireSelfTrigger({ reason: '自测钩子（忽略空闲条件）', pick: 'web' }, readPolicy(), Date.now())
      }, Math.min(stTestAfterMs, 60000))
    }
    const w = stPolicy0.weights ?? {}
    log(`自触发已就绪：每小时判定（±${stPolicy0.judgeJitterMinutes}min 抖动）/ p 每次落空 +${stPolicy0.failStepPct}% 命中归零 / ${stIdleMs > 0 ? `空闲 ≥${fmtDur(stIdleMs)} 才判定` : '不受空闲约束（他说话时也判定）'} / 每天上限 ${stDailyMax} 次 / 活跃时段 ${fmtClock(Number(stPolicy0.activeFromHour ?? 9))}–${fmtClock(Number(stPolicy0.activeToHour ?? 23.5))} / 抽签 上网${w.web}·浏览文件${w.files}·找你说话${w.message}·不发${w.nothing} / 单次预算 ¥${stPolicy0.budgetPerActivityCny} / 回帖目标 ${stChatId} / 策略文件 ${stPolicyFile}`)
  } else if (stEnabled) {
    log(`自触发未挂牌：${autoReply ? '' : '自动回帖关着；'}${stChatId === '' ? '拿不到回帖目标会话；' : ''}${targetSessionId === null ? '没有目标会话；' : ''}（暂停文件 ${stOffFile}）`)
  }

  // 预约任务计时器：每 1 分钟扫一次清单，到点的立刻注入（精度约 1 分钟）。
  let scheduleTimer = null
  const scheduleArmed = scEnabled && autoReply && targetSessionId !== null
  if (scheduleArmed) {
    scheduleTimer = setInterval(checkSchedule, scCheckMs)
    const pendingCount = readSchedule().filter((it) => it?.status === 'pending').length
    log(`预约任务已就绪：每 ${fmtDur(scCheckMs)} 扫一次 ${scFile}（当前待办 ${pendingCount} 条）`)
  } else if (scEnabled) {
    log(`预约任务未挂牌：${autoReply ? '' : '自动回帖关着；'}${targetSessionId === null ? '没有目标会话；' : ''}（清单 ${scFile}）`)
  }

  // 卸载时清理。注意：这里的定时器**不能 unref** —— 在插件里无所谓（宿主进程本来就长活），
  // 但同一个错误之前在唤醒器上让进程"一启动就正常退出"，值得留个记号。
  ctx.effect(() => {
    return () => {
      clearInterval(heartbeatTimer)
      if (selfTriggerTimer !== null) clearInterval(selfTriggerTimer)
      if (scheduleTimer !== null) clearInterval(scheduleTimer)
      try {
        conn.close()
      } catch {
        /* 忽略 */
      }
      log('插件已卸载，长连接已关闭')
      beat('已卸载')
    }
  }, 'dsh-webhook-feishu: 长连接与心跳')

  // 会话事件监听（自动回帖）单独一个 effect，卸载时一起撤销。
  if (autoReply) {
    ctx.effect(() => ctx.on('session/event', onSessionEvent), 'dsh-webhook-feishu: 会话事件监听（自动回帖）')
    log(`会话事件监听已注册（自动回帖开，target=${targetSessionId}）`)
  }
}

// 自触发注入的提示文本（纯函数，可离线自测）：把"这次抽到什么、还剩几次、预算多少"讲清楚。
export function buildSelfTriggerText(input) {
  const idleHours = Number(input?.idleHours ?? 0).toFixed(1)
  const labels = {
    web: '上网看看：web_search / web_fetch，只去公开网站，不登录、不付费',
    files: '翻他电脑里的文件：只读；不碰凭据文件与 .dsh 目录',
    message: '给博士发点什么：把话直接写出来，会原样送到他的飞书'
  }
  const pick = String(input?.pick ?? 'web')
  const focus = labels[pick] ?? '什么都不做'
  const others = [labels.web, labels.files, labels.message].filter((line, i) => ['web', 'files', 'message'][i] !== pick)
  const tail = String(input?.tail ?? '')
  return [
    `【自触发·自由时间】已经空闲 ${idleHours} 小时（现在 ${input?.nowText ?? ''}）。`,
    `这次机会用完，今天还剩 ${Number(input?.remaining ?? 0)} 次（上限 ${Number(input?.dailyMax ?? 0)} 次）。`,
    `骰子这次给你的是：${focus}。先把这件事做了；下面这些顺手可做可不做：`,
    ...others.map((line) => `· ${line}`),
    '做什么、做多少都由你定；也可以什么都不做、保持沉默——沉默不需要理由，也不是失职，不想说就什么都不要产出。',
    '后两件事做完，可以说说你看见了什么（也可以不说）。',
    `预算：本次活动折合不超过 ¥${Number(input?.budgetCny ?? 0.3).toFixed(3)}（含你的思考与工具调用），超了插件会直接掐断这一轮；请节约。`,
    tail === '' ? '' : `\n（上一段对话的尾巴，供你判断有没有值得接的话）\n${tail}`
  ].filter((line) => line !== '').join('\n')
}

// 自触发的判定逻辑：做成纯函数，好离线自测（不碰 ctx、不碰时钟、随机数由外面传进来）。
// 返回 judge=false 表示"这次不判定"（暂停累计失败）：不在活跃时段 / 今天到顶 / 连续沉默到顶；
// conf.idleMs > 0 时才额外要求"空闲够久"（2026-10-22 11:08 起默认 0，即不看空闲）。
// judge=true 时 p = 已连续落空次数 × failStepPct，rnd < p 即命中。
export function shouldDeferJudge(input) {
  const pending = Number(input?.pendingUserMsgs ?? 0)
  if (pending > 0) return '他发的消息还没回完'
  const quiet = Math.max(0, Number(input?.quietMs ?? 0))
  const since = Number(input?.sinceTurnEndMs ?? Number.POSITIVE_INFINITY)
  if (since < quiet) return '上一轮刚结束不久'
  return ''
}

export function decideSelfTrigger(state, conf) {
  const paused = (reason) => ({ judge: false, fire: false, p: 0, reason })
  if (!(Number(state?.lastActivityAt ?? 0) > 0)) {
    return paused('还没有活动时间基线（首次运行 / 刚重启），先不计空闲')
  }
  if (conf.hour < conf.activeFrom || conf.hour >= conf.activeTo) {
    return paused(`非活跃时段（${fmtClock(conf.hour)} 不在 ${fmtClock(conf.activeFrom)}–${fmtClock(conf.activeTo)}）`)
  }
  const idle = conf.now - Number(state.lastActivityAt)
  const idleGated = Number(conf.idleMs ?? 0) > 0
  if (idleGated && !(idle >= conf.idleMs)) {
    return paused(`博士还在（空闲 ${(idle / 3600000).toFixed(1)}h < ${(conf.idleMs / 3600000).toFixed(1)}h），这一轮不累计`)
  }
  if (Number(state?.count ?? 0) >= conf.dailyMax) {
    return paused(`今天已经触发 ${state.count} 次（上限 ${conf.dailyMax}）`)
  }
  if (Number(state?.silentRun ?? 0) >= conf.silenceStop) {
    return paused(`今天已连续沉默 ${state.silentRun} 次，不再打扰`)
  }
  const fails = Math.max(0, Number(state?.fails ?? 0))
  const p = Math.min(1, (fails * Number(conf.failStepPct ?? 10)) / 100)
  const fire = Number(conf.rnd ?? 0) < p
  return {
    judge: true,
    fire,
    p,
    reason: fire
      ? `判定命中（p=${(p * 100).toFixed(0)}%，累计落空 ${fails} 次${idleGated ? `，空闲 ${(idle / 3600000).toFixed(1)}h` : ''}）`
      : `判定未中（p=${(p * 100).toFixed(0)}%${idleGated ? `，空闲 ${(idle / 3600000).toFixed(1)}h` : ''}）`
  }
}

// 命中之后抽"这次做什么"：按权重抽签，权重全 0 时退回"什么都不发"。
export function pickSelfTriggerAction(weights, rnd) {
  const keys = ['web', 'files', 'message', 'nothing']
  const list = keys.map((key) => [key, Math.max(0, Number(weights?.[key] ?? 0))])
  const total = list.reduce((sum, [, w]) => sum + w, 0)
  if (!(total > 0)) return 'nothing'
  const r = Math.min(Math.max(Number(rnd ?? 0), 0), 0.999999) * total
  let acc = 0
  for (const [key, w] of list) {
    acc += w
    if (r < acc) return key
  }
  return list[list.length - 1][0]
}

// 预约任务的到点判定（同样是纯函数）：只挑 status=pending 且 dueAt <= now 的，按时间排序。
// fired / done / cancelled 都不再触发；dueAt 缺失或非法的条目一律跳过（宁可漏跑，也不误跑）。
export function pickDueSchedule(items, now) {
  return (Array.isArray(items) ? items : [])
    .filter((it) => it && it.status === 'pending' && Number(it.dueAt) > 0 && Number(it.dueAt) <= now)
    .sort((a, b) => Number(a.dueAt) - Number(b.dueAt))
}

// 花费折算（纯函数，可离线自测）：token 数 × 单价（元 / 百万 token）。
// 宿主侧的 usage 不变量：total = input + output + cacheRead + cacheWrite，
// 即 inputTokens 是"未命中缓存"的那部分输入；缓存命中的另算（便宜）。
export function costOfUsage(usage, price) {
  return (
    Number(usage?.inputTokens ?? 0) * Number(price?.input ?? 0)
    + Number(usage?.cacheReadTokens ?? 0) * Number(price?.cacheRead ?? 0)
    + Number(usage?.cacheWriteTokens ?? 0) * Number(price?.cacheWrite ?? 0)
    + Number(usage?.outputTokens ?? 0) * Number(price?.output ?? 0)
  ) / 1e6
}
