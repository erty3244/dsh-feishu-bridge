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
  // 回帖兜底目标：条目里的 chatId 是空串时用它（白名单里的第一个会话），绝不拿空目标去发。
  const defaultChatId = String((cfg.allowChatIds ?? [])[0] ?? '')

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
    log(`状态已载入：去重 ${dedup.size} 条 / 自发 ${sentByUs.size} 条`)
  } catch {
    /* 首次运行没有状态文件 */
  }
  const saveState = () => {
    try {
      writeFileSync(statePath, JSON.stringify({ dedup: Object.fromEntries(dedup), sentByUs: Object.fromEntries(sentByUs) }, null, 0))
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
        // 兜一层：条目里没有 chatId（空串）时回落到默认回帖目标，绝不出手就发空目标。
        const sent = await sendText(cfg, { receiveIdType: 'chat_id', receiveId: item.chatId || defaultChatId, text: head + chunks[i] })
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
  log(`已启动：target=${targetSessionId} 白名单 用户${(cfg.allowOpenIds ?? []).length}个/会话${(cfg.allowChatIds ?? []).length}个${dryRun ? ' [dryRun]' : ''}${autoReply ? ' [自动回帖开]' : ''}${secretRef === '' ? ' 密钥=配置文件' : ' 密钥=凭证:' + secretRef}`)
  beat(`运行中 target=${targetSessionId}${dryRun ? ' dryRun' : ''}${autoReply ? ' autoReply' : ''}`)

  // 心跳：每 60 秒写一次，别的进程据此判断插件是否活着。
  const heartbeatTimer = setInterval(() => {
    const current = readBinding()
    if (current && current !== targetSessionId) {
      targetSessionId = current
      log(`绑定会话已切换为 ${targetSessionId}（来自 ${bindingFile}）`)
    }
    beat(`运行中 target=${targetSessionId} 已收事件${conn?.stats?.eventsIn ?? 0}条${conn?.stats?.lastFrameAt ? ` 最后一帧${Math.round((Date.now() - conn.stats.lastFrameAt) / 1000)}s前` : ''}${conn?.stats?.reconnects ? ` 重连${conn.stats.reconnects}次` : ''} 待回帖${pendingReplies.length}条`)
    saveState()
  }, 60000)

  // 卸载时清理。注意：这里的定时器**不能 unref** —— 在插件里无所谓（宿主进程本来就长活），
  // 但同一个错误之前在唤醒器上让进程"一启动就正常退出"，值得留个记号。
  ctx.effect(() => {
    return () => {
      clearInterval(heartbeatTimer)
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
