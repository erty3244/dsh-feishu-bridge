/**
 * feishu.mjs —— 飞书开放平台的「最小客户端」（零第三方依赖）
 *
 * 为什么自己写而不装官方 SDK（@larksuiteoapi/node-sdk）：
 *   官方 SDK 会引入 axios / ws / protobufjs 等 7 个依赖包，而你要求「每次装插件都要确认来源安全」。
 *   长连接协议本身很薄（一次 HTTP 请求拿地址 + 一个二进制帧协议），
 *   Node 24 自带 WebSocket 和 fetch，所以第一版**不引入任何第三方包**，代码全部可审。
 *   如果后面发现协议细节太费劲，再评估装官方 SDK（那时我会先做安全审查）。
 *
 * 本文件里的标注约定：
 *   【已实测】= 我已经在这台电脑上真的发过请求、看到过响应
 *   【待验证】= 依据公开协议写的，等你的 App ID/Secret 到手后用真实连接核对
 */

import { readFileSync } from 'node:fs';

// ─────────────────────────────────────────────────────────────
// 一、配置读取
// ─────────────────────────────────────────────────────────────

/**
 * 读取 feishu-config.json。
 * 密钥单独放文件而不是写进代码/环境变量，理由：文件可以随时删除、权限可控、不会进聊天记录。
 */
/** v5：密钥可以在每次用时现取（先查 DSH 凭证，再回退配置文件明文）。 */
async function secretOf(cfg) {
  if (typeof cfg.resolveSecret === 'function') {
    const value = await cfg.resolveSecret();
    if (typeof value === 'string' && value !== '') return value;
  }
  return cfg.appSecret;
}

export function readConfig(configPath) {
  let raw;
  try {
    raw = readFileSync(configPath, 'utf8');
  } catch (error) {
    throw new Error(`读不到配置文件 ${configPath}：${error.message}`);
  }
  let cfg;
  try {
    cfg = JSON.parse(raw);
  } catch (error) {
    throw new Error(`配置文件不是合法 JSON：${error.message}`);
  }
  if (typeof cfg.appId !== 'string' || cfg.appId.trim() === '') {
    throw new Error('配置文件缺少 appId');
  }
  const hasPlainSecret = typeof cfg.appSecret === 'string' && cfg.appSecret.trim() !== '';
  const hasSecretRef = typeof cfg.appSecretRef === 'string' && cfg.appSecretRef.trim() !== '';
  if (!hasPlainSecret && !hasSecretRef) {
    throw new Error('配置文件既没有 appSecret 明文、也没有 appSecretRef（DSH 凭证名）');
  }
  // 默认值：白名单为空数组 = 谁都进不来（安全默认，宁可先收不到也不放行陌生人）
  cfg.allowOpenIds ??= [];
  cfg.allowChatIds ??= [];
  cfg.dshProfile ??= 'headless';
  cfg.dshWorkdir ??= process.cwd();
  cfg.taskTimeoutMs ??= 900000;   // 单任务 15 分钟上限
  cfg.debug ??= false;            // 诊断日志（会打印每条消息的来源，比较吵）
  // 【修 bug·2026-10-14】connect() 里读的是 cfg.dumpFrames，但这里从来没给过默认值，
  // 所以它恒为 undefined，"需要排查协议细节时把配置里的 dumpFrames 改成 true" 这句注释是空头支票。
  cfg.dumpFrames ??= false;       // true 时把长连接原始帧打印成十六进制（很吵，只在查协议时开）
  // 工作模式：inbox = 只把消息写进收件箱（丙方案，默认）；headless = 自己 spawn dsh 跑一轮（旧行为）。
  cfg.mode ??= 'inbox';
  if (cfg.mode !== 'inbox' && cfg.mode !== 'headless') {
    throw new Error(`配置里的 mode 只能是 "inbox" 或 "headless"，现在是：${cfg.mode}`);
  }
  // 只给 headless 模式用：spawn 出来的 dsh 子进程继承这个权限模式。
  // 对应 dsh-base/cordis.patch.yml:229 的 `mode: process.env.DSH_PERMISSION_MODE ?? 'workspace-write'`。
  cfg.dshPermissionMode ??= 'workspace-write';
  return cfg;
}

// ─────────────────────────────────────────────────────────────
// 二、飞书 REST API（用 Node 自带 fetch，不装 axios）
// ─────────────────────────────────────────────────────────────

const BASE = 'https://open.feishu.cn/open-apis';

/**
 * 取长连接的接入地址。
 *
 * 【已实测】用无效凭据 POST 这个地址，飞书返回 HTTP 200 + JSON：
 *   {"code":1000040346, "msg":"app_id is invalid", "data":{"URL":""}}
 * 注意三点：
 *   1. 必须是 POST（GET 会 404）——这是实测出来的，不是猜的；
 *   2. 请求体的字段名是 **大驼峰** AppID / AppSecret，不是 snake_case；
 *   3. 成功后接入地址在 **data.URL**（大写 URL），不是 data.url。
 *
 * @param {{appId:string, appSecret:string}} cfg
 * @returns {Promise<string>} 形如 wss://open.feishu.cn/connect?device_id=...&...&service_id=...
 */
export async function fetchWsEndpoint(cfg) {
  const response = await fetch('https://open.feishu.cn/callback/ws/endpoint', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    // 注释：飞书这个接口对 body 字段名的大小写敏感，写错了会报 app_id is invalid。
    body: JSON.stringify({ AppID: cfg.appId, AppSecret: await secretOf(cfg) }),
  });
  if (!response.ok) {
    throw new Error(`取长连接地址失败：HTTP ${response.status}`);
  }
  const body = await response.json();
  if (body.code !== 0 || !body.data?.URL) {
    throw new Error(`取长连接地址失败：code=${body.code} msg=${body.msg}`);
  }
  return { url: body.data.URL, clientConfig: body.data.ClientConfig ?? null };
}

/**
 * 取 tenant_access_token（应用身份令牌）。
 *
 * 发消息要用它做 Authorization: Bearer <token>。
 * 有效期约 2 小时；这个函数内部做了缓存，提前 5 分钟续期。
 *
 * 这个接口是飞书最老牌的接口，走的是 snake_case 字段：
 *   POST /auth/v3/tenant_access_token/internal  {app_id, app_secret} → {tenant_access_token, expire}
 * 【已实测】未验证（需要真实 App ID）。
 */
let tokenCache = { value: null, expiresAt: 0 };

export async function getTenantToken(cfg, { force = false } = {}) {
  const now = Date.now();
  if (!force && tokenCache.value && now < tokenCache.expiresAt) return tokenCache.value;

  const response = await fetch(`${BASE}/auth/v3/tenant_access_token/internal`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ app_id: cfg.appId, app_secret: await secretOf(cfg) }),
  });
  const body = await response.json();
  if (body.code !== 0 || !body.tenant_access_token) {
    throw new Error(`取 tenant_access_token 失败：code=${body.code} msg=${body.msg}`);
  }
  tokenCache = {
    value: body.tenant_access_token,
    // expire 单位是秒；提前 5 分钟（300 秒）视为过期，避免边界上刚好失效
    expiresAt: now + Math.max(0, (body.expire ?? 7200) - 300) * 1000,
  };
  return tokenCache.value;
}

/**
 * 发一条文本消息。
 *
 * receiveIdType 的含义（注释）：
 *   - 'open_id'  ：面向「某个用户」的稳定匿名 ID（同一个用户在同一个应用里恒定）
 *   - 'chat_id'  ：面向「某个会话/群」的 ID
 *   - 'user_id'  ：企业内工号类 ID（要求应用有对应权限）
 * 我们收到消息时用哪个字段，就原样用同一个字段回，最省事也最不容易错。
 *
 * 【已实测】未验证（需要真实 App ID）。
 */
export async function sendText(cfg, { receiveIdType, receiveId, text }) {
  const token = await getTenantToken(cfg);
  const response = await fetch(`${BASE}/im/v1/messages?receive_id_type=${receiveIdType}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json; charset=utf-8',
      Authorization: `Bearer ${token}`,
    },
    // 注释：飞书的消息体是「JSON 的字符串」——content 字段本身是个字符串，里面再放 JSON。
    // 写成对象会报错，这是飞书 API 里最容易踩的坑之一。
    body: JSON.stringify({
      receive_id: receiveId,
      msg_type: 'text',
      content: JSON.stringify({ text }),
    }),
  });
  const body = await response.json();
  if (body.code !== 0) {
    throw new Error(`发消息失败：code=${body.code} msg=${body.msg}`);
  }
  return body.data;
}

// ─────────────────────────────────────────────────────────────
// 三、长连接的二进制帧协议
// ─────────────────────────────────────────────────────────────

/**
 * 飞书长连接用的是自己定义的二进制格式，不是 WebSocket 文本消息。
 * 它基于 Google 的 Protocol Buffers 编码风格（"protobuf wire format"），
 * 但没有 .proto 文件，字段号是飞书私有的 —— 下面的编号是【待验证】的推测，
 * 等真实连接上来后用 debug 模式把帧打成十六进制核对。
 *
 * protobuf 的两种基本编码（注释）：
 *   1. varint（变长整数）：每字节低 7 位是数据，最高位表示「后面还有没有」。
 *      这样小数字只占 1 字节，大数字才变长，省流量。
 *   2. length-delimited：先写一个 varint 当长度，再跟那么多个字节。
 */

/** 写一个无符号 varint。 */
export function writeVarint(value) {
  const out = [];
  let n = BigInt(value);
  if (n < 0n) throw new Error('varint 不接受负数');
  do {
    // 0x7f = 低 7 位；0x80 = 最高位置 1 表示「还有下一字节」
    let byte = Number(n & 0x7fn);
    n >>= 7n;
    if (n > 0n) byte |= 0x80;
    out.push(byte);
  } while (n > 0n);
  return Buffer.from(out);
}

/** 读一个无符号 varint，返回 [值(BigInt), 新偏移]。 */
export function readVarint(buf, offset = 0) {
  let result = 0n;
  let shift = 0n;
  let pos = offset;
  for (;;) {
    if (pos >= buf.length) throw new Error('varint 越界（数据被截断）');
    const byte = buf[pos++];
    result |= BigInt(byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) break;
    shift += 7n;
  }
  return [result, pos];
}

/** 组装一个 length-delimited 字段：字段头 varint + 长度 varint + 内容。 */
function field(fieldNumber, payload) {
  const head = writeVarint((fieldNumber << 3) | 2); // 低 3 位 2 = length-delimited
  return Buffer.concat([head, writeVarint(payload.length), payload]);
}

/** varint 字段（低 3 位 0）。 */
function varintField(fieldNumber, value) {
  return Buffer.concat([writeVarint(fieldNumber << 3), writeVarint(value)]);
}

/**
 * 解析一个帧，返回一张「字段号 → 值」的表。
 * 【待验证】只按 wire type 0（varint）和 2（length-delimited）解析，
 * 遇到不认识的 wire type 会抛错而不是猜。
 */
export function parseFrame(buf) {
  const fields = [];
  let pos = 0;
  while (pos < buf.length) {
    let tag;
    [tag, pos] = readVarint(buf, pos);
    const fieldNumber = Number(tag >> 3n);
    const wireType = Number(tag & 0x7n);
    if (wireType === 0) {
      let value;
      [value, pos] = readVarint(buf, pos);
      fields.push({ fieldNumber, wireType, value });
    } else if (wireType === 2) {
      let length;
      [length, pos] = readVarint(buf, pos);
      const len = Number(length);
      const payload = buf.subarray(pos, pos + len);
      if (payload.length !== len) throw new Error('length-delimited 字段越界');
      pos += len;
      fields.push({ fieldNumber, wireType, payload });
    } else {
      throw new Error(`不支持的 protobuf wire type ${wireType}（字段 ${fieldNumber}），原始字节：${buf.toString('hex')}`);
    }
  }
  return fields;
}

/**
 * 长连接的帧格式（【已实测 + 官方 SDK 源码核对，2026-10-14】）
 *
 *   Frame {
 *     uint64 SeqID            = 1;   // 帧序号
 *     uint64 LogID            = 2;   // 服务端日志号，回包时原样带回
 *     int32  service          = 3;   // 服务标识（实测 33554678）
 *     int32  method           = 4;   // 0 = control（心跳等） 1 = data（事件等）；**不存在 2**
 *     repeated Header headers = 5;   // 每条 header 内部又是 {1:key, 2:value}
 *     string payload_encoding = 6;
 *     string payload_type     = 7;
 *     bytes  payload          = 8;   // 业务负载：JSON 文本
 *     string LogIDNew         = 9;
 *   }
 *
 * 【我犯过的错，留作教训】曾经把 payload 判成「字段号 1032」，于是解析出来的 payload 永远是 null，
 * 只能靠按内容扫描兜底。真相：真实报文那一段是 `42 99 05 …` —— `42` 就是**字段 8、wire type 2**
 * 的 tag（0x42 = 66 = (8 << 3) | 2），紧随其后的 `99 05` 是长度 varint。
 * 三重独立印证：① 官方 Python SDK `pbbp2_pb2.py` 里 `payload` 的 number = 8；
 * ② 官方 Node SDK 生成的编码器 `writer.uint32(/* id 8, wireType 2 =*\/66).bytes(message.payload)`；
 * ③ 拿我自己抓的 frame.hex 跑一遍 parseFrame：字段 8 正是 666 字节的 `{"schema":"2.0",…}`。
 */
export const FRAME_FIELD = {
  SEQ: 1,
  LOG_ID: 2,
  SERVICE: 3,
  METHOD: 4,
  HEADERS: 5,
  PAYLOAD_ENCODING: 6,
  PAYLOAD_TYPE: 7,
  PAYLOAD: 8,
  LOG_ID_NEW: 9,
};

/**
 * 从整帧字节里按内容特征找事件负载 —— **兜底用**。
 *
 * 正常路径是字段 8（见 FRAME_FIELD 的注释）；这个函数只在「字段 8 没解析出 JSON」时启用，
 * 防的是飞书改字段号或分片导致的意外。它按 `{"schema":"…"` 这类内容特征扫描到最后一个 `}`，
 * 不依赖任何字段号。
 *
 * @returns {{ payload: Buffer, fieldNumber: number, scanned: true } | null}
 */
export function findEventPayload(buf) {
  const probe = buf.toString('latin1');
  for (const marker of ['{"schema":"2.0"', '{"schema":"1.0"', '{"schema"']) {
    const at = probe.indexOf(marker);
    if (at < 0) continue;
    // 从起点到最后一个 } 截出来交给 JSON.parse 收尾（JSON 自描述，不怕多截一点）
    const end = probe.lastIndexOf('}');
    if (end >= at) return { payload: buf.subarray(at, end + 1), fieldNumber: -1, scanned: true };
  }
  return null;
}

/**
 * 帧的 method（官方 SDK 里的 FrameType），**只有两个值**。
 * 【已纠正】旧代码写的是 `METHOD = { EVENT: 0, PING: 1, PONG: 2 }` —— 三个值全错：
 * 事件帧的 method 是 1（data）不是 0，而且**根本不存在 method=2**（我早期臆造的 "PONG 应答帧"）。
 * 真正区分心跳/事件看的是 header 里的 `type`（ping/pong/event/card），不是 method。
 */
export const FRAME_TYPE = { CONTROL: 0, DATA: 1 };

/** header 里 `type` 的可能取值。 */
export const MESSAGE_TYPE = { EVENT: 'event', CARD: 'card', PING: 'ping', PONG: 'pong' };

/** 组装一个帧。 */
export function buildFrame({ seqId = 0, logId = 0, service = 0, method, payload = Buffer.alloc(0), headers = [] }) {
  const parts = [];
  if (seqId) parts.push(varintField(FRAME_FIELD.SEQ, seqId));
  if (logId) parts.push(varintField(FRAME_FIELD.LOG_ID, logId));
  if (service) parts.push(varintField(FRAME_FIELD.SERVICE, service));
  parts.push(varintField(FRAME_FIELD.METHOD, method));
  for (const [key, value] of headers) {
    const entry = Buffer.concat([field(1, Buffer.from(key, 'utf8')), field(2, Buffer.from(value, 'utf8'))]);
    parts.push(field(FRAME_FIELD.HEADERS, entry));
  }
  if (payload.length) parts.push(field(FRAME_FIELD.PAYLOAD, payload));
  return Buffer.concat(parts);
}

/** 把帧解析成便于阅读的对象。 */
export function decodeFrame(buf) {
  const out = { seqId: 0, logId: 0, service: 0, method: null, headers: [], payload: null };
  for (const f of parseFrame(buf)) {
    switch (f.fieldNumber) {
      case FRAME_FIELD.SEQ: out.seqId = Number(f.value); break;
      case FRAME_FIELD.LOG_ID: out.logId = Number(f.value); break;
      case FRAME_FIELD.SERVICE: out.service = Number(f.value); break;
      case FRAME_FIELD.METHOD: out.method = Number(f.value); break;
      case FRAME_FIELD.PAYLOAD: out.payload = f.payload; break;
      case FRAME_FIELD.HEADERS: {
        // header 内部又是两个 length-delimited 字段：1=key，2=value
        try {
          const inner = parseFrame(f.payload);
          const key = inner.find((x) => x.fieldNumber === 1)?.payload?.toString('utf8') ?? '';
          const value = inner.find((x) => x.fieldNumber === 2)?.payload?.toString('utf8') ?? '';
          out.headers.push([key, value]);
        } catch { /* header 解析失败不影响主流程 */ }
        break;
      }
      default: break; // 未知字段直接忽略（protobuf 的向前兼容约定）
    }
  }
  return out;
}

// ─────────────────────────────────────────────────────────────
// 四、长连接客户端
// ─────────────────────────────────────────────────────────────

/**
 * 建立长连接并持续接收事件。
 *
 * @param {object} options
 * @param {{appId:string, appSecret:string, debug?:boolean}} options.cfg
 * @param {(payload:any, meta:{eventType:string}) => void} options.onEvent  收到事件时回调
 * @param {(message:string) => void} options.log                             诊断日志
 * @returns {{ close: () => void, stats: object }}
 */
export function connect(cfg, onEvent, log) {
  const stats = { framesIn: 0, eventsIn: 0, acks: 0, pings: 0, pongs: 0, pingsSent: 0, pingFailures: 0, reconnects: 0, connectedAt: null, lastFrameAt: null };
  let ws = null;
  let closed = false;
  let seq = 0;
  let serviceId = null;                  // 从取址接口返回的 URL 里带出来（实测 33554678）
  let pingTimer = null;
  // 心跳间隔由服务端在 pong 帧里下发（默认 120 秒）。心跳**必须由客户端主动发**。
  const runtime = { pingIntervalMs: 120000 };

  const debug = (...args) => { if (cfg.debug) log('[debug] ' + args.join(' ')); };

  async function open() {
    if (closed) return;
    try {
      const { url } = await fetchWsEndpoint(cfg);
      log('长连接地址已获取，正在拨号…');
      // 注释：接入地址形如 wss://msg-frontier.feishu.cn/ws/v2?fpid=…&service_id=33554678，
      // 发心跳帧时要带上这个 service。
      const serviceInUrl = /[?&]service_id=(\d+)/.exec(url);
      if (serviceInUrl) serviceId = Number(serviceInUrl[1]);
      ws = new WebSocket(url);

      // 注释：Node 自带 WebSocket 的二进制数据以 Blob 形式到达（默认 binaryType 是 'blob'），
      // 必须改成 'arraybuffer' 才能直接拿到字节。
      ws.binaryType = 'arraybuffer';

      ws.addEventListener('open', () => {
        stats.connectedAt = Date.now();
        log('长连接已建立 ✅（现在你可以在手机飞书里给机器人发消息了）');
        startPing();
      });

      ws.addEventListener('message', (event) => {
        const buf = Buffer.from(event.data);
        stats.framesIn++;
        stats.lastFrameAt = Date.now();
        // 注释：默认只打帧长，不打整帧十六进制——一帧 1.2KB 的 hex 进日志很吵。
        // 需要排查协议细节时把配置里的 dumpFrames 改成 true。
        if (cfg.dumpFrames) debug(`收到帧 ${buf.length} 字节: ${buf.toString('hex')}`);
        else debug(`收到帧 ${buf.length} 字节`);
        let frame;
        try {
          frame = decodeFrame(buf);
        } catch (error) {
          log(`帧解析失败（已忽略）：${error.message}`);
          return;
        }
        debug(`解析结果 method=${frame.method} service=${frame.service} 字段数=${frame.headers.length}`);
        // 注释：必须把原始 buf 一并传进去——handleFrame 找不到 JSON 载荷时要靠扫描整帧兜底。
        // （这里曾经漏传，导致帧一到就抛 ReferenceError，表现为"日志有帧但毫无反应"。）
        // 注释：handleFrame 现在是 async（事件处理完要等结果再回执），所以用 catch 接住，
        // 不能再套同步 try/catch——同步 try/catch 拦不住 Promise 里的异常。
        handleFrame(frame, buf).catch((error) => log(`处理帧出错（不影响后续帧）：${error.stack ?? error.message}`));
      });

      ws.addEventListener('close', (event) => {
        if (closed) return;
        if (pingTimer) { clearInterval(pingTimer); pingTimer = null; }
        log(`长连接断开（code=${event.code}）${event.reason ? ' ' + event.reason : ''}，3 秒后重连…`);
        stats.reconnects++;
        setTimeout(open, 3000);
      });

      ws.addEventListener('error', () => {
        // 注释：Node 的 error 事件往往不带可读信息（TypeError、message 为空），
        // 细节点靠随后的 close 事件或 debug 日志判断。
        log('长连接报错（细节见 debug 日志）');
      });
    } catch (error) {
      log(`建立长连接失败：${error.message}；5 秒后重试`);
      stats.reconnects++;
      setTimeout(open, 5000);
    }
  }

  /** 把一段字节按 JSON 解析出来，失败返回 null（不抛）。 */
  function tryParseJson(buffer) {
    if (!buffer || !buffer.length) return null;
    try {
      return JSON.parse(buffer.toString('utf8'));
    } catch {
      return null;
    }
  }

  /**
   * 客户端主动发心跳。**必须由我们发**：
   * 官方两版 SDK 都是客户端每 PingInterval 秒发一个 control 帧
   * （headers 只有一条 `type=ping`，payload 为空，SeqID/LogID 都是 0），服务端**不回 pong 应答**。
   * 旧代码把这件事理解反了：它"收到心跳就回 pong"，而真相是"我们自己定期发 ping"。
   *
   * ⚠️ 这里的 setInterval **绝不能 unref()**——unref 的含义是"这个定时器不算数"，
   * 会让进程在没有其它任务时直接退出（唤醒器上已经踩过一次）。
   */
  function startPing(immediate = true) {
    if (pingTimer) clearInterval(pingTimer);
    const sendPing = () => {
      if (closed || ws?.readyState !== 1 /* OPEN */) return;
      try {
        ws.send(buildFrame({
          seqId: 0,
          logId: 0,
          service: serviceId ?? 0,
          method: FRAME_TYPE.CONTROL,
          headers: [['type', MESSAGE_TYPE.PING]],
        }));
        stats.pingsSent++;
        stats.pingFailures = 0;
        // 只报第一条：既是"心跳功能活着"的证据，又不至于每 2 分钟刷一行日志
        if (stats.pingsSent === 1) log(`已发送心跳 ping（service=${serviceId ?? '未知'}），之后每 ${Math.round(runtime.pingIntervalMs / 1000)} 秒一次`);
      } catch (error) {
        // 注释：睡眠/换网之后 socket 可能是"半死"的——系统既不报 error 也不报 close，
        // 唯一的症状就是发不出去。连续两次发不出去就主动断开，交给 close 分支去重连。
        stats.pingFailures++;
        log(`心跳发送失败（第 ${stats.pingFailures} 次）：${error.message}`);
        if (stats.pingFailures >= 2) {
          stats.pingFailures = 0;
          log('心跳连续失败：主动断开以触发重连');
          try { ws?.close(); } catch { /* 忽略 */ }
        }
      }
    };
    // 连上就先发一个：既当保活探针，也让服务端有机会把心跳参数用 pong 下发
    if (immediate) sendPing();
    pingTimer = setInterval(sendPing, runtime.pingIntervalMs);
  }

  /** 从 header 列表里取一个值（header 是 [[key, value], …]）。 */
  function getHeader(frame, key) {
    const found = frame.headers?.find(([k]) => k === key);
    return found ? found[1] : null;
  }

  /**
   * 回执（ACK）。
   *
   * 【已实测依据】官方两版 SDK（Python lark-oapi 1.7.4 `ws/client.py:337-359`、
   * Node @larksuiteoapi/node-sdk 1.75.0 `handleEventData`）在事件处理完成之后，
   * 都是**把收到的那一帧原样打回去**，只做两处改动：
   *   ① headers 追加一条 `biz_rt`（处理耗时，纯观测字段，服务端不依赖）；
   *   ② payload 换成 `{"code":200}`（处理失败时 `{"code":500}`；有返回值时另加 `data` = base64(JSON)）。
   * SeqID / LogID / service / method 全部原样保留。
   * 这就是「确认收到」的唯一手段——不回执，飞书就每 ~20 秒重投同一条事件。
   * type=card 的卡片回调两端都**不回执**，照做。
   */
  function sendAck(frame, code, result, elapsedMs) {
    const body = { code };
    if (result !== undefined && result !== null) {
      body.data = Buffer.from(JSON.stringify(result), 'utf8').toString('base64');
    }
    const ack = buildFrame({
      seqId: frame.seqId,
      logId: frame.logId,
      service: frame.service,
      method: FRAME_TYPE.DATA,                 // 与入站帧同为 data 帧
      headers: [...frame.headers, ['biz_rt', String(elapsedMs)]],
      payload: Buffer.from(JSON.stringify(body), 'utf8'),
    });
    try {
      ws.send(ack);
      stats.acks++;
      debug(`已回执 code=${code}（${ack.length} 字节）`);
    } catch (error) {
      log(`回执发送失败：${error.message}`);
    }
  }

  async function handleFrame(frame, buf) {
    if (frame.service) serviceId = frame.service;   // 兜底：从入站帧学 service
    const type = getHeader(frame, 'type');

    // ── control 帧（method = 0）：心跳类 ─────────────────────────────
    if (frame.method === FRAME_TYPE.CONTROL) {
      if (type === MESSAGE_TYPE.PING) {
        // 服务端→客户端方向的心跳：官方客户端**什么都不回**。
        // 【教训】旧代码在这里回了一个 method=2 的空帧，还把它当成了协议要求——那是我臆造的。
        stats.pings++;
        return;
      }
      if (type === MESSAGE_TYPE.PONG) {
        // 服务端用 pong 下发心跳/重连参数（payload 是 JSON 的 ClientConfig）
        const conf = tryParseJson(frame.payload);
        if (conf && typeof conf === 'object') {
          if (Number(conf.PingInterval) > 0) runtime.pingIntervalMs = Number(conf.PingInterval) * 1000;
          debug(`心跳参数已更新：PingInterval=${conf.PingInterval}s（${runtime.pingIntervalMs} ms）`);
          startPing(false);   // 用新间隔重排（不再立刻多发一个）
        }
        stats.pongs++;
        return;
      }
      debug(`control 帧（type=${type}），忽略`);
      return;
    }

    // ── data 帧（method = 1）：事件 / 卡片 ───────────────────────────
    let payload = tryParseJson(frame.payload);
    if (!payload) {
      const scanned = findEventPayload(buf);
      if (scanned) {
        payload = tryParseJson(scanned.payload);
        if (payload) debug(`已从整帧扫描里找到载荷（${scanned.payload.length} 字节）`);
      }
    }
    if (!payload) {
      debug(`data 帧（type=${type}）里没有解析出 JSON 负载，忽略`);
      return;
    }

    stats.eventsIn++;
    // 飞书 v2 事件信封：{ schema, header: { event_type, event_id, … }, event: {…} }
    const eventType = payload.header?.event_type ?? payload.header?.eventType ?? 'unknown';
    debug(`事件类型=${eventType}（type=${type}）`);

    const startedAt = Date.now();
    if (type === MESSAGE_TYPE.CARD) {
      // 卡片回调：两端官方 SDK 都直接 return、不回执
      try {
        await onEvent(payload, { eventType, ackable: false });
      } catch (error) {
        log(`卡片回调处理出错：${error.stack ?? error.message}`);
      }
      return;
    }

    let code = 200;
    let result;
    try {
      result = await onEvent(payload, { eventType, ackable: true });
    } catch (error) {
      // 处理失败 → 回 code=500（照官方做法），服务端会重投
      code = 500;
      log(`事件处理抛错（将以 code=500 回执）：${error.stack ?? error.message}`);
    }
    sendAck(frame, code, result, Date.now() - startedAt);
  }

  open();

  return {
    stats,
    close() {
      closed = true;
      if (pingTimer) { clearInterval(pingTimer); pingTimer = null; }
      try { ws?.close(); } catch { /* 忽略关闭异常 */ }
    },
  };
}

// ─────────────────────────────────────────────────────────────
// 五、消息事件 → 纯文本任务
// ─────────────────────────────────────────────────────────────

/**
 * 从 im.message.receive_v1 事件里提取「谁、在哪个会话、说了什么、怎么回」。
 * 【待验证】字段路径依据公开文档；接通后靠真实消息核对。
 */
export function extractMessage(payload) {
  const event = payload?.event;
  const message = event?.message;
  if (!message) return null;

  let text = '';
  let parsed = {};
  try {
    // 注释：message.content 又是「JSON 的字符串」，文本消息里形如 {"text":"@_user_1 你好"}
    parsed = JSON.parse(message.content ?? '{}');
    text = typeof parsed.text === 'string' ? parsed.text : '';
  } catch {
    text = '';
  }

  // 群里 @ 机器人时，文本里会插入形如 @_user_1 的占位符，要清掉
  text = text.replace(/@_user_\d+/g, '').trim();

  return {
    messageId: message.message_id,
    chatId: message.chat_id,
    chatType: message.chat_type,          // 'p2p'（私聊）或 'group'（群）
    senderOpenId: event?.sender?.sender_id?.open_id ?? null,
    senderUserId: event?.sender?.sender_id?.user_id ?? null,
    senderType: event?.sender?.sender_type ?? null, // 'user' 或 'app'
    text,
    // 附件相关（2026-10-17 加）：image_key / file_key 用来调 resources 接口下载原件。
    messageType: message.message_type ?? 'text',
    imageKey: typeof parsed.image_key === 'string' ? parsed.image_key : null,
    fileKey: typeof parsed.file_key === 'string' ? parsed.file_key : null,
    fileName: typeof parsed.file_name === 'string' ? parsed.file_name : null,
    raw: message,
  };
}
