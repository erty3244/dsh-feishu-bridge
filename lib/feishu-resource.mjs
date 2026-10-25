// 飞书「收图片 / 收文件」用的资源下载（2026-10-17 新增）
//
// 依据：GET /open-apis/im/v1/messages/{message_id}/resources/{file_key}?type=image|file
//   - type=image ：消息里 message_type='image'，content 形如 {"image_key":"img_v3_..."}
//   - type=file  ：消息里 message_type='file'，content 形如 {"file_key":"file_v3_...","file_name":"x.pdf"}
// 约束与 feishu.mjs 一致：只用 node: 内置能力 + 同目录模块，不引入第三方依赖。

import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

import { getTenantToken } from './feishu.mjs';

const BASE = 'https://open.feishu.cn/open-apis';

/** 飞书不返回 content-type，按魔数自己认。认不出返回 null（别硬当 jpeg，宿主会校验字节）。 */
export function sniffImageMediaType(buf) {
  if (!buf || buf.length < 12) return null;
  if (buf[0] === 0xff && buf[1] === 0xd8 && buf[2] === 0xff) return 'image/jpeg';
  if (buf[0] === 0x89 && buf[1] === 0x50 && buf[2] === 0x4e && buf[3] === 0x47) return 'image/png';
  if (buf.slice(0, 3).toString('latin1') === 'GIF') return 'image/gif';
  if (buf.slice(0, 4).toString('latin1') === 'RIFF' && buf.slice(8, 12).toString('latin1') === 'WEBP') return 'image/webp';
  return null;
}

/**
 * 下载某条消息里挂的资源，返回 Buffer。
 * 失败时飞书返回的是小体积 JSON（{"code":…,"msg":…}），这里翻译成人能读的错误。
 */
export async function fetchMessageResource(cfg, { messageId, fileKey, type }) {
  const token = await getTenantToken(cfg);
  const url = `${BASE}/im/v1/messages/${encodeURIComponent(messageId)}/resources/${encodeURIComponent(fileKey)}?type=${encodeURIComponent(type)}`;
  const response = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const buf = Buffer.from(await response.arrayBuffer());
  if (!response.ok) {
    let hint = '';
    try {
      const j = JSON.parse(buf.toString('utf8'));
      hint = ` code=${j.code} msg=${j.msg}`;
    } catch {
      /* 不是 JSON 就算了 */
    }
    throw new Error(`下载资源失败：HTTP ${response.status}${hint}`);
  }
  return buf;
}

/**
 * 把收到的文件落盘。原件一字不改，只是换个安全文件名，存到 <dir>/<日期>/ 下。
 * 文件名取消息 id 后 8 位做前缀，避免同一天同名文件互相覆盖。
 */
export function saveInboundFile(dir, messageId, fileName, buf) {
  const safe =
    String(fileName ?? 'file')
      .replace(/[\\/:*?"<>|\u0000-\u001f]/g, '_')
      .trim()
      .slice(0, 120) || 'file';
  const dayDir = join(dir, new Date().toISOString().slice(0, 10));
  mkdirSync(dayDir, { recursive: true });
  const target = join(dayDir, `${String(messageId ?? 'msg').slice(-8)}_${safe}`);
  writeFileSync(target, buf);
  return target;
}
