# dsh-feishu-bridge

把飞书（Lark）自建机器人接到 DeepSeek Harness：**手机上发一条飞书消息，DSH 里就开一轮；这一轮的回复自动回到飞书**。走飞书自建应用的**长连接**，不需要公网 IP、不需要内网穿透、不需要回调地址。

零第三方依赖（只用 Node 内置模块），自己实现飞书长连接的帧编解码。

> English: A zero-dependency bridge that connects a Feishu/Lark self-built bot to DeepSeek Harness over the official WebSocket long connection. Send a Feishu message → a DSH turn starts → the answer is posted back to the same chat. No public IP, no tunnel, no callback URL required.

---

## 它解决什么

DSH 跑在自己电脑上，人经常不在电脑前。这个插件让飞书成为本地 DSH 的输入输出口：

- **入站**：飞书消息 → 投递进指定 DSH 会话（默认把发送者、chat、消息 id 一起带上）
- **出站**：该会话这一轮的最终回复 → 发回同一条私聊或群
- **不串线**：每一轮与触发它的那条消息用 `rpcId` 精确配对，并发时也不会把 A 的回答回给 B
- **不重复**：状态文件里记 message_id，重连后补送的旧消息不会被执行两遍；延迟送达会标注「晚了 N 分钟」
- **可换绑**：目标会话写在 `bindingFile` 里，只有一行，改完下一轮生效，不用重启
- **折叠注入（可选）**：从系统侧主动注入的消息（下面「自由时间」就是）会带 `source.kind / form:'notice'`，在对话界面里显示成上下文条目，而不是伪装成用户说的话

## 安装

```bash
dsh plugin --profile desktop add ./dsh-feishu-bridge
```

或者在 DSH 桌面端的 **设置 → 插件** 里从目录安装。装完重启一次宿主（插件在宿主进程里起长连接）。

## 飞书侧准备（一次性，约 10 分钟）

1. 打开 [飞书开放平台](https://open.feishu.cn/) → 创建**企业自建应用**
2. 在「凭证与基础信息」里取 **App ID / App Secret**
3. 「添加应用能力」→ 打开**机器人**
4. 「权限管理」至少给：
   - `im:message`（读取用户发给机器人的消息）
   - `im:message:send_as_bot`（以机器人身份发消息）
   - （要下载图片/文件再加 `im:resource`）
5. 「事件与回调」→ 订阅方式选 **使用长连接接收事件** → 添加事件 `im.message.receive_v1`
6. 「版本管理与发布」创建版本并发布（企业内自建应用通常自己就能审核）
7. 把机器人拉进你要用的群，或直接私聊它

## 配置

插件的配置走 profile 的 `cordis.patch.yml`，凭据单独放一个 JSON 文件（不放进插件配置，避免跟着插件一起被备份/导出）：

```yaml
- insert:
    - id: dsh-feishu-bridge
      name: 'dsh-feishu-bridge'
      config:
        configPath: 'C:\Users\you\.dsh\feishu-config.json'
        bindingFile: 'C:\Users\you\.dsh\feishu-target-session.txt'
        stateDir: 'C:\Users\you\.dsh\feishu-bridge'
```

`feishu-config.json`：

```json
{
  "appId": "cli_xxx",
  "appSecret": "xxx",
  "allowOpenIds": ["ou_xxx"],
  "allowChatIds": ["oc_xxx"],
  "note": "allowOpenIds / allowChatIds 是白名单；留空 = 谁都不放行"
}
```

- `bindingFile`：只有一行，内容是要投递的目标会话 id（`session-xxxx`）。改一行、下一轮生效。
- 也可以直接在 config 里写 `sessionId`，`bindingFile` 优先生效。
- **白名单留空等于谁都不放行**，不是放行所有人——这点别反着理解。
- 密钥可以不放明文：配 `appSecretRef` + DSH 的凭证机制（`ctx.credentials.resolve`），配置文件里只留引用名。

## 可选：自由时间（self-trigger）

插件里带了一个「定时判定 + 概率累计 + 四选一」的主动行为模块：每隔一小时判定一次，掷骰子决定这次自由时间是「上网 / 翻文件 / 找你说话 / 什么都不发」，命中前每次落空累计 +N% 概率。

**默认是关的。** 打开需要再加一个策略文件（`selftrigger-policy.json`，`enabled: true`），并且想清楚一件事：它会让 AI 在你没说话的时候主动占用一次模型调用。不需要就别开。

## 安全与权限声明

这个插件在宿主里能碰到的范围，如实列出（市场审查口径）：

| 能力 | 用途 |
|---|---|
| `network` | 只连 `open.feishu.cn`（长连接 WSS + 发消息 / 取资源 HTTPS） |
| `fs-read` | 读 `configPath`、`bindingFile` |
| `fs-write` | 只写 `stateDir` 下的状态、日志、心跳 |
| `host-runtime` | `sessionController.resolveAgent` + `agent.followup` 投递消息 |
| `credentials`（可选） | 仅在配了 `appSecretRef` 时读一个具名凭证 |

不做的事：不开子进程、不执行 shell、不扫描磁盘、不读凭据文件、不上报任何遥测。

## 兼容性

- 测于 DSH `0.2.1-rc.2`（desktop profile），Node ≥ 22
- 依赖的宿主接口：`ctx.sessionController`（`resolveAgent` / `prompt`）、`ctx.on('session/event')`、`ctx.credentials.resolve`
- DSH 的内部接口会变。插件里对拿不到的对象都做了兜底与日志，宿主升级后先看 `stateDir` 下的日志再升级

## 已知限制

- 消息投递进来时，目标会话必须「活着」（在侧栏点开过一次）；否则会返回错误并要求先打开
- 如果目标会话的历史里带坏了的数据（模型接口直接 400），任何新轮次都会失败——换一个新会话再改 `bindingFile` 即可
- 群消息默认不逐条转（避免刷屏），要按 @ 过滤请配合外部轮询脚本

## License

MIT
