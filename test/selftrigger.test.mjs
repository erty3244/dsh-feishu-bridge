// 离线自测：只测自触发的纯逻辑（累计概率判定、抽签、注入文案、花费折算）。
// 不碰 ctx、不碰真时钟；随机数由外面传进去，所以结果是确定的。跑完即删。
// 用法：node _test-selftrigger.mjs
import { decideSelfTrigger, pickSelfTriggerAction, buildSelfTriggerText, costOfUsage } from '../lib/index.js'

const NOW = 1_700_000_000_000
const H = 3600e3
const base = {
  now: NOW,
  hour: 10,
  idleMs: 0,
  dailyMax: 3,
  activeFrom: 9,
  activeTo: 23.5,
  silenceStop: 3,
  failStepPct: 10,
  rnd: 0.5
}
const S = (o = {}) => ({ lastActivityAt: NOW - 2 * H, lastOutboundAt: 0, lastFireAt: 0, day: '2026-10-22', count: 0, silentRun: 0, fails: 0, ...o })

let pass = 0
let fail = 0
const check = (label, got, want) => {
  const ok = JSON.stringify(got) === JSON.stringify(want)
  if (ok) pass += 1
  else { fail += 1; console.log(`✗ ${label}\n   想要 ${JSON.stringify(want)}\n   实际 ${JSON.stringify(got)}`) }
}

// ── 1. 暂停（judge=false）：这些情况不累计失败 ─────────────────────
const pauses = [
  ['凌晨 3 点（非活跃时段）', S(), { hour: 3 }],
  ['23:30 边界（不含）', S(), { hour: 23.5 }],
  ['显式开空闲门槛（idleMs=1h）且只空闲 0.5h', S({ lastActivityAt: NOW - 0.5 * H }), { idleMs: 1 * H }],
  ['今天已触发 3 次（到顶）', S({ count: 3 }), {}],
  ['连续沉默 3 次', S({ silentRun: 3 }), {}]
]
for (const [label, state, conf] of pauses) {
  const v = decideSelfTrigger(state, { ...base, ...conf })
  check(`暂停：${label}`, { judge: v.judge, fire: v.fire }, { judge: false, fire: false })
}

// 2026-10-22 11:08 博士改口：不再受"他说不说话"约束——刚发过言也要判定
const fresh = decideSelfTrigger(S({ lastActivityAt: NOW - 60 * 1000 }), base)
check('不看空闲：1 分钟前刚说过话也判定', { judge: fresh.judge }, { judge: true })

// ── 2. 累计概率：p = 落空次数 × 步长，rnd < p 才命中 ───────────────
const pCases = [
  ['首次判定 p=0（初始概率 0，必不中）', S({ fails: 0 }), 0.0, 0],
  ['p=0 时哪怕 rnd=0.999 也不中', S({ fails: 0 }), 0.999, 0],
  ['落空 1 次 p=10%，rnd 5% 命中', S({ fails: 1 }), 0.05, 0.1],
  ['落空 1 次 p=10%，rnd 20% 不中', S({ fails: 1 }), 0.2, 0.1],
  ['落空 3 次 p=30%，rnd 25% 命中', S({ fails: 3 }), 0.25, 0.3],
  ['落空 10 次 p=100%，rnd 99% 命中', S({ fails: 10 }), 0.99, 1],
  ['落空 20 次 p 封顶 100%', S({ fails: 20 }), 0.5, 1]
]
for (const [label, state, rnd, wantP] of pCases) {
  const v = decideSelfTrigger(state, { ...base, rnd })
  check(`概率：${label} 的 p`, v.p, wantP)
  check(`概率：${label} 的 fire`, v.fire, rnd < wantP)
}

// 边界：rnd 正好等于 p 时不算命中（严格小于）
const edge = decideSelfTrigger(S({ fails: 1 }), { ...base, rnd: 0.1 })
check('边界：rnd 恰好等于 p=10% 时不命中', edge.fire, false)

// 命中时 reason 里要能看出 p 和落空次数
const hit = decideSelfTrigger(S({ fails: 2 }), { ...base, rnd: 0.01 })
check('命中 reason 带 p 与落空次数', /p=20%/.test(hit.reason) && /落空 2 次/.test(hit.reason), true)

// ── 3. 抽签权重：web 40 / files 20 / message 30 / nothing 10 ───────
const W = { web: 40, files: 20, message: 30, nothing: 10 }
const picks = [
  [0.0, 'web'], [0.399, 'web'],
  [0.40, 'files'], [0.599, 'files'],
  [0.60, 'message'], [0.899, 'message'],
  [0.90, 'nothing'], [0.9999, 'nothing']
]
for (const [rnd, want] of picks) check(`抽签 rnd=${rnd}`, pickSelfTriggerAction(W, rnd), want)
check('权重全 0 时退回 nothing', pickSelfTriggerAction({ web: 0, files: 0, message: 0, nothing: 0 }, 0.5), 'nothing')

// 20 万次抽签的分布，应接近 40/20/30/10
const tally = { web: 0, files: 0, message: 0, nothing: 0 }
const N = 200000
for (let i = 0; i < N; i += 1) tally[pickSelfTriggerAction(W, Math.random())] += 1
const pct = Object.fromEntries(Object.entries(tally).map(([k, v]) => [k, Number(((v / N) * 100).toFixed(1))]))
const distOk = Math.abs(pct.web - 40) < 1 && Math.abs(pct.files - 20) < 1 && Math.abs(pct.message - 30) < 1 && Math.abs(pct.nothing - 10) < 1
check(`抽签分布 20 万次 ≈ 40/20/30/10（实际 ${JSON.stringify(pct)}）`, distOk, true)

// ── 4. 注入文案：必须点名这次抽到什么 ─────────────────────────────
for (const [pick, keyword] of [['web', '上网看看'], ['files', '翻他电脑里的文件'], ['message', '给博士发点什么']]) {
  const text = buildSelfTriggerText({ idleHours: '2.1', remaining: 2, dailyMax: 3, pick, budgetCny: 0.3, tail: '', nowText: '2026-10-22 14:00:00' })
  check(`文案：pick=${pick} 有指定句`, text.includes('骰子这次给你的是') && text.includes(keyword), true)
  check(`文案：pick=${pick} 保留"可以沉默"`, text.includes('也可以什么都不做、保持沉默'), true)
  check(`文案：pick=${pick} 带预算`, text.includes('¥0.300'), true)
}

// ── 5. 期望间隔：步长 10% 时平均约 4.7 小时（蒙特卡洛） ────────────
let total = 0
const TRIALS = 200000
for (let i = 0; i < TRIALS; i += 1) {
  let fails = 0
  let hours = 0
  for (;;) {
    hours += 1
    const p = Math.min(1, fails * 0.1)
    if (Math.random() < p) break
    fails += 1
    if (hours > 200) break
  }
  total += hours
}
const meanHours = total / TRIALS
check(`平均间隔 ≈4.7 小时（实际 ${meanHours.toFixed(2)}h）`, meanHours > 4.4 && meanHours < 5.0, true)

// ── 6. 花费折算 ───────────────────────────────────────────────────
const usage = { inputTokens: 100000, cacheReadTokens: 1000000, cacheWriteTokens: 50000, outputTokens: 20000 }
// 0.1*2 + 1*0.2 + 0.05*2 + 0.02*3 = 0.2+0.2+0.1+0.06 = 0.56
check('花费折算 ¥0.56', Number(costOfUsage(usage, { input: 2, cacheRead: 0.2, cacheWrite: 2, output: 3 }).toFixed(4)), 0.56)
check('没有 usage → 0', costOfUsage(undefined, { input: 2, output: 3 }), 0)

console.log(fail === 0 ? `\n✅ 全部通过：${pass} 项` : `\n❌ ${fail} 项失败 / 共 ${pass + fail} 项`)
process.exit(fail === 0 ? 0 : 1)
