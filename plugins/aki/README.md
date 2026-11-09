# aki 插件（aki.io · DeepSeek V4.1 Flash）

把 aki.io 的 Model Hub Playground（榜单模型 DeepSeek-V4.1-Flash-1017）接进网关，
暴露成一个 **C 类纯文本** 站点插件（无 function tools）。按 `docs/plugin-requirements.md`
规范实现，stdin/stdout NDJSON-RPC。

## 形态

- 端点：`POST /api/call/{endpoint}` 提交拿 `job_id`，`POST /api/progress/{endpoint}` 轮询
  到 `job_state: done` 取 `job_result.text`。
- 认证：匿名访客 key 可直接提交，无需注册；注册用户的 key 走控制台 `api_key` 覆盖。
- 流式：拿到完整答案后按 40 字符切片推 `delta`，最后 `usage` + `done`。

## 构建

```bash
cd plugins/aki
go build -o bin/aki .
# 自检（打印模型清单，不连上游）
./bin/aki --selfcheck
```

## 网关配置片段（`configs/gateway.yaml`）

```yaml
- name: aki
  command: ./plugins/aki/bin/aki
  priority: 30
  tools: none
  timeout: "300s"      # 轮询上限 4 分钟，留足
  per_plugin_rps: 0.05 # 每出口 IP 3 次/天，压到极低避免浪费额度
  models: [deepseek-v4.1-flash]
  auth:
    type: api_key
    optional_fields: [api_key]   # 缺省用内置访客 key，可留空
  env:
    AKI_ENDPOINT: "deepseek_v4_1_flash"
    AKI_PROXIES: "http://u1:p1@hostA:1080,http://u2:p2@hostB:1080,..."  # 出口 IP 池
```

## 依赖的环境变量（`env`）

| 变量 | 默认 | 说明 |
|---|---|---|
| `AKI_BASE` | `https://aki.io/api` | 上游根 |
| `AKI_ENDPOINT` | `deepseek_v4_1_flash` | 端点名（榜单前 20 模型） |
| `AKI_MODELS` | `deepseek-v4.1-flash` | 逗号分隔的对外模型 id |
| `AKI_KEY` | `45dd3f5c-…`（内置访客 key） | 匿名默认 key，可换成自己的 |
| `AKI_PROXIES` | （空） | 逗号分隔代理列表 = 出口 IP 池，见下方 |
| `AKI_PER_IP_DAILY` | `3` | 单出口 IP 每日次数上限（本地台账，可观测/下调） |

凭证字段：`api_key`（可选，注册用户 key）+ `proxy`（可选，网关单条代理）。

## 出口 IP 台账与额度摊薄（核心机制）

实测结论：这把共享 key 的「每日 3 次」**记在出口 IP 上，不是记在 key 上**
——同一把 key 换一个没烧满的 IP，`validate_key` 立即回 `success`。所以提额的
正解不是「每 IP 造新 key」（上游没有发 key 端点，随机 UUID 一律拒），而是
**把请求摊到多个出口 IP 上，每个 IP 各吃 3 次**：

- `AKI_PROXIES` 配 N 条代理（N 个出口 IP）≈ `3 × N` 次/天。要 ~100 次/天就放 ~34 条。
- 每次 `chat` 插件按「该出口今日本地台账用量最少」挑一个出口发请求（网关给了
  `credential.proxy` 则优先走它，满了再换池里其它出口）。
- 命中站点 `daily limit` 时把该出口在本地台账**标满**，下次自动跳过换 IP；
  本地台账（`AKI_PER_IP_DAILY`）也会提前拦住已知的满额出口，不再浪费上游请求。
- 台账按 **UTC 日** 自动清零，无需手动重置。整池都满才报「今日各出口 3 次额度
  均用尽，请扩容 AKI_PROXIES 池或明早重置」。

> 注：台账是进程内存态，网关重启会清零重新计数（上限是「近似」而非精确记账）。
> 需要严格记账可后续把台账落盘，但会引入每 IP 的持久化状态。

## 屏蔽网站内置提示词

该站点前端会自动注入一段预设 system 人设（`CHAT_TEMPLATES`，如
"You are a helpful, respectful and honest assistant named Deepseek…"）。
本插件 **不复刻** 这段内置提示，而是：

1. 只用网关下发的 `system`/`messages` 构造 `chat_context`；
2. `buildChatContext` 检测并 **过滤掉任何携带内置人设特征的 system 消息**
   （`builtinSystemMarkers` 多语言匹配），只保留用户自定义 system（并入首条 user）；
3. 思考链：上游若仍返回 `