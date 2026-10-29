# dsh_harness_bridge 设计：在 N.E.K.O. 里运行 DeepSeek Harness 插件

## 目标

让 DSH（DeepSeek Harness）插件包的工具在 N.E.K.O. 对话里可用，不改 DSH 包本身。
做不到的部分要明确报告，不做假装兼容。

## DSH 插件的形态（兼容对象）

> **注意**：`./engine` 不是 DSH 官方契约。上游 `deepseek-harness`（f3803c9）全仓没有这个导出，
> 官方包表面只有 `.`（宿主）和 `./client`（浏览器）。`./engine` 是 fairy 系等第三方分发包
> 自己的约定，本插件针对它设计。换成别的 DSH 分发包可能全部 `no-engine`——那不是缺陷，
> 是对方没有这一面。

一个包最多三个面，`package.json` 的 `exports` 声明：

| 面 | 导出 | 依赖什么 | 本插件是否支持 |
| --- | --- | --- | --- |
| agent 面 | `./engine` → `apply(ctx)` + 可选 `inject` 数组 | DSH agent ctx（cordis 风格） | **支持**（经 shim ctx） |
| 宿主桥面 | `.` | `webServer` / `settings` / 会话等宿主服务 | 不支持（报 `no-engine`） |
| 客户端 | `./client` | 浏览器 `__ModuleLoader__` + DSH UI 插槽 | 不支持 |

agent 面是唯一与宿主 UI 无关、又承载"给模型用的能力"的部分，所以兼容层只对它。

## 架构

```
N.E.K.O. 对话 ──LLM tool call──▶ DshBridgePlugin (Python, 插件进程)
                                   │ JSON lines over stdio
                                   ▼
                          sidecar/host.mjs (Node)
                                   │ import(<包>/engine) ; apply(shimCtx)
                                   ▼
                          DSH 工具 execute(args)
```

- Node sidecar 由插件启动、随插件停止；`node` 依次取配置 `node_path` → PATH →
  DSH 自带的 `~/.dsh/dsh-runtimes/dsh-primary-runtime/dependencies/node`。
- sidecar 进程的 `DSH_HOME` 设为配置的 DSH 主目录，引擎读取的配置镜像
  （如 `$DSH_HOME/fairy-memory/config.json`）与 DSH 里是同一份。
- stdout 只走协议帧，引擎的 `console.*` 日志全部改道 stderr，再进插件 debug 日志。

## 事件循环所有权（不可回退的设计约束）

N.E.K.O. 的插件宿主**不是**一个长驻事件循环：`plugin/core/host.py:1391` 用
`asyncio.run(...)` 跑 `lifecycle.startup`，`:1804` 又用**另一个** `asyncio.run(...)`
跑命令循环，TRIGGER 消息由后者派发。两次 `asyncio.run` 之间，前一个 loop 会被关闭。

因此"在 `_start()` 里 `create_subprocess_exec` + 常驻读任务"是**错的**：

1. loop A 关闭 → `_read_stdout` 被取消，读任务消失；
2. 子进程不会被关 loop 杀掉，pid 还活着、管道还开着；
3. 之后每次工具调用在 loop B 里跑，写进 stdin 的请求**没人读回复**；
4. 表现为"启动成功、工具已注册、但每次调用都失败"，且 `_error.log` 一条不写。

设计上把连接锁在自有 loop 上：`SidecarClient` 起一条后台线程跑 `loop.run_forever()`，
`start` / `stop` / `request` 全部经 `run_coroutine_threadsafe` 投递过去，`_Connection`
只在那个 loop 里被碰。调用方在哪个 loop 无所谓，插件被 `asyncio.run` 包多少次都无所谓。

回归测试锁住这条：`test_sidecar_survives_the_hosts_separate_event_loops` 用
`asyncio.run(start)` / `asyncio.run(request)` / `asyncio.run(stop)` 分开跑；
`test_sidecar_start_and_stop_are_idempotent` 锁重复 start 不产生第二个 sidecar。

## shim ctx 的 API 映射

| DSH ctx | shim 行为 | N.E.K.O. 侧对应 |
| --- | --- | --- |
| `tools.register(tool)` | 记录工具 | `register_llm_tool(name=前缀+名, parameters, timeout=timeoutMs+5s≤300s)` |
| `tool.execute(args, {signal})` | 带超时与 AbortSignal 调用 | handler 返回 `{"output": ...}`；失败 `is_error` + 错误码 |
| `tool.output.render` | 用于生成文本结果 | 优先把渲染文本作为 `output` 给模型 |
| `systemPrompt.section` | 记录段落，函数式 text 按需求值 | **不自动注入**；`prompt_sections` 入口可读出；`dsh_bridge_instructions` 工具可让模型主动读取 |
| `systemPrompt.getSectionOrder(name)` | 查本文件内拷贝的 DSH 分配表（`SECTION_ORDERS`，源自上游 `core/system-prompt/src/index.ts`），未知名字落外部区间；每次查询记 `degraded` | 无对应物 |
| `systemPrompt.getContextOrder(name)` | 同上，查 `CONTEXT_ORDERS` | 无对应物 |
| `on('agent/pre-step')` | 记录；读段落前执行一次 | 无（N.E.K.O. 没有每步钩子） |
| `logger` | 两种形态都支持：服务形态 `logger.warn(...)`（含 `info/warn/error/debug/success/log`），工厂形态 `logger('name')` 返回带作用域的子 logger（可嵌套）。逐行写 stderr，格式 `[dsh:<包名>] <level> <msg>` | 由 `on_stderr` 落到插件 debug 日志（`[sidecar] …`）；stdout 只走协议帧 |
| `inject([svc], child)` | 只在 svc 全属 `tools`/`systemPrompt`/`logger` 时执行 child，否则跳过并记入 `skipped` | — |
| 顶层 `export inject = [...]` | 含未支持的服务 → 整包 `incompatible`，不调用 apply | — |
| `provide` / `get` | sidecar 内共享一张表（不跨宿主）；重名 provide 先到先得并记 error | — |
| `effect(callback)` | 执行并把 disposer 存起来，包回滚时按逆序执行 | — |
| `tool.timeoutMs` | 正有限数才采信；省略或非正数落 30s 默认 | 与上游一致（省略=无截止时间在 N.E.K.O. 侧仍需 300s 上限） |
| 其他属性（`sessionProjections`、`workspaces`…） | 读取即抛 `UNSUPPORTED_CTX`，回滚该包已注册的工具，报 `incompatible` | — |

`tools.presentAs`（native/ptc 呈现方式）记为 degraded，无对应物。

`logger` 是补出来的，不是装饰。读取未支持的 ctx 属性会立刻抛 `UNSUPPORTED_CTX`，因此
`ctx.logger?.warn?.(...)` 这种防御性写法救不了自己 —— 属性访问先于可选链求值。
dsh-web 的 `dsh-liangshen` 恰好在"依赖服务缺失"这条路径上 warn，于是它的优雅降级
变成了我们的硬失败（实测 `missing: ["logger"]`、`status: incompatible`）。补上后同样的
探针包变成 `ok`。

但这不等于 dsh-web 能用了：它的 22 个包**全部**缺 `exports["./engine"]`（它们走
`dsh.bundle.patch` → `cordis.patch.yml` 的 profile bundle 挂载模型），实测 22/22 报
`no-engine`，进不到 ctx 这一层。补 `logger` 的意义在真实 DSH 生态的**其它**包里 ——
把日志当可选依赖是常见写法，而 `ctx.webServer`（14 个包引用 54 处）才是跨不过去的那道墙。

## 兼容性状态

`status` 入口返回每个包的状态：

| 状态 | 含义 |
| --- | --- |
| `ok` | 全部能力就位 |
| `partial` | 工具可用；有段落未注入、或跳过了 `inject` 子块、或有 degraded 项 |
| `incompatible` | 需要 shim 不提供的服务（`missing` 列出），**或注册期出错（重名工具、畸形工具、apply 抛非 UNSUPPORTED_CTX 的异常）**，未加载 |
| `no-engine` | 只有宿主桥面/客户端 |
| `not-found` / `load-failed` | 路径错 / import 失败（多为依赖未装） |

`status` 同时给出这几个字段：`running`、`problem`、`packages`、`chat_tools`
（模型工具名 → DSH 工具名）、`meta_tools`（本插件另外挂的工具，目前只有
`dsh_bridge_instructions`）、`unconfirmed`（主服务 5 秒内没确认、已本地回滚的名字）。

`dsh_bridge_instructions` 是 `register_llm_tool` 注册的**元工具**，不是 DSH 包提供的：
它把各包的段落按包名、按 order 拼成一段文本，并在末尾列出没加载成功的包及原因。
理由见「已知边界」第 1 条——段落进不了系统提示，只能让模型自己来取。

## 对 Fairy-DSH 包的实测（`~/.dsh`，2026-10-29）

| 包 | 结果 | 工具 |
| --- | --- | --- |
| dsh-fairy-roleplay | partial（1 个段落未注入；`getSectionOrder('PLAN_POLICY')` 取到 500，段落 503） | `roleplay_check`、`roleplay_style` |
| dsh-fairy-memory | partial（2 个段落未注入；同上，段落 502 / 504） | `memory_recall`、`memory_remember` |
| dsh-fairy-eval | partial（跳过 `settings` 子块：设置卡里的值读不到，回退环境变量 `TYPESAFE_*`） | `typesafe_eval` |
| dsh-fairy-modes | incompatible：缺 `sessionProjections` | — |
| dsh-fairy-search / voice / persona 等 | no-engine | — |

## 已知边界与后续

1. **系统提示段落不注入，但可以读**。N.E.K.O. SDK 里没有任何注入点：在 `plugin/sdk/` 搜
   `system_prompt|systemPrompt|prompt_section|add_prompt|prompt_contributor` 零命中。
   替代路径是 `dsh_bridge_instructions` 工具：它返回所有段落拼好的文本，开头写明
   "这些不在你的系统提示里"，并列出没加载成功的包及原因。**但它不是自动的**——模型得主动调，
   所以它只能降低"胡猜 DSH 约定"的概率，不能替代系统提示。
   另一个坑：普通 `@plugin_entry` **不会**进 `/api/tools`，只有 `__llm_tool__*`
   （装饰器路径与 `register_llm_tool` 两条，见 `plugin/sdk/plugin/base.py:495-500`）才会。
   所以"把东西做成 entry 给模型看"必须走 `register_llm_tool`，光加 `@plugin_entry` 没用。
2. **`settings` 服务不提供，而且不该按 `settings.read(ns)` 去做**。
   上游 f3803c9 的 `@deepseek-ai/dsh-settings` 只暴露 `ctx.settings: SettingsForms`
   （`describe()` / `mutate()` / `present()`，见 `packages/settings/settings/src/index.ts`），
   全仓搜不到 `settings.read`、`settings.register`、`settingsScope`。
   而 fairy-eval 的 `lib/engine.js:54` 调的是 `settingsCtx.settings.read('fairy-eval')`，
   它自己的 `test/contract.test.js:35` 里还手写了一个"与 dsh 的 settings 服务同形"的桩
   （带 `read`/`register`）——**这是 fairy 侧假设的接口，不是上游契约**，性质同 `./engine`。
   所以不要做"把 `read(ns)` 映射到 `$DSH_HOME` 下的设置文件"：那会读一份 DSH 设置卡
   永远不会写入的存储，用户改了设置也不生效，比 `skipped` 更糟。
   真要支持，先拿到 fairy-full preset 的 shim 源码，或等 fairy 改用上游 `SettingsForms`。
3. **会话类能力不做**（fairy-modes 的模式/流水线、`session_recall`）。
   `sessionProjections` 确实是上游服务（`SessionProjectionRegistry`，有
   `register` / `stateOf` / `checkpoint`），fairy-modes 用到 `register` 与 `stateOf`；
   但 `stateOf` 的状态来自 DSH 的会话事件流，N.E.K.O. 不向插件暴露该事件源。
   硬做只能返回初始态：模式切换会"看起来成功了，然后立刻忘掉"——这是假兼容，不做。
4. 工具注册在插件启动时做一次；main_server 重启后需要用 `reload` 入口重注册。
   每次注册后会向 `GET /api/tools` 核对一遍：宿主没确认的名字会回滚并从
   `chat_tools` 移入 `unconfirmed`，避免 `status` 报告模型其实调不到的工具。
5. 依赖：包必须已在 DSH 主目录里装好 `node_modules`（DSH 部署脚本会做）。
6. **段落顺序用的是拷贝表**。`build` 内置 DSH 的 `SECTION_ORDERS` / `CONTEXT_ORDERS`
   （copy 自上游 f3803c9），因此数值与真 DSH 一致，但上游改表后会滞后。
   未知名字落 20000 起的外部区间。每次查询都记 `degraded`，因为答案来自本插件而非 DSH。
7. **大结果分批传输，不再截断**。一帧过不了 4 MB 就切成多个 `{i, n, data}` 有序帧
   （每帧 1 MB），Python 侧按 id 组装后再 `json.loads`，所以几 MB 的工具结果能完整到达。
   只有超过 `MAX_RESULT_CHARS`（默认 64 MB，可用 `DSH_BRIDGE_MAX_RESULT_CHARS` 覆盖）
   才回 `truncated: true`——那是防止一个失控工具把插件内存顶爆的兜底。
   切分点不会落在代理对中间，避免半截字符。
8. **`provide`/`get` 只在 sidecar 内共享**，不跨宿主；`effect` 的 disposer 只在该包回滚时执行。
9. **sidecar 的存活由插件进程负责**。插件被强杀（不走 `shutdown`）时 node 子进程会残留，
   下次启动会另起一个；`stop` 走正常路径时 `stdin` 一关 node 自己退出，实测 3 次重启只剩 1 个。
10. **`asyncio.run` 边界上的状态会被丢掉**。除 sidecar 外，任何"启动时建好的 asyncio 原语"
    （长连接、`Event`、`Queue`）都会在 loop A 关闭后失效，必须和 sidecar 一样自带 loop 或改成线程原语。

## 安全

- 只加载配置里列出的包目录，不扫描、不下载。被加载的 JS 与 N.E.K.O. 插件同权限运行，
  只应指向自己信任的 DSH 安装。
- 错误信息截断到 500 字符；fairy 诊断通道本身会抹掉 key/token 类字段。
- 工具名统一加前缀（默认 `dsh_`），并按 `[A-Za-z0-9_-]` 规整，与其他插件重名时加数字后缀。
- 注销时向 `POST /api/tools/unregister` 带 `expected_source=plugin:<自身 id>`，
  服务端据此拒绝跨来源删除，不会误删其他插件的同名工具。
- 配置界面的 `packages` 描述已写明"被加载的代码以本插件权限运行"，因为绝对路径是允许的。
