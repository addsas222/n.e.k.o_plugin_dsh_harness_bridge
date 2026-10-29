/**
 * dsh_bridge sidecar: loads the agent half (`./engine`) of DeepSeek Harness
 * plugin packages into a shim ctx and serves their tools over JSON lines on
 * stdio. One request per line `{id, op, ...}`, one reply per line
 * `{id, ok, result | error}`. stdout carries protocol frames only; engine logs
 * (the fairy diagnostics sink is `console`) are rerouted to stderr.
 *
 * The shim implements the services an engine can actually use outside a DSH
 * agent realm. Anything else is reported, not faked:
 * - a top-level `inject` export naming an unsupported service → incompatible;
 * - reading an unsupported ctx property during apply → incompatible;
 * - `ctx.inject([...unsupported], child)` → the child is skipped (partial),
 *   which is the same outcome DSH gives a deployment composing none of them.
 */
import { readFile } from 'node:fs/promises';
import { createInterface } from 'node:readline';
import { isAbsolute, join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const writeFrame = (frame) => process.stdout.write(`${JSON.stringify(frame)}\n`);
const toText = (value) => {
  if (typeof value === 'string') return value;
  try { return JSON.stringify(value); } catch { return String(value); }
};
for (const level of ['log', 'info', 'warn', 'error', 'debug']) {
  console[level] = (...parts) => process.stderr.write(`${parts.map(toText).join(' ')}\n`);
}

/** ctx services the shim provides; `inject` lists are checked against this. */
const SERVICES = new Set(['tools', 'systemPrompt', 'logger']);
const MAX_ERROR_CHARS = 500;
const DEFAULT_TIMEOUT_MS = 30_000;
/* One frame's text budget: about half the Python side's 8 MiB stream limit, leaving
 * headroom so the peer's StreamReader never hits its own limit and dies. Replies above
 * this are split into ordered chunk frames rather than truncated. */
const MAX_FRAME_CHARS = 4_000_000;
/* Bigger replies travel as several frames of this size. Each stays far below the peer's
 * 8 MiB limit, so nothing is ever lost to a StreamReader ValueError. */
const CHUNK_CHARS = 1_000_000;
/* Absolute ceiling. Past this the reply is truncated instead of buffered, so one runaway
 * tool cannot make the peer hold an unbounded string. Env override exists for tests. */
const MAX_RESULT_CHARS = positiveInt(process.env.DSH_BRIDGE_MAX_RESULT_CHARS, 64_000_000);
/* Where fallback prompt orders start for names the tables below do not know: past every
 * allocated slot, and upstream allows an external contribution any finite order. */
const EXTERNAL_SECTION_ORDER_BASE = 20000;

/* Central prompt positions, copied from packages/core/system-prompt/src/index.ts
 * (SECTION_ORDERS / CONTEXT_ORDERS) at deepseek-harness f3803c9, 2026-10-28. Copied rather
 * than imported because this sidecar deliberately depends on none of DSH's packages.
 * Values are what DSH itself would hand out; only the snapshot can go stale. */
const SECTION_ORDERS = {
  HARNESS_IDENTITY: -1000,
  DEPLOYMENT_PERSONA_PREFIX: 0,
  PLAN_POLICY: 500,
  TEAM_POLICY: 600,
  PTC_ONLY: 800,
  FILE_REFERENCE: 900,
  TOOL_BASH: 1000,
  TOOL_PWSH: 1010,
  TOOL_READ: 1100,
  TOOL_WRITE: 1200,
  TOOL_EDIT: 1300,
  TOOL_GLOB: 1400,
  TOOL_GREP: 1500,
  TOOL_JOBS: 1600,
  TOOL_PTY: 1700,
  TOOL_WEB_SEARCH: 2000,
  TOOL_WEB_FETCH: 2100,
  TOOL_LSP: 2200,
  TOOL_SESSION_QUERY: 2300,
  TOOL_GOAL: 2400,
  TOOL_WORKFLOW: 2600,
  TOOL_RALPH: 2700,
  TOOL_SUBAGENT: 2800,
  TOOL_REPORT: 2900,
  TOOL_COMPUTER_USE: 3000,
  MCP_SERVERS: 3100,
  TOOLS_SDK: 5000,
  DELIVERABLE_FILE_REFERENCES: 9000,
  STRUCTURED_OUTPUT: 9900,
  HARNESS_SOURCE: 10000,
  WEB_SURFACE: 10100,
  DEPLOYMENT_PERSONA_SUFFIX: 10200,
};

/* Runtime-context positions (`getContextOrder`), same source. */
const CONTEXT_ORDERS = {
  WORKING_DIRECTORY: 100,
  SANDBOX_POLICY: 110,
  APPROVAL_POLICY: 115,
  SUBAGENT_DELEGATION: 120,
  TEAM_IDENTITY: 125,
  HARNESS_SOURCE: 200,
  WEB_SURFACE: 210,
};

const packages = [];
const tools = new Map();
/* Services shared across packages. Cordis resolves `ctx.get` up the context tree; here it
 * degrades to "visible to every package in this sidecar". */
const provided = new Map();

/* Upstream contract (packages/core/tools/src/contracts.ts): `timeoutMs` must be a positive
 * finite number, and omitting it means "no deadline". A non-positive value is treated as
 * omitted and falls back to the default budget instead of becoming a 0 ms deadline. */
function budgetOf(tool) {
  const declared = tool.timeoutMs;
  return Number.isFinite(declared) && declared > 0 ? declared : DEFAULT_TIMEOUT_MS;
}

/* A misconfigured env override must not disable the ceiling: fall back, do not zero it. */
function positiveInt(raw, fallback) {
  const value = Number(raw);
  return Number.isFinite(value) && value > 0 ? Math.floor(value) : fallback;
}

/* A high surrogate at the end of a chunk would be an unprintable half pair after slicing.
 * Back off one code unit so every chunk is well-formed UTF-16 on its own. */
function isHighSurrogate(code) {
  return code >= 0xd800 && code <= 0xdbff;
}

function coded(message, code, extra = {}) {
  return Object.assign(new Error(message), { code }, extra);
}

function engineEntry(manifest) {
  const entry = manifest?.exports?.['./engine'];
  if (typeof entry === 'string') return entry;
  if (entry && typeof entry === 'object') return entry.import || entry.default || null;
  return null;
}

/* cordis' `logger` is both a service and a factory: packages log through the service
 * (`ctx.logger?.warn?.(msg)`) and through a scoped child (`ctx.logger('name').info(msg)`).
 * Leaving it out was not cosmetic. A property read on the shim throws, so optional
 * chaining cannot save a package whose only offence is defensive logging — dsh-liangshen
 * warns this way exactly when a service is absent, which turned "degrade quietly" into a
 * hard load failure. Lines go to stderr with the rest of the engine's diagnostics;
 * stdout carries frames only, so nothing here may touch it. */
function createLogger(scope) {
  const write = (name, level) => (...parts) => {
    const prefix = name ? `[dsh:${name}]` : '[dsh]';
    process.stderr.write(`${prefix} ${level} ${parts.map(toText).join(' ')}\n`);
  };
  /* A child logger inherits the parent scope, so `logger('a')('b')` reads a:b. */
  const logger = (child) => createLogger(child ? (scope ? `${scope}:${child}` : String(child)) : scope);
  for (const level of ['info', 'warn', 'error', 'debug', 'success']) {
    logger[level] = write(scope, level);
  }
  logger.log = logger.info;
  return logger;
}

function createShim(record) {
  /* Resolve one centrally allocated order. A known name gets the value from the copied
   * table; anything else gets a stable number from the external range. Either way the
   * lookup is recorded, because both answers come from this sidecar rather than DSH. */
  const orderFor = (label, table, name) => {
    const key = String(name);
    if (Object.hasOwn(table, key)) {
      record.degraded.add(`${label}(${key}) -> ${table[key]} (copied table)`);
      return table[key];
    }
    const fallbackKey = `${label}:${key}`;
    if (!record.orders.has(fallbackKey)) {
      record.orders.set(fallbackKey, EXTERNAL_SECTION_ORDER_BASE + record.orders.size);
    }
    const order = record.orders.get(fallbackKey);
    record.degraded.add(`${label}(${key}) -> ${order} (fallback)`);
    return order;
  };
  const base = {
    logger: createLogger(record.id),
    tools: {
      register(tool) {
        if (!tool || typeof tool.name !== 'string' || typeof tool.execute !== 'function') {
          record.errors.push('tools.register: malformed tool');
          return () => {};
        }
        if (tools.has(tool.name)) {
          record.errors.push(`tools.register: duplicate tool ${tool.name} (kept ${tools.get(tool.name).record.id})`);
          return () => {};
        }
        tools.set(tool.name, { tool, record });
        record.tools.push(tool.name);
        return () => tools.delete(tool.name);
      },
      // Presentation (native/ptc) is a DSH prompt concern with no counterpart here.
      presentAs(mode) {
        record.degraded.add(`tools.presentAs(${mode})`);
        return () => {};
      },
    },
    systemPrompt: {
      section(definition) {
        record.sections.push(definition);
        return () => {
          const index = record.sections.indexOf(definition);
          if (index >= 0) record.sections.splice(index, 1);
        };
      },
      /* Real DSH packages ask the service where their section belongs rather than hard
       * coding a number (packages/context/file-reference-local/src/index.ts uses this
       * without a guard), so a missing method turns an otherwise compatible engine into a
       * TypeError. Same name, same number, every call. */
      getSectionOrder(name) {
        return orderFor('systemPrompt.getSectionOrder', SECTION_ORDERS, name);
      },
      getContextOrder(name) {
        return orderFor('systemPrompt.getContextOrder', CONTEXT_ORDERS, name);
      },
    },
    inject(deps, callback) {
      const missing = (Array.isArray(deps) ? deps : []).filter((dep) => !SERVICES.has(dep));
      if (missing.length > 0) {
        for (const dep of missing) record.skipped.add(dep);
        return () => {};
      }
      callback(ctx);
      return () => {};
    },
    on(event, handler) {
      if (typeof handler === 'function') record.hooks.push({ event, handler });
      return () => {};
    },
    effect(callback) {
      let dispose;
      try {
        dispose = callback();
      } finally {
        /* Keep every disposer: cordis unwinds registrations on teardown, and rollback of a
         * half-applied engine must run them too. */
        if (typeof dispose === 'function') record.disposers.push(dispose);
      }
      return typeof dispose === 'function' ? dispose : () => {};
    },
    provide(name, value) {
      if (provided.has(name)) {
        record.errors.push(`provide: duplicate service ${String(name)} (kept earlier)`);
        return;
      }
      provided.set(name, value);
    },
    get(name) {
      return provided.get(name);
    },
  };
  const ctx = new Proxy(base, {
    get(target, property) {
      if (typeof property === 'symbol' || property in target) return target[property];
      if (property === 'then') return undefined;
      throw coded(`dsh_bridge does not provide ctx.${String(property)}`, 'UNSUPPORTED_CTX', { property: String(property) });
    },
  });
  return ctx;
}

function statusOf(record) {
  if (record.status) return record.status;
  /* Registration-time errors (duplicate names, malformed tools) are fatal upstream
   * (packages/core/tools/src/index.ts rejects an already-registered name), so they must not
   * masquerade as "partly working". Only skipped inject blocks, degraded features and
   * un-injected sections count as partial. */
  if (record.errors.length > 0) return 'incompatible';
  if (record.skipped.size > 0 || record.degraded.size > 0 || record.sections.length > 0) return 'partial';
  return 'ok';
}

function describe(record) {
  return {
    id: record.id,
    path: record.path,
    status: statusOf(record),
    missing: [...record.missing],
    skipped: [...record.skipped],
    degraded: [...record.degraded, ...(record.sections.length > 0 ? ['systemPrompt.section (not auto-injected)'] : [])],
    errors: record.errors,
    tools: record.tools.map((name) => {
      const { tool } = tools.get(name);
      return {
        name,
        description: typeof tool.description === 'string' ? tool.description : '',
        parameters: tool.parameters && typeof tool.parameters === 'object' ? tool.parameters : { type: 'object', properties: {} },
        /* null means the engine declared no budget; the caller then uses the default. */
        timeoutMs: Number.isFinite(tool.timeoutMs) && tool.timeoutMs > 0 ? tool.timeoutMs : null,
      };
    }),
    sections: record.sections.map((section) => ({ name: section.name, order: section.order ?? null })),
  };
}

async function loadPackage(dshHome, spec) {
  const path = isAbsolute(spec) ? spec : resolve(dshHome, spec);
  const record = {
    id: spec, path, status: null, missing: new Set(), skipped: new Set(), degraded: new Set(),
    errors: [], tools: [], sections: [], hooks: [], orders: new Map(), disposers: [],
  };
  packages.push(record);
  let manifest;
  try {
    manifest = JSON.parse(await readFile(join(path, 'package.json'), 'utf8'));
  } catch (error) {
    record.status = 'not-found';
    record.errors.push(`package.json: ${error.code || error.message}`);
    return describe(record);
  }
  record.id = typeof manifest.name === 'string' ? manifest.name : spec;
  const entry = engineEntry(manifest);
  if (!entry) {
    // Host bridge / client bundle only: these need the DSH host and browser.
    record.status = 'no-engine';
    return describe(record);
  }
  let engine;
  try {
    engine = await import(pathToFileURL(join(path, entry)).href);
  } catch (error) {
    record.status = 'load-failed';
    record.errors.push(`import: ${(error.code || '')} ${error.message}`.trim().slice(0, MAX_ERROR_CHARS));
    return describe(record);
  }
  for (const dep of Array.isArray(engine.inject) ? engine.inject : []) {
    if (!SERVICES.has(dep)) record.missing.add(dep);
  }
  if (record.missing.size > 0 || typeof engine.apply !== 'function') {
    if (typeof engine.apply !== 'function') record.errors.push('engine has no apply()');
    record.status = 'incompatible';
    return describe(record);
  }
  const before = new Set(tools.keys());
  try {
    await engine.apply(createShim(record));
  } catch (error) {
    if (error?.code === 'UNSUPPORTED_CTX') record.missing.add(error.property);
    else record.errors.push(`apply: ${error?.message || error}`.slice(0, MAX_ERROR_CHARS));
    // Roll back whatever the half-applied engine registered.
    for (const name of tools.keys()) if (!before.has(name)) tools.delete(name);
    // Unwind its effects too, newest first, the way cordis disposes a plugin.
    for (const dispose of record.disposers.splice(0).reverse()) {
      try {
        dispose();
      } catch (error) {
        record.errors.push(`dispose: ${error?.message || error}`.slice(0, MAX_ERROR_CHARS));
      }
    }
    record.tools = [];
    record.sections = [];
    record.hooks = [];
    record.status = 'incompatible';
  }
  return describe(record);
}

async function runPreStep(record) {
  for (const { event, handler } of record.hooks) {
    if (event !== 'agent/pre-step') continue;
    try {
      await handler({}, async () => {});
    } catch (error) {
      console.warn(`[dsh_bridge] ${record.id} pre-step: ${error?.message || error}`);
    }
  }
}

async function renderSections() {
  const rendered = [];
  for (const record of packages) {
    await runPreStep(record);
    for (const section of record.sections) {
      const text = typeof section.text === 'function' ? await section.text() : section.text;
      rendered.push({ package: record.id, name: section.name, order: section.order ?? null, text: String(text ?? '') });
    }
  }
  return rendered.sort((a, b) => (a.order ?? 0) - (b.order ?? 0));
}

function renderText(tool, args, value) {
  if (typeof tool.output?.render === 'function') {
    const parts = tool.output.render(args, value);
    if (Array.isArray(parts)) return parts.filter((part) => part?.type === 'text').map((part) => part.text).join('\n');
  }
  return toText(value);
}

async function callTool({ tool: name, args = {}, timeoutMs }) {
  const entry = tools.get(name);
  if (!entry) throw coded(`unknown tool ${name}`, 'UNKNOWN_TOOL');
  const { tool } = entry;
  const budget = Number.isFinite(timeoutMs) && timeoutMs > 0 ? timeoutMs : budgetOf(tool);
  const controller = new AbortController();
  let timer;
  const expired = new Promise((_, reject) => {
    timer = setTimeout(() => {
      controller.abort();
      reject(coded(`${name} timed out after ${budget}ms`, 'TIMEOUT'));
    }, budget);
  });
  try {
    const value = await Promise.race([tool.execute(args, { signal: controller.signal }), expired]);
    return { value: value ?? null, text: renderText(tool, args, value) };
  } finally {
    clearTimeout(timer);
  }
}

async function handle(message) {
  switch (message.op) {
    case 'ping':
      return { node: process.version };
    case 'load': {
      const dshHome = typeof message.dshHome === 'string' ? message.dshHome : '';
      const report = [];
      for (const spec of Array.isArray(message.packages) ? message.packages : []) {
        report.push(await loadPackage(dshHome, spec));
      }
      return { packages: report };
    }
    case 'call':
      return callTool(message);
    case 'sections':
      return { sections: await renderSections() };
    default:
      throw coded(`unknown op ${message.op}`, 'UNKNOWN_OP');
  }
}

/* Keep every frame small. The peer reads one JSON line per frame with an 8 MiB stream
 * limit; an oversized line makes its StreamReader raise, which used to kill the reader
 * task and wedge the bridge. Replies that do not fit one frame are split into ordered
 * chunks (`{i, n, data}`) that the peer reassembles, so a large result arrives whole.
 * Only a reply above MAX_RESULT_CHARS is truncated. */
function writeResult(id, result) {
  let text;
  try {
    text = JSON.stringify(result);
  } catch {
    // Cycles or BigInt: the frame itself must still be valid JSON.
    writeFrame({ id, ok: true, result: { truncated: true, text: toText(result?.text ?? result).slice(0, MAX_FRAME_CHARS) } });
    return;
  }
  if (text.length <= MAX_FRAME_CHARS) {
    writeFrame({ id, ok: true, result });
    return;
  }
  if (text.length > MAX_RESULT_CHARS) {
    writeFrame({ id, ok: true, result: { truncated: true, text: String(result?.text ?? '').slice(0, MAX_FRAME_CHARS) } });
    return;
  }
  const parts = [];
  for (let start = 0; start < text.length;) {
    let end = Math.min(start + CHUNK_CHARS, text.length);
    if (end < text.length && isHighSurrogate(text.charCodeAt(end - 1))) end -= 1;
    parts.push(text.slice(start, end));
    start = end;
  }
  parts.forEach((data, index) => writeFrame({ id, ok: true, chunk: { i: index, n: parts.length, data } }));
}

async function reply(message) {
  try {
    writeResult(message.id, await handle(message));
  } catch (error) {
    writeFrame({
      id: message.id,
      ok: false,
      error: { code: String(error?.code || 'TOOL_FAILED'), message: String(error?.message || error).slice(0, MAX_ERROR_CHARS) },
    });
  }
}

/* Tool calls run concurrently; every other op is a barrier, so a call never
 * observes a half-finished load and `sections` sees every loaded package. */
let barrier = Promise.resolve();
const pending = new Set();
const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
lines.on('line', (line) => {
  if (line.trim() === '') return;
  let message;
  try {
    message = JSON.parse(line);
  } catch {
    writeFrame({ id: null, ok: false, error: { code: 'BAD_FRAME', message: 'invalid JSON' } });
    return;
  }
  const task = message.op === 'call'
    ? barrier.then(() => reply(message))
    : (barrier = Promise.all([barrier, ...pending]).then(() => reply(message)));
  pending.add(task);
  task.finally(() => pending.delete(task));
});
lines.on('close', () => {
  void Promise.allSettled([barrier, ...pending]).then(() => process.exit(0));
});
