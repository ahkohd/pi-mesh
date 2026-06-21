import { keyHint, truncateToVisualLines, type ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { basename, delimiter, dirname, join } from "node:path";
import { homedir, hostname } from "node:os";

const CONTROL = process.env.PI_MESH_CONTROL_URL ?? "http://127.0.0.1:7372";
const PROTOCOL_VERSION = 3;
const requireFromHere = createRequire(import.meta.url);
const BIN = process.env.PI_MESH_BIN ?? bundledBin("pi-mesh") ?? "pi-mesh";
const ALIASES = join(appDataDir(), "pi-mesh", "aliases.json");
const STATE_ENTRY = "pi-mesh-state";
const ADJ = [
  "brave", "calm", "clever", "cosmic", "curious", "dapper", "dusty", "fuzzy", "gentle", "glad",
  "golden", "happy", "honest", "jolly", "lazy", "lucky", "neon", "nimble", "quiet", "rapid",
  "rusty", "shiny", "sleepy", "solar", "tidy", "tiny", "velvet", "witty", "zesty", "zippy",
];
const NOUN = [
  "badger", "beaver", "bobcat", "falcon", "ferret", "fox", "gecko", "heron", "koala", "lemur",
  "lynx", "marmot", "moose", "otter", "panda", "penguin", "quokka", "rabbit", "raven", "seal",
  "sloth", "sparrow", "tiger", "turtle", "weasel", "whale", "wombat", "yak", "zebra", "zorilla",
];

type AgentInfo = {
  id: string;
  alias: string;
  title?: string;
  cwd: string;
  runtime?: any;
};

type MeshMsg = {
  from: string;
  to: string;
  id: string;
  kind: "send" | "request";
  body: unknown;
  from_agent: AgentInfo;
};

type MeshState = { on?: boolean; peer?: string };

let agentId: string | undefined;
let agentAlias: string | undefined;
let agentTitle: string | undefined;
let agentCwd = middlePath(process.cwd());
let agentRuntime: Record<string, unknown> | undefined;
let pollAbort: AbortController | undefined;
let heartbeat: NodeJS.Timeout | undefined;
let pendingReplyIds: string[] = [];

export default function (pi: ExtensionAPI) {
  pi.registerFlag("mesh-on", {
    description: "Start pi-mesh for this session",
    type: "boolean",
    default: false,
  });

  pi.on("session_start", async (_event, ctx) => {
    const state = persistedMeshState(ctx);
    if (!pi.getFlag("mesh-on") && !state.on) return;
    try {
      await turnMeshOn(pi, ctx, state.peer);
    } catch (error) {
      ctx.ui.notify(`mesh on failed: ${error instanceof Error ? error.message : String(error)}`, "error");
    }
  });

  pi.registerCommand("mesh", {
    description: "pi-mesh: /mesh on [peer], /mesh off, /mesh list, /mesh alias [name]",
    handler: async (args, ctx) => {
      const [cmd = "list", ...restParts] = args.trim().split(/\s+/).filter(Boolean);
      const rest = restParts.join(" ") || undefined;

      if (cmd === "on") {
        await turnMeshOn(pi, ctx, rest);
        saveMeshState(pi, true, rest);
        ctx.ui.notify(`mesh on: ${agentAlias} (${agentId})`, "info");
        return;
      }

      if (cmd === "off") {
        saveMeshState(pi, false);
        await meshOff();
        ctx.ui.notify("mesh off", "info");
        return;
      }

      if (cmd === "list") {
        ctx.ui.notify(await agentListText(), "info");
        return;
      }

      if (cmd === "alias") {
        const id = agentId ?? makeAgentId(ctx);
        if (!rest) {
          ctx.ui.notify(`alias: ${await loadAlias(id)}\nid: ${id}`, "info");
          return;
        }
        const alias = `${slug(rest)}@${machine()}`;
        await saveAlias(id, alias);
        agentId = id;
        agentAlias = alias;
        if (await daemonCompatible()) await registerSelf();
        ctx.ui.notify(`alias: ${alias}`, "info");
        return;
      }

      ctx.ui.notify("usage: /mesh on [peer] | off | list | alias [name]", "error");
    },
  });

  pi.registerTool({
    name: "mesh_on",
    label: "Mesh On",
    description: "Register this Pi session with pi-mesh.",
    parameters: Type.Object({
      peer: Type.Optional(Type.String({ description: "Optional peer service address host:port" })),
    }),
    async execute(_id, params, _signal, _onUpdate, ctx) {
      await turnMeshOn(pi, ctx, params.peer);
      saveMeshState(pi, true, params.peer);
      return { content: [{ type: "text", text: await agentListText() }], details: currentAgent() ?? {} };
    },
  });

  pi.registerTool({
    name: "mesh_off",
    label: "Mesh Off",
    description: "Unregister this Pi session from pi-mesh.",
    parameters: Type.Object({}),
    async execute() {
      saveMeshState(pi, false);
      await meshOff();
      return { content: [{ type: "text", text: "mesh off" }], details: {} };
    },
  });

  pi.registerTool({
    name: "agent_list",
    label: "Agent List",
    description: "List pi-mesh agents available by id and alias.",
    parameters: Type.Object({}),
    async execute() {
      const current = currentAgent();
      try {
        const list = await getJson("/local/list");
        return { content: [{ type: "text", text: formatList(list, current) }], details: { current, ...list } };
      } catch {
        return { content: [{ type: "text", text: meshOffText() }], details: { current } };
      }
    },
  });

  pi.registerTool({
    name: "agent_send",
    label: "Agent Send",
    description: "Send a fire-and-forget message to another pi-mesh agent.",
    parameters: Type.Object({
      to: Type.String({ description: "Target agent id or alias, e.g. clever-otter@mbp" }),
      message: Type.String({ description: "Message to send" }),
    }),
    renderCall(params, theme, context) {
      return textComponent(outgoingPreview("send to", params.to, params.message, context.expanded, theme), context.expanded);
    },
    async execute(_id, params) {
      if (!agentId) throw new Error("mesh is off; run /mesh on");
      await post("/local/send", { from: agentId, to: params.to, body: params.message });
      return { content: [{ type: "text", text: "sent" }], details: {} };
    },
  });

  pi.registerTool({
    name: "agent_request",
    label: "Agent Request",
    description: "Ask another pi-mesh agent and wait for its reply.",
    parameters: Type.Object({
      to: Type.String({ description: "Target agent id or alias, e.g. clever-otter@mbp" }),
      message: Type.String({ description: "Request text" }),
      timeout_seconds: Type.Optional(Type.Number({ description: "Timeout seconds, default 30" })),
    }),
    renderCall(params, theme, context) {
      return textComponent(outgoingPreview("request to", params.to, params.message, context.expanded, theme), context.expanded);
    },
    async execute(_id, params) {
      if (!agentId) throw new Error("mesh is off; run /mesh on");
      const timeoutMs = Math.max(1, params.timeout_seconds ?? 30) * 1000;
      const res = await post("/local/request", {
        from: agentId,
        to: params.to,
        body: params.message,
        timeout_ms: timeoutMs,
      });
      return { content: [{ type: "text", text: String((res as any).body ?? "") }], details: {} };
    },
  });

  pi.on("agent_end", async (event: any) => {
    const id = pendingReplyIds.shift();
    if (!id) return;
    const body = lastAssistantText(event.messages) || "";
    await post("/local/reply", { id, body }).catch(() => undefined);
  });

  pi.on("session_shutdown", async () => {
    await meshOff().catch(() => undefined);
  });
}

async function turnMeshOn(pi: ExtensionAPI, ctx: any, peer?: string) {
  const id = makeAgentId(ctx);
  const alias = await loadAlias(id);
  agentId = id;
  agentAlias = alias;
  agentTitle = currentTitle(pi);
  agentCwd = currentCwd(ctx);
  agentRuntime = currentRuntime(ctx);
  await ensureDaemon();
  if (peer) await post("/local/peer", { addr: peer });
  await registerSelf();
  startPolling(pi, id);
  startHeartbeat(pi, ctx);
}

async function ensureDaemon() {
  if (await daemonCompatible()) return;
  await post("/local/shutdown", {}).catch(() => undefined);
  await sleep(150);

  const child = spawn(BIN, ["daemon"], { detached: true, stdio: "ignore", env: serviceEnv(BIN) });
  child.unref();

  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    if (await daemonCompatible()) return;
    await sleep(150);
  }
  throw new Error(`failed to start ${BIN}`);
}

function bundledBin(name: string) {
  const packageName = platformPackageName();
  if (!packageName) return undefined;

  try {
    const packageJson = requireFromHere.resolve(`${packageName}/package.json`);
    const binary = join(dirname(packageJson), "bin", executableName(name));
    return existsSync(binary) ? binary : undefined;
  } catch {
    return undefined;
  }
}

function platformPackageName() {
  if (process.platform === "darwin" && process.arch === "arm64") return "@ahkohd/pi-mesh-darwin-arm64";
  if (process.platform === "darwin" && process.arch === "x64") return "@ahkohd/pi-mesh-darwin-x64";
  if (process.platform === "linux" && process.arch === "arm64") return "@ahkohd/pi-mesh-linux-arm64-gnu";
  if (process.platform === "linux" && process.arch === "x64") return "@ahkohd/pi-mesh-linux-x64-gnu";
  return undefined;
}

function executableName(name: string) {
  return process.platform === "win32" ? `${name}.exe` : name;
}

function serviceEnv(bin: string): NodeJS.ProcessEnv {
  if (!isPathLike(bin)) return process.env;
  return {
    ...process.env,
    PATH: [dirname(bin), process.env.PATH].filter(Boolean).join(delimiter),
  };
}

function isPathLike(bin: string) {
  return bin.includes("/") || bin.includes("\\");
}

async function daemonCompatible() {
  try {
    const health = await getJson("/health");
    return health.protocol === PROTOCOL_VERSION;
  } catch {
    return false;
  }
}

async function registerSelf() {
  if (!agentId || !agentAlias) return;
  await post("/local/register", { id: agentId, alias: agentAlias, title: agentTitle, cwd: agentCwd, runtime: agentRuntime });
}

function startPolling(pi: ExtensionAPI, id: string) {
  pollAbort?.abort();
  pollAbort = new AbortController();
  void pollLoop(pi, id, pollAbort.signal);
}

async function pollLoop(pi: ExtensionAPI, id: string, signal: AbortSignal) {
  while (!signal.aborted) {
    try {
      const res = await fetch(`${CONTROL}/local/next?agent=${encodeURIComponent(id)}`, { signal });
      if (res.status === 204) continue;
      if (!res.ok) throw new Error(await res.text());
      const msg = (await res.json()) as MeshMsg;
      if (msg.kind === "request") pendingReplyIds.push(msg.id);
      pi.sendUserMessage(formatIncoming(msg), { deliverAs: "followUp" } as any);
    } catch {
      if (!signal.aborted) await sleep(1000);
    }
  }
}

function startHeartbeat(pi: ExtensionAPI, ctx: any) {
  clearInterval(heartbeat);
  heartbeat = setInterval(() => {
    agentTitle = currentTitle(pi);
    agentCwd = currentCwd(ctx);
    agentRuntime = currentRuntime(ctx);
    void registerSelf().catch(() => undefined);
  }, 1_000);
}

async function meshOff() {
  pollAbort?.abort();
  pollAbort = undefined;
  clearInterval(heartbeat);
  heartbeat = undefined;
  if (agentId) await post("/local/unregister", { id: agentId }).catch(() => undefined);
  agentId = undefined;
  agentAlias = undefined;
  agentTitle = undefined;
  agentCwd = middlePath(process.cwd());
  agentRuntime = undefined;
  pendingReplyIds = [];
}

function saveMeshState(pi: ExtensionAPI, on: boolean, peer?: string) {
  pi.appendEntry(STATE_ENTRY, { on, peer });
}

function persistedMeshState(ctx: any): MeshState {
  const entries = ctx.sessionManager?.getBranch?.() ?? ctx.sessionManager?.getEntries?.() ?? [];
  for (const entry of [...entries].reverse()) {
    if (entry.type === "custom" && entry.customType === STATE_ENTRY) return (entry.data ?? {}) as MeshState;
  }
  return {};
}

function currentTitle(pi: ExtensionAPI) {
  return pi.getSessionName()?.trim() || undefined;
}

function currentCwd(ctx: any) {
  return middlePath(typeof ctx.cwd === "string" ? ctx.cwd : process.cwd());
}

function middlePath(path: string, max = 40) {
  const home = homedir().replace(/\\/g, "/");
  const normalized = path.replace(/\\/g, "/");
  const p = home && normalized === home
    ? "~"
    : home && normalized.startsWith(`${home}/`)
      ? `~/${normalized.slice(home.length + 1)}`
      : normalized;
  if (p.length <= max) return p;
  const root = p.startsWith("~/") ? `~/${p.split("/")[1]}` : p.split("/").slice(0, 2).join("/") || p;
  const leaf = basename(p).slice(-Math.max(8, max - root.length - 5));
  return `${root}/.../${leaf}`;
}

function currentRuntime(ctx: any) {
  const model = ctx.model;
  if (!model) return undefined;
  const usage = ctx.getContextUsage?.();
  return {
    model: model.name ?? model.id,
    provider: model.provider,
    context: usage && {
      used: usage.tokens,
      total: usage.contextWindow,
      free: usage.tokens == null ? null : Math.max(0, usage.contextWindow - usage.tokens),
      percent: usage.percent,
    },
  };
}

function makeAgentId(ctx: any) {
  const file = ctx.sessionManager?.getSessionFile?.();
  const raw = file ? basename(file).replace(/\.(jsonl|json)$/i, "") : `ephemeral-${process.pid}`;
  return `${slug(raw)}@${machine()}`;
}

async function loadAlias(id: string) {
  const aliases = await readAliases();
  if (aliases[id]) return aliases[id];
  const alias = `${funnyName(id)}@${machine()}`;
  await saveAlias(id, alias);
  return alias;
}

async function readAliases(): Promise<Record<string, string>> {
  try {
    return JSON.parse(await readFile(ALIASES, "utf8"));
  } catch {
    return {};
  }
}

async function saveAlias(id: string, alias: string) {
  const aliases = await readAliases();
  aliases[id] = alias;
  await mkdir(dirname(ALIASES), { recursive: true });
  await writeFile(ALIASES, JSON.stringify(aliases, null, 2));
}

function appDataDir() {
  if (process.platform === "darwin") return join(homedir(), "Library", "Application Support");
  if (process.platform === "win32") return process.env.APPDATA ?? join(homedir(), "AppData", "Roaming");
  return process.env.XDG_DATA_HOME ?? join(homedir(), ".local", "share");
}

function funnyName(input: string) {
  const bytes = createHash("sha256").update(input).digest();
  return `${ADJ[bytes[0] % ADJ.length]}-${NOUN[bytes[1] % NOUN.length]}`;
}

function machine() {
  return slug(hostname());
}

function slug(s: string) {
  return s.toLowerCase().replace(/[^a-z0-9._-]+/g, "-").replace(/^-+|-+$/g, "") || "agent";
}

function formatIncoming(msg: MeshMsg) {
  const text = typeof msg.body === "string" ? msg.body : JSON.stringify(msg.body);
  const from = agentLabel(msg.from_agent);
  const id = `\nid: ${msg.from_agent.id}`;
  if (msg.kind === "request") {
    return `pi-mesh request from ${from}${id}\n\n${text}\n\nReply normally. Your final answer will be returned to ${from}.`;
  }
  return `pi-mesh message from ${from}${id}\n\n${text}`;
}

function outgoingPreview(action: string, to: string, message: string, expanded: boolean, theme?: any) {
  return `${action} ${theme?.fg?.("accent", to) ?? to}\n${expanded ? message : trimPreview(message)}`;
}

function trimPreview(text: string) {
  if (text.length <= 800) return text;
  return `${text.slice(0, 800)}... (${text.length - 800} more chars, ${keyHint("app.tools.expand", "to expand")})`;
}

function textComponent(text: string, expanded: boolean) {
  return {
    render: (width: number) => {
      const lines = truncateToVisualLines(text, Number.MAX_SAFE_INTEGER, Math.max(1, width)).visualLines;
      if (expanded || lines.length <= 12) return lines;
      const hint = `... (${lines.length - 12} more lines, ${keyHint("app.tools.expand", "to expand")})`;
      return [...lines.slice(0, 12), ...truncateToVisualLines(hint, 1, Math.max(1, width)).visualLines];
    },
    invalidate: () => undefined,
  };
}

function agentLabel(agent: AgentInfo) {
  return `${agent.alias}${agent.title ? ` - ${agent.title}` : ""} ${agent.cwd}${runtimeLabel(agent)}`;
}

function lastAssistantText(messages: any[]) {
  const msg = [...(messages ?? [])].reverse().find((m) => m?.role === "assistant");
  const content = msg?.content;
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    return content.map((p) => (typeof p === "string" ? p : p?.text ?? "")).join("").trim();
  }
  return "";
}

async function agentListText() {
  const current = currentAgent();
  try {
    return formatList(await getJson("/local/list"), current);
  } catch {
    return meshOffText();
  }
}

function currentAgent() {
  return agentId && agentAlias ? { id: agentId, alias: agentAlias, title: agentTitle, cwd: agentCwd, runtime: agentRuntime } : undefined;
}

function meshOffText() {
  return "current: mesh off (this Pi session is not registered)";
}

function formatList(list: any, current?: AgentInfo) {
  const show = (x: any) => `${agentLabel(x)} (${x.id}) ${x.addr}${current?.id === x.id ? " [me]" : ""}`;
  const local = (list.local ?? []).map(show).join("\n  ") || "none";
  const remote = (list.remote ?? []).map(show).join("\n  ") || "none";
  const me = current ? `${agentLabel(current)} (${current.id})` : "mesh off (this Pi session is not registered)";
  return `current: ${me}\nservice: ${list.self}\nlocal:\n  ${local}\nremote:\n  ${remote}`;
}

function runtimeLabel(x: any) {
  const model = x.runtime?.model;
  if (!model) return "";
  const provider = x.runtime?.provider ? `@${x.runtime.provider}` : "";
  const free = x.runtime?.context?.free;
  return ` [${model}${provider}${typeof free === "number" ? `, ${free} ctx free` : ""}]`;
}

async function getJson(path: string) {
  const res = await fetch(`${CONTROL}${path}`);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

async function post(path: string, body: unknown) {
  const res = await fetch(`${CONTROL}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
