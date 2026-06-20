import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { basename, join } from "node:path";
import { homedir, hostname } from "node:os";

const CONTROL = process.env.PI_MESH_CONTROL_URL ?? "http://127.0.0.1:7372";
const BIN = process.env.PI_MESH_BIN ?? "pi-mesh";
const ALIASES = join(homedir(), ".pi", "mesh", "aliases.json");

const ADJ = ["brave", "calm", "clever", "cosmic", "fuzzy", "glad", "lazy", "neon", "quiet", "rapid"];
const NOUN = ["badger", "beaver", "falcon", "otter", "panda", "raven", "tiger", "yak", "zebra", "koala"];

type MeshMsg = {
  from: string;
  to: string;
  id: string;
  re?: string | null;
  kind: "send" | "request";
  body: unknown;
};

let agentId: string | undefined;
let agentAlias: string | undefined;
let pollAbort: AbortController | undefined;
let heartbeat: NodeJS.Timeout | undefined;
let pendingReplyIds: string[] = [];

export default function (pi: ExtensionAPI) {
  pi.registerCommand("mesh", {
    description: "pi-mesh: /mesh on [seed], /mesh off, /mesh list, /mesh alias [name]",
    handler: async (args, ctx) => {
      const [cmd = "list", rest] = splitOnce(args.trim());

      if (cmd === "on") {
        const id = makeAgentId(ctx);
        const alias = await loadAlias(id);
        agentId = id;
        agentAlias = alias;
        await ensureDaemon();
        if (rest) await post("/local/seed", { addr: rest });
        await registerSelf();
        startPolling(pi, id);
        startHeartbeat();
        ctx.ui.notify(`mesh on: ${alias} (${id})`, "info");
        return;
      }

      if (cmd === "off") {
        await meshOff();
        ctx.ui.notify("mesh off", "info");
        return;
      }

      if (cmd === "list") {
        const list = await getJson("/local/list");
        ctx.ui.notify(formatList(list), "info");
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
        if (await daemonUp()) await registerSelf();
        ctx.ui.notify(`alias: ${alias}`, "info");
        return;
      }

      ctx.ui.notify("usage: /mesh on [seed] | off | list | alias [name]", "error");
    },
  });

  pi.registerTool({
    name: "agent_list",
    label: "Agent List",
    description: "List pi-mesh agents available by id and alias.",
    parameters: Type.Object({}),
    async execute() {
      const list = await getJson("/local/list");
      return { content: [{ type: "text", text: formatList(list) }], details: list };
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
    async execute(_id, params) {
      if (!agentId) throw new Error("mesh is off; run /mesh on");
      await post("/local/send", { from: agentId, to: params.to, body: params.message });
      return { content: [{ type: "text", text: `sent to ${params.to}` }], details: {} };
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

async function ensureDaemon() {
  if (await daemonUp()) return;

  const child = spawn(BIN, ["daemon"], { detached: true, stdio: "ignore" });
  child.unref();

  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    if (await daemonUp()) return;
    await sleep(150);
  }
  throw new Error(`failed to start ${BIN}`);
}

async function daemonUp() {
  try {
    await getJson("/health");
    return true;
  } catch {
    return false;
  }
}

async function registerSelf() {
  if (!agentId || !agentAlias) return;
  await post("/local/register", { id: agentId, alias: agentAlias });
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

function startHeartbeat() {
  clearInterval(heartbeat);
  heartbeat = setInterval(() => {
    void registerSelf().catch(() => undefined);
  }, 15_000);
}

async function meshOff() {
  pollAbort?.abort();
  pollAbort = undefined;
  clearInterval(heartbeat);
  heartbeat = undefined;
  if (agentId && agentAlias) await post("/local/unregister", { id: agentId, alias: agentAlias }).catch(() => undefined);
  agentId = undefined;
  agentAlias = undefined;
  pendingReplyIds = [];
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
  await mkdir(join(homedir(), ".pi", "mesh"), { recursive: true });
  await writeFile(ALIASES, JSON.stringify(aliases, null, 2));
}

function funnyName(seed: string) {
  const bytes = createHash("sha256").update(seed).digest();
  return `${ADJ[bytes[0] % ADJ.length]}-${NOUN[bytes[1] % NOUN.length]}`;
}

function machine() {
  return slug(hostname());
}

function slug(s: string) {
  return s.toLowerCase().replace(/[^a-z0-9._-]+/g, "-").replace(/^-+|-+$/g, "") || "agent";
}

function splitOnce(s: string): [string | undefined, string | undefined] {
  const trimmed = s.trim();
  if (!trimmed) return [undefined, undefined];
  const i = trimmed.search(/\s/);
  if (i < 0) return [trimmed, undefined];
  return [trimmed.slice(0, i), trimmed.slice(i).trim() || undefined];
}

function formatIncoming(msg: MeshMsg) {
  const text = typeof msg.body === "string" ? msg.body : JSON.stringify(msg.body);
  if (msg.kind === "request") {
    return `pi-mesh request from ${msg.from}:\n\n${text}\n\nReply normally. Your final answer will be returned to ${msg.from}.`;
  }
  return `pi-mesh message from ${msg.from}:\n\n${text}`;
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

function formatList(list: any) {
  const show = (x: any) => `${x.alias} (${x.id}) ${x.addr}`;
  const local = (list.local ?? []).map(show).join("\n  ") || "none";
  const remote = (list.remote ?? []).map(show).join("\n  ") || "none";
  return `self: ${list.self}\nlocal:\n  ${local}\nremote:\n  ${remote}`;
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
