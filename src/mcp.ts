import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { createRequire } from "node:module";
import { hostname } from "node:os";
import { z } from "zod/v4";

const CONTROL = process.env.PI_MESH_CONTROL_URL ?? "http://127.0.0.1:7372";
const FROM = process.env.PI_MESH_MCP_NAME ?? `mcp@${hostname()}`;
const requireFromHere = createRequire(import.meta.url);
const { version } = requireFromHere("../package.json") as { version: string };

const server = new McpServer({ name: "pi-mesh", version });

server.registerTool(
  "agent_list",
  { description: "List pi-mesh agents available by id and alias." },
  async () => text(await agentList()),
);

server.registerTool(
  "agent_send",
  {
    description: "Send a fire-and-forget message to another pi-mesh agent.",
    inputSchema: {
      to: z.string().describe("Target agent id or alias"),
      message: z.string().describe("Message to send"),
    },
  },
  async ({ to, message }) => {
    await request("/local/send", { from: FROM, to, body: message });
    return text(`sent to ${to}`);
  },
);

server.registerTool(
  "agent_request",
  {
    description: "Ask another pi-mesh agent and wait for its reply.",
    inputSchema: {
      to: z.string().describe("Target agent id or alias"),
      message: z.string().describe("Request text"),
      timeout_seconds: z.number().optional().describe("Timeout seconds, default 30"),
    },
  },
  async ({ to, message, timeout_seconds }) => {
    const timeoutMs = Math.max(1, timeout_seconds ?? 30) * 1000;
    const res = await request("/local/request", { from: FROM, to, body: message, timeout_ms: timeoutMs });
    return text(String(res.body ?? ""));
  },
);

async function main() {
  await server.connect(new StdioServerTransport());
}

async function agentList() {
  try {
    const list = await request("/local/list");
    const show = (x: any) => `${x.alias} (${x.id}) ${x.addr}`;
    const local = (list.local ?? []).map(show).join("\n  ") || "none";
    const remote = (list.remote ?? []).map(show).join("\n  ") || "none";
    return `client: ${FROM}\nservice: ${list.self}\nlocal:\n  ${local}\nremote:\n  ${remote}`;
  } catch {
    return "pi-mesh daemon not running; run /mesh on in Pi";
  }
}

async function request(path: string, body?: unknown) {
  const res = await fetch(`${CONTROL}${path}`, body === undefined ? undefined : {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json() as Promise<any>;
}

function text(text: string) {
  return { content: [{ type: "text" as const, text }] };
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
