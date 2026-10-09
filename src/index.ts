#!/usr/bin/env node

/**
 * MCP Server for GLPI v3.0
 *
 * Major changes vs v2:
 *   - Unified HTTP layer with auto-reauth, structured errors, retries.
 *   - List tools accept start/limit/fetch_all/forcedisplay/criteria/sort/order
 *     (backward-compatible: `limit` alone still works).
 *   - New `glpi_count` and `glpi_search_v2` (multi-criteria, forcedisplay).
 *   - High-level `glpi_search_tickets` with friendly params (status/assigned/...).
 *   - `glpi_get_ticket_timeline` merges followups+tasks+solutions+validations.
 *   - `glpi_tickets_stats_by` ventilation by status/category/technician/entity/month.
 *   - Link, validation, document, SLA, satisfaction tools.
 *   - Field-id mapping via /listSearchOptions for resilience across GLPI versions.
 */

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { createServer, IncomingMessage } from 'node:http';
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
  ListResourcesRequestSchema,
  ReadResourceRequestSchema,
  ErrorCode,
  McpError,
} from '@modelcontextprotocol/sdk/types.js';
import { readFile } from 'node:fs/promises';
import { basename, extname } from 'node:path';
import { z } from 'zod';
import { GlpiClient, GlpiConfig, ListOptions } from './glpi-client.js';
import { GlpiError } from './http.js';
import { SearchCriterion, SearchType, SearchLink } from './search.js';

// ---------------------------------------------------------------------------
// Validation Schemas
// ---------------------------------------------------------------------------

const listArgsSchema = z.object({
  start: z.number().int().min(0).optional(),
  limit: z.number().int().min(1).max(10000).optional(),
  range: z.string().optional(),
  sort: z.string().optional(),
  order: z.enum(['ASC', 'DESC']).optional(),
  expand_dropdowns: z.boolean().optional(),
  criteria: z.array(z.unknown()).optional(),
  fetch_all: z.boolean().optional(),
}).passthrough();

const ticketReadSchema = z.object({
  id: z.number().int().min(1),
  with_logs: z.boolean().optional(),
}).passthrough();

const ticketSearchSchema = z.object({
  status: z.number().optional(),
  assigned_user_id: z.number().optional(),
  assigned_group_id: z.number().optional(),
  requester_user_id: z.number().optional(),
  category_id: z.number().optional(),
  entity_id: z.number().optional(),
  priority: z.number().optional(),
  urgency: z.number().optional(),
  date_from: z.string().optional(),
  date_to: z.string().optional(),
  text_search: z.string().optional(),
  open_only: z.boolean().optional(),
  start: z.number().int().min(0).optional(),
  limit: z.number().int().min(1).max(10000).optional(),
}).passthrough();

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const TICKET_STATUS: Record<number, string> = {
  1: 'New',
  2: 'Processing (assigned)',
  3: 'Processing (planned)',
  4: 'Pending',
  5: 'Solved',
  6: 'Closed',
};

const TICKET_URGENCY: Record<number, string> = {
  1: 'Very low',
  2: 'Low',
  3: 'Medium',
  4: 'High',
  5: 'Very high',
};

const PROBLEM_STATUS: Record<number, string> = {
  1: 'New', 2: 'Accepted', 3: 'Planned', 4: 'Pending', 5: 'Solved', 6: 'Closed',
};

const CHANGE_STATUS: Record<number, string> = {
  1: 'New', 2: 'Evaluation', 3: 'Approval', 4: 'Accepted', 5: 'Pending',
  6: 'Test', 7: 'Qualification', 8: 'Applied', 9: 'Review', 10: 'Closed',
  11: 'Refused', 12: 'Canceled',
};

const VALIDATION_STATUS: Record<number, string> = {
  1: 'Waiting', 2: 'Granted', 3: 'Refused',
};

// Standard Ticket search-option field ids (GLPI ≥ 9.5). Fallbacks; the
// SearchOptions cache is used to resolve friendly names dynamically.
const TICKET_FIELDS = {
  id: 2,
  name: 1,
  status: 12,
  date: 15,
  date_mod: 19,
  solvedate: 17,
  closedate: 16,
  priority: 3,
  urgency: 10,
  impact: 11,
  category: 7,
  entity: 80,
  requester_user: 4,
  technician_user: 5,
  technician_group: 8,
  type: 14,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function envInt(name: string): number | undefined {
  const raw = process.env[name];
  if (!raw) return undefined;
  const n = parseInt(raw, 10);
  if (isNaN(n) || n < 0) throw new Error(`${name} must be a non-negative integer, got "${raw}"`);
  return n;
}

// ---------------------------------------------------------------------------
// 结果体积控制
//
// 企业微信等客户端对单次工具返回的内容长度有限制,返回过大时平台会直接报
// “暂时无法回答问题,请稍后再试”。这里做三层保护:
//   1) 紧凑 JSON(去掉缩进,体积约省 30%~50%)
//   2) 过长的字符串字段自动截断(如超长的工单内容 / 跟进正文)
//   3) 整体仍超限时,自动裁剪数组条数,并在结果里给出“请缩小查询范围”的提示
// 相关系数可用环境变量调整。
// ---------------------------------------------------------------------------

/** 单次工具返回的最大字符数(默认约 24KB)。 */
const MAX_RESULT_CHARS = envInt('MCP_MAX_RESULT_CHARS') ?? 24000;
/** 单个字符串字段最长保留的字符数(默认 2000)。 */
const MAX_STRING_CHARS = envInt('MCP_MAX_STRING_CHARS') ?? 2000;
/** 列表类工具的默认返回条数。 */
const DEFAULT_LIST_LIMIT = envInt('MCP_DEFAULT_LIMIT') ?? 20;
/** fetch_all 时的最大行数上限。 */
const MAX_FETCH_ALL_ROWS = envInt('MCP_MAX_FETCH_ALL_ROWS') ?? 200;
/** 单次请求条数的硬上限(防止一次拉取过多数据)。 */
const MAX_LIST_LIMIT = envInt('MCP_MAX_LIST_LIMIT') ?? 200;

const TRUNCATE_HINT =
  '结果过大已自动截断。请缩小查询范围后重试:减小 limit、增加时间范围/状态/关键词等筛选条件,' +
  '或先用 glpi_count 查询总量,再分批分页获取。';

/**
 * Extract the service token from an incoming request.
 *
 * Several credential transports are accepted so the same server works with
 * different MCP clients:
 *   - `Authorization: Bearer <token>`   -> standard MCP clients
 *   - `X-API-Key: <token>`              -> generic API-key clients
 *   - `?apiKey=<token>` / `?token=`     -> 企业微信智能机器人「添加 MCP 插件」
 *                                          (授权方式 = Service token/API key,
 *                                           位置 = Query, 参数名 = apiKey)
 */
function extractRequestToken(req: IncomingMessage): string | undefined {
  const auth = req.headers.authorization;
  if (auth) {
    // 既支持标准的 `Bearer <token>`,也容忍直接填写裸 token 的情况
    // (企业微信「位置=Header,参数名=Authorization」时可能不带 Bearer 前缀)。
    const token = auth.startsWith('Bearer ') ? auth.substring(7).trim() : auth.trim();
    if (token) return token;
  }

  const apiKeyHeader = req.headers['x-api-key'];
  const headerToken = Array.isArray(apiKeyHeader) ? apiKeyHeader[0] : apiKeyHeader;
  if (headerToken && headerToken.trim()) return headerToken.trim();

  // req.url is e.g. "/mcp?apiKey=xxx" — parse it so query params are readable
  // regardless of the request path.
  try {
    const url = new URL(req.url || '/', 'http://localhost');
    const qsToken = url.searchParams.get('apiKey') ?? url.searchParams.get('token');
    if (qsToken && qsToken.trim()) return qsToken.trim();
  } catch {
    /* ignore malformed URL */
  }

  return undefined;
}

/**
 * 兼容性补齐 Accept 头。
 *
 * MCP Streamable HTTP 传输层要求 POST 的 Accept 必须同时包含
 * `application/json` 和 `text/event-stream`,否则 SDK 会直接返回
 * 406 Not Acceptable —— 企业微信等客户端会因此报「插件工具获取失败」。
 *
 * 注意:@hono/node-server 是从 Node 的 rawHeaders 构造 Web 请求头的,
 * 所以必须同时修改 rawHeaders 与 headers 才生效。
 */
function ensureMcpAcceptHeader(req: IncomingMessage): void {
  // 只对 POST 补齐:GET 若被补齐会被 SDK 当成 SSE 长连接挂住。
  if (req.method !== 'POST') return;

  const existing = req.headers.accept || '';
  if (existing.includes('application/json') && existing.includes('text/event-stream')) {
    return;
  }

  const merged = 'application/json, text/event-stream';
  req.rawHeaders.push('Accept', merged);
  req.headers.accept = existing ? `${existing}, ${merged}` : merged;
}

function checkMcpToken(req: IncomingMessage): boolean {
  const expectedToken = process.env.MCP_SERVICE_TOKEN;

  // No token configured -> leave the endpoint open (a warning is logged at
  // startup). Recommended: always set MCP_SERVICE_TOKEN in production.
  if (!expectedToken) {
    return true;
  }

  return extractRequestToken(req) === expectedToken;
}

// ---------------------------------------------------------------------------
// 人类可读日志
// ---------------------------------------------------------------------------

type LogLevel = 'INFO' | 'WARN' | 'ERROR';

/** 时间戳,例如 2026-10-08 15:04:05.123 */
function timestamp(): string {
  const d = new Date();
  const p = (n: number, len = 2) => String(n).padStart(len, '0');
  return (
    `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ` +
    `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${p(d.getMilliseconds(), 3)}`
  );
}

/** 统一输出格式:时间 [级别] 内容 */
function log(level: LogLevel, message: string): void {
  console.error(`${timestamp()} [${level.padEnd(5)}] ${message}`);
}

/** 取真实调用方 IP:优先经过反向代理时携带的 X-Forwarded-For / X-Real-IP。 */
function getClientIp(req: IncomingMessage): string {
  const xff = req.headers['x-forwarded-for'];
  const first = Array.isArray(xff) ? xff[0] : xff;
  if (typeof first === 'string' && first.trim()) return first.split(',')[0].trim();

  const real = req.headers['x-real-ip'];
  const realVal = Array.isArray(real) ? real[0] : real;
  if (typeof realVal === 'string' && realVal.trim()) return realVal.trim();

  return req.socket?.remoteAddress || 'unknown';
}

/** 把工具入参压成一行,便于阅读(超长内容截断,最多展示 8 个参数)。 */
function summarizeArgs(args: unknown): string {
  if (!args || typeof args !== 'object') return '';
  const entries = Object.entries(args as Record<string, unknown>).filter(
    ([, v]) => v !== undefined && v !== null && v !== ''
  );
  if (!entries.length) return '';

  const shown = entries.slice(0, 8).map(([k, v]) => {
    let val: string;
    if (typeof v === 'string') val = `"${v.length > 40 ? v.slice(0, 40) + '…' : v}"`;
    else if (Array.isArray(v)) val = `[${v.length} 项]`;
    else if (v && typeof v === 'object') val = '{…}';
    else val = String(v);
    return `${k}=${val}`;
  });

  return ' (' + shown.join(', ') + (entries.length > 8 ? ', …' : '') + ')';
}

/** 从工具返回结果里提取一小段摘要(取第一段文本,截断到 160 字符)。 */
function summarizeResult(result: unknown): string {
  const content = (result as { content?: unknown })?.content;
  if (!Array.isArray(content) || !content.length) return '';
  const first = content[0] as { type?: string; text?: string } | undefined;
  if (first && first.type === 'text' && typeof first.text === 'string') {
    const compact = first.text.replace(/\s+/g, ' ').trim();
    return ` → ${compact.length > 160 ? compact.slice(0, 160) + '…' : compact}`;
  }
  return '';
}

/** 每个 HTTP 请求的上下文,用于把调用方 IP 关联到具体的工具调用。 */
interface LogContext {
  ip: string;
}

function getConfig(): GlpiConfig {
  const url = process.env.GLPI_URL;
  if (!url) throw new Error('GLPI_URL environment variable is required');
  try {
    new URL(url);
  } catch {
    throw new Error(`GLPI_URL is not a valid URL: "${url}"`);
  }

  const userToken = process.env.GLPI_USER_TOKEN;
  const username = process.env.GLPI_USERNAME;
  const password = process.env.GLPI_PASSWORD;
  if (!userToken && !(username && password)) {
    throw new Error(
      'No authentication configured. Set GLPI_USER_TOKEN, or GLPI_USERNAME + GLPI_PASSWORD.'
    );
  }

  return {
    url,
    appToken: process.env.GLPI_APP_TOKEN,
    userToken,
    username,
    password,
    timeoutMs: envInt('GLPI_TIMEOUT_MS'),
    maxRetries: envInt('GLPI_MAX_RETRIES'),
  };
}

/** 把 limit 参数规整到 [1, MAX_LIST_LIMIT] 区间。 */
function clampLimit(value: unknown): number {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return DEFAULT_LIST_LIMIT;
  return Math.min(Math.floor(n), MAX_LIST_LIMIT);
}

/**
 * Parse common list-tool arguments into a ListOptions.
 *
 * Accepts (in order of precedence):
 *   - `range`: "START-END" string passed through as-is
 *   - `start` + `limit`: assembled into range
 *   - `limit` alone: range = "0-{limit-1}" (backward-compat with v2)
 */
function parseListArgs(args: Record<string, unknown> | undefined): ListOptions {
  const opts: ListOptions = {};
  if (!args) return { range: `0-${DEFAULT_LIST_LIMIT - 1}`, expand_dropdowns: true };

  if (typeof args.range === 'string') {
    opts.range = args.range;
  } else if (args.start !== undefined || args.limit !== undefined) {
    const start = (args.start as number) ?? 0;
    const limit = clampLimit(args.limit);
    opts.range = `${start}-${start + limit - 1}`;
  } else {
    opts.range = `0-${DEFAULT_LIST_LIMIT - 1}`;
  }

  if (args.sort !== undefined) opts.sort = args.sort as number;
  if (args.order) opts.order = args.order as 'ASC' | 'DESC';
  if (args.is_deleted !== undefined) opts.is_deleted = args.is_deleted as boolean;
  if (args.include_deleted !== undefined) opts.is_deleted = args.include_deleted as boolean;
  // Default expand_dropdowns to true for human-readable output.
  opts.expand_dropdowns =
    args.expand_dropdowns === false ? false : true;
  return opts;
}

interface CriteriaArg {
  field: number | string;
  searchtype: SearchType;
  value: string | number | boolean;
  link?: SearchLink;
}

async function resolveCriteria(
  client: GlpiClient,
  itemtype: string,
  raw: CriteriaArg[]
): Promise<SearchCriterion[]> {
  return Promise.all(
    raw.map(async (c) => ({
      field: (await client.searchOptions.resolveField(itemtype, c.field)) ??
        (typeof c.field === 'number' ? c.field : 0),
      searchtype: c.searchtype,
      value: c.value,
      link: c.link,
    }))
  );
}

/** 递归截断过长的字符串字段,避免单条记录(超长的工单内容/跟进正文)撑爆体积。 */
function shortenLongStrings(value: unknown): unknown {
  if (typeof value === 'string') {
    return value.length > MAX_STRING_CHARS
      ? value.slice(0, MAX_STRING_CHARS) + `…(原文共 ${value.length} 字符,已截断)`
      : value;
  }
  if (Array.isArray(value)) return value.map(shortenLongStrings);
  if (value && typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
      out[k] = shortenLongStrings(v);
    }
    return out;
  }
  return value;
}

/**
 * 把结果序列化成文本:紧凑 JSON + 体积兜底。
 * 超限时优先裁剪数组条数(并保留 totalcount),同时给出“请缩小查询范围”的提示,
 * 让大模型能自己改用更精确的查询,而不是把整个结果硬塞进上下文。
 */
function serializeResult(payload: unknown): string {
  const safe = shortenLongStrings(payload);
  const full = JSON.stringify(safe);
  if (full.length <= MAX_RESULT_CHARS) return full;

  // 形状一:对象里带数组(搜索/列表结果,如 data / devices / timeline)
  // 注意:要对“所有”数组一起裁剪,否则像 { tried_accounts:[...], devices:[...] } 这类
  // 结果会因为只裁了那个小数组,真正的大数组仍把体积撑爆。
  if (safe && !Array.isArray(safe) && typeof safe === 'object') {
    const container = safe as Record<string, unknown>;
    const arrayKeys = Object.keys(container).filter((k) => Array.isArray(container[k]));
    const maxLen = Math.max(0, ...arrayKeys.map((k) => (container[k] as unknown[]).length));

    if (maxLen > 0) {
      for (let keep = maxLen; keep >= 1; keep = Math.floor(keep / 2)) {
        const candidate: Record<string, unknown> = { ...container };
        for (const k of arrayKeys) {
          const arr = candidate[k] as unknown[];
          if (arr.length > keep) candidate[k] = arr.slice(0, keep);
        }
        candidate.returned = keep;
        candidate.truncated = true;
        candidate.note = TRUNCATE_HINT;
        const out = JSON.stringify(candidate);
        if (out.length <= MAX_RESULT_CHARS) return out;
      }

      const emptied: Record<string, unknown> = { ...container };
      for (const k of arrayKeys) emptied[k] = [];
      emptied.returned = 0;
      emptied.truncated = true;
      emptied.note = TRUNCATE_HINT;
      return JSON.stringify(emptied);
    }
  }

  // 形状二:顶层就是数组
  if (Array.isArray(safe)) {
    const arr = safe as unknown[];
    for (let keep = arr.length; keep >= 1; keep = Math.floor(keep / 2)) {
      const out = JSON.stringify({
        data: arr.slice(0, keep),
        returned: keep,
        truncated: true,
        note: TRUNCATE_HINT,
      });
      if (out.length <= MAX_RESULT_CHARS) return out;
    }
    return JSON.stringify({ returned: 0, truncated: true, note: TRUNCATE_HINT });
  }

  // 形状三:没有可裁剪的数组,退化为 preview(逐步收缩直到满足上限)
  let budget = Math.max(0, MAX_RESULT_CHARS - TRUNCATE_HINT.length - 80);
  for (let i = 0; i < 12; i++) {
    const out = JSON.stringify({ truncated: true, note: TRUNCATE_HINT, preview: full.slice(0, budget) });
    if (out.length <= MAX_RESULT_CHARS) return out;
    budget = Math.floor(budget * 0.8);
  }
  return JSON.stringify({ truncated: true, note: TRUNCATE_HINT });
}

function text(obj: unknown) {
  return { content: [{ type: 'text' as const, text: serializeResult(obj) }] };
}

function formatTicketSummary(t: any) {
  return {
    id: t.id,
    name: t.name,
    status: TICKET_STATUS[t.status] ?? t.status,
    urgency: TICKET_URGENCY[t.urgency] ?? t.urgency,
    priority: TICKET_URGENCY[t.priority] ?? t.priority,
    date: t.date,
    date_mod: t.date_mod,
    entities_id: t.entities_id,
    itilcategories_id: t.itilcategories_id,
  };
}

// ---------------------------------------------------------------------------
// Server setup
// ---------------------------------------------------------------------------

let client: GlpiClient;

/**
 * Build a fresh MCP Server with every tool/resource registered.
 *
 * It is created per HTTP request (stateless Streamable HTTP): each concurrent
 * caller — e.g. several colleagues talking to the same WeCom bot at once —
 * gets its own JSON-RPC request/response mapping and never collides with
 * another caller's request ids.
 */
function createMcpServer(ctx: LogContext): Server {
  const server = new Server(
    { name: 'mcp-glpi', version: '3.0.0' },
    { capabilities: { tools: {}, resources: {} } }
  );

// ---------------------------------------------------------------------------
// Tool registration
// ---------------------------------------------------------------------------

const LIST_TOOL_COMMON_PROPS = {
  start: { type: 'number', description: '分页起始偏移,默认 0' },
  limit: { type: 'number', description: '本次返回条数,默认 20(上限 200)。返回过大时会自动截断,请优先加筛选条件' },
  range: { type: 'string', description: '直接指定 "起始-结束" 范围(含两端),会覆盖 start/limit' },
  sort: { type: 'number', description: '按字段 id 排序(可用 glpi_list_search_options 查字段 id)' },
  order: { type: 'string', enum: ['ASC', 'DESC'], description: '排序方向:ASC 升序 / DESC 降序' },
  expand_dropdowns: { type: 'boolean', description: '把外键 id 解析成可读名称,默认 true' },
};

/** MIME types for glpi_upload_document, keyed by lowercase file extension. */
const UPLOAD_MIME_TYPES: Record<string, string> = {
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.webp': 'image/webp',
  '.pdf': 'application/pdf',
  '.txt': 'text/plain',
  '.log': 'text/plain',
  '.csv': 'text/csv',
  '.zip': 'application/zip',
};

// ---------------------------------------------------------------------------
// Tool safety annotations (MCP ToolAnnotations)
//
// Derived from the tool name so every current and future tool gets hints:
//   - list/get/search/count/stats  -> readOnlyHint
//   - delete                       -> destructiveHint (data loss possible)
//   - update/set/assign            -> destructiveHint (overwrites existing data)
//   - create/add/link/attach       -> additive write (non-destructive, non-idempotent)
// openWorldHint is false everywhere: tools only reach the configured GLPI.
// ---------------------------------------------------------------------------

interface ToolAnnotations {
  readOnlyHint?: boolean;
  destructiveHint?: boolean;
  idempotentHint?: boolean;
  openWorldHint?: boolean;
}

function toolAnnotations(name: string): ToolAnnotations {
  if (/^glpi_(list_|get_|find_|search|count$|tickets_stats)/.test(name)) {
    return { readOnlyHint: true, openWorldHint: false };
  }
  if (/^glpi_delete_/.test(name)) {
    return { readOnlyHint: false, destructiveHint: true, idempotentHint: true, openWorldHint: false };
  }
  if (/^glpi_(update_|set_|assign_)/.test(name)) {
    return { readOnlyHint: false, destructiveHint: true, idempotentHint: true, openWorldHint: false };
  }
  // create / add / link / attach: additive writes. Re-running duplicates data.
  return { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false };
}

function annotate<T extends { name: string }>(tool: T): T & { annotations: ToolAnnotations } {
  return { ...tool, annotations: toolAnnotations(tool.name) };
}

// ---------------------------------------------------------------------------
// 工具中文说明(面向大模型的使用指南)
//
// 这些文本会在 tools/list 里作为 description 返回给客户端(企业微信智能机器人等),
// 是大模型判断“何时调用、传什么参数”的主要依据,所以写得具体一些:
//   - 适用场景 / 不适用场景
//   - 关键参数取值(如工单状态码的中文含义)
//   - 必要的调用示例
// 未列出的工具会沿用其原有的英文 description。
// ---------------------------------------------------------------------------
const TOOL_DESCRIPTIONS: Record<string, string> = {
  // ===== 工单 · 查询 =====
  glpi_list_tickets:
    '列出工单(不带筛选,按时间倒序)。用户说“看一下最近的工单/都有哪些工单”时用。' +
    '如需按条件筛选(状态、处理人、关键词等),请改用 glpi_search_tickets。' +
    '参数 status:1=新 2=处理中(已分配) 3=处理中(已计划) 4=待处理 5=已解决 6=已关闭。',
  glpi_get_ticket:
    '按 id 查看单张工单详情(含状态/紧急度中文标签,以及跟进数、任务数、解决方案数)。' +
    '用户明确报出工单号(如“工单 12345”)时用。参数 id 必填;with_logs=true 可附带历史记录。',
  glpi_get_ticket_timeline:
    '按时间顺序汇总一张工单的全部动态(跟进 + 任务 + 解决方案 + 审批)。' +
    '用户问“这张工单都处理过什么/时间线是怎样的”时用。参数 id。',
  glpi_search_tickets:
    '按条件查询工单(查工单的首选工具)。' +
    '支持:status(1~6)、assigned_user_id(处理人)、assigned_group_id(处理组)、requester_user_id(报障人)、' +
    'category_id、entity_id、priority/urgency(1~5)、date_from/date_to、text_search(标题关键词)、open_only(只看未完结)。' +
    '示例:未完结工单 → open_only=true;查某人的工单 → 先用 glpi_search_user 拿到 users_id 再传 assigned_user_id;' +
    '标题含“打印机” → text_search="打印机"。',
  glpi_get_ticket_followups: '列出指定工单的跟进(沟通)记录。参数 ticket_id 必填。',
  glpi_get_ticket_tasks: '列出指定工单的任务(含工时)。参数 ticket_id 必填。',
  glpi_get_ticket_solutions: '列出指定工单的解决方案。参数 ticket_id 必填。',
  glpi_get_ticket_validations: '列出指定工单的审批记录。参数 ticket_id 必填。',
  glpi_get_ticket_documents: '列出指定工单的附件文档。参数 ticket_id 必填。',

  // ===== 工单 · 写入 =====
  glpi_create_ticket:
    '新建工单。必填 name(标题)、content(内容/描述);' +
    '可选 type(1=故障 2=请求)、urgency/priority(1~5)、category_id、entity_id、' +
    'requester_user_id(报障人)、user_id_assign/group_id_assign(直接指派处理人/组)。' +
    '用户说“帮我建个工单/报修一下”时用;建单前建议先与用户确认标题与描述。',
  glpi_update_ticket:
    '修改工单字段(标题、内容、状态、紧急度、优先级、分类)。参数 id 必填 + 需要修改的字段。' +
    '状态:1=新 2=处理中(已分配) 3=处理中(已计划) 4=待处理 5=已解决 6=已关闭。',
  glpi_delete_ticket:
    '⚠️ 危险操作:删除工单。force=true 会彻底清除且不可恢复。执行前必须获得用户明确确认。',
  glpi_add_followup: '给工单追加一条跟进/回复。参数 ticket_id、content。用户说“帮我回复他/记录一下”时用。',
  glpi_add_task: '给工单添加任务,可记录工时。参数 ticket_id 必填;可选 content、actiontime(秒)等。',
  glpi_add_solution: '给工单添加解决方案(通常随后会把工单状态置为已解决)。参数 ticket_id、content。',
  glpi_assign_ticket:
    '把工单指派给用户或用户组。参数 id 必填;type:1=请求人 2=处理人 3=观察者;' +
    '并传 users_id(指派给某人)或 groups_id(指派给某组)。',
  glpi_link_tickets: '关联两张工单。link_type:1=关联 2=重复 3=父子关系。',
  glpi_add_ticket_validation: '为工单发起审批请求,指定审批人。参数 ticket_id 与 users_id;审批人会收到待办。',
  glpi_set_validation_status: '处理审批:通过传 status=2,拒绝传 status=3。参数 validation_id,可选 comment。',
  glpi_upload_document:
    '把服务器本地文件上传为 GLPI 文档,可同时挂到某张工单。参数 file_path(服务器上的绝对路径);' +
    '可选 ticket_id、name。注意:文件必须位于 MCP 服务所在机器上。',
  glpi_attach_document_to_ticket: '把已存在的文档挂到工单上。参数 ticket_id、document_id。',
  glpi_get_ticket_satisfaction: '查询工单的满意度调查结果(评分与评价)。参数 ticket_id。',
  glpi_list_overdue_tickets: '列出已超过解决时限(SLA)且尚未完结的工单。回答“有没有超期的工单”时用。',

  // ===== 问题单 / 变更单 =====
  glpi_list_problems: '列出问题单(Problem)。状态:1=新 2=已接受 3=已计划 4=待处理 5=已解决 6=已关闭。',
  glpi_get_problem: '按 id 查看问题单详情(含状态中文标签)。',
  glpi_create_problem: '新建问题单。必填 name;可选 content 等。',
  glpi_update_problem: '修改问题单字段。参数 id 必填 + 需要修改的字段。',
  glpi_list_changes:
    '列出变更单(Change)。状态:1=新 2=评估 3=审批 4=已接受 5=待处理 6=测试 7=确认 8=已实施 9=复查 10=已关闭 11=已拒绝 12=已取消。',
  glpi_get_change: '按 id 查看变更单详情(含状态中文标签)。',
  glpi_create_change: '新建变更单。必填 name;可选 content 等。',
  glpi_update_change: '修改变更单字段。参数 id 必填 + 需要修改的字段。',

  // ===== 资产 =====
  glpi_find_user_devices:
    '【查找“某个人的设备”首选】按使用人查找其名下的设备,会同时按“中文姓名”和“域账号”两路查询,结果去重合并后输出。' +
    '匹配字段:优先用 Computer 的 contact(IT 维护字段,通常记录使用人的域账号或姓名)。' +
    '用法:中文姓名传 name(如 张三),域账号传 account(如 san.zhang);两路都会查,每台设备会带 matched_by 标注命中来源。' +
    '域账号规则:全拼“名.姓”小写(张三 → san.zhang);重名账号会在末尾追加数字序号(从 1 开始),' +
    '工具会自动尝试 san.zhang、san.zhang1、san.zhang2…(默认 5 个,可用 variants 调整)。' +
    '只知道中文姓名时,请先把它转成拼音域账号,然后用 name + account 两个参数一起调用。',
  glpi_list_computers:
    '列出计算机资产(支持分页与排序)。' +
    '注意:若要查“某个人的电脑/设备”,请优先使用 glpi_find_user_devices(按 contact 字段 + 姓名匹配);' +
    '也可用 glpi_search_v2 指定 Computer 的 contact 字段自行筛选。',
  glpi_get_computer:
    '按 id 查看计算机详情。可选:with_softwares=true 带出已装软件,with_networkports/with_connections 带出网口与连接,with_documents 带出文档。',
  glpi_create_computer:
    '新增计算机到资产库。必填 name(通常为计算机名);常用字段:contact(使用人域账号,IT 维护)、serial(序列号)、' +
    'otherserial(资产编号)、locations_id(位置)、states_id(状态)、computertypes_id(类型)、manufacturers_id(厂商)。',
  glpi_update_computer:
    '修改计算机(如变更使用人 contact、位置 locations_id、状态 states_id)。参数 id 必填 + 需要修改的字段。',
  glpi_delete_computer: '⚠️ 危险操作:删除计算机资产。force=true 会彻底清除且不可恢复。执行前需用户确认。',
  glpi_create_software: '新增软件资产。必填 name;可选 comment、manufacturers_id、softwarecategories_id。',
  glpi_list_softwares: '列出软件资产。支持分页/排序。',
  glpi_get_software: '按 id 查看软件详情。',
  glpi_list_network_equipments: '列出网络设备(交换机/路由器/AP 等)。支持分页/排序。',
  glpi_get_network_equipment: '按 id 查看网络设备详情。',
  glpi_list_printers: '列出打印机。支持分页/排序。',
  glpi_get_printer: '按 id 查看打印机详情。',
  glpi_list_monitors: '列出显示器。支持分页/排序。',
  glpi_get_monitor: '按 id 查看显示器详情。',
  glpi_list_phones: '列出电话(IP 话机等)。支持分页/排序。',
  glpi_get_phone: '按 id 查看电话详情。',

  // ===== 知识库 =====
  glpi_list_knowbase: '列出知识库文章。用于浏览有哪些文章。',
  glpi_get_knowbase_item: '按 id 查看知识库文章内容。',
  glpi_search_knowbase: '按标题关键词搜索知识库文章。用户问“有没有相关操作手册/解决办法”时用。',
  glpi_create_knowbase_item: '新建知识库文章。必填 name;可选 content 等。',

  // ===== 合同 / 供应商 / 位置 / 项目 =====
  glpi_list_contracts: '列出合同。支持分页/排序。',
  glpi_get_contract: '按 id 查看合同详情。',
  glpi_create_contract: '新建合同。必填 name 等。',
  glpi_list_suppliers: '列出供应商。支持分页/排序。',
  glpi_get_supplier: '按 id 查看供应商详情。',
  glpi_create_supplier: '新建供应商。',
  glpi_list_locations: '列出位置(地点/机房/楼层等),用于解释工单与资产的 locations_id。',
  glpi_get_location: '按 id 查看位置详情。',
  glpi_create_location: '新建位置。',
  glpi_list_projects: '列出项目。',
  glpi_get_project: '按 id 查看项目详情。',
  glpi_create_project: '新建项目。必填 name 等。',
  glpi_update_project: '修改项目(进度、起止日期、内容等)。',

  // ===== 用户 / 用户组 =====
  glpi_list_users:
    '列出用户(默认只返回启用中的账号)。' +
    '域账号即用户的登录名(login/name 字段),规则:全拼“名.姓”,小写,例如 san.zhang;' +
    '重名时在末尾追加数字序号,从 1 开始(如 san.zhang1、san.zhang2)。',
  glpi_get_user: '按 id 查看用户详情(登录名/域账号、姓名、邮箱、所属组、实体)。',
  glpi_search_user:
    '按登录名或姓名模糊搜索用户,用于拿到 users_id。' +
    '需要“查某人的工单/某人的设备”时,先用这个把姓名或域账号换成用户 id,再传给其它工具。',
  glpi_create_user: '新建用户。常用 name(登录名,即域账号)、realname/firstname(姓名)、email 等。',
  glpi_list_groups: '列出用户组(用于指派工单、查看组内成员)。',
  glpi_get_group: '按 id 查看用户组详情。',
  glpi_create_group: '新建用户组。',
  glpi_add_user_to_group: '把用户加入用户组。参数 users_id、groups_id。',

  // ===== 分类 / 实体 / 文档 =====
  glpi_list_categories: '列出工单分类(建单或筛选工单时用于 category_id)。',
  glpi_list_entities: '列出实体(公司/组织架构),用于按 entity_id 过滤数据。',
  glpi_get_entity: '按 id 查看实体详情。',
  glpi_list_documents: '列出文档(附件)记录。',
  glpi_get_document: '按 id 查看文档信息。',

  // ===== 统计 =====
  glpi_get_ticket_stats: '按状态统计工单数量。可选 entity_id、date_from/date_to(YYYY-MM-DD)。',
  glpi_get_asset_stats: '各类资产的总数统计(计算机/显示器/打印机/网络设备/电话/软件)。',
  glpi_tickets_stats_by:
    '按维度汇总工单数量:dimension 取 status / category / technician / entity / month;可选日期区间与实体。' +
    '示例:“本月各类工单占比” → dimension="category" 并传日期范围。',

  // ===== 会话 / 通用检索 =====
  glpi_get_session_info: '查看当前 API 账号的权限概况(当前身份、可用身份、可见实体)。排查权限问题时用。',
  glpi_search_v2:
    '通用多条件检索(适用于工单与各类资产)。' +
    'criteria 每项为 {field, searchtype, value, link};searchtype 取 contains/notcontains/equals/notequals/lessthan/morethan/under/notunder/empty/notempty;' +
    '多项之间用 link 连接:AND / OR / AND NOT / OR NOT。' +
    'field 可传字段名(会动态解析为字段 id)或字段 id。' +
    '示例:查某人的电脑 → itemtype="Computer",criteria=[{field:"contact",searchtype:"contains",value:"san.zhang"}]。' +
    '不熟悉字段时先用 glpi_list_search_options 查。',
  glpi_count: '只返回符合条件的记录总数(轻量查询),适合回答“有多少个/几条”这类问题。',
  glpi_list_search_options:
    '列出某类对象的可搜索字段(field_id ↔ 名称 ↔ 数据类型),用于构造 glpi_search_v2 的 criteria。',
  glpi_search: '【已废弃,请改用 glpi_search_v2】仅保留单条件搜索以兼容旧调用。',
};

/** 用中文说明覆盖工具自带 description(未收录的工具保持原样)。 */
function withDoc<T extends { name: string; description: string }>(tool: T): T {
  const doc = TOOL_DESCRIPTIONS[tool.name];
  return doc ? { ...tool, description: doc } : tool;
}

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    // ============== READ — TICKETS ==============
    {
      name: 'glpi_list_tickets',
      description: 'List tickets. Supports start/limit/range/sort/order/status filter.',
      inputSchema: {
        type: 'object',
        properties: {
          ...LIST_TOOL_COMMON_PROPS,
          status: { type: 'number', description: '1=New 2=Assigned 3=Planned 4=Pending 5=Solved 6=Closed' },
        },
      },
    },
    {
      name: 'glpi_get_ticket',
      description: 'Get a ticket with status/urgency labels and counts of linked items.',
      inputSchema: {
        type: 'object',
        properties: {
          id: { type: 'number' },
          with_logs: { type: 'boolean' },
        },
        required: ['id'],
      },
    },
    {
      name: 'glpi_get_ticket_timeline',
      description: 'Full chronological timeline of a ticket: followups + tasks + solutions + validations, sorted by date.',
      inputSchema: {
        type: 'object',
        properties: { id: { type: 'number' } },
        required: ['id'],
      },
    },
    {
      name: 'glpi_search_tickets',
      description: 'High-level ticket search with friendly params. Use this instead of glpi_search_v2 for tickets.',
      inputSchema: {
        type: 'object',
        properties: {
          status: { type: 'number', description: '1..6 (see status reference)' },
          assigned_user_id: { type: 'number' },
          assigned_group_id: { type: 'number' },
          requester_user_id: { type: 'number' },
          category_id: { type: 'number' },
          entity_id: { type: 'number' },
          priority: { type: 'number', description: '1=Very low .. 5=Very high' },
          urgency: { type: 'number', description: '1..5' },
          date_from: { type: 'string', description: 'YYYY-MM-DD HH:MM:SS' },
          date_to: { type: 'string', description: 'YYYY-MM-DD HH:MM:SS' },
          text_search: { type: 'string', description: 'Free text in title' },
          open_only: { type: 'boolean', description: 'Status < 5 only' },
          start: { type: 'number' },
          limit: { type: 'number' },
          fetch_all: { type: 'boolean', description: 'Paginate until totalcount; capped by max_rows (default 1000).' },
          max_rows: { type: 'number' },
          order: { type: 'string', enum: ['ASC', 'DESC'] },
          sort: { type: 'number' },
        },
      },
    },
    {
      name: 'glpi_get_ticket_followups',
      description: 'List followups of a ticket.',
      inputSchema: { type: 'object', properties: { ticket_id: { type: 'number' } }, required: ['ticket_id'] },
    },
    {
      name: 'glpi_get_ticket_tasks',
      description: 'List tasks of a ticket.',
      inputSchema: { type: 'object', properties: { ticket_id: { type: 'number' } }, required: ['ticket_id'] },
    },
    {
      name: 'glpi_get_ticket_solutions',
      description: 'List solutions of a ticket.',
      inputSchema: { type: 'object', properties: { ticket_id: { type: 'number' } }, required: ['ticket_id'] },
    },
    {
      name: 'glpi_get_ticket_validations',
      description: 'List validations (approvals) of a ticket.',
      inputSchema: { type: 'object', properties: { ticket_id: { type: 'number' } }, required: ['ticket_id'] },
    },
    {
      name: 'glpi_get_ticket_documents',
      description: 'List documents (attachments) of a ticket.',
      inputSchema: { type: 'object', properties: { ticket_id: { type: 'number' } }, required: ['ticket_id'] },
    },

    // ============== WRITE — TICKETS ==============
    {
      name: 'glpi_create_ticket',
      description: 'Create a new ticket.',
      inputSchema: {
        type: 'object',
        properties: {
          name: { type: 'string' },
          content: { type: 'string' },
          urgency: { type: 'number' },
          impact: { type: 'number' },
          priority: { type: 'number' },
          type: { type: 'number', description: '1=Incident, 2=Request' },
          category_id: { type: 'number' },
          entity_id: { type: 'number' },
          user_id_assign: { type: 'number' },
          group_id_assign: { type: 'number' },
          requester_user_id: { type: 'number' },
          requester_group_id: { type: 'number' },
          time_to_resolve: { type: 'string' },
        },
        required: ['name', 'content'],
      },
    },
    {
      name: 'glpi_update_ticket',
      description: 'Update fields of a ticket.',
      inputSchema: {
        type: 'object',
        properties: {
          id: { type: 'number' },
          name: { type: 'string' },
          content: { type: 'string' },
          status: { type: 'number' },
          urgency: { type: 'number' },
          priority: { type: 'number' },
          impact: { type: 'number' },
          itilcategories_id: { type: 'number' },
        },
        required: ['id'],
      },
    },
    {
      name: 'glpi_delete_ticket',
      description: '⚠️ DESTRUCTIVE: delete a ticket. force=true purges (irrecoverable).',
      inputSchema: {
        type: 'object',
        properties: { id: { type: 'number' }, force: { type: 'boolean' } },
        required: ['id'],
      },
    },
    {
      name: 'glpi_add_followup',
      description: 'Add a followup comment to a ticket.',
      inputSchema: {
        type: 'object',
        properties: {
          ticket_id: { type: 'number' },
          content: { type: 'string' },
          is_private: { type: 'boolean' },
        },
        required: ['ticket_id', 'content'],
      },
    },
    {
      name: 'glpi_add_task',
      description: 'Add a task (with time tracking) to a ticket.',
      inputSchema: {
        type: 'object',
        properties: {
          ticket_id: { type: 'number' },
          content: { type: 'string' },
          actiontime: { type: 'number' },
          is_private: { type: 'boolean' },
          state: { type: 'number', description: '0=Info 1=Todo 2=Done' },
          users_id_tech: { type: 'number' },
          groups_id_tech: { type: 'number' },
        },
        required: ['ticket_id', 'content'],
      },
    },
    {
      name: 'glpi_add_solution',
      description: 'Add a solution to a ticket.',
      inputSchema: {
        type: 'object',
        properties: {
          ticket_id: { type: 'number' },
          content: { type: 'string' },
          solutiontypes_id: { type: 'number' },
        },
        required: ['ticket_id', 'content'],
      },
    },
    {
      name: 'glpi_assign_ticket',
      description: 'Assign a ticket to a user OR a group. type: 1=requester, 2=assigned, 3=observer.',
      inputSchema: {
        type: 'object',
        properties: {
          ticket_id: { type: 'number' },
          user_id: { type: 'number' },
          group_id: { type: 'number' },
          type: { type: 'number' },
        },
        required: ['ticket_id'],
      },
    },
    {
      name: 'glpi_link_tickets',
      description: 'Link two tickets. link_type: 1=link 2=duplicate 3=parent.',
      inputSchema: {
        type: 'object',
        properties: {
          parent_id: { type: 'number' },
          child_id: { type: 'number' },
          link_type: { type: 'number' },
        },
        required: ['parent_id', 'child_id'],
      },
    },
    {
      name: 'glpi_add_ticket_validation',
      description: 'Request a validation (approval) on a ticket. The chosen user receives the approval request.',
      inputSchema: {
        type: 'object',
        properties: {
          ticket_id: { type: 'number' },
          users_id_validate: { type: 'number', description: 'User asked to validate' },
          comment_submission: { type: 'string' },
        },
        required: ['ticket_id', 'users_id_validate'],
      },
    },
    {
      name: 'glpi_set_validation_status',
      description: 'Approve (2) or refuse (3) an existing TicketValidation. Provide optional comment.',
      inputSchema: {
        type: 'object',
        properties: {
          validation_id: { type: 'number' },
          status: { type: 'number', enum: [2, 3], description: '2=granted, 3=refused' },
          comment_validation: { type: 'string' },
        },
        required: ['validation_id', 'status'],
      },
    },
    {
      name: 'glpi_upload_document',
      description:
        'Upload a local file (by path) as a GLPI Document. If ticket_id is set, the document is also attached to that ticket.',
      inputSchema: {
        type: 'object',
        properties: {
          file_path: {
            type: 'string',
            description: 'Path of the file on the machine running this server',
          },
          name: { type: 'string', description: 'Document title (default: file name)' },
          ticket_id: {
            type: 'number',
            description: 'If set, attach the uploaded document to this ticket',
          },
        },
        required: ['file_path'],
      },
    },
    {
      name: 'glpi_attach_document_to_ticket',
      description: 'Link an existing document (uploaded separately) to a ticket via Document_Item.',
      inputSchema: {
        type: 'object',
        properties: {
          ticket_id: { type: 'number' },
          document_id: { type: 'number' },
        },
        required: ['ticket_id', 'document_id'],
      },
    },
    {
      name: 'glpi_get_ticket_satisfaction',
      description: 'Get satisfaction survey data (score, comment) for a ticket.',
      inputSchema: {
        type: 'object',
        properties: { ticket_id: { type: 'number' } },
        required: ['ticket_id'],
      },
    },
    {
      name: 'glpi_list_overdue_tickets',
      description: 'List tickets whose SLA resolution deadline (time_to_resolve) is in the past and status < 5.',
      inputSchema: {
        type: 'object',
        properties: {
          entity_id: { type: 'number' },
          limit: { type: 'number' },
        },
      },
    },

    // ============== PROBLEMS / CHANGES ==============
    {
      name: 'glpi_list_problems',
      description: 'List problems.',
      inputSchema: { type: 'object', properties: LIST_TOOL_COMMON_PROPS },
    },
    {
      name: 'glpi_get_problem',
      description: 'Get a problem with status label.',
      inputSchema: { type: 'object', properties: { id: { type: 'number' } }, required: ['id'] },
    },
    {
      name: 'glpi_create_problem',
      description: 'Create a problem.',
      inputSchema: {
        type: 'object',
        properties: {
          name: { type: 'string' }, content: { type: 'string' },
          urgency: { type: 'number' }, impact: { type: 'number' }, priority: { type: 'number' },
          category_id: { type: 'number' },
        },
        required: ['name', 'content'],
      },
    },
    {
      name: 'glpi_update_problem',
      description: 'Update a problem.',
      inputSchema: {
        type: 'object',
        properties: {
          id: { type: 'number' }, name: { type: 'string' }, content: { type: 'string' },
          status: { type: 'number' }, urgency: { type: 'number' },
        },
        required: ['id'],
      },
    },
    {
      name: 'glpi_list_changes',
      description: 'List changes.',
      inputSchema: { type: 'object', properties: LIST_TOOL_COMMON_PROPS },
    },
    {
      name: 'glpi_get_change',
      description: 'Get a change with status label.',
      inputSchema: { type: 'object', properties: { id: { type: 'number' } }, required: ['id'] },
    },
    {
      name: 'glpi_create_change',
      description: 'Create a change.',
      inputSchema: {
        type: 'object',
        properties: {
          name: { type: 'string' }, content: { type: 'string' },
          urgency: { type: 'number' }, impact: { type: 'number' }, priority: { type: 'number' },
          category_id: { type: 'number' },
        },
        required: ['name', 'content'],
      },
    },
    {
      name: 'glpi_update_change',
      description: 'Update a change.',
      inputSchema: {
        type: 'object',
        properties: {
          id: { type: 'number' }, name: { type: 'string' }, content: { type: 'string' },
          status: { type: 'number' },
        },
        required: ['id'],
      },
    },

    // ============== ASSETS ==============
    ...[
      'computers', 'softwares', 'network_equipments', 'printers', 'monitors', 'phones',
    ].flatMap((asset) => {
      const singular = asset.replace(/s$/, '');
      return [
        {
          name: `glpi_list_${asset}`,
          description: `List ${asset.replace('_', ' ')}.`,
          inputSchema: { type: 'object', properties: LIST_TOOL_COMMON_PROPS },
        },
        {
          name: `glpi_get_${singular}`,
          description: `Get a ${singular.replace('_', ' ')} by id.`,
          inputSchema: {
            type: 'object',
            properties: {
              id: { type: 'number' },
              with_softwares: { type: 'boolean' },
              with_networkports: { type: 'boolean' },
              with_connections: { type: 'boolean' },
              with_documents: { type: 'boolean' },
            },
            required: ['id'],
          },
        },
      ];
    }),
    {
      name: 'glpi_create_computer',
      description: 'Add a computer to inventory.',
      inputSchema: {
        type: 'object',
        properties: {
          name: { type: 'string' },
          serial: { type: 'string' }, otherserial: { type: 'string' },
          contact: { type: 'string' }, comment: { type: 'string' },
          locations_id: { type: 'number' }, states_id: { type: 'number' },
          computertypes_id: { type: 'number' }, manufacturers_id: { type: 'number' },
        },
        required: ['name'],
      },
    },
    {
      name: 'glpi_update_computer',
      description: 'Update a computer.',
      inputSchema: {
        type: 'object',
        properties: {
          id: { type: 'number' }, name: { type: 'string' }, serial: { type: 'string' },
          comment: { type: 'string' }, locations_id: { type: 'number' }, states_id: { type: 'number' },
        },
        required: ['id'],
      },
    },
    {
      name: 'glpi_delete_computer',
      description: '⚠️ DESTRUCTIVE: delete a computer. force=true purges.',
      inputSchema: { type: 'object', properties: { id: { type: 'number' }, force: { type: 'boolean' } }, required: ['id'] },
    },
    {
      // 按使用人查设备:中文姓名 + 域账号 两路都查,结果合并
      name: 'glpi_find_user_devices',
      description:
        'Find devices belonging to a user by matching the Computer "contact" field (IT-maintained) and device name, using BOTH the Chinese name and the domain account.',
      inputSchema: {
        type: 'object',
        properties: {
          name: { type: 'string', description: '使用人的中文姓名,如 张三(会按此查询一路)' },
          account: { type: 'string', description: '使用人的域账号,如 san.zhang(会连同重名序号 san.zhang1、san.zhang2… 一起查一路)' },
          user: { type: 'string', description: '(兼容参数)姓名或域账号;等价于同时传 name 与 account' },
          variants: { type: 'number', description: '域账号重名序号尝试到几(默认 5):san.zhang、san.zhang1 … san.zhangN' },
          limit: { type: 'number', description: '最多返回条数,默认 20(上限 200)' },
        },
      },
    },
    {
      name: 'glpi_create_software',
      description: 'Add software.',
      inputSchema: {
        type: 'object',
        properties: {
          name: { type: 'string' }, comment: { type: 'string' },
          manufacturers_id: { type: 'number' }, softwarecategories_id: { type: 'number' },
        },
        required: ['name'],
      },
    },

    // ============== KB / CONTRACTS / SUPPLIERS / LOCATIONS / PROJECTS ==============
    {
      name: 'glpi_list_knowbase',
      description: 'List KB articles.',
      inputSchema: { type: 'object', properties: LIST_TOOL_COMMON_PROPS },
    },
    {
      name: 'glpi_get_knowbase_item',
      description: 'Get a KB article.',
      inputSchema: { type: 'object', properties: { id: { type: 'number' } }, required: ['id'] },
    },
    {
      name: 'glpi_search_knowbase',
      description: 'Search KB articles by free text in title (field id resolved dynamically).',
      inputSchema: {
        type: 'object',
        properties: { query: { type: 'string' }, limit: { type: 'number' } },
        required: ['query'],
      },
    },
    {
      name: 'glpi_create_knowbase_item',
      description: 'Create a KB article.',
      inputSchema: {
        type: 'object',
        properties: {
          name: { type: 'string' }, answer: { type: 'string' },
          is_faq: { type: 'boolean' }, knowbaseitemcategories_id: { type: 'number' },
        },
        required: ['name', 'answer'],
      },
    },
    {
      name: 'glpi_list_contracts',
      description: 'List contracts.',
      inputSchema: { type: 'object', properties: LIST_TOOL_COMMON_PROPS },
    },
    {
      name: 'glpi_get_contract',
      description: 'Get a contract.',
      inputSchema: { type: 'object', properties: { id: { type: 'number' } }, required: ['id'] },
    },
    {
      name: 'glpi_create_contract',
      description: 'Create a contract.',
      inputSchema: {
        type: 'object',
        properties: {
          name: { type: 'string' }, num: { type: 'string' },
          begin_date: { type: 'string' }, duration: { type: 'number' },
          notice: { type: 'number' }, comment: { type: 'string' },
        },
        required: ['name'],
      },
    },
    {
      name: 'glpi_list_suppliers',
      description: 'List suppliers.',
      inputSchema: { type: 'object', properties: LIST_TOOL_COMMON_PROPS },
    },
    {
      name: 'glpi_get_supplier',
      description: 'Get a supplier.',
      inputSchema: { type: 'object', properties: { id: { type: 'number' } }, required: ['id'] },
    },
    {
      name: 'glpi_create_supplier',
      description: 'Create a supplier.',
      inputSchema: {
        type: 'object',
        properties: {
          name: { type: 'string' }, address: { type: 'string' }, postcode: { type: 'string' },
          town: { type: 'string' }, country: { type: 'string' }, website: { type: 'string' },
          phonenumber: { type: 'string' }, email: { type: 'string' },
        },
        required: ['name'],
      },
    },
    {
      name: 'glpi_list_locations',
      description: 'List locations.',
      inputSchema: { type: 'object', properties: LIST_TOOL_COMMON_PROPS },
    },
    {
      name: 'glpi_get_location',
      description: 'Get a location.',
      inputSchema: { type: 'object', properties: { id: { type: 'number' } }, required: ['id'] },
    },
    {
      name: 'glpi_create_location',
      description: 'Create a location.',
      inputSchema: {
        type: 'object',
        properties: {
          name: { type: 'string' }, address: { type: 'string' }, postcode: { type: 'string' },
          town: { type: 'string' }, building: { type: 'string' }, room: { type: 'string' },
          locations_id: { type: 'number' },
        },
        required: ['name'],
      },
    },
    {
      name: 'glpi_list_projects',
      description: 'List projects.',
      inputSchema: { type: 'object', properties: LIST_TOOL_COMMON_PROPS },
    },
    {
      name: 'glpi_get_project',
      description: 'Get a project.',
      inputSchema: { type: 'object', properties: { id: { type: 'number' } }, required: ['id'] },
    },
    {
      name: 'glpi_create_project',
      description: 'Create a project.',
      inputSchema: {
        type: 'object',
        properties: {
          name: { type: 'string' }, code: { type: 'string' }, content: { type: 'string' },
          priority: { type: 'number' }, plan_start_date: { type: 'string' },
          plan_end_date: { type: 'string' }, users_id: { type: 'number' }, groups_id: { type: 'number' },
        },
        required: ['name'],
      },
    },
    {
      name: 'glpi_update_project',
      description: 'Update a project (progress, dates, content).',
      inputSchema: {
        type: 'object',
        properties: {
          id: { type: 'number' }, name: { type: 'string' }, content: { type: 'string' },
          percent_done: { type: 'number' },
          real_start_date: { type: 'string' }, real_end_date: { type: 'string' },
        },
        required: ['id'],
      },
    },

    // ============== USERS / GROUPS / CATEGORIES / ENTITIES / DOCUMENTS ==============
    {
      name: 'glpi_list_users',
      description: 'List users. active_only defaults to true (uses search criteria, not searchText).',
      inputSchema: {
        type: 'object',
        properties: { ...LIST_TOOL_COMMON_PROPS, active_only: { type: 'boolean' } },
      },
    },
    {
      name: 'glpi_get_user',
      description: 'Get a user.',
      inputSchema: { type: 'object', properties: { id: { type: 'number' } }, required: ['id'] },
    },
    {
      name: 'glpi_search_user',
      description: 'Search a user by login name (exact "contains" on name field).',
      inputSchema: { type: 'object', properties: { name: { type: 'string' } }, required: ['name'] },
    },
    {
      name: 'glpi_create_user',
      description: 'Create a user.',
      inputSchema: {
        type: 'object',
        properties: {
          name: { type: 'string' }, password: { type: 'string' },
          realname: { type: 'string' }, firstname: { type: 'string' },
          email: { type: 'string' }, phone: { type: 'string' }, profiles_id: { type: 'number' },
        },
        required: ['name'],
      },
    },
    {
      name: 'glpi_list_groups',
      description: 'List groups.',
      inputSchema: { type: 'object', properties: LIST_TOOL_COMMON_PROPS },
    },
    {
      name: 'glpi_get_group',
      description: 'Get a group.',
      inputSchema: { type: 'object', properties: { id: { type: 'number' } }, required: ['id'] },
    },
    {
      name: 'glpi_create_group',
      description: 'Create a group.',
      inputSchema: {
        type: 'object',
        properties: {
          name: { type: 'string' }, comment: { type: 'string' },
          is_requester: { type: 'boolean' }, is_assign: { type: 'boolean' },
        },
        required: ['name'],
      },
    },
    {
      name: 'glpi_add_user_to_group',
      description: 'Add a user to a group.',
      inputSchema: {
        type: 'object',
        properties: { user_id: { type: 'number' }, group_id: { type: 'number' }, is_manager: { type: 'boolean' } },
        required: ['user_id', 'group_id'],
      },
    },
    {
      name: 'glpi_list_categories',
      description: 'List ticket categories.',
      inputSchema: { type: 'object', properties: LIST_TOOL_COMMON_PROPS },
    },
    {
      name: 'glpi_list_entities',
      description: 'List entities.',
      inputSchema: { type: 'object', properties: LIST_TOOL_COMMON_PROPS },
    },
    {
      name: 'glpi_get_entity',
      description: 'Get an entity.',
      inputSchema: { type: 'object', properties: { id: { type: 'number' } }, required: ['id'] },
    },
    {
      name: 'glpi_list_documents',
      description: 'List documents.',
      inputSchema: { type: 'object', properties: LIST_TOOL_COMMON_PROPS },
    },
    {
      name: 'glpi_get_document',
      description: 'Get a document.',
      inputSchema: { type: 'object', properties: { id: { type: 'number' } }, required: ['id'] },
    },

    // ============== STATS ==============
    {
      name: 'glpi_get_ticket_stats',
      description: 'Ticket counts by status. Optional filters: entity, date_from, date_to.',
      inputSchema: {
        type: 'object',
        properties: {
          entity_id: { type: 'number' },
          date_from: { type: 'string', description: 'YYYY-MM-DD' },
          date_to: { type: 'string', description: 'YYYY-MM-DD' },
        },
      },
    },
    {
      name: 'glpi_get_asset_stats',
      description: 'Total counts per asset type (Computer/Monitor/Printer/NetworkEquipment/Phone/Software).',
      inputSchema: { type: 'object', properties: {} },
    },
    {
      name: 'glpi_tickets_stats_by',
      description: 'Ticket count broken down by a dimension (status / category / technician / entity / month). Optional period filter.',
      inputSchema: {
        type: 'object',
        properties: {
          dimension: {
            type: 'string',
            enum: ['status', 'category', 'technician', 'entity', 'month'],
          },
          date_from: { type: 'string' },
          date_to: { type: 'string' },
          entity_id: { type: 'number' },
        },
        required: ['dimension'],
      },
    },

    // ============== SESSION ==============
    {
      name: 'glpi_get_session_info',
      description: 'Active profile + available profiles + entities.',
      inputSchema: { type: 'object', properties: {} },
    },

    // ============== GENERIC SEARCH / COUNT ==============
    {
      name: 'glpi_search_v2',
      description: 'Multi-criteria search. Use criteria[]: {field, searchtype, value, link}. Supports forcedisplay, sort, order, start/limit, fetch_all.',
      inputSchema: {
        type: 'object',
        properties: {
          itemtype: { type: 'string' },
          criteria: {
            type: 'array',
            items: {
              type: 'object',
              properties: {
                field: { description: 'field_id (number) OR friendly name resolved via listSearchOptions' },
                searchtype: {
                  type: 'string',
                  enum: ['contains', 'notcontains', 'equals', 'notequals', 'lessthan', 'morethan', 'under', 'notunder', 'empty', 'notempty'],
                },
                value: {},
                link: { type: 'string', enum: ['AND', 'OR', 'AND NOT', 'OR NOT'] },
              },
              required: ['field', 'searchtype', 'value'],
            },
          },
          forcedisplay: { type: 'array', items: { type: 'number' } },
          start: { type: 'number' },
          limit: { type: 'number' },
          sort: { type: 'number' },
          order: { type: 'string', enum: ['ASC', 'DESC'] },
          fetch_all: { type: 'boolean' },
          max_rows: { type: 'number' },
          expand_dropdowns: { type: 'boolean' },
        },
        required: ['itemtype'],
      },
    },
    {
      name: 'glpi_count',
      description: 'Return totalcount for an itemtype + criteria (cheap range=0-0 probe).',
      inputSchema: {
        type: 'object',
        properties: {
          itemtype: { type: 'string' },
          criteria: {
            type: 'array',
            items: {
              type: 'object',
              properties: {
                field: {},
                searchtype: { type: 'string' },
                value: {},
                link: { type: 'string' },
              },
              required: ['field', 'searchtype', 'value'],
            },
          },
        },
        required: ['itemtype'],
      },
    },
    {
      name: 'glpi_list_search_options',
      description: 'Discover the searchable fields of an itemtype (returns field_id → name/uid/datatype). Useful to build criteria for glpi_search_v2.',
      inputSchema: {
        type: 'object',
        properties: { itemtype: { type: 'string' } },
        required: ['itemtype'],
      },
    },

    // ============== legacy compat: keep glpi_search (mono-criterion) as deprecated alias ==============
    {
      name: 'glpi_search',
      description: '[DEPRECATED — prefer glpi_search_v2] Single-criterion search (kept for backward compat).',
      inputSchema: {
        type: 'object',
        properties: {
          itemtype: { type: 'string' },
          field: { type: 'number' },
          searchtype: { type: 'string' },
          value: { type: 'string' },
        },
        required: ['itemtype', 'field', 'searchtype', 'value'],
      },
    },
  ].map(annotate).map(withDoc),
}));

// ---------------------------------------------------------------------------
// Tool dispatch
// ---------------------------------------------------------------------------

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: argsRaw } = request.params;
  const args = (argsRaw ?? {}) as Record<string, unknown>;
  return await runTool(name, args, ctx);
});

/** 记录一次工具调用的调用方、参数、结果与耗时,然后转交实现。 */
async function runTool(name: string, args: Record<string, unknown>, ctx: LogContext): Promise<any> {
  const startedAt = Date.now();
  log('INFO', `[${ctx.ip}] → 调用 ${name}${summarizeArgs(args)}`);

  try {
    const result = await toolImpl(name, args);
    log('INFO', `[${ctx.ip}] ← ${name} 成功 (${Date.now() - startedAt}ms)${summarizeResult(result)}`);
    return result;
  } catch (error) {
    log(
      'ERROR',
      `[${ctx.ip}] ← ${name} 失败 (${Date.now() - startedAt}ms): ` +
        `${error instanceof Error ? error.message : String(error)}`
    );
    throw error;
  }
}

/** 工具实现本体(与日志解耦)。 */
async function toolImpl(name: string, args: Record<string, unknown>): Promise<any> {
  try {
    switch (name) {
      // ==== TICKETS — read ====
      case 'glpi_list_tickets': {
        const validated = listArgsSchema.parse(args);
        const opts = parseListArgs(validated);
        let tickets = await client.getTickets({ ...opts, order: opts.order ?? 'DESC' });
        if (typeof validated.status === 'number') {
          tickets = tickets.filter((t: any) => t.status === validated.status);
        }
        return text(tickets.map(formatTicketSummary));
      }

      case 'glpi_get_ticket': {
        const validated = ticketReadSchema.parse(args);
        const { id, with_logs } = validated;
        const [ticket, followups, tasks, solutions] = await Promise.all([
          client.getTicket(id, { with_logs }),
          client.getTicketFollowups(id),
          client.getTicketTasks(id),
          client.getTicketSolutions(id),
        ]);
        return text({
          ...ticket,
          status_label: TICKET_STATUS[(ticket as any).status],
          urgency_label: TICKET_URGENCY[(ticket as any).urgency],
          priority_label: TICKET_URGENCY[(ticket as any).priority],
          counts: {
            followups: followups.length,
            tasks: tasks.length,
            solutions: solutions.length,
          },
        });
      }

      case 'glpi_get_ticket_timeline': {
        const id = args.id as number;
        if (!id) throw new McpError(ErrorCode.InvalidParams, 'id required');
        const [followups, tasks, solutions, validations] = await Promise.all([
          client.getTicketFollowups(id),
          client.getTicketTasks(id),
          client.getTicketSolutions(id),
          client.getTicketValidations(id),
        ]);
        const timeline = [
          ...followups.map((f: any) => ({ kind: 'followup', date: f.date_creation ?? f.date, ...f })),
          ...tasks.map((t: any) => ({ kind: 'task', date: t.date_creation ?? t.date, ...t })),
          ...solutions.map((s: any) => ({ kind: 'solution', date: s.date_creation ?? s.date, ...s })),
          ...validations.map((v: any) => ({
            kind: 'validation',
            date: v.submission_date ?? v.date_creation ?? v.date,
            status_label: VALIDATION_STATUS[v.status] ?? v.status,
            ...v,
          })),
        ].sort((a, b) => (a.date ?? '').localeCompare(b.date ?? ''));
        return text({ ticket_id: id, count: timeline.length, timeline });
      }

      case 'glpi_search_tickets': {
        ticketSearchSchema.parse(args);
        const criteria: SearchCriterion[] = [];
        const push = (c: SearchCriterion) => {
          if (criteria.length > 0 && !c.link) c.link = 'AND';
          criteria.push(c);
        };
        if (args.status !== undefined) push({ field: TICKET_FIELDS.status, searchtype: 'equals', value: args.status as number });
        if (args.assigned_user_id !== undefined) push({ field: TICKET_FIELDS.technician_user, searchtype: 'equals', value: args.assigned_user_id as number });
        if (args.assigned_group_id !== undefined) push({ field: TICKET_FIELDS.technician_group, searchtype: 'equals', value: args.assigned_group_id as number });
        if (args.requester_user_id !== undefined) push({ field: TICKET_FIELDS.requester_user, searchtype: 'equals', value: args.requester_user_id as number });
        if (args.category_id !== undefined) push({ field: TICKET_FIELDS.category, searchtype: 'equals', value: args.category_id as number });
        if (args.entity_id !== undefined) push({ field: TICKET_FIELDS.entity, searchtype: 'equals', value: args.entity_id as number });
        if (args.priority !== undefined) push({ field: TICKET_FIELDS.priority, searchtype: 'equals', value: args.priority as number });
        if (args.urgency !== undefined) push({ field: TICKET_FIELDS.urgency, searchtype: 'equals', value: args.urgency as number });
        if (args.date_from) push({ field: TICKET_FIELDS.date, searchtype: 'morethan', value: args.date_from as string });
        if (args.date_to) push({ field: TICKET_FIELDS.date, searchtype: 'lessthan', value: args.date_to as string });
        if (args.text_search) push({ field: TICKET_FIELDS.name, searchtype: 'contains', value: args.text_search as string });
        if (args.open_only) push({ field: TICKET_FIELDS.status, searchtype: 'lessthan', value: 5 });

        const result = await client.search.search('Ticket', {
          criteria,
          start: (args.start as number) ?? 0,
          limit: clampLimit(args.limit),
          fetchAll: args.fetch_all as boolean,
          maxRows: (args.max_rows as number) ?? MAX_FETCH_ALL_ROWS,
          sort: args.sort as number,
          order: (args.order as 'ASC' | 'DESC') ?? 'DESC',
          expandDropdowns: true,
        });

        return text({
          totalcount: result.totalcount,
          count: result.count,
          data: result.data,
        });
      }

      case 'glpi_get_ticket_followups': {
        const id = args.ticket_id as number;
        if (!id) throw new McpError(ErrorCode.InvalidParams, 'ticket_id required');
        return text(await client.getTicketFollowups(id));
      }
      case 'glpi_get_ticket_tasks': {
        const id = args.ticket_id as number;
        if (!id) throw new McpError(ErrorCode.InvalidParams, 'ticket_id required');
        return text(await client.getTicketTasks(id));
      }
      case 'glpi_get_ticket_solutions': {
        const id = args.ticket_id as number;
        if (!id) throw new McpError(ErrorCode.InvalidParams, 'ticket_id required');
        return text(await client.getTicketSolutions(id));
      }
      case 'glpi_get_ticket_validations': {
        const id = args.ticket_id as number;
        if (!id) throw new McpError(ErrorCode.InvalidParams, 'ticket_id required');
        return text(await client.getTicketValidations(id));
      }
      case 'glpi_get_ticket_documents': {
        const id = args.ticket_id as number;
        if (!id) throw new McpError(ErrorCode.InvalidParams, 'ticket_id required');
        return text(await client.getTicketDocuments(id));
      }

      // ==== TICKETS — write ====
      case 'glpi_create_ticket': {
        const name = args.name as string;
        const content = args.content as string;
        if (!name || !content) throw new McpError(ErrorCode.InvalidParams, 'name and content required');
        const result = await client.createTicket({
          name,
          content,
          urgency: (args.urgency as number) ?? 3,
          impact: args.impact as number,
          priority: args.priority as number,
          type: (args.type as number) ?? 1,
          itilcategories_id: args.category_id as number,
          entities_id: args.entity_id as number,
          _users_id_assign: args.user_id_assign as number,
          _groups_id_assign: args.group_id_assign as number,
          _users_id_requester: args.requester_user_id as number,
          _groups_id_requester: args.requester_group_id as number,
          time_to_resolve: args.time_to_resolve as string,
        });
        return text({ success: true, ...result });
      }

      case 'glpi_update_ticket': {
        const id = args.id as number;
        if (!id) throw new McpError(ErrorCode.InvalidParams, 'id required');
        const updates: Record<string, unknown> = {};
        ['name', 'content', 'status', 'urgency', 'priority', 'impact', 'itilcategories_id'].forEach((k) => {
          if (args[k] !== undefined) updates[k] = args[k];
        });
        await client.updateTicket(id, updates as any);
        return text({ success: true, id });
      }

      case 'glpi_delete_ticket': {
        const id = args.id as number;
        if (!id) throw new McpError(ErrorCode.InvalidParams, 'id required');
        await client.deleteTicket(id, args.force as boolean);
        return text({ success: true, id, purged: !!args.force });
      }

      case 'glpi_add_followup': {
        const ticket_id = args.ticket_id as number;
        const content = args.content as string;
        if (!ticket_id || !content) throw new McpError(ErrorCode.InvalidParams, 'ticket_id and content required');
        const result = await client.addTicketFollowup(ticket_id, content, args.is_private as boolean);
        return text({ success: true, followup_id: result.id });
      }

      case 'glpi_add_task': {
        const ticket_id = args.ticket_id as number;
        const content = args.content as string;
        if (!ticket_id || !content) throw new McpError(ErrorCode.InvalidParams, 'ticket_id and content required');
        const result = await client.addTicketTask(ticket_id, content, {
          is_private: args.is_private as boolean,
          actiontime: args.actiontime as number,
          state: args.state as number,
          users_id_tech: args.users_id_tech as number,
          groups_id_tech: args.groups_id_tech as number,
        });
        return text({ success: true, task_id: result.id });
      }

      case 'glpi_add_solution': {
        const ticket_id = args.ticket_id as number;
        const content = args.content as string;
        if (!ticket_id || !content) throw new McpError(ErrorCode.InvalidParams, 'ticket_id and content required');
        const result = await client.addTicketSolution(ticket_id, content, args.solutiontypes_id as number);
        return text({ success: true, solution_id: result.id });
      }

      case 'glpi_assign_ticket': {
        const ticket_id = args.ticket_id as number;
        if (!ticket_id) throw new McpError(ErrorCode.InvalidParams, 'ticket_id required');
        const user_id = args.user_id as number;
        const group_id = args.group_id as number;
        if (!user_id && !group_id) {
          throw new McpError(ErrorCode.InvalidParams, 'user_id or group_id required');
        }
        const result = await client.assignTicket(ticket_id, {
          users_id: user_id,
          groups_id: group_id,
          type: args.type as number,
        });
        return text({ success: true, assignment_id: result.id });
      }

      case 'glpi_link_tickets': {
        const parent_id = args.parent_id as number;
        const child_id = args.child_id as number;
        if (!parent_id || !child_id) throw new McpError(ErrorCode.InvalidParams, 'parent_id and child_id required');
        const result = await client.linkTickets(parent_id, child_id, (args.link_type as number) ?? 1);
        return text({ success: true, link_id: result.id });
      }

      case 'glpi_add_ticket_validation': {
        const ticket_id = args.ticket_id as number;
        const users_id_validate = args.users_id_validate as number;
        if (!ticket_id || !users_id_validate) {
          throw new McpError(ErrorCode.InvalidParams, 'ticket_id and users_id_validate required');
        }
        const result = await client.addTicketValidation(ticket_id, {
          users_id_validate,
          comment_submission: args.comment_submission as string,
        });
        return text({ success: true, validation_id: result.id });
      }

      case 'glpi_set_validation_status': {
        const validation_id = args.validation_id as number;
        const status = args.status as 2 | 3;
        if (!validation_id || (status !== 2 && status !== 3)) {
          throw new McpError(ErrorCode.InvalidParams, 'validation_id and status (2 or 3) required');
        }
        await client.setTicketValidationStatus(
          validation_id,
          status,
          args.comment_validation as string
        );
        return text({ success: true, validation_id, status_label: VALIDATION_STATUS[status] });
      }

      case 'glpi_upload_document': {
        const filePath = args.file_path as string;
        if (!filePath) throw new McpError(ErrorCode.InvalidParams, 'file_path required');
        let data: Uint8Array;
        try {
          data = await readFile(filePath);
        } catch (err) {
          throw new McpError(
            ErrorCode.InvalidParams,
            `cannot read file "${filePath}": ${err instanceof Error ? err.message : err}`
          );
        }
        const filename = basename(filePath);
        const ticket_id = args.ticket_id as number | undefined;
        // Linking via the manifest (itemtype/items_id) lets GLPI create the
        // Document_Item itself, which also works for restricted profiles that
        // cannot POST Document_Item directly.
        const document = await client.uploadDocument({
          filename,
          data,
          name: args.name as string | undefined,
          mimeType: UPLOAD_MIME_TYPES[extname(filename).toLowerCase()],
          ...(ticket_id ? { itemtype: 'Ticket', items_id: ticket_id } : {}),
        });
        return text({ success: true, document_id: document.id, ...(ticket_id ? { ticket_id } : {}) });
      }

      case 'glpi_attach_document_to_ticket': {
        const ticket_id = args.ticket_id as number;
        const document_id = args.document_id as number;
        if (!ticket_id || !document_id) {
          throw new McpError(ErrorCode.InvalidParams, 'ticket_id and document_id required');
        }
        const result = await client.attachDocumentToTicket(ticket_id, document_id);
        return text({ success: true, link_id: result.id });
      }

      case 'glpi_get_ticket_satisfaction': {
        const ticket_id = args.ticket_id as number;
        if (!ticket_id) throw new McpError(ErrorCode.InvalidParams, 'ticket_id required');
        return text(await client.getTicketSatisfaction(ticket_id));
      }

      case 'glpi_list_overdue_tickets': {
        const now = new Date().toISOString().slice(0, 19).replace('T', ' ');
        const criteria: SearchCriterion[] = [
          { field: TICKET_FIELDS.status, searchtype: 'lessthan', value: 5 },
          // time_to_resolve search-option id is typically 18; fall back to 18.
          { field: 18, searchtype: 'lessthan', value: now, link: 'AND' },
          { field: 18, searchtype: 'notempty', value: '', link: 'AND' },
        ];
        if (args.entity_id !== undefined) {
          criteria.push({ field: TICKET_FIELDS.entity, searchtype: 'equals', value: args.entity_id as number, link: 'AND' });
        }
        const result = await client.search.search('Ticket', {
          criteria,
          limit: (args.limit as number) ?? 50,
          expandDropdowns: true,
          order: 'ASC',
          sort: 18,
        });
        return text({
          totalcount: result.totalcount,
          count: result.count,
          data: result.data,
        });
      }

      // ==== PROBLEMS / CHANGES ====
      case 'glpi_list_problems': {
        const list = await client.getProblems({ ...parseListArgs(args), order: 'DESC' });
        return text(list.map((p: any) => ({
          id: p.id, name: p.name,
          status: PROBLEM_STATUS[p.status] ?? p.status,
          urgency: TICKET_URGENCY[p.urgency] ?? p.urgency,
          date: p.date,
        })));
      }
      case 'glpi_get_problem': {
        const id = args.id as number;
        const p = await client.getProblem(id);
        return text({
          ...p,
          status_label: PROBLEM_STATUS[(p as any).status],
          urgency_label: TICKET_URGENCY[(p as any).urgency],
        });
      }
      case 'glpi_create_problem': {
        const result = await client.createProblem({
          name: args.name as string,
          content: args.content as string,
          urgency: args.urgency as number,
          impact: args.impact as number,
          priority: args.priority as number,
          itilcategories_id: args.category_id as number,
        });
        return text({ success: true, ...result });
      }
      case 'glpi_update_problem': {
        const id = args.id as number;
        const updates: Record<string, unknown> = {};
        ['name', 'content', 'status', 'urgency'].forEach((k) => {
          if (args[k] !== undefined) updates[k] = args[k];
        });
        await client.updateProblem(id, updates as any);
        return text({ success: true, id });
      }

      case 'glpi_list_changes': {
        const list = await client.getChanges({ ...parseListArgs(args), order: 'DESC' });
        return text(list.map((c: any) => ({
          id: c.id, name: c.name,
          status: CHANGE_STATUS[c.status] ?? c.status,
          urgency: TICKET_URGENCY[c.urgency] ?? c.urgency,
          date: c.date,
        })));
      }
      case 'glpi_get_change': {
        const id = args.id as number;
        const c = await client.getChange(id);
        return text({
          ...c,
          status_label: CHANGE_STATUS[(c as any).status],
          urgency_label: TICKET_URGENCY[(c as any).urgency],
        });
      }
      case 'glpi_create_change': {
        const result = await client.createChange({
          name: args.name as string,
          content: args.content as string,
          urgency: args.urgency as number,
          impact: args.impact as number,
          priority: args.priority as number,
          itilcategories_id: args.category_id as number,
        });
        return text({ success: true, ...result });
      }
      case 'glpi_update_change': {
        const id = args.id as number;
        const updates: Record<string, unknown> = {};
        ['name', 'content', 'status'].forEach((k) => {
          if (args[k] !== undefined) updates[k] = args[k];
        });
        await client.updateChange(id, updates as any);
        return text({ success: true, id });
      }

      // ==== ASSETS ====
      case 'glpi_list_computers':
        return text(await client.getComputers(parseListArgs(args)));
      case 'glpi_get_computer':
        return text(await client.getComputer(args.id as number, {
          with_softwares: args.with_softwares as boolean,
          with_connections: args.with_connections as boolean,
          with_networkports: args.with_networkports as boolean,
          with_documents: args.with_documents as boolean,
        }));
      case 'glpi_create_computer':
        return text({ success: true, ...(await client.createComputer(args)) });
      case 'glpi_update_computer': {
        const id = args.id as number;
        const updates = { ...args }; delete (updates as any).id;
        await client.updateComputer(id, updates as any);
        return text({ success: true, id });
      }
      case 'glpi_delete_computer':
        await client.deleteComputer(args.id as number, args.force as boolean);
        return text({ success: true, id: args.id, purged: !!args.force });

      case 'glpi_list_softwares':
        return text(await client.getSoftwares(parseListArgs(args)));
      case 'glpi_get_software':
        return text(await client.getSoftware(args.id as number));
      case 'glpi_create_software':
        return text({ success: true, ...(await client.createSoftware(args)) });

      case 'glpi_list_network_equipments':
        return text(await client.getNetworkEquipments(parseListArgs(args)));
      case 'glpi_get_network_equipment':
        return text(await client.getNetworkEquipment(args.id as number, {
          with_networkports: args.with_networkports as boolean,
        }));

      case 'glpi_list_printers':
        return text(await client.getPrinters(parseListArgs(args)));
      case 'glpi_get_printer':
        return text(await client.getPrinter(args.id as number));

      case 'glpi_list_monitors':
        return text(await client.getMonitors(parseListArgs(args)));
      case 'glpi_get_monitor':
        return text(await client.getMonitor(args.id as number));

      case 'glpi_list_phones':
        return text(await client.getPhones(parseListArgs(args)));
      case 'glpi_get_phone':
        return text(await client.getPhone(args.id as number));

      // ==== KB / CONTRACTS / SUPPLIERS / LOCATIONS / PROJECTS ====
      case 'glpi_list_knowbase':
        return text(await client.getKnowbaseItems(parseListArgs(args)));
      case 'glpi_get_knowbase_item':
        return text(await client.getKnowbaseItem(args.id as number));
      case 'glpi_search_knowbase':
        return text(await client.searchKnowbase(args.query as string, (args.limit as number) ?? 50));
      case 'glpi_create_knowbase_item': {
        const result = await client.createKnowbaseItem({
          name: args.name as string,
          answer: args.answer as string,
          is_faq: args.is_faq ? 1 : 0,
          knowbaseitemcategories_id: args.knowbaseitemcategories_id as number,
        });
        return text({ success: true, ...result });
      }

      case 'glpi_list_contracts':
        return text(await client.getContracts(parseListArgs(args)));
      case 'glpi_get_contract':
        return text(await client.getContract(args.id as number));
      case 'glpi_create_contract':
        return text({ success: true, ...(await client.createContract(args)) });

      case 'glpi_list_suppliers':
        return text(await client.getSuppliers(parseListArgs(args)));
      case 'glpi_get_supplier':
        return text(await client.getSupplier(args.id as number));
      case 'glpi_create_supplier':
        return text({ success: true, ...(await client.createSupplier(args)) });

      case 'glpi_list_locations':
        return text(await client.getLocations(parseListArgs(args)));
      case 'glpi_get_location':
        return text(await client.getLocation(args.id as number));
      case 'glpi_create_location':
        return text({ success: true, ...(await client.createLocation(args)) });

      case 'glpi_list_projects':
        return text(await client.getProjects(parseListArgs(args)));
      case 'glpi_get_project':
        return text(await client.getProject(args.id as number));
      case 'glpi_create_project':
        return text({ success: true, ...(await client.createProject(args)) });
      case 'glpi_update_project': {
        const id = args.id as number;
        const updates: Record<string, unknown> = {};
        ['name', 'content', 'percent_done', 'real_start_date', 'real_end_date'].forEach((k) => {
          if (args[k] !== undefined) updates[k] = args[k];
        });
        await client.updateProject(id, updates as any);
        return text({ success: true, id });
      }

      // ==== USERS / GROUPS ====
      case 'glpi_list_users':
        return text(await client.getUsers({
          ...parseListArgs(args),
          is_active: args.active_only === false ? false : true,
        }));
      case 'glpi_get_user':
        return text(await client.getUser(args.id as number));
      case 'glpi_search_user':
        return text(await client.getUserByName(args.name as string));
      case 'glpi_create_user':
        return text({ success: true, ...(await client.createUser({
          name: args.name as string,
          password: args.password as string,
          realname: args.realname as string,
          firstname: args.firstname as string,
          email: args.email as string,
          phone: args.phone as string,
          profiles_id: args.profiles_id as number,
        })) });

      case 'glpi_list_groups':
        return text(await client.getGroups(parseListArgs(args)));
      case 'glpi_get_group':
        return text(await client.getGroup(args.id as number));
      case 'glpi_create_group':
        return text({ success: true, ...(await client.createGroup({
          name: args.name as string,
          comment: args.comment as string,
          is_requester: args.is_requester ? 1 : 0,
          is_assign: args.is_assign ? 1 : 0,
        })) });
      case 'glpi_add_user_to_group':
        return text({ success: true, ...(await client.addUserToGroup(
          args.user_id as number,
          args.group_id as number,
          args.is_manager as boolean
        )) });

      case 'glpi_list_categories':
        return text(await client.getCategories(parseListArgs(args)));
      case 'glpi_list_entities':
        return text(await client.getEntities(parseListArgs(args)));
      case 'glpi_get_entity':
        return text(await client.getEntity(args.id as number));
      case 'glpi_list_documents':
        return text(await client.getDocuments(parseListArgs(args)));
      case 'glpi_get_document':
        return text(await client.getDocument(args.id as number));

      // ==== STATS ====
      case 'glpi_get_ticket_stats': {
        const stats = await client.getTicketStats({
          entity_id: args.entity_id as number,
          date_from: args.date_from as string,
          date_to: args.date_to as string,
        });
        return text({
          ...stats,
          summary: `${stats.total} tickets — new:${stats.new} processing:${stats.processing} pending:${stats.pending} solved:${stats.solved} closed:${stats.closed}`,
        });
      }

      case 'glpi_get_asset_stats': {
        const stats = await client.getAssetStats();
        return text({ ...stats, total: stats.computers + stats.monitors + stats.printers + stats.networkEquipments + stats.phones });
      }

      case 'glpi_tickets_stats_by': {
        const dimension = args.dimension as 'status' | 'category' | 'technician' | 'entity' | 'month';
        const base: SearchCriterion[] = [];
        if (args.entity_id !== undefined) base.push({ field: TICKET_FIELDS.entity, searchtype: 'equals', value: args.entity_id as number });
        if (args.date_from) base.push({ field: TICKET_FIELDS.date, searchtype: 'morethan', value: args.date_from as string, link: 'AND' });
        if (args.date_to) base.push({ field: TICKET_FIELDS.date, searchtype: 'lessthan', value: args.date_to as string, link: 'AND' });

        const counts: Record<string, number> = {};

        if (dimension === 'status') {
          for (const [statusId, label] of Object.entries(TICKET_STATUS)) {
            const c: SearchCriterion[] = [
              { field: TICKET_FIELDS.status, searchtype: 'equals', value: Number(statusId) },
              ...base.map((b, i) => ({ ...b, link: 'AND' as const })),
            ];
            counts[label] = await client.search.count('Ticket', c);
          }
        } else if (dimension === 'category') {
          const cats = await client.getCategories({ range: '0-199' });
          for (const cat of cats as any[]) {
            const c: SearchCriterion[] = [
              { field: TICKET_FIELDS.category, searchtype: 'equals', value: cat.id },
              ...base.map((b) => ({ ...b, link: 'AND' as const })),
            ];
            const n = await client.search.count('Ticket', c);
            if (n > 0) counts[cat.completename ?? cat.name] = n;
          }
        } else if (dimension === 'technician') {
          const users = await client.getUsers({ range: '0-199', is_active: true });
          for (const u of users) {
            const c: SearchCriterion[] = [
              { field: TICKET_FIELDS.technician_user, searchtype: 'equals', value: u.id },
              ...base.map((b) => ({ ...b, link: 'AND' as const })),
            ];
            const n = await client.search.count('Ticket', c);
            if (n > 0) counts[`${u.firstname ?? ''} ${u.realname ?? ''} (${u.name})`.trim()] = n;
          }
        } else if (dimension === 'entity') {
          const entities = await client.getEntities({ range: '0-99' });
          for (const e of entities as any[]) {
            const c: SearchCriterion[] = [
              { field: TICKET_FIELDS.entity, searchtype: 'equals', value: e.id },
              ...base.map((b) => ({ ...b, link: 'AND' as const })),
            ];
            const n = await client.search.count('Ticket', c);
            if (n > 0) counts[e.completename ?? e.name] = n;
          }
        } else if (dimension === 'month') {
          // Compute monthly buckets between date_from and date_to (or last 6 months).
          const to = args.date_to ? new Date(args.date_to as string) : new Date();
          const from = args.date_from ? new Date(args.date_from as string) : new Date(to.getFullYear(), to.getMonth() - 5, 1);
          const cursor = new Date(from.getFullYear(), from.getMonth(), 1);
          while (cursor <= to) {
            const monthStart = new Date(cursor.getFullYear(), cursor.getMonth(), 1);
            const monthEnd = new Date(cursor.getFullYear(), cursor.getMonth() + 1, 1);
            const fmt = (d: Date) => d.toISOString().slice(0, 10) + ' 00:00:00';
            const monthCriteria: SearchCriterion[] = [
              { field: TICKET_FIELDS.date, searchtype: 'morethan', value: fmt(monthStart) },
              { field: TICKET_FIELDS.date, searchtype: 'lessthan', value: fmt(monthEnd), link: 'AND' },
            ];
            if (args.entity_id !== undefined) {
              monthCriteria.push({ field: TICKET_FIELDS.entity, searchtype: 'equals', value: args.entity_id as number, link: 'AND' });
            }
            const key = `${cursor.getFullYear()}-${String(cursor.getMonth() + 1).padStart(2, '0')}`;
            counts[key] = await client.search.count('Ticket', monthCriteria);
            cursor.setMonth(cursor.getMonth() + 1);
          }
        } else {
          throw new McpError(ErrorCode.InvalidParams, `Unknown dimension: ${dimension}`);
        }

        return text({ dimension, counts, total: Object.values(counts).reduce((s, n) => s + n, 0) });
      }

      // ==== SESSION ====
      case 'glpi_get_session_info': {
        const [profile, profiles, entities] = await Promise.all([
          client.getActiveProfile(),
          client.getMyProfiles(),
          client.getMyEntities(),
        ]);
        return text({ active_profile: profile, available_profiles: profiles, entities });
      }

      // ==== SEARCH ====
      case 'glpi_find_user_devices': {
        const cnName = String(args.name ?? '').trim();
        const accountRaw = String(args.account ?? '').trim();
        const legacyUser = String(args.user ?? '').trim();
        if (!cnName && !accountRaw && !legacyUser) {
          throw new McpError(
            ErrorCode.InvalidParams,
            '请至少提供 name(中文姓名)或 account(域账号)/ user'
          );
        }
        const variantCount = Math.min(Math.max(Number(args.variants ?? 5) || 5, 1), 20);
        const limit = clampLimit(args.limit);

        // 域账号重名时会在末尾追加数字序号(从 1 开始):san.zhang、san.zhang1、san.zhang2…
        const expandAccount = (v: string): string[] => {
          const out = [v];
          if (v.includes('.') && /^[A-Za-z0-9._-]+$/.test(v)) {
            for (let i = 1; i <= variantCount; i++) out.push(`${v}${i}`);
          }
          return out;
        };

        // 查询顺序:先中文姓名,再域账号(两路都查,结果合并)
        const terms: string[] = [];
        const addTerms = (list: string[]) => {
          for (const t of list) if (t && !terms.includes(t)) terms.push(t);
        };
        addTerms(cnName ? [cnName] : []);
        addTerms(accountRaw ? expandAccount(accountRaw) : []);
        addTerms(legacyUser ? expandAccount(legacyUser) : []);

        // contact 是 IT 维护字段(通常记录使用人的域账号或姓名);name 作为兜底匹配。
        const contactField = await client.searchOptions.resolveField('Computer', 'contact');
        const nameField = (await client.searchOptions.resolveField('Computer', 'name')) ?? 1;

        const rawCriteria: CriteriaArg[] = [];
        for (const t of terms) {
          if (contactField !== undefined) {
            rawCriteria.push({ field: contactField, searchtype: 'contains', value: t });
          }
          rawCriteria.push({ field: nameField, searchtype: 'contains', value: t });
        }
        rawCriteria.forEach((c, i) => {
          if (i > 0) c.link = 'OR';
        });

        const criteria = await resolveCriteria(client, 'Computer', rawCriteria);
        const result = await client.search.search('Computer', {
          criteria,
          start: 0,
          limit,
          expandDropdowns: true,
        });

        // 合并两路结果并按设备 id 去重,同时标注命中来源(matched_by)
        const seen = new Set<string>();
        const devices: Array<Record<string, unknown>> = [];
        for (const d of ((result as { data?: Array<Record<string, unknown>> }).data ?? [])) {
          const key = String(d?.id ?? JSON.stringify(d));
          if (seen.has(key)) continue;
          seen.add(key);
          const haystack = `${d?.contact ?? ''} ${d?.name ?? ''}`.toLowerCase();
          const matchedBy = terms.filter((t) => haystack.includes(t.toLowerCase()));
          devices.push({ ...d, matched_by: matchedBy });
        }

        return text({
          name: cnName || undefined,
          account: accountRaw || undefined,
          tried_terms: terms,
          contact_field_resolved: contactField !== undefined,
          totalcount: (result as { totalcount?: number }).totalcount,
          count: devices.length,
          devices,
        });
      }

      case 'glpi_search_v2': {
        const itemtype = args.itemtype as string;
        if (!itemtype) throw new McpError(ErrorCode.InvalidParams, 'itemtype required');
        const rawCriteria = (args.criteria as CriteriaArg[]) ?? [];
        const criteria = await resolveCriteria(client, itemtype, rawCriteria);
        const result = await client.search.search(itemtype, {
          criteria,
          forcedisplay: args.forcedisplay as number[],
          start: args.start as number,
          limit: clampLimit(args.limit),
          sort: args.sort as number,
          order: args.order as 'ASC' | 'DESC',
          fetchAll: args.fetch_all as boolean,
          maxRows: (args.max_rows as number) ?? MAX_FETCH_ALL_ROWS,
          expandDropdowns: args.expand_dropdowns !== false,
        });
        return text(result);
      }

      case 'glpi_count': {
        const itemtype = args.itemtype as string;
        if (!itemtype) throw new McpError(ErrorCode.InvalidParams, 'itemtype required');
        const rawCriteria = (args.criteria as CriteriaArg[]) ?? [];
        const criteria = await resolveCriteria(client, itemtype, rawCriteria);
        const totalcount = await client.search.count(itemtype, criteria);
        return text({ itemtype, totalcount });
      }

      case 'glpi_list_search_options': {
        const itemtype = args.itemtype as string;
        if (!itemtype) throw new McpError(ErrorCode.InvalidParams, 'itemtype required');
        const cat = await client.searchOptions.get(itemtype);
        const entries = Array.from(cat.byId.values()).map((o) => ({
          id: o.id, name: o.name, uid: o.uid, table: o.table,
          field: o.field, datatype: o.datatype,
          available_searchtypes: o.available_searchtypes,
        }));
        return text({ itemtype, count: entries.length, options: entries });
      }

      // legacy
      case 'glpi_search': {
        const itemtype = args.itemtype as string;
        const field = args.field as number;
        const searchtype = args.searchtype as SearchType;
        const value = args.value as string;
        if (!itemtype || field === undefined || !searchtype || value === undefined) {
          throw new McpError(ErrorCode.InvalidParams, 'itemtype, field, searchtype, value required');
        }
        const result = await client.search.search(itemtype, {
          criteria: [{ field, searchtype, value }],
          expandDropdowns: true,
        });
        return text(result);
      }

      default:
        throw new McpError(ErrorCode.MethodNotFound, `Unknown tool: ${name}`);
    }
  } catch (error) {
    if (error instanceof McpError) throw error;
    if (error instanceof z.ZodError) {
      const issues = error.issues
        .map((i) => `${i.path.join('.') || '(root)'}: ${i.message}`)
        .join('; ');
      throw new McpError(ErrorCode.InvalidParams, `Invalid arguments for ${name}: ${issues}`);
    }
    if (error instanceof GlpiError) {
      const detail = error.glpiCode
        ? `${error.glpiCode}${error.glpiMessage ? ' — ' + error.glpiMessage : ''}`
        : error.message;
      throw new McpError(
        ErrorCode.InternalError,
        `GLPI API error on ${name} (HTTP ${error.status}): ${detail}`
      );
    }
    throw new McpError(
      ErrorCode.InternalError,
      `Error executing ${name}: ${error instanceof Error ? error.message : String(error)}`
    );
  }
}

// ---------------------------------------------------------------------------
// Resources
// ---------------------------------------------------------------------------

server.setRequestHandler(ListResourcesRequestSchema, async () => ({
  resources: [
    { uri: 'glpi://tickets/open', name: 'Open Tickets', description: 'Tickets with status < 5', mimeType: 'application/json' },
    { uri: 'glpi://tickets/recent', name: 'Recent Tickets', description: 'Most recent tickets', mimeType: 'application/json' },
    { uri: 'glpi://problems/open', name: 'Open Problems', description: 'Open problems', mimeType: 'application/json' },
    { uri: 'glpi://changes/pending', name: 'Pending Changes', description: 'Pending changes', mimeType: 'application/json' },
    { uri: 'glpi://computers', name: 'Computers', description: 'Computers', mimeType: 'application/json' },
    { uri: 'glpi://groups', name: 'Groups', description: 'Groups', mimeType: 'application/json' },
    { uri: 'glpi://categories', name: 'Categories', description: 'ITIL categories', mimeType: 'application/json' },
    { uri: 'glpi://stats/tickets', name: 'Ticket Statistics', description: 'Ticket counts', mimeType: 'application/json' },
    { uri: 'glpi://stats/assets', name: 'Asset Statistics', description: 'Asset counts', mimeType: 'application/json' },
  ],
}));

server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
  const { uri } = request.params;
  try {
    switch (uri) {
      case 'glpi://tickets/open': {
        const result = await client.search.search('Ticket', {
          criteria: [{ field: TICKET_FIELDS.status, searchtype: 'lessthan', value: 5 }],
          limit: 100,
          order: 'DESC',
          sort: TICKET_FIELDS.date_mod,
          expandDropdowns: true,
        });
        return { contents: [{ uri, mimeType: 'application/json', text: JSON.stringify(result.data, null, 2) }] };
      }
      case 'glpi://tickets/recent': {
        const tickets = await client.getTickets({ range: '0-19', order: 'DESC', expand_dropdowns: true });
        return { contents: [{ uri, mimeType: 'application/json', text: JSON.stringify(tickets, null, 2) }] };
      }
      case 'glpi://problems/open': {
        const problems = await client.getProblems({ range: '0-99' });
        const open = (problems as any[]).filter((p) => p.status < 5);
        return { contents: [{ uri, mimeType: 'application/json', text: JSON.stringify(open, null, 2) }] };
      }
      case 'glpi://changes/pending': {
        const changes = await client.getChanges({ range: '0-99' });
        const pending = (changes as any[]).filter((c) => c.status < 8);
        return { contents: [{ uri, mimeType: 'application/json', text: JSON.stringify(pending, null, 2) }] };
      }
      case 'glpi://computers':
        return { contents: [{ uri, mimeType: 'application/json', text: JSON.stringify(await client.getComputers({ range: '0-99', is_deleted: false }), null, 2) }] };
      case 'glpi://groups':
        return { contents: [{ uri, mimeType: 'application/json', text: JSON.stringify(await client.getGroups({ range: '0-99' }), null, 2) }] };
      case 'glpi://categories':
        return { contents: [{ uri, mimeType: 'application/json', text: JSON.stringify(await client.getCategories({ range: '0-99' }), null, 2) }] };
      case 'glpi://stats/tickets':
        return { contents: [{ uri, mimeType: 'application/json', text: JSON.stringify(await client.getTicketStats(), null, 2) }] };
      case 'glpi://stats/assets':
        return { contents: [{ uri, mimeType: 'application/json', text: JSON.stringify(await client.getAssetStats(), null, 2) }] };
      default:
        throw new McpError(ErrorCode.InvalidRequest, `Unknown resource: ${uri}`);
    }
  } catch (error) {
    if (error instanceof McpError) throw error;
    throw new McpError(
      ErrorCode.InternalError,
      `Error reading resource: ${error instanceof Error ? error.message : String(error)}`
    );
  }
});

  return server;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  try {
    const config = getConfig();
    client = new GlpiClient(config);

    try {
      await client.initSession();
      log('INFO', 'GLPI 会话初始化成功');
    } catch (error) {
      log(
        'WARN',
        `启动时无法连接 GLPI(${error instanceof Error ? error.message : error}),将在首次请求时建立会话`
      );
    }

    const host = process.env.MCP_HOST || '0.0.0.0';
    const port = Number(process.env.MCP_PORT || '3000');

    if (!process.env.MCP_SERVICE_TOKEN) {
      log('WARN', '未设置 MCP_SERVICE_TOKEN,/mcp 接口处于无鉴权状态');
    }

    // The MCP Server + transport are created per request inside the handler
    // below (stateless Streamable HTTP), so concurrent callers stay isolated.
    
    
    const httpServer = createServer(async (req, res) => {
      // Parse the URL once. Do NOT compare `req.url === '/mcp'` directly:
      // clients such as 企业微信 append credentials, e.g. "/mcp?apiKey=...".
      let pathname = '/';
      try {
        pathname = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`).pathname;
      } catch {
        pathname = (req.url || '/').split('?')[0];
      }

      // Permissive CORS so browser-based MCP clients / debug pages work.
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
      res.setHeader(
        'Access-Control-Allow-Headers',
        'Content-Type, Authorization, X-API-Key, Mcp-Session-Id, Accept, Last-Event-ID'
      );
      res.setHeader('Access-Control-Expose-Headers', 'Mcp-Session-Id');

      if (req.method === 'OPTIONS') {
        res.writeHead(204);
        res.end();
        return;
      }
    
      if (pathname === '/health') {
        res.writeHead(200, {
          'Content-Type': 'application/json',
        });
    
        res.end(JSON.stringify({
          status: 'ok',
          service: 'mcp-glpi',
        }));
    
        return;
      }
    
      if (pathname !== '/mcp') {
        res.writeHead(404, {
          'Content-Type': 'application/json',
        });
    
        res.end(JSON.stringify({
          error: 'Not Found',
        }));
    
        return;
      }
    
      if (!checkMcpToken(req)) {
        res.writeHead(401, {
          'Content-Type': 'application/json',
          'WWW-Authenticate': 'Bearer',
        });
    
        res.end(JSON.stringify({
          error: 'Unauthorized',
        }));
    
        return;
      }
    
      // 无状态模式不提供 GET(SSE)流;显式 405,避免请求被挂住直到超时。
      if (req.method !== 'POST') {
        res.writeHead(405, {
          'Content-Type': 'application/json',
          Allow: 'POST',
        });
        res.end(JSON.stringify({ error: 'Method Not Allowed', allow: 'POST' }));
        return;
      }

      const clientIp = getClientIp(req);
      const reqStartedAt = Date.now();

      // 访问日志:时间 / 调用方 IP / 方法 / 路径 / 状态码 / 耗时
      res.on('finish', () => {
        const status = res.statusCode;
        const level: LogLevel = status >= 500 ? 'ERROR' : status >= 400 ? 'WARN' : 'INFO';
        log(level, `${clientIp} ${req.method} ${pathname} ${status} ${Date.now() - reqStartedAt}ms`);
      });

      // 兼容企业微信等客户端:补齐 Accept 头,避免 SDK 直接返回 406。
      ensureMcpAcceptHeader(req);

      // One Server + transport per request => full isolation between callers.
      const mcpServer = createMcpServer({ ip: clientIp });
      mcpServer.onerror = (error: unknown) => {
        log('ERROR', `${clientIp} MCP server 异常: ${error instanceof Error ? error.message : String(error)}`);
      };

      const transport = new StreamableHTTPServerTransport({
        sessionIdGenerator: undefined,
        // 默认用 application/json 直接返回(而不是 SSE 流),对企微等客户端
        // 兼容性更好,也不受反向代理缓冲影响。需要 SSE 时设置 MCP_JSON_RESPONSE=0。
        enableJsonResponse: process.env.MCP_JSON_RESPONSE !== '0',
      });
      transport.onerror = (error: unknown) => {
        log('ERROR', `${clientIp} 传输层异常: ${error instanceof Error ? error.message : String(error)}`);
      };

      res.on('close', () => {
        transport.close();
        mcpServer.close();
      });

      try {
        await mcpServer.connect(transport);
        await transport.handleRequest(req, res);
      } catch (error) {
        log(
          'ERROR',
          `${clientIp} 请求处理失败: ${error instanceof Error ? error.stack || error.message : String(error)}`
        );
    
        if (!res.headersSent) {
          res.writeHead(500, {
            'Content-Type': 'application/json',
          });
    
          res.end(JSON.stringify({
            error: 'Internal Server Error',
            message:
              error instanceof Error
                ? error.message
                : String(error),
          }));
        }
      }
    });

    // 兜底日志:避免异常被静默吞掉,方便定位 500。
    process.on('unhandledRejection', (reason) => {
      log('ERROR', `未处理的 Promise 拒绝: ${reason instanceof Error ? reason.stack || reason.message : String(reason)}`);
    });
    process.on('uncaughtException', (error) => {
      log('ERROR', `未捕获的异常: ${error.stack || error.message}`);
    });

    httpServer.listen(port, host, () => {
      log('INFO', `MCP GLPI Server v3.0 已启动 (Streamable HTTP),监听 http://${host}:${port}/mcp`);
    });

    const shutdown = async () => {
      try {
        await client.killSession();
      } catch (error) {
        log('WARN', `关闭时销毁 GLPI 会话失败: ${error instanceof Error ? error.message : error}`);
      }

      httpServer.close(() => {
        process.exit(0);
      });
    };

    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);

  } catch (error) {
    log('ERROR', `启动失败: ${error instanceof Error ? error.stack || error.message : String(error)}`);
    process.exit(1);
  }
}

main();