interface Env {
  DB: D1Database;
  CPA_BASE_URL: string;
  CPA_MANAGEMENT_KEY: string;
  CPA_AUTH_HEADER?: string;
  CPA_AUTH_SCHEME?: string;
  DASHBOARD_TOKEN?: string;
  POLL_COUNT?: string;
  POLL_BATCHES?: string;
}

interface NormalizedUsageEvent {
  id: string;
  timestamp: string;
  source: string | null;
  authIndex: string | null;
  model: string | null;
  provider: string | null;
  status: number | null;
  success: number;
  latencyMs: number | null;
  inputTokens: number;
  outputTokens: number;
  reasoningTokens: number;
  cachedTokens: number;
  totalTokens: number;
  costUsd: number | null;
  requestId: string | null;
  rawJson: string;
}

const JSON_HEADERS = {
  "content-type": "application/json; charset=utf-8",
  "cache-control": "no-store"
};

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders() });
    }

    try {
      if (url.pathname === "/") return htmlResponse(renderDashboard());
      if (url.pathname === "/health") return jsonResponse({ ok: true });

      if (url.pathname === "/api/collect" && request.method === "POST") {
        requireDashboardAuth(request, env);
        return jsonResponse(await collectUsage(env));
      }

      if (url.pathname === "/api/summary") {
        requireDashboardAuth(request, env, true);
        return jsonResponse(await getSummary(env, url));
      }

      if (url.pathname === "/api/by-model") {
        requireDashboardAuth(request, env, true);
        return jsonResponse(await getGroupedUsage(env, url, "model"));
      }

      if (url.pathname === "/api/by-source") {
        requireDashboardAuth(request, env, true);
        return jsonResponse(await getGroupedUsage(env, url, "source"));
      }

      if (url.pathname === "/api/recent") {
        requireDashboardAuth(request, env, true);
        return jsonResponse(await getRecentEvents(env, url));
      }

      return jsonResponse({ error: "Not found" }, 404);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      const status = message === "Unauthorized" ? 401 : message === "Forbidden" ? 403 : 500;
      return jsonResponse({ error: message }, status);
    }
  },

  async scheduled(_controller: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    ctx.waitUntil(collectUsage(env));
  }
};

async function collectUsage(env: Env): Promise<{ fetched: number; inserted: number; batches: number }> {
  assertCollectorConfig(env);

  const count = clampInt(env.POLL_COUNT, 100, 1, 500);
  const maxBatches = clampInt(env.POLL_BATCHES, 5, 1, 20);
  let fetched = 0;
  let inserted = 0;
  let batches = 0;

  for (let batchIndex = 0; batchIndex < maxBatches; batchIndex += 1) {
    const records = await fetchUsageBatch(env, count);
    if (records.length === 0) break;

    const events = await Promise.all(records.map((record, index) => normalizeUsageEvent(record, index)));
    const result = await insertUsageEvents(env, events);

    fetched += records.length;
    inserted += result.inserted;
    batches += 1;

    if (records.length < count) break;
  }

  return { fetched, inserted, batches };
}

async function fetchUsageBatch(env: Env, count: number): Promise<unknown[]> {
  const endpoint = new URL("/v0/management/usage-queue", normalizeBaseUrl(env.CPA_BASE_URL));
  endpoint.searchParams.set("count", String(count));

  const response = await fetch(endpoint.toString(), {
    headers: buildCpaHeaders(env)
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`CPA usage queue request failed: ${response.status} ${body.slice(0, 240)}`);
  }

  const payload = await response.json();
  if (Array.isArray(payload)) return payload;
  if (payload && typeof payload === "object") {
    const objectPayload = payload as Record<string, unknown>;
    for (const key of ["items", "records", "data", "usage"]) {
      const value = objectPayload[key];
      if (Array.isArray(value)) return value;
    }
  }

  throw new Error("CPA usage queue response is not an array");
}

async function normalizeUsageEvent(record: unknown, index: number): Promise<NormalizedUsageEvent> {
  const objectRecord = isObject(record) ? record : {};
  const tokens = findObject(objectRecord, "tokens") ?? findObject(objectRecord, "usage") ?? {};
  const rawJson = JSON.stringify(record);
  const timestamp = normalizeTimestamp(readString(objectRecord, ["timestamp", "created_at", "time"]));
  const requestId = readString(objectRecord, ["id", "request_id", "requestId", "trace_id"]);

  const inputTokens = readNumber(tokens, ["input_tokens", "prompt_tokens", "input", "prompt"]) ?? 0;
  const outputTokens = readNumber(tokens, ["output_tokens", "completion_tokens", "output", "completion"]) ?? 0;
  const reasoningTokens = readNumber(tokens, ["reasoning_tokens", "reasoning"]) ?? 0;
  const cachedTokens = readNumber(tokens, ["cached_tokens", "cache_read_input_tokens", "cache_read_tokens", "cached"]) ?? 0;
  const totalTokens = readNumber(tokens, ["total_tokens", "total"]) ?? inputTokens + outputTokens + reasoningTokens;
  const failObject = findObject(objectRecord, "fail") ?? {};
  const status = readNumber(objectRecord, ["status", "status_code", "statusCode"]) ?? readNumber(failObject, ["status_code", "status", "statusCode"]);
  const explicitFailed = readBoolean(objectRecord, ["failed"]);
  const explicitSuccess = readBoolean(objectRecord, ["success", "ok"]);
  const success = explicitSuccess
    ?? (explicitFailed !== null ? !explicitFailed : null)
    ?? (typeof status === "number"
      ? status < 400
      : !readString(objectRecord, ["error", "error_message"]) && !readString(failObject, ["body", "error", "message"]));

  return {
    id: requestId ?? await stableId(rawJson, timestamp, index),
    timestamp,
    source: readString(objectRecord, ["source", "account", "email", "user"]),
    authIndex: readString(objectRecord, ["auth_index", "authIndex"]),
    model: readString(objectRecord, ["model", "model_name", "modelName"]),
    provider: readString(objectRecord, ["provider", "service"]),
    status,
    success: success ? 1 : 0,
    latencyMs: readNumber(objectRecord, ["latency_ms", "latencyMs", "duration_ms", "durationMs"]),
    inputTokens,
    outputTokens,
    reasoningTokens,
    cachedTokens,
    totalTokens,
    costUsd: readNumber(objectRecord, ["cost_usd", "costUsd", "cost"]),
    requestId,
    rawJson
  };
}

async function insertUsageEvents(env: Env, events: NormalizedUsageEvent[]): Promise<{ inserted: number }> {
  if (events.length === 0) return { inserted: 0 };

  const statements = events.map((event) =>
    env.DB.prepare(
      `INSERT OR IGNORE INTO usage_events (
        id, timestamp, source, auth_index, model, provider, status, success, latency_ms,
        input_tokens, output_tokens, reasoning_tokens, cached_tokens, total_tokens,
        cost_usd, request_id, raw_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    ).bind(
      event.id,
      event.timestamp,
      event.source,
      event.authIndex,
      event.model,
      event.provider,
      event.status,
      event.success,
      event.latencyMs,
      event.inputTokens,
      event.outputTokens,
      event.reasoningTokens,
      event.cachedTokens,
      event.totalTokens,
      event.costUsd,
      event.requestId,
      event.rawJson
    )
  );

  const results = await env.DB.batch(statements);
  const inserted = results.reduce((total, result) => total + (result.meta.changes ?? 0), 0);
  return { inserted };
}

async function getSummary(env: Env, url: URL): Promise<Record<string, unknown>> {
  const where = rangeWhere(url);
  const row = await env.DB.prepare(
    `SELECT
      COUNT(*) AS requests,
      SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) AS success_requests,
      SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) AS failed_requests,
      COALESCE(SUM(input_tokens), 0) AS input_tokens,
      COALESCE(SUM(output_tokens), 0) AS output_tokens,
      COALESCE(SUM(reasoning_tokens), 0) AS reasoning_tokens,
      COALESCE(SUM(cached_tokens), 0) AS cached_tokens,
      COALESCE(SUM(total_tokens), 0) AS total_tokens,
      COALESCE(SUM(cost_usd), 0) AS cost_usd,
      ROUND(AVG(latency_ms), 2) AS avg_latency_ms
    FROM usage_events ${where.sql}`
  ).bind(...where.params).first();

  return row ?? {};
}

async function getGroupedUsage(env: Env, url: URL, column: "model" | "source"): Promise<unknown[]> {
  const where = rangeWhere(url);
  const result = await env.DB.prepare(
    `SELECT
      COALESCE(${column}, 'unknown') AS name,
      COUNT(*) AS requests,
      COALESCE(SUM(input_tokens), 0) AS input_tokens,
      COALESCE(SUM(output_tokens), 0) AS output_tokens,
      COALESCE(SUM(total_tokens), 0) AS total_tokens,
      COALESCE(SUM(cost_usd), 0) AS cost_usd
    FROM usage_events
    ${where.sql}
    GROUP BY COALESCE(${column}, 'unknown')
    ORDER BY total_tokens DESC, requests DESC
    LIMIT 50`
  ).bind(...where.params).all();

  return result.results;
}

async function getRecentEvents(env: Env, url: URL): Promise<unknown[]> {
  const limit = clampInt(url.searchParams.get("limit"), 50, 1, 200);
  const result = await env.DB.prepare(
    `SELECT
      timestamp, source, auth_index, model, provider, status, success, latency_ms,
      input_tokens, output_tokens, reasoning_tokens, cached_tokens, total_tokens, cost_usd, request_id
    FROM usage_events
    ORDER BY timestamp DESC
    LIMIT ?`
  ).bind(limit).all();

  return result.results;
}

function rangeWhere(url: URL): { sql: string; params: string[] } {
  const range = url.searchParams.get("range") ?? "7d";
  const ranges: Record<string, string> = {
    "1h": "-1 hour",
    "24h": "-1 day",
    "7d": "-7 days",
    "30d": "-30 days",
    "90d": "-90 days"
  };

  if (range === "all") return { sql: "", params: [] };
  return { sql: "WHERE datetime(timestamp) >= datetime('now', ?)", params: [ranges[range] ?? ranges["7d"]] };
}

function requireDashboardAuth(request: Request, env: Env, allowPublicRead = false): void {
  if (!env.DASHBOARD_TOKEN && allowPublicRead) return;
  if (!env.DASHBOARD_TOKEN) throw new Error("Forbidden");

  const header = request.headers.get("authorization") ?? "";
  if (header !== `Bearer ${env.DASHBOARD_TOKEN}`) throw new Error("Unauthorized");
}

function assertCollectorConfig(env: Env): void {
  if (!env.CPA_BASE_URL) throw new Error("CPA_BASE_URL is not configured");
  if (!env.CPA_MANAGEMENT_KEY) throw new Error("CPA_MANAGEMENT_KEY is not configured");
}

function buildCpaHeaders(env: Env): HeadersInit {
  const authHeader = env.CPA_AUTH_HEADER || "Authorization";
  const authScheme = env.CPA_AUTH_SCHEME ?? "Bearer";
  const authValue = authScheme.trim() ? `${authScheme.trim()} ${env.CPA_MANAGEMENT_KEY}` : env.CPA_MANAGEMENT_KEY;

  return {
    "accept": "application/json",
    [authHeader]: authValue
  };
}

function normalizeBaseUrl(value: string): string {
  return value.endsWith("/") ? value : `${value}/`;
}

function normalizeTimestamp(value: string | null): string {
  const date = value ? new Date(value) : new Date();
  return Number.isNaN(date.getTime()) ? new Date().toISOString() : date.toISOString();
}

function readString(source: Record<string, unknown>, keys: string[]): string | null {
  for (const key of keys) {
    const value = source[key];
    if (typeof value === "string" && value.trim()) return value.trim();
    if (typeof value === "number") return String(value);
  }
  return null;
}

function readNumber(source: Record<string, unknown>, keys: string[]): number | null {
  for (const key of keys) {
    const value = source[key];
    if (typeof value === "number" && Number.isFinite(value)) return value;
    if (typeof value === "string" && value.trim() && Number.isFinite(Number(value))) return Number(value);
  }
  return null;
}

function readBoolean(source: Record<string, unknown>, keys: string[]): boolean | null {
  for (const key of keys) {
    const value = source[key];
    if (typeof value === "boolean") return value;
    if (typeof value === "string" && ["true", "false"].includes(value.toLowerCase())) return value.toLowerCase() === "true";
  }
  return null;
}

function findObject(source: Record<string, unknown>, key: string): Record<string, unknown> | null {
  const value = source[key];
  return isObject(value) ? value : null;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function clampInt(value: string | null | undefined, fallback: number, min: number, max: number): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed)) return fallback;
  return Math.min(Math.max(parsed, min), max);
}

async function stableId(rawJson: string, timestamp: string, index: number): Promise<string> {
  const input = new TextEncoder().encode(`${timestamp}:${index}:${rawJson}`);
  const digest = await crypto.subtle.digest("SHA-256", input);
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

function jsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { ...JSON_HEADERS, ...corsHeaders() }
  });
}

function htmlResponse(html: string): Response {
  return new Response(html, {
    headers: {
      "content-type": "text/html; charset=utf-8",
      "cache-control": "no-store"
    }
  });
}

function corsHeaders(): HeadersInit {
  return {
    "access-control-allow-origin": "*",
    "access-control-allow-methods": "GET, POST, OPTIONS",
    "access-control-allow-headers": "authorization, content-type"
  };
}

function renderDashboard(): string {
  return `<!doctype html>
<html lang="zh-CN" data-theme="dark">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>CPA Usage Dashboard</title>
  <script src="https://cdn.tailwindcss.com"><\/script>
  <script>
    tailwind.config = {
      darkMode: ['selector', '[data-theme="dark"]'],
      theme: {
        extend: {
          fontFamily: {
            sans: ['Avenir Next', 'Gill Sans', 'Trebuchet MS', 'sans-serif'],
            serif: ['ui-serif', 'Georgia', 'Cambria', 'Times New Roman', 'Times', 'serif'],
            mono: ['SF Mono', 'Monaco', 'Inconsolata', 'Fira Code', 'Roboto Mono', 'monospace']
          },
          colors: {
            paper: { DEFAULT: '#F9F6F0', strong: '#FDF8F1', dark: '#141413', 'dark-strong': '#1E1E1E' },
            ink: { DEFAULT: '#191919', muted: '#6F6156', dark: '#E6E6E6', 'dark-muted': '#A3A3A3' },
            terra: { DEFAULT: '#DA7756', light: '#E8936F', dark: '#C96442' },
            teal: { DEFAULT: '#2E6F5E', light: '#3D8F7A' }
          }
        }
      }
    }
  <\/script>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Lora:wght@400;500;600&display=swap" rel="stylesheet">
  <style>
    :root { color-scheme: dark; }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: #141413;
      color: #E6E6E6;
      font-family: 'Lora', ui-serif, Georgia, Cambria, 'Times New Roman', serif;
      line-height: 1.6;
    }
    body::before {
      content: '';
      position: fixed;
      inset: 0;
      background-image: radial-gradient(circle at 1px 1px, rgba(255,255,255,0.03) 1px, transparent 0);
      background-size: 20px 20px;
      pointer-events: none;
      z-index: 0;
    }
    body > div { position: relative; z-index: 1; }

    .card {
      background: #1E1E1E;
      border: 1px solid rgba(255,255,255,0.08);
      border-radius: 0.625rem;
      transition: all 0.2s ease;
    }
    .card:hover {
      box-shadow: 0 4px 12px rgba(0,0,0,0.3);
    }

    .stat-card { position: relative; overflow: hidden; transition: all 0.2s ease; }
    .stat-card:hover { transform: translateY(-1px); }
    .stat-card::after {
      content: '';
      position: absolute;
      bottom: 0; left: 0; right: 0;
      height: 2px;
      background: linear-gradient(90deg, transparent, var(--accent-color, #DA7756), transparent);
      opacity: 0;
      transition: opacity 0.2s;
    }
    .stat-card:hover::after { opacity: 1; }

    .btn-primary {
      background: #DA7756;
      color: #F9F6F0;
      border: none;
      padding: 8px 20px;
      border-radius: 999px;
      font-family: 'Avenir Next', Gill Sans, Trebuchet MS, sans-serif;
      font-weight: 500;
      font-size: 13px;
      cursor: pointer;
      transition: all 0.15s ease;
      letter-spacing: 0.01em;
    }
    .btn-primary:hover { background: #C96442; transform: translateY(-1px); box-shadow: 0 4px 12px rgba(218,119,86,0.25); }
    .btn-primary:active { transform: scale(0.98); }

    .btn-secondary {
      background: transparent;
      color: #A3A3A3;
      border: 1px solid rgba(255,255,255,0.12);
      padding: 8px 20px;
      border-radius: 999px;
      font-family: 'Avenir Next', Gill Sans, Trebuchet MS, sans-serif;
      font-weight: 500;
      font-size: 13px;
      cursor: pointer;
      transition: all 0.15s ease;
    }
    .btn-secondary:hover { background: rgba(255,255,255,0.05); color: #E6E6E6; border-color: rgba(255,255,255,0.2); }

    select, input[type="password"] {
      background: rgba(255,255,255,0.04);
      border: 1px solid rgba(255,255,255,0.1);
      color: #E6E6E6;
      padding: 8px 14px;
      border-radius: 10px;
      font-family: 'Avenir Next', Gill Sans, Trebuchet MS, sans-serif;
      font-size: 13px;
      outline: none;
      transition: all 0.2s;
    }
    select:focus, input[type="password"]:focus {
      border-color: #DA7756;
      box-shadow: 0 0 0 2px rgba(218,119,86,0.15);
    }
    select { appearance: none; cursor: pointer; }

    table { width: 100%; border-collapse: separate; border-spacing: 0; font-size: 13px; }
    th {
      color: #A3A3A3;
      font-family: 'Avenir Next', Gill Sans, Trebuchet MS, sans-serif;
      font-weight: 600;
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      padding: 10px 14px;
      text-align: left;
      border-bottom: 1px solid rgba(255,255,255,0.06);
    }
    td {
      padding: 10px 14px;
      border-bottom: 1px solid rgba(255,255,255,0.04);
      white-space: nowrap;
      color: #E6E6E6;
    }
    tr:hover td { background: rgba(255,255,255,0.02); }

    .badge {
      display: inline-flex;
      align-items: center;
      gap: 4px;
      padding: 2px 10px;
      border-radius: 999px;
      font-family: 'Avenir Next', Gill Sans, Trebuchet MS, sans-serif;
      font-size: 11px;
      font-weight: 600;
      letter-spacing: 0.02em;
    }
    .badge-success { background: rgba(46,111,94,0.2); color: #3D8F7A; }
    .badge-error { background: rgba(218,119,86,0.15); color: #E8936F; }
    .badge-neutral { background: rgba(163,163,163,0.1); color: #A3A3A3; }

    .error-toast {
      background: rgba(218,119,86,0.08);
      border: 1px solid rgba(218,119,86,0.2);
      color: #E8936F;
      padding: 12px 16px;
      border-radius: 10px;
      font-size: 13px;
      font-family: 'Avenir Next', Gill Sans, Trebuchet MS, sans-serif;
    }

    .section-title {
      font-family: 'Avenir Next', Gill Sans, Trebuchet MS, sans-serif;
      font-weight: 600;
      font-size: 14px;
      color: #E6E6E6;
      letter-spacing: -0.01em;
    }

    @keyframes shimmer {
      0%, 100% { opacity: 0.35; }
      50% { opacity: 0.85; }
    }
    .shimmer { animation: shimmer 2s ease-in-out infinite; }

    .fade-in { animation: fadeIn 0.4s ease-out; }
    @keyframes fadeIn { from { opacity: 0; transform: translateY(6px); } to { opacity: 1; transform: translateY(0); } }

    @media (max-width: 768px) {
      .stats-grid { grid-template-columns: repeat(2, minmax(0, 1fr)) !important; }
      .toolbar { flex-direction: column; align-items: stretch !important; }
    }
  </style>
</head>
<body class="min-h-screen">
  <div class="max-w-5xl mx-auto px-4 py-8 sm:px-6 lg:px-8">

    <!-- Header -->
    <header class="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4 mb-8">
      <div class="flex items-center gap-3.5">
        <div class="w-10 h-10 rounded-xl bg-terra/15 flex items-center justify-center">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#DA7756" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 20V10"/><path d="M18 20V4"/><path d="M6 20v-4"/></svg>
        </div>
        <div>
          <h1 class="text-lg font-semibold tracking-tight" style="font-family:'Avenir Next',Gill Sans,sans-serif;color:#E6E6E6;">CPA Usage</h1>
          <p class="text-xs" style="color:#A3A3A3;font-family:'Avenir Next',Gill Sans,sans-serif;">CLIProxyAPI 用量统计</p>
        </div>
      </div>
      <div class="toolbar flex items-center gap-2 flex-wrap">
        <select id="range" class="min-w-[90px]">
          <option value="1h">1 小时</option>
          <option value="24h">24 小时</option>
          <option value="7d" selected>7 天</option>
          <option value="30d">30 天</option>
          <option value="90d">90 天</option>
          <option value="all">全部</option>
        </select>
        <input id="token" type="password" placeholder="面板令牌" class="w-36">
        <button id="refresh" class="btn-secondary flex items-center gap-1.5">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12a9 9 0 0 0-9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/><path d="M3 3v5h5"/><path d="M3 12a9 9 0 0 0 9 9 9.75 9.75 0 0 0 6.74-2.74L21 16"/><path d="M16 16h5v5"/></svg>
          刷新
        </button>
        <button id="collect" class="btn-primary flex items-center gap-1.5">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
          采集
        </button>
      </div>
    </header>

    <!-- Error -->
    <div id="error" class="error-toast mb-5 hidden"></div>

    <!-- Loading -->
    <div id="loading" class="hidden text-center py-16">
      <div class="shimmer mx-auto mb-4 w-8 h-8 rounded-full border-2 border-terra/30 border-t-terra"></div>
      <p class="text-sm" style="color:#A3A3A3;font-family:'Avenir Next',Gill Sans,sans-serif;">加载中...</p>
    </div>

    <!-- Stats -->
    <div id="metrics" class="stats-grid grid grid-cols-2 md:grid-cols-4 gap-3 mb-6"></div>

    <!-- Model & Source Tables -->
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-4">
      <div class="card p-5 fade-in">
        <div class="flex items-center justify-between mb-4">
          <h2 class="section-title">按模型</h2>
          <span class="badge badge-neutral" id="model-count">0</span>
        </div>
        <div class="overflow-x-auto"><table id="models"></table></div>
      </div>
      <div class="card p-5 fade-in">
        <div class="flex items-center justify-between mb-4">
          <h2 class="section-title">按来源</h2>
          <span class="badge badge-neutral" id="source-count">0</span>
        </div>
        <div class="overflow-x-auto"><table id="sources"></table></div>
      </div>
    </div>

    <!-- Recent Events -->
    <div class="card p-5 fade-in">
      <div class="flex items-center justify-between mb-4">
        <h2 class="section-title">最近请求</h2>
        <span class="badge badge-neutral" id="recent-count">0</span>
      </div>
      <div class="overflow-x-auto"><table id="recent"></table></div>
    </div>
  </div>

  <script>
    var $ = function(s) { return document.querySelector(s); };
    var tokenInput = $("#token");
    var savedToken = localStorage.getItem("dashboardToken");
    if (savedToken) tokenInput.value = savedToken;

    function getHeaders() {
      var token = tokenInput.value.trim();
      if (token) localStorage.setItem("dashboardToken", token);
      return token ? { authorization: "Bearer " + token } : {};
    }

    function api(path, options) {
      options = options || {};
      return fetch(path, { method: options.method, headers: Object.assign({}, getHeaders(), options.headers || {}) })
        .then(function(res) {
          return res.json().then(function(data) {
            if (!res.ok) throw new Error(data.error || res.statusText);
            return data;
          });
        });
    }

    function fmt(n) { return new Intl.NumberFormat().format(Number(n || 0)); }
    function money(n) { return "$" + Number(n || 0).toFixed(4); }

    function icon(name) {
      var icons = {
        requests: '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg>',
        tokens: '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="M12 6v12"/><path d="M8 10h8"/></svg>',
        input: '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M15 3h4a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2h-4"/><polyline points="10 17 15 12 10 7"/><line x1="15" y1="12" x2="3" y2="12"/></svg>',
        output: '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></svg>',
        cost: '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg>',
        latency: '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>',
        cache: '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/><polyline points="3.27 6.96 12 12.01 20.73 6.96"/><line x1="12" y1="22.08" x2="12" y2="12"/></svg>',
        error: '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>'
      };
      return icons[name] || '';
    }

    function statCard(label, value, accentVar, iconKey) {
      return '<div class="stat-card card p-4 fade-in" style="--accent-color:' + accentVar + '">' +
        '<div class="flex items-center gap-2 mb-2.5">' +
          '<span style="color:' + accentVar + '">' + icon(iconKey) + '</span>' +
          '<span class="text-xs font-medium uppercase tracking-wider" style="color:#A3A3A3;font-family:Avenir Next,Gill Sans,sans-serif;">' + label + '</span>' +
        '</div>' +
        '<div class="text-xl font-semibold" style="color:#E6E6E6;font-family:Avenir Next,Gill Sans,sans-serif;">' + value + '</div>' +
      '</div>';
    }

    function renderMetrics(s) {
      $("#metrics").innerHTML = [
        statCard("请求数", fmt(s.requests), "#DA7756", "requests"),
        statCard("总 Tokens", fmt(s.total_tokens), "#E8936F", "tokens"),
        statCard("费用", money(s.cost_usd), "#2E6F5E", "cost"),
        statCard("平均延迟", Math.round(s.avg_latency_ms || 0) + " ms", "#A3A3A3", "latency"),
        statCard("输入 Tokens", fmt(s.input_tokens), "#DA7756", "input"),
        statCard("输出 Tokens", fmt(s.output_tokens), "#E8936F", "output"),
        statCard("缓存 Tokens", fmt(s.cached_tokens), "#A3A3A3", "cache"),
        statCard("失败数", fmt(s.failed_requests), "#C96442", "error")
      ].join("");
    }

    function statusBadge(value) {
      return value == 1
        ? '<span class="badge badge-success"><span style="display:inline-block;width:5px;height:5px;border-radius:50%;background:#3D8F7A;"></span> 成功</span>'
        : '<span class="badge badge-error"><span style="display:inline-block;width:5px;height:5px;border-radius:50%;background:#E8936F;"></span> 失败</span>';
    }

    function renderTable(sel, columns, rows, countId) {
      if (countId) $(countId).textContent = rows.length;
      if (!rows.length) {
        $(sel).innerHTML = '<tbody><tr><td colspan="' + columns.length + '" style="text-align:center;color:#A3A3A3;padding:32px 14px;font-style:italic;">暂无数据</td></tr></tbody>';
        return;
      }
      var head = "<thead><tr>" + columns.map(function(c) { return "<th>" + c.label + "</th>"; }).join("") + "</tr></thead>";
      var body = "<tbody>" + rows.map(function(row) {
        return "<tr>" + columns.map(function(c) {
          if (c.format === "time") return '<td style="color:#A3A3A3;font-family:ui-monospace,monospace;font-size:12px;">' + formatTime(row[c.key]) + "</td>";
          if (c.key === "success") return "<td>" + statusBadge(row[c.key]) + "</td>";
          if (c.key === "cost_usd") return '<td style="color:#3D8F7A;font-weight:500;">' + money(row[c.key]) + "</td>";
          if (c.key === "latency_ms") return '<td style="color:#A3A3A3;font-family:ui-monospace,monospace;font-size:12px;">' + fmt(row[c.key]) + " ms</td>";
          if (c.key === "total_tokens") return '<td style="font-weight:500;">' + fmt(row[c.key]) + "</td>";
          return "<td>" + formatCell(row[c.key]) + "</td>";
        }).join("") + "</tr>";
      }).join("") + "</tbody>";
      $(sel).innerHTML = head + body;
    }

    function formatCell(v) {
      if (typeof v === "number") return fmt(v);
      if (v === null || v === undefined) return '<span style="color:#6F6156;">—</span>';
      return String(v);
    }

    function formatTime(iso) {
      if (!iso) return '—';
      var d = new Date(iso);
      if (isNaN(d)) return iso;
      var pad = function(n) { return String(n).padStart(2, '0'); };
      return pad(d.getMonth()+1) + '/' + pad(d.getDate()) + ' ' + pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds());
    }

    function showError(msg) {
      var el = $("#error");
      if (msg) { el.textContent = msg; el.classList.remove("hidden"); }
      else { el.textContent = ""; el.classList.add("hidden"); }
    }

    function load() {
      showError("");
      $("#loading").classList.remove("hidden");
      var range = $("#range").value;
      Promise.all([
        api("/api/summary?range=" + range),
        api("/api/by-model?range=" + range),
        api("/api/by-source?range=" + range),
        api("/api/recent?limit=50")
      ]).then(function(results) {
        var summary = results[0], models = results[1], sources = results[2], recent = results[3];
        renderMetrics(summary);
        var groupedCols = [
          { key: "name", label: "名称" },
          { key: "requests", label: "请求" },
          { key: "input_tokens", label: "输入" },
          { key: "output_tokens", label: "输出" },
          { key: "total_tokens", label: "总量" },
          { key: "cost_usd", label: "费用" }
        ];
        renderTable("#models", groupedCols, models, "#model-count");
        renderTable("#sources", groupedCols, sources, "#source-count");
        renderTable("#recent", [
          { key: "timestamp", label: "时间", format: "time" },
          { key: "source", label: "来源" },
          { key: "model", label: "模型" },
          { key: "success", label: "状态" },
          { key: "total_tokens", label: "Tokens" },
          { key: "latency_ms", label: "延迟" }
        ], recent, "#recent-count");
      }).catch(function(e) { showError(e.message); })
      .then(function() { $("#loading").classList.add("hidden"); });
    }

    $("#refresh").addEventListener("click", load);
    $("#range").addEventListener("change", load);
    $("#collect").addEventListener("click", function() {
      api("/api/collect", { method: "POST" }).then(function() { return load(); }).catch(function(e) { showError(e.message); });
    });
    load();
  <\/script>
</body>
</html>`;
}
