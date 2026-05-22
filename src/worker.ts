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
  return `<!DOCTYPE html>
<html lang="zh-CN" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CPA Analytics - Gemini Style</title>
    <!-- 引入 Tailwind CSS -->
    <script src="https://cdn.tailwindcss.com"></script>
    <!-- 引入 Lucide 图标 -->
    <script src="https://unpkg.com/lucide@latest"></script>
    <!-- Google Fonts: Outfit 带来类似 Google Sans 的现代几何感 -->
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">

    <script>
        tailwind.config = {
            darkMode: 'class',
            theme: {
                extend: {
                    fontFamily: {
                        sans: ['Outfit', 'sans-serif'],
                        mono: ['JetBrains Mono', 'monospace'],
                    },
                    colors: {
                        gemini: {
                            bg: '#131314',
                            surface: '#1e1f20',
                            surfaceHover: '#282a2c',
                            text: '#e3e3e3',
                            textSecondary: '#c4c7c5',
                            border: '#333538'
                        }
                    },
                    animation: {
                        'gradient-x': 'gradient-x 10s ease infinite',
                        'sparkle': 'sparkle 2s ease-in-out infinite',
                    },
                    keyframes: {
                        'gradient-x': {
                            '0%, 100%': {
                                'background-size': '200% 200%',
                                'background-position': 'left center'
                            },
                            '50%': {
                                'background-size': '200% 200%',
                                'background-position': 'right center'
                            },
                        },
                        'sparkle': {
                            '0%, 100%': { transform: 'scale(1) rotate(0deg)', opacity: 1 },
                            '50%': { transform: 'scale(1.1) rotate(5deg)', opacity: 0.8 },
                        }
                    }
                }
            }
        }
    </script>
    <style>
        body {
            background-color: #131314;
            color: #e3e3e3;
            overflow-x: hidden;
            scroll-behavior: smooth;
        }

        /* 隐藏滚动条但保留功能，类似原生App */
        ::-webkit-scrollbar { width: 4px; height: 4px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: #333538; border-radius: 10px; }
        ::-webkit-scrollbar-thumb:hover { background: #4a4d51; }

        /* Gemini 标志性的文字渐变 */
        .gemini-gradient-text {
            background: linear-gradient(74deg, #4285f4 0, #9b72cb 46%, #d96570 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text; color: transparent;
        }

        .gemini-gradient-bg {
            background: linear-gradient(74deg, #4285f4 0, #9b72cb 46%, #d96570 100%);
        }

        /* 顶部微妙的环境光 */
        .ambient-light {
            position: fixed;
            top: -20vh;
            left: 10vw;
            width: 80vw;
            height: 40vh;
            background: radial-gradient(ellipse at center, rgba(155, 114, 203, 0.15) 0%, rgba(19, 19, 20, 0) 70%);
            z-index: -1;
            pointer-events: none;
        }

        /* Gemini 风格的卡片：大圆角，极简背景，无明显边框 */
        .gemini-card {
            background-color: #1e1f20;
            border-radius: 24px;
            transition: background-color 0.2s ease;
        }
        .gemini-card:hover {
            background-color: #282a2c;
        }

        /* Gemini 输入框风格（药丸形状，深灰底色） */
        .gemini-input {
            background-color: #1e1f20;
            border: 1px solid transparent;
            color: #e3e3e3;
            border-radius: 9999px; /* pill shape */
            transition: all 0.2s ease;
        }
        .gemini-input:focus-within {
            background-color: #282a2c;
        }
        select option { background: #1e1f20; color: #e3e3e3; }

        /* 表格样式重写 */
        table { width: 100%; border-collapse: separate; border-spacing: 0; }
        th {
            color: #c4c7c5;
            font-weight: 500;
            font-size: 0.85rem;
            padding: 16px;
            border-bottom: 1px solid #333538;
            text-align: left;
        }
        td {
            padding: 16px;
            border-bottom: 1px solid rgba(51, 53, 56, 0.5);
            font-size: 0.9rem;
        }
        tbody tr { transition: background-color 0.2s ease; }
        tbody tr:hover { background-color: rgba(255, 255, 255, 0.02); }
        tbody tr:last-child td { border-bottom: none; }

        /* 状态指示器小圆点 */
        .status-dot {
            width: 8px; height: 8px; border-radius: 50%; display: inline-block;
        }
    </style>
</head>
<body class="antialiased min-h-screen flex flex-col font-sans selection:bg-[#9b72cb] selection:text-white">

    <!-- 顶部环境光 -->
    <div class="ambient-light"></div>

    <!-- 导航与控制栏 -->
    <header class="w-full max-w-6xl mx-auto px-4 sm:px-6 pt-8 pb-4 flex flex-col md:flex-row justify-between items-start md:items-center gap-6 z-10">

        <!-- Logo / 标题区 -->
        <div class="flex items-center gap-3">
            <div class="animate-sparkle">
                <i data-lucide="sparkles" class="w-8 h-8" style="color: #a47cf6;"></i>
            </div>
            <h1 class="text-3xl font-medium tracking-tight">
                <span class="gemini-gradient-text animate-gradient-x">CPA Analytics</span>
            </h1>
        </div>

        <!-- Gemini 风格工具栏 -->
        <div class="flex flex-wrap items-center gap-3 w-full md:w-auto">
            <!-- 下拉选择 (Pill) -->
            <div class="relative gemini-input flex items-center px-4 py-2 hover:bg-[#282a2c] cursor-pointer">
                <select id="range" class="appearance-none bg-transparent outline-none text-sm font-medium pr-6 cursor-pointer w-full h-full text-gemini-textSecondary">
                    <option value="24h">24 小时</option>
                    <option value="7d" selected>7 天</option>
                    <option value="30d">30 天</option>
                    <option value="all">全部时间</option>
                </select>
                <i data-lucide="chevron-down" class="w-4 h-4 text-gemini-textSecondary absolute right-4 pointer-events-none"></i>
            </div>

            <!-- Token 输入 (Pill) -->
            <div class="relative flex-1 md:w-56 gemini-input flex items-center px-4 py-2 hover:bg-[#282a2c]">
                <i data-lucide="key" class="w-4 h-4 text-gemini-textSecondary shrink-0 mr-2"></i>
                <input id="token" type="password" placeholder="输入面板 Token" class="bg-transparent outline-none w-full text-sm text-gemini-text placeholder-gemini-textSecondary">
            </div>

            <!-- 操作按钮 (Pill) -->
            <button id="refresh" class="gemini-input px-5 py-2 flex items-center justify-center hover:bg-[#282a2c] transition-colors group">
                <i data-lucide="rotate-cw" class="w-4 h-4 text-gemini-textSecondary group-hover:text-white transition-colors"></i>
            </button>

            <!-- 主色调按钮 -->
            <button id="collect" class="gemini-gradient-bg px-6 py-2 rounded-full text-white text-sm font-medium hover:opacity-90 transition-opacity flex items-center gap-2 shadow-lg shadow-[#9b72cb]/20 active:scale-95">
                <i data-lucide="zap" class="w-4 h-4 fill-white/20"></i>
                <span>立即采集</span>
            </button>
        </div>
    </header>

    <!-- 主内容区 -->
    <main class="flex-1 max-w-6xl w-full mx-auto px-4 sm:px-6 pb-12 space-y-6 z-10">

        <!-- 错误提示 -->
        <div id="error" class="hidden w-full p-4 rounded-2xl bg-red-500/10 text-red-400 text-sm flex items-center gap-3">
            <i data-lucide="alert-circle" class="w-5 h-5 shrink-0"></i>
            <span id="error-text"></span>
        </div>

        <!-- 欢迎语 (Gemini 标志性大标题) -->
        <div class="py-4">
            <h2 class="text-4xl font-medium gemini-gradient-text animate-gradient-x inline-block mb-2">Hello, Admin</h2>
            <p class="text-gemini-textSecondary text-lg">以下是您最近的 API 调用使用情况。</p>
        </div>

        <!-- 核心指标网格 -->
        <div id="metrics" class="grid grid-cols-2 md:grid-cols-4 gap-4 sm:gap-5">
            <!-- JS 动态注入 -->
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-2 gap-5 mt-2">
            <!-- 按模型 -->
            <section class="gemini-card p-6 flex flex-col h-full">
                <div class="flex items-center gap-3 mb-4 px-2">
                    <i data-lucide="box" class="w-5 h-5 text-[#9b72cb]"></i>
                    <h2 class="text-lg font-medium text-white">按模型</h2>
                </div>
                <div class="overflow-x-auto flex-1">
                    <table id="models" class="w-full text-left whitespace-nowrap"></table>
                </div>
            </section>

            <!-- 按来源 -->
            <section class="gemini-card p-6 flex flex-col h-full">
                <div class="flex items-center gap-3 mb-4 px-2">
                    <i data-lucide="globe-2" class="w-5 h-5 text-[#4285f4]"></i>
                    <h2 class="text-lg font-medium text-white">按来源</h2>
                </div>
                <div class="overflow-x-auto flex-1">
                    <table id="sources" class="w-full text-left whitespace-nowrap"></table>
                </div>
            </section>
        </div>

        <!-- 最近请求 -->
        <section class="gemini-card p-6">
            <div class="flex items-center gap-3 mb-4 px-2">
                <i data-lucide="history" class="w-5 h-5 text-[#d96570]"></i>
                <h2 class="text-lg font-medium text-white">最近请求记录</h2>
            </div>
            <div class="overflow-x-auto">
                <table id="recent" class="w-full text-left whitespace-nowrap"></table>
            </div>
        </section>

    </main>

    <!-- 核心逻辑脚本 -->
    <script>
        // 初始化静态图标
        lucide.createIcons();

        const tokenInput = document.querySelector("#token");
        const savedToken = localStorage.getItem("dashboardToken");
        if (savedToken) tokenInput.value = savedToken;

        function headers() {
            const token = tokenInput.value.trim();
            if (token) localStorage.setItem("dashboardToken", token);
            return token ? { authorization: "Bearer " + token } : {};
        }

        async function api(path, options = {}) {
            const response = await fetch(path, { ...options, headers: { ...headers(), ...(options.headers || {}) } });
            const data = await response.json();
            if (!response.ok) throw new Error(data.error || response.statusText);
            return data;
        }

        function format(value) {
            return new Intl.NumberFormat().format(Number(value || 0));
        }

        function money(value) {
            return "\$" + Number(value || 0).toFixed(4);
        }

        function formatLabel(value) {
            return typeof value === "number" ? format(value) : String(value ?? 0);
        }

        function showError(msg) {
            const errDiv = document.querySelector("#error");
            const errText = document.querySelector("#error-text");
            if (msg) {
                errText.textContent = msg;
                errDiv.classList.remove("hidden");
            } else {
                errDiv.classList.add("hidden");
                errText.textContent = "";
            }
        }

        // ==========================================
        // UI 渲染逻辑增强：Gemini 风格的指标卡片
        // ==========================================
        function renderMetrics(summary) {
            const items = [
                { label: "请求总数", value: summary.requests, icon: "activity", desc: "API Calls" },
                { label: "总 Tokens", value: summary.total_tokens, icon: "coins", desc: "Total Used" },
                { label: "输入 Tokens", value: summary.input_tokens, icon: "corner-right-down", desc: "Prompt" },
                { label: "输出 Tokens", value: summary.output_tokens, icon: "corner-right-up", desc: "Completion" },
                { label: "失败请求", value: summary.failed_requests, icon: "alert-triangle", desc: "Errors" },
                { label: "总费用", value: money(summary.cost_usd), icon: "badge-dollar-sign", desc: "Estimated" },
                { label: "平均延迟", value: (summary.avg_latency_ms || 0) + " ms", icon: "timer", desc: "Speed" },
                { label: "缓存 Tokens", value: summary.cached_tokens, icon: "hard-drive", desc: "Saved" }
            ];

            const html = items.map(item => '<div class="gemini-card p-5 sm:p-6 flex flex-col justify-between min-h-[120px] group cursor-default relative overflow-hidden">'
                + '<div class="flex justify-between items-start mb-4">'
                + '<div class="flex flex-col">'
                + '<span class="text-gemini-text text-sm font-medium">' + item.label + '</span>'
                + '<span class="text-gemini-textSecondary text-[11px] uppercase tracking-wider mt-0.5">' + item.desc + '</span>'
                + '</div>'
                + '<i data-lucide="' + item.icon + '" class="w-4 h-4 text-gemini-textSecondary group-hover:text-[#9b72cb] transition-colors"></i>'
                + '</div>'
                + '<strong class="text-2xl sm:text-3xl font-light font-mono text-white truncate' + (item.label === "失败请求" && item.value > 0 ? ' !text-red-400 font-medium' : '') + '">'
                + (item.label === "总费用" ? item.value : formatLabel(item.value))
                + '</strong>'
                + '</div>'
            ).join("");

            document.querySelector("#metrics").innerHTML = html;
            lucide.createIcons();
        }

        // ==========================================
        // UI 渲染逻辑增强：Gemini 风格干净的表格
        // ==========================================
        function renderTable(selector, columns, rows) {
            var head = "<thead><tr>" + columns.map(function(column) {
                return '<th class="' + (column.align === 'right' ? 'text-right' : 'text-left') + '">' + column.label + '</th>';
            }).join("") + "</tr></thead>";

            var body = "<tbody>" + rows.map(function(row) {
                return "<tr>" + columns.map(function(column) {
                    var cellValue = row[column.key];
                    var displayValue = formatCell(cellValue, column.key);
                    var tdClass = column.align === 'right' ? 'text-right' : 'text-left';

                    if (column.key === 'success') {
                        var isSuccess = cellValue === true || cellValue === 1 || cellValue === 'true';
                        displayValue = isSuccess
                            ? '<div class="flex items-center gap-2"><span class="status-dot bg-green-500"></span><span class="text-gemini-textSecondary">成功</span></div>'
                            : '<div class="flex items-center gap-2"><span class="status-dot bg-red-500"></span><span class="text-red-400">失败</span></div>';
                    } else if (column.key === 'cost_usd' || column.label === '费用') {
                        displayValue = '<span class="text-gemini-text font-mono">' + displayValue + '</span>';
                    } else if (column.key === 'name' || column.key === 'model') {
                        displayValue = '<span class="text-white font-medium">' + displayValue + '</span>';
                    } else if (column.key === 'source') {
                        displayValue = '<span class="bg-[#282a2c] px-2.5 py-1 rounded-md text-xs text-gemini-textSecondary border border-[#333538]">' + displayValue + '</span>';
                    } else if (typeof cellValue === 'number') {
                        displayValue = '<span class="font-mono text-gemini-textSecondary">' + displayValue + '</span>';
                    } else {
                        displayValue = '<span class="text-gemini-textSecondary">' + displayValue + '</span>';
                    }

                    return '<td class="' + tdClass + '">' + displayValue + '</td>';
                }).join("") + "</tr>";
            }).join("") + "</tbody>";

            document.querySelector(selector).innerHTML = head + body;
        }

        function formatCell(value, key) {
            if (key === 'cost_usd') return money(value);
            if (typeof value === "number") return format(value);
            if (value === null || value === undefined) return "-";
            return String(value);
        }

        // ==========================================
        // 数据加载核心逻辑
        // ==========================================
        async function load() {
            showError("");
            const refreshBtn = document.querySelector("#refresh");
            refreshBtn.classList.add("opacity-50", "pointer-events-none");
            var icon = refreshBtn.querySelector('svg') || refreshBtn.querySelector('i');
            if (icon) icon.classList.add("animate-spin");

            const range = document.querySelector("#range").value;
            try {
                const [summary, models, sources, recent] = await Promise.all([
                    api("/api/summary?range=" + range),
                    api("/api/by-model?range=" + range),
                    api("/api/by-source?range=" + range),
                    api("/api/recent?limit=50")
                ]);

                renderMetrics(summary);

                const groupedColumns = [
                    { key: "name", label: "名称" },
                    { key: "requests", label: "请求数", align: "right" },
                    { key: "input_tokens", label: "输入", align: "right" },
                    { key: "output_tokens", label: "输出", align: "right" },
                    { key: "total_tokens", label: "总计", align: "right" },
                    { key: "cost_usd", label: "产生费用", align: "right" }
                ];

                renderTable("#models", groupedColumns, models);
                renderTable("#sources", groupedColumns, sources);

                renderTable("#recent", [
                    { key: "timestamp", label: "时间" },
                    { key: "source", label: "调用来源" },
                    { key: "model", label: "请求模型" },
                    { key: "success", label: "状态" },
                    { key: "total_tokens", label: "Tokens", align: "right" },
                    { key: "latency_ms", label: "耗时 (ms)", align: "right" }
                ], recent);

            } catch (error) {
                showError(error.message);
            } finally {
                refreshBtn.classList.remove("opacity-50", "pointer-events-none");
                icon = refreshBtn.querySelector('svg') || refreshBtn.querySelector('i');
                if (icon) icon.classList.remove("animate-spin");
            }
        }

        document.querySelector("#refresh").addEventListener("click", load);
        document.querySelector("#range").addEventListener("change", load);
        document.querySelector("#collect").addEventListener("click", async (e) => {
            const btn = e.currentTarget;
            btn.classList.add("opacity-50", "pointer-events-none");
            try {
                await api("/api/collect", { method: "POST" });
                await load();
            } catch (error) {
                showError(error.message);
            } finally {
                btn.classList.remove("opacity-50", "pointer-events-none");
            }
        });

        // 初次加载
        load();
    </script>
</body>
</html>`;
}
