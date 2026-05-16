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
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>CPA 用量统计</title>
  <style>
    :root { color-scheme: light dark; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
    body { margin: 0; background: #f7f8fa; color: #17181c; }
    main { max-width: 1120px; margin: 0 auto; padding: 28px 18px 48px; }
    header { display: flex; justify-content: space-between; gap: 16px; align-items: center; margin-bottom: 20px; }
    h1 { font-size: 28px; line-height: 1.2; margin: 0; font-weight: 720; }
    button, select, input { font: inherit; border: 1px solid #d6d9df; background: #fff; color: inherit; border-radius: 6px; padding: 8px 10px; }
    button { cursor: pointer; background: #1769e0; color: white; border-color: #1769e0; }
    .toolbar { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
    .metrics { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; margin-bottom: 18px; }
    .metric, section { background: #fff; border: 1px solid #e0e3e8; border-radius: 8px; }
    .metric { padding: 14px; min-width: 0; }
    .metric span { display: block; color: #626976; font-size: 12px; margin-bottom: 6px; }
    .metric strong { display: block; font-size: 22px; overflow-wrap: anywhere; }
    section { padding: 14px; margin-top: 12px; overflow-x: auto; }
    h2 { font-size: 16px; margin: 0 0 10px; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { text-align: left; border-bottom: 1px solid #eceff3; padding: 8px 6px; white-space: nowrap; }
    th { color: #626976; font-weight: 600; }
    .error { color: #b42318; margin-top: 10px; }
    @media (max-width: 760px) { header { align-items: flex-start; flex-direction: column; } .metrics { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
    @media (prefers-color-scheme: dark) {
      body { background: #101216; color: #f4f6f8; }
      button, select, input, .metric, section { background: #171a21; border-color: #303642; }
      th, .metric span { color: #aab2c0; }
      th, td { border-bottom-color: #2a303b; }
    }
  </style>
</head>
<body>
  <main>
    <header>
      <h1>CPA 用量统计</h1>
      <div class="toolbar">
        <select id="range">
          <option value="24h">24h</option>
          <option value="7d" selected>7d</option>
          <option value="30d">30d</option>
          <option value="all">All</option>
        </select>
        <input id="token" type="password" placeholder="面板令牌">
        <button id="refresh">刷新</button>
        <button id="collect">立即采集</button>
      </div>
    </header>
    <div id="error" class="error"></div>
    <div class="metrics" id="metrics"></div>
    <section>
      <h2>按模型</h2>
      <table id="models"></table>
    </section>
    <section>
      <h2>按来源</h2>
      <table id="sources"></table>
    </section>
    <section>
      <h2>最近请求</h2>
      <table id="recent"></table>
    </section>
  </main>
  <script>
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
      return "$" + Number(value || 0).toFixed(4);
    }

    function renderMetrics(summary) {
      const items = [
        ["请求数", summary.requests],
        ["总 tokens", summary.total_tokens],
        ["输入 tokens", summary.input_tokens],
        ["输出 tokens", summary.output_tokens],
        ["失败数", summary.failed_requests],
        ["费用", money(summary.cost_usd)],
        ["平均延迟", (summary.avg_latency_ms || 0) + " ms"],
        ["缓存 tokens", summary.cached_tokens]
      ];
      document.querySelector("#metrics").innerHTML = items.map(([label, value]) => '<div class="metric"><span>' + label + '</span><strong>' + formatLabel(value) + '</strong></div>').join("");
    }

    function formatLabel(value) {
      return typeof value === "number" ? format(value) : String(value ?? 0);
    }

    function renderTable(selector, columns, rows) {
      const head = "<thead><tr>" + columns.map((column) => "<th>" + column.label + "</th>").join("") + "</tr></thead>";
      const body = "<tbody>" + rows.map((row) => "<tr>" + columns.map((column) => "<td>" + formatCell(row[column.key]) + "</td>").join("") + "</tr>").join("") + "</tbody>";
      document.querySelector(selector).innerHTML = head + body;
    }

    function formatCell(value) {
      if (typeof value === "number") return format(value);
      if (value === null || value === undefined) return "";
      return String(value);
    }

    async function load() {
      document.querySelector("#error").textContent = "";
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
          { key: "requests", label: "请求" },
          { key: "input_tokens", label: "输入" },
          { key: "output_tokens", label: "输出" },
          { key: "total_tokens", label: "总量" },
          { key: "cost_usd", label: "费用" }
        ];
        renderTable("#models", groupedColumns, models);
        renderTable("#sources", groupedColumns, sources);
        renderTable("#recent", [
          { key: "timestamp", label: "时间" },
          { key: "source", label: "来源" },
          { key: "model", label: "模型" },
          { key: "success", label: "成功" },
          { key: "total_tokens", label: "Tokens" },
          { key: "latency_ms", label: "延迟" }
        ], recent);
      } catch (error) {
        document.querySelector("#error").textContent = error.message;
      }
    }

    document.querySelector("#refresh").addEventListener("click", load);
    document.querySelector("#range").addEventListener("change", load);
    document.querySelector("#collect").addEventListener("click", async () => {
      try {
        await api("/api/collect", { method: "POST" });
        await load();
      } catch (error) {
        document.querySelector("#error").textContent = error.message;
      }
    });
    load();
  </script>
</body>
</html>`;
}
