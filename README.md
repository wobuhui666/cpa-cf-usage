# CPA Cloudflare Usage

Cloudflare Workers + D1 collector for CLIProxyAPI usage telemetry.

## Security first

Do not put Cloudflare Global API Keys or CPA management keys in source files.
Use `wrangler login` for Cloudflare auth, and `wrangler secret put` for runtime secrets.

If a Global API Key was posted in chat, rotate it in Cloudflare immediately.

## CPA requirements

Use CLIProxyAPI v6.10.8 or newer when possible. The Worker consumes:

```txt
GET /v0/management/usage-queue?count=100
Authorization: Bearer <CPA_MANAGEMENT_KEY>
```

Enable usage telemetry in CPA:

```yaml
usage-statistics-enabled: true
redis-usage-queue-retention-seconds: 3600
```

Only one consumer should drain the usage queue.

## Deploy

Install dependencies:

```bash
npm install
```

Login to Cloudflare:

```bash
npx wrangler login
```

Create D1:

```bash
npm run db:create
```

Copy the returned `database_id` into `wrangler.toml`, replacing:

```txt
REPLACE_WITH_D1_DATABASE_ID
```

Apply migrations:

```bash
npm run db:migrate
```

Set secrets:

```bash
npm run secret:cpa-url
npm run secret:cpa-key
npm run secret:dashboard-token
```

Recommended values:

```txt
CPA_BASE_URL=https://your-cpa.example.com
CPA_MANAGEMENT_KEY=<CPA management key>
DASHBOARD_TOKEN=<random long password>
```

The default CPA auth header is:

```txt
Authorization: Bearer <CPA_MANAGEMENT_KEY>
```

If your CPA fork expects `X-API-Key`, add these vars in `wrangler.toml`:

```toml
CPA_AUTH_HEADER = "X-API-Key"
CPA_AUTH_SCHEME = ""
```

Deploy:

```bash
npm run deploy
```

## Endpoints

```txt
GET  /
GET  /health
GET  /api/summary?range=7d
GET  /api/by-model?range=7d
GET  /api/by-source?range=7d
GET  /api/recent?limit=50
POST /api/collect
```

When `DASHBOARD_TOKEN` is set, API requests need:

```txt
Authorization: Bearer <DASHBOARD_TOKEN>
```

The dashboard stores this token only in the browser local storage.
