CREATE TABLE IF NOT EXISTS usage_events (
  id TEXT PRIMARY KEY,
  timestamp TEXT NOT NULL,
  source TEXT,
  auth_index TEXT,
  model TEXT,
  provider TEXT,
  status INTEGER,
  success INTEGER NOT NULL DEFAULT 1,
  latency_ms INTEGER,
  input_tokens INTEGER NOT NULL DEFAULT 0,
  output_tokens INTEGER NOT NULL DEFAULT 0,
  reasoning_tokens INTEGER NOT NULL DEFAULT 0,
  cached_tokens INTEGER NOT NULL DEFAULT 0,
  total_tokens INTEGER NOT NULL DEFAULT 0,
  cost_usd REAL,
  request_id TEXT,
  raw_json TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_usage_events_timestamp ON usage_events (timestamp);
CREATE INDEX IF NOT EXISTS idx_usage_events_model ON usage_events (model);
CREATE INDEX IF NOT EXISTS idx_usage_events_source ON usage_events (source);
CREATE INDEX IF NOT EXISTS idx_usage_events_success ON usage_events (success);
