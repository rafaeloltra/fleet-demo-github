# Running this demo on someone else's machine

For when the Confluent Cloud backend (environment, cluster, 10 topics, Flink pipeline, Bedrock
connection) is **already deployed** — this is not the `terraform apply` guide, that's
`fleet-context-engine/SETUP.md`. This is the short path for a colleague who just wants to open the
console and see the real pipeline, without touching Terraform, AWS, or the Confluent Cloud
console at all.

## What they need from you (the person who ran `terraform apply`)

Eight values, read from your deployed stack with `terraform output -raw <name>` inside
`terraform/`:

```
KAFKA_BOOTSTRAP_ENDPOINT       terraform output -raw kafka_bootstrap_endpoint
KAFKA_REST_ENDPOINT            terraform output -raw kafka_rest_endpoint
KAFKA_CLUSTER_ID               terraform output -raw kafka_cluster_id
KAFKA_API_KEY                  terraform output -raw app_manager_kafka_api_key
KAFKA_API_SECRET               terraform output -raw app_manager_kafka_api_secret
SCHEMA_REGISTRY_ENDPOINT       terraform output -raw schema_registry_rest_endpoint
SCHEMA_REGISTRY_API_KEY        terraform output -raw app_manager_schema_registry_api_key
SCHEMA_REGISTRY_API_SECRET     terraform output -raw app_manager_schema_registry_api_secret
```

Send these to them over a secure channel (password manager share, not Slack/email/chat — same
reasoning as `terraform.tfvars.example`'s own warning: anything pasted in plaintext chat should
be treated as compromised and rotated).

**They do NOT need:** the org-level `confluent_cloud_api_key`/`confluent_cloud_api_secret` (that's
only for running Terraform itself), the `bedrock_aws_*` keys (Bedrock is called from Flink,
running inside Confluent Cloud — their laptop never talks to AWS), Terraform, or Python. The
`app_manager` key above already has enough scope (`CloudClusterAdmin`) to read topics and run the
demo-injection buttons.

## What they need on their own machine

1. **Node.js 18+** and `npm`.
2. **This folder**, or at minimum: `fleet-context-engine/` (all `.js` files + `package.json`, not
   `node_modules` — they'll run `npm install`), `fleet-intelligence-console.html`, and `fonts/`
   (the console's self-hosted webfont). They don't need `terraform/`, `seed-data/`, or the deck.

## Steps

```bash
cd fleet-context-engine
npm install

export KAFKA_BOOTSTRAP_ENDPOINT="..."
export KAFKA_REST_ENDPOINT="..."
export KAFKA_CLUSTER_ID="..."
export KAFKA_API_KEY="..."
export KAFKA_API_SECRET="..."
export SCHEMA_REGISTRY_ENDPOINT="..."
export SCHEMA_REGISTRY_API_KEY="..."
export SCHEMA_REGISTRY_API_SECRET="..."

npm start
```

Check stderr for:
```
[fleet-context-engine] mode: LIVE (real Confluent Cloud + Flink + Bedrock)
[fleet-telemetry-simulator] started - producing continuous real telemetry/weather/traffic/parcel data
[fleet-context-engine] HTTP API listening on http://localhost:8787
```
If it instead says `mode: SIMULATED`, one of the eight env vars above is missing or misspelled —
`fleet-context-engine` silently falls back rather than failing loudly (see `SETUP.md` for why).

Leave that running, then in another terminal/window:

```bash
open fleet-intelligence-console.html
```

Every tab (Overview, Safety, Delivery, Maintenance, Live Pipeline) polls `http://localhost:8787`
every ~3s — nothing in the HTML file itself needs configuring. Give the background Kafka consumer
~15-30s after `npm start` to catch up before the numbers look fully populated (the top-bar status
text says "consumer still catching up…" until then).

## Demoing it

- **Live Overview** — the map, KPIs, ticker, and AI Advisor feed.
- **Safety / Delivery / Maintenance** — the three business-consumer dashboards, each reading a
  different slice of the same real pipeline.
- **Live Pipeline** — raw diagnostics: connection state, pipeline mode/source, unfiltered
  recommendation feed across all three domains. Useful if something looks off.
- **Demo controls** (left sidebar) — "Inject REAL engine spike" / "Inject REAL traffic incident"
  produce a real record to the real topic and let the real Flink + Bedrock pipeline react
  (usually under a minute). "Reset real pipeline" is destructive — it purges all 10 topics — so
  don't click it mid-demo unless you mean to.

## Troubleshooting

- **Top bar stuck on "Not connected"** — `fleet-context-engine` isn't running, or it's running on a
  different port. Check `curl http://localhost:8787/api/health`.
- **Port 8787 already in use** — something else is already running (maybe a stale
  `fleet-context-engine` from an earlier session): `lsof -ti:8787 -sTCP:LISTEN | xargs kill`, then
  `npm start` again.
- **Numbers look sparse / mostly zero** — the topics may be freshly deployed with little history
  yet. `telemetry-simulator.js` keeps producing continuously once `fleet-context-engine` is running
  (see the table in the main `README.md`), so it fills in within a few minutes; the one-shot
  Python seed script in `seed-data/` (needs its own Confluent Cloud + Schema Registry env vars,
  see that script's docstring) can backfill a realistic 485-record batch instantly if you want a
  fuller dashboard right away — but only the person with Schema Registry write access needs to
  run that, not every demo machine.

## Optional: connecting an MCP client to it

Not required to see the console. `fleet-context-engine` has no MCP server of its own — RTCE
(Confluent Cloud's native managed MCP server) is the only MCP path into this project's data, for
**Option 1: Claude Code/Desktop** or **Option 2: VS Code Copilot**. See
`fleet-context-engine/SETUP.md` section 3 (or `SETUP-RTCE-COPILOT.md` for Copilot) — that path
needs its own Confluent API key, independent of the env vars above.
