# Fleet Context Engine — Setup

This runs the fleet context engine: a background Kafka consumer plus a
small HTTP API that `fleet-intelligence-console.html` polls for every view.
It has no MCP server of its own — **Confluent Cloud's native RTCE (Real-Time
Context Engine) is the only MCP path into this project's data** (see step 3),
for both Claude Code/Desktop and VS Code Copilot.

It runs in one of two modes, chosen automatically based on whether the
Confluent Cloud environment variables below are set:

- **LIVE** — a background Kafka consumer (kafkajs) tails the real 10 topics
  from `terraform/` and serves fleet state read from the real pipeline:
  real seeded/simulated telemetry → real Flink-derived business events →
  real Bedrock-generated recommendations.
- **SIMULATED** (default, no setup required) — an in-memory simulator
  generates synthetic fleet state, same as the HTML dashboard.

Check stderr on startup to see which mode is active:
```
[fleet-context-engine] mode: LIVE (real Confluent Cloud + Flink + Bedrock)
```

## 1. Install and run it

```bash
cd fleet-context-engine
npm install
npm start
```

Check stderr for the mode line above, then `[fleet-context-engine] HTTP API
listening on http://localhost:8787`. Press Ctrl+C to stop.

## 1b. (Optional) Enable LIVE mode

Requires `terraform apply` to have been run already (see `terraform/`).
Get every value below with `terraform output -raw <name>` from the
`terraform/` directory:

```bash
export KAFKA_BOOTSTRAP_ENDPOINT="$(terraform output -raw kafka_bootstrap_endpoint)"
export KAFKA_REST_ENDPOINT="$(terraform output -raw kafka_rest_endpoint)"
export KAFKA_CLUSTER_ID="$(terraform output -raw kafka_cluster_id)"
export KAFKA_API_KEY="$(terraform output -raw app_manager_kafka_api_key)"
export KAFKA_API_SECRET="$(terraform output -raw app_manager_kafka_api_secret)"
export SCHEMA_REGISTRY_ENDPOINT="$(terraform output -raw schema_registry_rest_endpoint)"
export SCHEMA_REGISTRY_API_KEY="$(terraform output -raw app_manager_schema_registry_api_key)"
export SCHEMA_REGISTRY_API_SECRET="$(terraform output -raw app_manager_schema_registry_api_secret)"
npm start
```

(`app_manager`'s key is used for both reading and writing here since it
already has `CloudClusterAdmin` - covers both directions with one
credential set, simpler than juggling a separate read-only identity.)

The consumer takes ~15-30s to catch up on first connect (Kafka consumer
group join + tailing from the beginning of all 10 topics) - the HTTP API
may show partial/empty data until it catches up.

## 2. For the actual demo moment

The strongest way to show this live: have the HTML dashboard open on one
screen and an MCP client connected to RTCE (step 3 below — Claude
Code/Desktop or VS Code Copilot) open next to it. Ask it something like
"what does `vehicle.telemetry` look like for a high-risk vehicle right
now" — the answer, read straight from the real topics via RTCE, will line
up with what's on screen, because both are ultimately reading the same
real pipeline. That's the pitch: **the dashboard and RTCE are two views
onto one live source of truth, not two separate systems that might
disagree.**

## 3. Connect an MCP client via Confluent's native RTCE (Real-Time Context Engine)

Confluent Cloud has its own fully-managed MCP server (RTCE) that reads
straight from the real topics — no code from this folder involved at all.
This is the only MCP path into this project.

The steps below are **Option 1: Claude Code/Desktop**. For **Option 2: VS
Code Copilot**, use `fleet-context-engine/SETUP-RTCE-COPILOT.md` instead —
same RTCE server, Copilot-specific config format.

**Verified working end-to-end** against this project's environment
(`env-ko352m`) via `claude mcp add --transport http` — `claude mcp list`
showed `confluent-rtce ... ✔ Connected`. The steps below are exactly what
made that connection succeed, including the Global-key requirement, which
is the part that previously 401'd.

**Prerequisites** (already satisfied in this project): cluster on AWS in a
supported region (`ap-southeast-2` qualifies), and RTCE toggled on per topic
— Console: cluster → **Topics** → **Context engine** column → **Off** status
link → turn on (or `confluent rtce rtce-topic create --cloud aws --region
ap-southeast-2 --topic-name <topic>`; already handled for all 10 topics by
`terraform/rtce.tf`).

**1. Create a Global-scoped API key** — this is the part that 401s if you
skip it: a regular Kafka/Cloud API key (even with `EnvironmentAdmin`) does
**not** work, it must be created with key scope = Global.

```bash
# Console: Administration → API keys → + Add API key → My account → Global
# CLI:
confluent api-key create --resource global \
  --service-account <SERVICE_ACCOUNT_ID> \
  --description "RTCE MCP key"
```

Save the key and secret — the secret is only shown once.

**2. Generate the Basic-auth token** (RTCE authenticates MCP requests with
HTTP Basic auth over this Base64 token, not a bearer token):

```bash
export KEY="<API_KEY>"
export SECRET="<API_SECRET>"
export TOKEN=$(echo -n "${KEY}:${SECRET}" | base64)
```

**3. Build the MCP endpoint URL** from values already in this project's
terraform outputs:

```bash
export ORG_ID="$(terraform output -raw flink_organization_id)"
export ENV_ID="$(terraform output -raw flink_environment_id)"
export LKC_ID="$(terraform output -raw kafka_cluster_id)"
export RTCE_URL="https://mcp.ap-southeast-2.aws.confluent.cloud/mcp/v1/context-engine/organizations/${ORG_ID}/environments/${ENV_ID}/kafka-clusters/${LKC_ID}"
```

**4. Connect it:**

```bash
# Claude Code
claude mcp add --transport http confluent-rtce "$RTCE_URL" \
  --header "Authorization: Basic $TOKEN"
```

```json
// Claude Desktop / any streamable-HTTP MCP client (claude_desktop_config.json)
{
  "mcpServers": {
    "confluent-rtce": {
      "url": "<RTCE_URL>",
      "headers": { "Authorization": "Basic <TOKEN>" }
    }
  }
}
```

Run this from the project root you actually want the server scoped to —
`claude mcp add` registers it per-project in `~/.claude.json`, keyed off
your current working directory at the time you run it.

**The Basic-auth token (API key + secret) is stored in plaintext** in
`~/.claude.json` (Claude Code) or `claude_desktop_config.json` (Claude
Desktop) after this step, and will also appear in plaintext in any
transcript/terminal output where you ran the `--header` command or read
the config back. Treat it like any other credential — delete the key
(`confluent api-key delete <API_KEY>`) once you're done with the demo,
and don't reuse it elsewhere.

If this session was already running before you added the server, its
tools won't show up until you start a new Claude Code session (or restart
Claude Desktop) — same as connecting any other MCP server mid-session.

**5. Ask it things like** "what topics are available", "describe the schema
for vehicle.telemetry", "show me the 10 most recent records in
traffic.incidents" — it calls RTCE's `list_topics` / `get_metadata` /
`query_data` tools directly against the real topics.

RTCE only exposes generic, raw topic-query tools — no fleet-specific
reasoning (fleet summaries, high-risk vehicle lists) and no demo-injection.
Use the dashboard (step 2 above) for the curated demo narrative; use RTCE
for ad-hoc "show me the raw data" exploration alongside it.

## How LIVE mode actually reads the pipeline

`context-engine.js`'s `LiveContextEngine` runs a background `kafkajs`
consumer subscribed to all 10 real topics from the beginning, and rebuilds
the exact same in-memory shape the simulator uses (`vehicles` Map,
`weather` Map, rolling logs) as messages arrive - so every public method
(`getFleetSummary`, `listHighRisk`, etc.) is still an instant, synchronous
read from memory, served over the HTTP API in `http-server.js`.

**Why a background consumer instead of querying Flink per request:** that
was the first approach tried, and it doesn't work well for this - each
ad-hoc Flink SQL pull query is a real job submission that takes 10-60+
seconds and competes with the pipeline's own persistent streaming jobs for
the compute pool's CFU capacity. Tailing Kafka directly in the background
(the same pattern the pipeline itself uses) is both faster and avoids that
contention entirely.

**Demo-injection in LIVE mode**: the HTML console's "Inject REAL engine
spike" / "Inject REAL traffic incident" buttons (`POST /api/inject-incident`
/ `POST /api/inject-traffic-incident` on the HTTP API, calling
`contextEngine.injectIncident()` / `injectTrafficIncident()`) produce a real
Confluent-wire-encoded record (via `confluent-client.js`) directly to
`vehicle.telemetry` / `traffic.incidents`. This flows through the *real*
Flink pipeline exactly like any other record - the real business-derivation
job picks it up, and the real Bedrock-backed model generates a real
recommendation - so give it a beat (usually well under a minute) before
checking the AI Advisor feed or querying via RTCE for the result. In
simulated mode these same buttons instead update in-memory state instantly,
since there's no real pipeline to route through.

**Resetting LIVE mode**: `POST /api/reset` on the HTTP API (also exposed as
the "🗑 Reset real pipeline" button on the HTML console's Live Pipeline
view) purges all 10 real topics and clears this process's in-memory view.
**This permanently deletes real data - every seeded record and every real
Bedrock recommendation generated so far - and cannot be undone.** It works
by briefly dropping each topic's `retention.ms` to force Kafka's log
cleaner to delete existing messages, then restoring the original value
(~15-20s total) - deliberately *not* by dropping/recreating the topics,
which would also delete their registered schemas and require restarting
all 6 persistent Flink jobs. The topics, their schemas, the Flink jobs, and
the Bedrock connection are all left completely untouched; only the
messages inside the topics are deleted. HTTP API + console button only,
since it's a destructive admin action, not something to expose
conversationally.
