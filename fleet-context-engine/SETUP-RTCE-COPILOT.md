# Confluent Real-Time Context Engine (RTCE) — VS Code Copilot Chat Setup

This connects GitHub Copilot Chat directly to Confluent Cloud's fully-managed
RTCE MCP server, so Copilot can query the real Kafka topics from this
project's pipeline — no code from `fleet-context-engine/` involved.

```
GitHub Copilot → RTCE MCP Server → Live Kafka data (this project's cluster)
```

RTCE only serves live business data to Copilot; it does not manage Flink or
Kafka resources — that's `terraform/` and the Confluent CLI/Console.

RTCE exposes generic, raw topic-query tools (`list_topics`, `get_metadata`,
`query_data`) — it's the only MCP path into this project's data.
`fleet-context-engine` has no MCP server of its own; it only serves the
HTML dashboard over HTTP. See `SETUP.md` for the Claude Desktop/Code
equivalent of these same RTCE steps.

## Prerequisites

Already satisfied in this project:
- Cluster on AWS in a supported region (`ap-southeast-2` qualifies).
- RTCE toggled on per topic — Console: cluster → **Topics** → **Context
  engine** column → **Off** status link → turn on, or:
  ```bash
  confluent rtce rtce-topic create --cloud aws --region ap-southeast-2 \
    --topic-name <topic>
  ```

## 1. Create a Global-scoped API key

A regular Kafka/Cloud API key (even with `EnvironmentAdmin`) will 401 here —
the key must be created with scope = **Global**.

```bash
# Console: Administration → API keys → + Add API key → My account → Global
# CLI:
confluent api-key create --resource global \
  --service-account <SERVICE_ACCOUNT_ID> \
  --description "RTCE MCP key (Copilot)"
```

Save the key and secret — the secret is only shown once.

## 2. Generate the Basic-auth token

RTCE authenticates MCP requests with HTTP Basic auth over a Base64 token,
not a bearer token:

```bash
export KEY="<API_KEY>"
export SECRET="<API_SECRET>"
export TOKEN=$(echo -n "${KEY}:${SECRET}" | base64)
```

## 3. Build the MCP endpoint URL

From this project's terraform outputs (run from `terraform/`):

```bash
export ORG_ID="$(terraform output -raw flink_organization_id)"
export ENV_ID="$(terraform output -raw flink_environment_id)"
export LKC_ID="$(terraform output -raw kafka_cluster_id)"
export RTCE_URL="https://mcp.ap-southeast-2.aws.confluent.cloud/mcp/v1/context-engine/organizations/${ORG_ID}/environments/${ENV_ID}/kafka-clusters/${LKC_ID}"
```

## 4. Set the URL, then let VS Code prompt for the token

`.vscode/mcp.json` is checked into this repo already, with the URL from
step 3 filled in and the token externalized as a VS Code `input` variable
instead of pasted in plaintext:

```json
{
  "inputs": [
    {
      "id": "confluent_rtce_token",
      "type": "promptString",
      "description": "Confluent RTCE Basic-auth token (base64 of GLOBAL-scoped API_KEY:API_SECRET)",
      "password": true
    }
  ],
  "servers": {
    "confluent-rtce": {
      "type": "http",
      "url": "<RTCE_URL>",
      "headers": {
        "Authorization": "Basic ${input:confluent_rtce_token}"
      }
    }
  }
}
```

If you're pointing this at a different cluster than the one already
committed, update `url` to the `$RTCE_URL` value from step 3 — VS Code does
not expand shell env vars inside `mcp.json`, so that part still needs a
literal value (it's an identifier, not a secret, so it's fine to commit).

The token itself is never written to disk: the first time VS Code starts
the `confluent-rtce` server it prompts for `confluent_rtce_token` (input
masked) and caches the value in its own encrypted secret storage, not in
`mcp.json`. Delete the key (`confluent api-key delete <API_KEY>`) once
you're done with the demo, and don't reuse it elsewhere.

## 5. Start the server in Copilot Chat

1. Open Copilot Chat in VS Code and switch to **Agent** mode.
2. VS Code should detect the `confluent-rtce` entry from `.vscode/mcp.json`
   and offer to start it — start it and allow its tools when prompted (or
   use the MCP server list in the Chat view's tools picker).
3. Confirm `confluent-rtce` shows as running/connected.

If Copilot was already open before you created `mcp.json`, reload the MCP
server list (or restart the Chat view) rather than restarting all of VS
Code.

## 6. Ask it things like

- "What topics are available?"
- "Describe the schema for `vehicle.telemetry`."
- "Show me the 10 most recent records in `traffic.incidents`."
- "Using the live data, what's the current status of vehicle VH-1004?"

Copilot calls RTCE's `list_topics` / `get_metadata` / `query_data` tools
directly against the real topics to answer.
