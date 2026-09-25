// rtce-client.js
//
// Thin client for Confluent Cloud's Real-Time Context Engine (RTCE) - the
// managed MCP server enabled on all 10 topics by terraform/rtce.tf. Speaks
// MCP's Streamable HTTP transport directly (session init -> tools/call) so
// a backend service can call listTopics/getMetadata/queryData as a plain
// API, without an MCP SDK or an LLM in the loop. Used by nl-query-agent.js
// as a second, independent read path from context-engine.js's own Kafka
// consumer (see confluent-client.js for why reads there don't use Flink
// SQL pull queries) - this one queries the live topics directly via RTCE's
// real SQL SELECT support, no Flink job submission involved.
//
// Configured via RTCE_URL, RTCE_API_KEY, RTCE_API_SECRET. RTCE_API_KEY/
// SECRET must be a Global-scoped Confluent Cloud API key - a regular Kafka
// or "Cloud resource management" key 401s here. See SETUP.md section 3 for
// how to create the key and build RTCE_URL from terraform outputs.

const RTCE_ENV = ['RTCE_URL', 'RTCE_API_KEY', 'RTCE_API_SECRET'];

function assertEnv(keys) {
  const missing = keys.filter((k) => !process.env[k]);
  if (missing.length) {
    throw new Error(`Missing environment variables: ${missing.join(', ')}. See SETUP.md.`);
  }
}

function basicAuth(key, secret) {
  return `Basic ${Buffer.from(`${key}:${secret}`).toString('base64')}`;
}

let sessionId = null;

// RTCE replies over SSE (`event: message\ndata: {...}`) even for a single
// non-streaming response - this pulls the JSON-RPC envelope out of that.
function parseSseJsonRpc(text) {
  const line = text.split('\n').find((l) => l.startsWith('data:'));
  if (!line) throw new Error(`Unexpected RTCE response (no SSE data line): ${text.slice(0, 200)}`);
  return JSON.parse(line.slice('data:'.length).trim());
}

async function rpcCall(method, params, { withSession = true, notification = false } = {}) {
  assertEnv(RTCE_ENV);
  const headers = {
    Authorization: basicAuth(process.env.RTCE_API_KEY, process.env.RTCE_API_SECRET),
    'Content-Type': 'application/json',
    Accept: 'application/json, text/event-stream',
  };
  if (withSession && sessionId) headers['Mcp-Session-Id'] = sessionId;

  // JSON-RPC notifications (e.g. notifications/initialized) must NOT carry
  // an "id" - that's what distinguishes them from a request expecting a
  // reply. RTCE 400s if one is present.
  const body = notification
    ? { jsonrpc: '2.0', method, params }
    : { jsonrpc: '2.0', id: Date.now(), method, params };

  const resp = await fetch(process.env.RTCE_URL, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  });
  const text = await resp.text();
  if (!resp.ok) throw new Error(`RTCE ${method} failed: ${resp.status} ${text.slice(0, 300)}`);

  const newSessionId = resp.headers.get('mcp-session-id');
  if (newSessionId) sessionId = newSessionId;

  if (resp.status === 202 || !text) return null; // notifications get no body
  const rpc = parseSseJsonRpc(text);
  if (rpc.error) throw new Error(`RTCE ${method} error: ${JSON.stringify(rpc.error)}`);
  return rpc.result;
}

async function ensureSession() {
  if (sessionId) return;
  await rpcCall('initialize', {
    protocolVersion: '2025-06-18',
    capabilities: {},
    clientInfo: { name: 'fleet-demo-nl-query', version: '0.1.0' },
  }, { withSession: false });
  await rpcCall('notifications/initialized', undefined, { notification: true });
}

async function callTool(name, toolArgs) {
  await ensureSession();
  let result;
  try {
    result = await rpcCall('tools/call', { name, arguments: toolArgs });
  } catch (err) {
    // Session may have expired server-side - reinitialize once and retry
    // before giving up, rather than surfacing a stale-session error.
    sessionId = null;
    await ensureSession();
    result = await rpcCall('tools/call', { name, arguments: toolArgs });
  }
  const text = result?.content?.[0]?.text;
  return text ? JSON.parse(text) : result;
}

export async function listTopics() {
  return callTool('listTopics', {});
}

export async function getMetadata(topicName) {
  return callTool('getMetadata', { topic_name: topicName });
}

export async function queryData(topicName, query, maxResultRows = 50) {
  return callTool('queryData', { topic_name: topicName, query, max_result_rows: maxResultRows });
}
